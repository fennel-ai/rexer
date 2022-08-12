package remote

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/value"
	"fennel/tier"
)

const (
	CACHE_NS = "remote.http"
)

func init() {
	if err := operators.Register(RemoteHttp{}); err != nil {
		log.Fatalf("Failed to register remote.http operator: %v", err)
	}
}

// Operator op.remote.http allows sending HTTP requests to remote servers.
// Only GET and POST methods are supported.
// For POST methods, users can specify the request body via the "body" kwarg.
// Users can also cache responses by specifying the "ttl" kwarg.
// The current implementation depends on in-built connection pooling in Go std
// library's http client.
// We also allow the user to specify the maximum number of concurrent requests
// that can be made to the remote server for each query. The user can estimate
// the max load on the service as (query qps * max concurrent requests per query).
//
// TODO:
// - Implement timeout
// - Implement batching
// - Implement configurable auto-retry
// - Implement support for client-certificates or self-signed server certificates.
type RemoteHttp struct {
	tr tier.Tier
}

func (r RemoteHttp) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return RemoteHttp{tr}, nil
}

var _ operators.Operator = RemoteHttp{}

func (r RemoteHttp) Apply(ctx context.Context, staticKwargs operators.Kwargs, in operators.InputIter, outs *value.List) error {
	// Validate arguments.
	method := string(staticKwargs.GetUnsafe("method").(value.String))
	if method != "GET" && method != "POST" {
		return fmt.Errorf("method must be one of \"GET\" or \"POST\"")
	}
	var rows []value.Value
	type idxResult struct {
		idx int
		val value.Value
		err error
	}
	var results []idxResult
	outCh := make(chan idxResult, 100)
	// Start a go routine to collect results. The results could arrive out of order,
	// so we will need to sort them later.
	doneCh := make(chan struct{})
	go func() {
		for r := range outCh {
			results = append(results, r)
		}
		close(doneCh)
	}()
	// Setup token-bucket to limit max concurrent requests.
	maxConcurrent := int(staticKwargs.GetUnsafe("concurrency").(value.Int))
	bucket := make(chan struct{}, maxConcurrent)
	cacheTtl := int(staticKwargs.GetUnsafe("ttl").(value.Int))
	// Waitgroup to track when all requests are complete.
	wg := &sync.WaitGroup{}
	for i := 0; in.HasMore(); i++ {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		rows = append(rows, heads[0])
		bucket <- struct{}{}
		wg.Add(1)
		go func(idx int, contextKwargs operators.Kwargs) {
			result := idxResult{
				idx: idx,
				val: staticKwargs.GetUnsafe("default"),
			}
			defer func() {
				outCh <- result
				<-bucket
				wg.Done()
			}()
			// If a cache key is specified, try to get the result from the cache.
			url := string(contextKwargs.GetUnsafe("url").(value.String))
			var resp *http.Response
			var cacheKey string
			switch method {
			case "GET":
				if cacheTtl >= 0 {
					cacheKey = url
					if v, ok := r.tr.PCache.Get(cacheKey, ""); ok {
						result.val = v.(value.Value)
						return
					}
				}
				resp, err = http.Get(url)
			case "POST":
				body := contextKwargs.GetUnsafe("body").String()
				if cacheTtl >= 0 {
					cacheKey = url + "##" + body
					if v, ok := r.tr.PCache.Get(cacheKey, CACHE_NS); ok {
						result.val = v.(value.Value)
						return
					}
				}
				resp, err = http.Post(url, "application/json", strings.NewReader(body))
			}
			if err != nil {
				result.err = fmt.Errorf("http error when calling %s: %w", url, err)
				return
			} else {
				b, err := io.ReadAll(resp.Body)
				if err != nil {
					result.err = fmt.Errorf("failed to read response body: %w", err)
					return
				}
				v, err := value.FromJSON(b)
				if err != nil {
					result.err = fmt.Errorf("failed to parse response (%s): %w", string(b), err)
					return
				}
				result.val = v
				if len(cacheKey) >= 0 {
					r.tr.PCache.SetWithTTL(cacheKey, v, int64(len(b)), time.Duration(cacheTtl)*time.Second, CACHE_NS)
				}
			}
		}(i, contextKwargs)
	}
	wg.Wait()
	close(outCh)
	<-doneCh
	// Sort results by index and append to output.
	sort.Slice(results, func(i, j int) bool {
		return results[i].idx < results[j].idx
	})
	field := string(staticKwargs.GetUnsafe("field").(value.String))
	outs.Grow(len(results))
	for i, r := range results {
		if r.err != nil {
			return r.err
		}
		var out value.Value
		if len(field) > 0 {
			d, ok := rows[i].(value.Dict)
			if !ok {
				return fmt.Errorf("row %d is not a dict", i)
			}
			d.Set(field, r.val)
			out = d
		} else {
			out = r.val
		}
		outs.Append(out)
	}
	return nil
}

func (r RemoteHttp) Signature() *operators.Signature {
	return operators.NewSignature("remote", "http").
		ParamWithHelp("url", value.Types.String, false, false, nil,
			"Server URL").
		ParamWithHelp("method", value.Types.String, true, true, value.String("GET"),
			"HTTP method - one of [\"GET\" (default), \"POST\"]").
		ParamWithHelp("body", value.Types.Any, false, true, value.String(""),
			"Request body (only usable for POST requests)").
		ParamWithHelp("timeout_ms", value.Types.Int, false, true, value.Int(1000),
			"Request timeout in milliseconds (default: 1000)").
		ParamWithHelp("default", value.Types.Any, false, false, value.Nil,
			"Default value in case of error(s) or timeout(s)").
		ParamWithHelp("concurrency", value.Types.Int, true, true, value.Int(1),
			"Number of requests that can be made concurrently for each query (default: 1)").
		ParamWithHelp("ttl", value.Types.Int, true, true, value.Int(-1),
			"Duration, in seconds, for which results can be cached").
		ParamWithHelp("field", value.Types.String, true, true, value.String(""),
			"String param that is used as key post evaluation of this operator")
}
