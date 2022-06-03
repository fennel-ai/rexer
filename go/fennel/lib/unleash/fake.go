package unleash

import (
	"encoding/json"
	"github.com/Unleash/unleash-client-go/v3/api"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// See: https://github.com/Unleash/unleash-client-go/issues/36

type FakeUnleashServer struct {
	sync.RWMutex
	srv      *httptest.Server
	features map[string]bool
}

func (f *FakeUnleashServer) Url() string {
	return f.srv.URL
}

func (f *FakeUnleashServer) Enable(feature string) {
	f.setEnabled(feature, true)
}

func (f *FakeUnleashServer) Disable(feature string) {
	f.setEnabled(feature, false)
}

func (f *FakeUnleashServer) setEnabled(feature string, enabled bool) {
	f.Lock()
	wasEnabled := f.features[feature]
	if enabled != wasEnabled {
		f.features[feature] = enabled
	}
	f.Unlock()
}

func (f *FakeUnleashServer) IsEnabled(feature string) bool {
	f.RLock()
	enabled := f.features[feature]
	f.RUnlock()
	return enabled
}

func (f *FakeUnleashServer) setAll(enabled bool) {
	for k := range f.features {
		f.setEnabled(k, enabled)
	}
}

func (f *FakeUnleashServer) EnableAll() {
	f.setAll(true)
}

func (f *FakeUnleashServer) DisableAll() {
	f.setAll(false)
}

func (f *FakeUnleashServer) handler(w http.ResponseWriter, req *http.Request) {
	switch req.Method + " " + req.URL.Path {
	case "GET /client/features":

		features := []api.Feature{}
		for k, v := range f.features {
			features = append(features, api.Feature{
				Name:    k,
				Enabled: v,
				Strategies: []api.Strategy{
					{
						Id:   0,
						Name: "default",
					},
				},
				CreatedAt: time.Time{},
			})
		}

		res := api.FeatureResponse{
			Response: api.Response{Version: 2},
			Features: features,
		}
		dec := json.NewEncoder(w)
		if err := dec.Encode(res); err != nil {
			println(err.Error())
		}
	case "POST /client/register":
		fallthrough
	case "POST /client/metrics":
		w.WriteHeader(200)
	default:
		w.Write([]byte("Unknown route"))
		w.WriteHeader(500)
	}
}

func NewFakeUnleash() *FakeUnleashServer {
	faker := &FakeUnleashServer{
		features: map[string]bool{},
	}
	faker.srv = httptest.NewServer(http.HandlerFunc(faker.handler))
	return faker
}