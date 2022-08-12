package remote

import (
	"testing"

	"fennel/lib/value"
	"fennel/test"
	"fennel/test/optest"
)

func TestRpc_Apply(t *testing.T) {
	op := RemoteHttp{}
	tier := test.Tier(t)
	// Set a non-existent country in the pcache to test response caching.
	cacheVal := value.NewDict(map[string]value.Value{"country": value.String("Fennel")})
	tier.PCache.Set("mycountry", cacheVal, 0)
	scenarios := []struct {
		inputs   []value.Value
		static   value.Dict
		context  []value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(2)}),
				value.NewDict(map[string]value.Value{"x": value.Int(3)}),
			},
			value.NewDict(map[string]value.Value{
				"method":      value.String("POST"),
				"concurrency": value.Int(5),
			}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"url":     value.String("https://countriesnow.space/api/v0.1/countries/capital"),
					"body":    value.NewDict(map[string]value.Value{"country": value.String("nigeria")}),
					"default": value.Nil,
				}),
				value.NewDict(map[string]value.Value{
					"url":     value.String("https://countriesnow.space/api/v0.1/countries/capital"),
					"body":    value.NewDict(map[string]value.Value{"country": value.String("india")}),
					"default": value.Nil,
				}),
				value.NewDict(map[string]value.Value{
					"url":     value.String("https://countriesnow.space/api/v0.1/countries/capital"),
					"body":    value.NewDict(map[string]value.Value{"country": value.String("spain")}),
					"default": value.Nil,
				}),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{
					"msg":   value.String("country and capitals retrieved"),
					"error": value.Bool(false),
					"data": value.NewDict(map[string]value.Value{
						"name":    value.String("Nigeria"),
						"capital": value.String("Abuja"),
						"iso2":    value.String("NG"),
						"iso3":    value.String("NGA"),
					}),
				}),
				value.NewDict(map[string]value.Value{
					"msg":   value.String("country and capitals retrieved"),
					"error": value.Bool(false),
					"data": value.NewDict(map[string]value.Value{
						"name":    value.String("India"),
						"capital": value.String("New Delhi"),
						"iso2":    value.String("IN"),
						"iso3":    value.String("IND"),
					}),
				}),
				value.NewDict(map[string]value.Value{
					"msg":   value.String("country and capitals retrieved"),
					"error": value.Bool(false),
					"data": value.NewDict(map[string]value.Value{
						"name":    value.String("Spain"),
						"capital": value.String("Madrid"),
						"iso2":    value.String("ES"),
						"iso3":    value.String("ESP"),
					}),
				}),
			},
		},
		// Incorrect "method"
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(3)}),
			},
			value.NewDict(map[string]value.Value{
				"method":      value.String("DELETE"),
				"concurrency": value.Int(5),
			}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"url":     value.String("https://countriesnow.space/api/v0.1/countries/capital"),
					"body":    value.NewDict(map[string]value.Value{"country": value.String("nigeria")}),
					"default": value.Nil,
				}),
			},
			true,
			[]value.Value{},
		},
		// Missing "url" field
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(3)}),
			},
			value.NewDict(map[string]value.Value{
				"method":      value.String("GET"),
				"concurrency": value.Int(5),
			}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"body":    value.NewDict(map[string]value.Value{"country": value.String("nigeria")}),
					"default": value.Nil,
				}),
			},
			true,
			[]value.Value{},
		},
		// Set "field"
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
			},
			value.NewDict(map[string]value.Value{
				"method":      value.String("POST"),
				"concurrency": value.Int(5),
				"field":       value.String("capital"),
			}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"url":     value.String("https://countriesnow.space/api/v0.1/countries/capital"),
					"body":    value.NewDict(map[string]value.Value{"country": value.String("nigeria")}),
					"default": value.Nil,
				}),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{
					"x": value.Int(1),
					"capital": value.NewDict(
						map[string]value.Value{
							"msg":   value.String("country and capitals retrieved"),
							"error": value.Bool(false),
							"data": value.NewDict(map[string]value.Value{
								"name":    value.String("Nigeria"),
								"capital": value.String("Abuja"),
								"iso2":    value.String("NG"),
								"iso3":    value.String("NGA"),
							}),
						}),
				}),
			},
		},
		// Test caching.
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
			},
			value.NewDict(map[string]value.Value{
				"ttl": value.Int(10),
			}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"url":     value.String("mycountry"),
					"default": value.Nil,
				}),
			},
			false,
			[]value.Value{
				cacheVal,
			},
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier, op, scene.static, [][]value.Value{scene.inputs}, scene.context)
		} else {
			optest.AssertEqual(t, tier, op, scene.static, [][]value.Value{scene.inputs}, scene.context, scene.expected)
		}
	}
}
