module fennel

go 1.18

require (
	capnproto.org/go/capnp/v3 v3.0.0-alpha.7
	github.com/AndreasBriese/bbloom v0.0.0-20190306092124-e2d15f34fcf9
	github.com/OneOfOne/xxhash v1.2.8
	github.com/Unleash/unleash-client-go/v3 v3.5.0
	github.com/alexflint/go-arg v1.4.2
	github.com/alicebob/miniredis/v2 v2.17.0
	github.com/buger/jsonparser v1.1.1
	github.com/cockroachdb/pebble v0.0.0-20220906142723-ade651d00072
	github.com/confluentinc/confluent-kafka-go v1.8.2
	github.com/detailyang/fastrand-go v0.0.0-20191106153122-53093851e761
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/gin-gonic/gin v1.8.1
	github.com/go-logr/stdr v1.2.2
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/heptiolabs/healthcheck v0.0.0-20211123025425-613501dd5deb
	github.com/jmoiron/sqlx v1.3.4
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/milvus-io/milvus-sdk-go/v2 v2.0.0
	github.com/prometheus/client_golang v1.12.1
	github.com/raulk/clock v1.1.0
	github.com/raulk/go-watchdog v1.2.0
	github.com/samber/lo v1.25.0
	github.com/samber/mo v1.0.0
	github.com/segmentio/fasthash v1.0.3
	github.com/sendgrid/sendgrid-go v3.11.1+incompatible
	github.com/stretchr/testify v1.8.0
	github.com/zeebo/xxh3 v1.0.2
	go.etcd.io/bbolt v1.3.6
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.33.0
	go.opentelemetry.io/contrib/propagators/aws v1.7.0
	go.opentelemetry.io/otel v1.8.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.7.0
	go.opentelemetry.io/otel/sdk v1.7.0
	go.opentelemetry.io/otel/trace v1.8.0
	go.uber.org/zap v1.21.0
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/appengine v1.6.6
	google.golang.org/genproto v0.0.0-20211118181313-81c1377c94b1
	google.golang.org/grpc v1.47.0
	google.golang.org/protobuf v1.28.0
	gorm.io/driver/mysql v1.3.6
	gorm.io/gorm v1.23.8
	gorm.io/plugin/soft_delete v1.2.0
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/Masterminds/semver/v3 v3.1.1 // indirect
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/containerd/cgroups v0.0.0-20201119153540-4cbc285b3327 // indirect
	github.com/coreos/go-systemd/v22 v22.1.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/elastic/gosigar v0.12.0 // indirect
	github.com/gin-contrib/gzip v0.0.6 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.11.0 // indirect
	github.com/goccy/go-json v0.9.8 // indirect
	github.com/godbus/dbus/v5 v5.0.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/gorilla/sessions v1.2.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opencontainers/runtime-spec v1.0.2 // indirect
	github.com/pelletier/go-toml/v2 v2.0.2 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	github.com/sendgrid/rest v2.6.9+incompatible // indirect
	github.com/twmb/murmur3 v1.1.5 // indirect
	github.com/ugorji/go/codec v1.2.7 // indirect
	go.opencensus.io v0.22.5 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.7.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.7.0 // indirect
	go.opentelemetry.io/proto/otlp v0.16.0 // indirect
	golang.org/x/exp v0.0.0-20220303212507-bbda1eaf7a17 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/DATA-DOG/go-sqlmock.v1 v1.3.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

require (
	github.com/alexflint/go-scalar v1.0.0 // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/aws/aws-sdk-go v1.43.45
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1-0.20220818201730-4093c561df10
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/gin-contrib/sessions v0.0.5
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/yuin/gopher-lua v0.0.0-20200816102855-ee81675732da // indirect
	go.uber.org/atomic v1.10.0
	go.uber.org/multierr v1.7.0 // indirect
	golang.org/x/net v0.0.0-20220708220712-1185a9018129
	golang.org/x/sys v0.0.0-20220818161305-2296e01440c6
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
