package resource

type Type uint8

/*
Resource represents any external resource that needs
to be initialized/closed with some dependency management.
The way to define any new resource is to create a struct that
implements Config interface. Using that config, materialize the
resource. Any initialization/setup should be done during this
materialization.

In addition to this explicit way, it is recommended to create
methods that provide a default resource using a default config.
In such cases, it's best if different default resources are provided
depending on whatever instance (test/prod/dev) the control is in.

*/

const (
	DBConnection  Type = 1
	DBTable            = 2
	RedisClient        = 3
	RedisCluster       = 4
	KafkaConsumer      = 5
	KafkaProducer      = 6
)

type Config interface {
	Materialize() (Resource, error)
}

type Resource interface {
	Close() error
	Teardown() error
	Type() Type
}
