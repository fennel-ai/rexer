package resource

type Type uint8

/*
Resource represents any external resource that needs
to be initialized/closed with some dependency management.
The way to define any new resource is to create a struct that
implements Config interface. Using that config, materialize the
resource. Any initialization/setup should be done during this
materialization.

*/

const (
	DBConnection  Type = 1
	RedisClient        = 2
	KafkaConsumer      = 3
	KafkaProducer      = 4
	Badger             = 5
	NitrousClient      = 6
)

type Config interface {
	Materialize() (Resource, error)
}

type Resource interface {
	Close() error
	Type() Type
	Scope
}
