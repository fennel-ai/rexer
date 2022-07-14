package connector

func FromProtoConfig(config *ConnectorConfig) Config {
	return Config{
		CursorField: config.CursorField,
	}
}

func ToProtoConfig(config Config) *ConnectorConfig {
	return &ConnectorConfig{
		CursorField: config.CursorField,
	}
}
