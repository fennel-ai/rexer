package kafka

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, kafka lib.Kafka) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO kafka (
        confluent_environment,
        confluent_cluster_id,
        confluent_cluster_name,
        kafka_bootstrap_servers,
        kafka_api_key,
        kafka_secret_key
    ) VALUES (
        :confluent_environment,
        :confluent_cluster_id,
        :confluent_cluster_name,
        :kafka_bootstrap_servers,
        :kafka_api_key,
        :kafka_secret_key
    )`, kafka)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
