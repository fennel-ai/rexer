package dataplane

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, dataplane lib.DataPlane) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO data_plane (
        aws_role,
        region,
        pulumi_stack,
        vpc_id,
        eks_instance_id,
        kafka_instance_id,
        db_instance_id,
        memory_db_instance_id,
        elasticache_instance_id
    ) VALUES (
        :aws_role,
        :region,
        :pulumi_stack,
        :vpc_id,
        :eks_instance_id,
        :kafka_instance_id,
        :db_instance_id,
        :memory_db_instance_id,
        :elasticache_instance_id
    )`, dataplane)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
