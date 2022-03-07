package elasticache

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, elasticache lib.ElastiCache) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO elasticache (
        cluster_id,
        cluster_security_group,
        primary_hostname,
        replica_hostname
    ) VALUES (
        :cluster_id,
        :cluster_security_group,
        :primary_hostname,
        :replica_hostname
    )`, elasticache)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
