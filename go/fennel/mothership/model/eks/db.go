package eks

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, eks lib.EKS) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO eks (
        cluster_id,
        min_instances,
        max_instances,
        instance_type
    ) VALUES (
        :cluster_id,
        :min_instances,
        :max_instances,
        :instance_type
    )`, eks)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
