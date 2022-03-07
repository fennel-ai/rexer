package tier

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, tier lib.Tier) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO tier (
        data_plane_id,
        customer_id,
        pulumi_stack,
        api_url,
        k8s_namespace
    ) VALUES (
        :data_plane_id,
        :customer_id,
        :pulumi_stack,
        :api_url,
        :k8s_namespace
    )`, tier)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
