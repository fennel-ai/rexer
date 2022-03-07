package db

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, db lib.DB) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO db (
        cluster_id,
        cluster_security_group,
        db_host,
        admin_username,
        admin_password
    ) VALUES (
        :cluster_id,
        :cluster_security_group,
        :db_host,
        :admin_username,
        :admin_password
        
    )`, db)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
