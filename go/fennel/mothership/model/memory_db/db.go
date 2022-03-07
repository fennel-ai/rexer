package memory_db

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, memoryDB lib.MemoryDB) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO memory_db (
        cluster_id,
        cluster_security_group,
        hostname
    ) VALUES (
        :cluster_id,
        :cluster_security_group,
        :hostname
    )`, memoryDB)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
