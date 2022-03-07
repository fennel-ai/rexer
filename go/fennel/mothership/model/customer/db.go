package customer

import (
	"fennel/mothership"
	"fennel/mothership/lib"
)

func Insert(mothership mothership.Mothership, customer lib.Customer) (uint32, error) {
	res, err := mothership.DB.NamedExec(`INSERT INTO customer (name) VALUES (:name)`, customer)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
