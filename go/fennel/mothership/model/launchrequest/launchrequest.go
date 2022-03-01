package launchrequest

import (
	"fennel/mothership"
)

func InsertRequest(m mothership.Mothership, config []byte, status []byte) (uint32, error) {
	query := "INSERT INTO launch_request(status, config) VALUES (CAST(? AS JSON), CAST(? AS JSON));"
	res, err := m.DB.Exec(query, status, config)
	if err != nil {
		return 0, err
	}
	reqID, err := res.LastInsertId()
	return uint32(reqID), err
}

func DeleteRequest(m mothership.Mothership, requestID uint32) error {
	query := "DELETE from launch_request WHERE launch_request_id=?;"
	_, err := m.DB.Exec(query, requestID)
	return err
}

func GetCompletedRequestIDs(m mothership.Mothership) ([]uint32, error) {
	query := `SELECT (launch_request_id) FROM launch_request WHERE JSON_EXTRACT(status, "$.state") = "COMPLETED";`
	res := make([]uint32, 0)
	err := m.DB.Select(&res, query)
	return res, err
}
