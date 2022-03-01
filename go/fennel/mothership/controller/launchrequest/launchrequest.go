package launchrequest

import (
	"fennel/mothership"
	"fennel/mothership/model/launchrequest"
)

func ProcessCompletedRequests(m mothership.Mothership) error {
	reqIDs, err := launchrequest.GetCompletedRequestIDs(m)
	if err != nil {
		return err
	}
	for _, reqID := range reqIDs {
		// TODO - do something
		err := launchrequest.DeleteRequest(m, reqID)
		if err != nil {
			return err
		}
	}
	return nil
}
