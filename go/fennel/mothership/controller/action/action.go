package action

import (
	"context"
	"fennel/client"
	lib "fennel/lib/action"
	"fennel/lib/ftypes"
	"net/http"
)

func Actions(c context.Context, actionType, actorType, actorID, targetType, targetID string) ([]lib.Action, error) {
	// TODO(xiao) client address
	cli, err := client.NewClient("http://localhost:2425", http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return cli.FetchActions(lib.ActionFetchRequest{
		ActionType: ftypes.ActionType(actionType),
		ActorType:  ftypes.OType(actorType),
		ActorID:    ftypes.OidType(actorID),
		TargetType: ftypes.OType(targetType),
		TargetID:   ftypes.OidType(targetID),
	})
}
