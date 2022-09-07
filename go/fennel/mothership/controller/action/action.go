package action

import (
	"context"
	"fennel/client"
	actionL "fennel/lib/action"
	"fennel/lib/ftypes"
	tierL "fennel/mothership/lib/tier"
	"net/http"
)

func Actions(c context.Context, tier tierL.Tier, actionType, actorType, actorID, targetType, targetID string) ([]actionL.Action, error) {
	cli, err := client.NewClient(tier.ApiUrl, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return cli.FetchActions(actionL.ActionFetchRequest{
		ActionType: ftypes.ActionType(actionType),
		ActorType:  ftypes.OType(actorType),
		ActorID:    ftypes.OidType(actorID),
		TargetType: ftypes.OType(targetType),
		TargetID:   ftypes.OidType(targetID),
	})
}
