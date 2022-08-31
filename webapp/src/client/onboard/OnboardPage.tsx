import OnboardSetupTeam from "./OnboardSetupTeam";

interface User {
    onboardStatus: number,
}

const ONBOARD_STATUS_SETUP_TEAM = 0;
const ONBOARD_STATUS_ABOUT_YOURSELF = 1;
const ONBOARD_STATUS_TIER_PROVISIONED = 2;
const ONBOARD_STATUS_TIER_NOT_AVAILABLE = 3;
const ONBOARD_STATUS_WELCOME = 4;
const ONBOARD_STATUS_DONE = 5;

function OnboardPage({user}: {user: User}) {
    switch (user.onboardStatus) {
        case ONBOARD_STATUS_SETUP_TEAM:
            return <OnboardSetupTeam />;
    }
    return (<div>Not implemented</div>);
}

export default OnboardPage;
