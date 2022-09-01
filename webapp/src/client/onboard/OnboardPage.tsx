import { useState } from "react";

import OnboardSetupTeam from "./OnboardSetupTeam";

interface User {
    onboardStatus: number,
    email: string,
    firstName: string,
}

const ONBOARD_STATUS_SETUP_TEAM = 0;
const ONBOARD_STATUS_ABOUT_YOURSELF = 1;
// const ONBOARD_STATUS_TIER_PROVISIONED = 2;
// const ONBOARD_STATUS_TIER_NOT_AVAILABLE = 3;
// const ONBOARD_STATUS_WELCOME = 4;
// const ONBOARD_STATUS_DONE = 5;

function OnboardPage({user}: {user: User}) {
    const [onboardStatus, setOnboardStatus] = useState<number>(user.onboardStatus);
    switch (onboardStatus) {
        case ONBOARD_STATUS_SETUP_TEAM:
            return <OnboardSetupTeam user={user} onOnboardStatusChange={(newStatus: number) => setOnboardStatus(newStatus)} />;
        case ONBOARD_STATUS_ABOUT_YOURSELF:
            return <OnboardAboutYourself />;
    }
    return (<div>Not implemented</div>);
}

function OnboardAboutYourself() {
    return (<div>About yourself</div>);
}

export default OnboardPage;
