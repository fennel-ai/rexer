import { useState } from "react";

import OnboardSetupTeam from "./OnboardSetupTeam";
import OnboardAboutYourself from "./OnboardAboutYourself";

interface User {
    onboardStatus: number,
    email: string,
    firstName: string,
}

const ONBOARD_STATUS_SETUP_TEAM = 0;
const ONBOARD_STATUS_ABOUT_YOURSELF = 1;
const ONBOARD_STATUS_TIER_PROVISION = 2;
// const ONBOARD_STATUS_TIER_PROVISIONED = 2;
// const ONBOARD_STATUS_TIER_NOT_AVAILABLE = 3;
// const ONBOARD_STATUS_WELCOME = 4;
// const ONBOARD_STATUS_DONE = 5;

function OnboardPage({user}: {user: User}) {
    const [onboardStatus, setOnboardStatus] = useState<number>(user.onboardStatus);

    const updateStatus = (newStatus: number) => setOnboardStatus(newStatus);

    switch (onboardStatus) {
        case ONBOARD_STATUS_SETUP_TEAM:
            return <OnboardSetupTeam user={user} onOnboardStatusChange={updateStatus} />
        case ONBOARD_STATUS_ABOUT_YOURSELF:
            return <OnboardAboutYourself onOnboardStatusChange={updateStatus} />;
        case ONBOARD_STATUS_TIER_PROVISION:
            return (<div>Tier provision</div>);
    }
    return (<div>Not implemented</div>);
}

export default OnboardPage;
