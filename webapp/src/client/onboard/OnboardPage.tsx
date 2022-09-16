import { useState } from "react";

import OnboardSetupTeam from "./OnboardSetupTeam";
import OnboardTierProvisioning from "./OnboardTierProvisioning";
import OnboardTierProvisioned, { type Tier } from "./OnboardTierProvisioned";

interface User {
    onboardStatus: number,
    email: string,
    firstName: string,
}

const ONBOARD_STATUS_SETUP_TEAM = 0;
// const ONBOARD_STATUS_ABOUT_YOURSELF = 1;
const ONBOARD_STATUS_TIER_PROVISIONING = 2;
const ONBOARD_STATUS_TIER_PROVISIONED = 3;
// const ONBOARD_STATUS_WELCOME = 4;
 const ONBOARD_STATUS_DONE = 5;

function OnboardPage({user}: {user: User}): JSX.Element {
    const [onboardStatus, setOnboardStatus] = useState<number>(user.onboardStatus);
    const [tier, setTier] = useState<Tier>();

    const updateStatus = (newStatus: number, tier?: Tier) => {
        setOnboardStatus(newStatus);
        if (tier) {
            setTier(tier);
        }
    }

    switch (onboardStatus) {
        case ONBOARD_STATUS_SETUP_TEAM:
            return <OnboardSetupTeam user={user} onOnboardStatusChange={updateStatus} />
        case ONBOARD_STATUS_TIER_PROVISIONING:
            return <OnboardTierProvisioning onOnboardStatusChange={updateStatus} />;
        case ONBOARD_STATUS_TIER_PROVISIONED:
            return <OnboardTierProvisioned tier={tier} onOnboardStatusChange={updateStatus} />;
        case ONBOARD_STATUS_DONE:
            window.location.href = "/";
    }
    return (<div>Not implemented</div>);
}

export default OnboardPage;
