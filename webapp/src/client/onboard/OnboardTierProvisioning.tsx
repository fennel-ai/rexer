import axios, { AxiosResponse } from "axios";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";

import OnboardStepper from "./OnboardStepper";
import { type Tier } from "./OnboardTierProvisioned";
import commonStyles from "./styles/Onboard.module.scss";

interface TierProvisionResponse {
    onboardStatus: number,
    tier?: Tier,
}

interface Props {
    onOnboardStatusChange: (status: number, tier?: Tier) => void,
}

function OnboardTierProvisioning({ onOnboardStatusChange }: Props) {
    const [loading, setLoading] = useState(false);

    const queryTierProvision = () => {
        setLoading(true);

        axios.post("/onboard/assign_tier")
            .then((response: AxiosResponse<TierProvisionResponse>) => {
                const { tier, onboardStatus } = response.data;
                setLoading(false);
                onOnboardStatusChange(onboardStatus, tier);
            })
            .catch(() => {
                setLoading(false);
                // TODO(xiao) error handling?
            });
    };
    useEffect(queryTierProvision, []);

    return (
        <div className={commonStyles.container}>
            <div className={commonStyles.logoAndName}>
                <img src="images/logo_name.svg" alt="logo" />
            </div>
            <OnboardStepper steps={3} activeStep={2} />
            <div className={commonStyles.content}>
                {loading ? <LoadingOutlined spin /> : <OnboardTierNotAvailable />}
            </div>
        </div>
    );
}

function OnboardTierNotAvailable() {
    return (
        <div className={commonStyles.title}>
            No available tiers right now! Please contact fennel support.
        </div>
    );
}

export default OnboardTierProvisioning;
