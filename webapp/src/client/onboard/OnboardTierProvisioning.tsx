import axios, { AxiosResponse } from "axios";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";

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
                <img src="images/logo.svg" alt="logo" />
                Fennel AI
            </div>
            {loading ? <LoadingOutlined spin /> : <OnboardTierNotAvailable />}
        </div>
    );
}

function OnboardTierNotAvailable() {
    // TODO(xiao)
    return (
        <div>
            No Available tiers right now! (xiao: UNFINISHED UI)
        </div>
    );
}

export default OnboardTierProvisioning;
