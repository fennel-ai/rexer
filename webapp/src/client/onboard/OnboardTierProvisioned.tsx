import axios, { AxiosResponse } from "axios";
import { Button } from "antd";
import { LoadingOutlined, LockOutlined, ArrowRightOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";

import commonStyles from "./styles/Onboard.module.scss";
import styles from "./styles/OnboardTierProvisioned.module.scss";

export interface Tier {
    apiUrl:   string,
    limit:    number,
    location: string,
}

interface TierResponse {
    tier: Tier,
}

interface Props {
    tier?: Tier,
    onOnboardStatusChange: (status: number, tier?: Tier) => void,
}

interface TierProvisionedResponse {
    onboardStatus: number,
}

function OnboardTierProvisioned(props: Props) {
    const [loading, setLoading] = useState<boolean>(false);
    const [loaded, setLoaded] = useState<boolean>(!!props.tier);
    const [tier, setTier] = useState<Tier | null>(props.tier || null);
    const [submitting, setSubmitting] = useState(false);

    if (!loaded) {
        const queryTier = () => {
            setLoading(true);

            axios.get("/onboard/tier")
                .then((response: AxiosResponse<TierResponse>) => {
                    setLoading(false);
                    setLoaded(true);
                    setTier(response.data.tier);
                })
                .catch(() => {
                    // TODO(xiao) error handling
                });
        }
        useEffect(queryTier, []);
    }

    if (loading || !tier) {
        return (
            <div className={commonStyles.container}>
                <div className={commonStyles.logoAndName}>
                    <img src="images/logo.svg" alt="logo" />
                    Fennel AI
                </div>
                <LoadingOutlined spin />
            </div>
        );
    }

    const onContinue = () => {
        setSubmitting(true);

        axios.post("/onboard/tier_provisioned")
            .then((response: AxiosResponse<TierProvisionedResponse>) => {
                const { onboardStatus } = response.data;
                setSubmitting(false);
                props.onOnboardStatusChange(onboardStatus, tier);
            })
            .catch(() => {
                // TODO(xiao) error handling
                setSubmitting(false);
            });
    };

    // TODO(xiao) polish UI
    return (
        <div className={commonStyles.container}>
            <div className={commonStyles.logoAndName}>
                <img src="images/logo.svg" alt="logo" />
                Fennel AI
            </div>
            <h4 className={commonStyles.title}>
                Congrats, we got you a pre-provisioned tier 🎉
            </h4>
            <div>
                As part of the basic plan you’ve been pre-provisioned the following tier.
            </div>
            <div className={styles.tierContainer}>
                <div className={styles.tierTitle}>
                    <LockOutlined />
                    Basic tier
                </div>
                <div>
                    <table className={styles.tierTable}>
                        <tbody>
                            <tr>
                                <td>Location</td>
                                <td>{tier.location}</td>
                            </tr>
                            <tr>
                                <td>URL</td>
                                <td>{tier.apiUrl}</td>
                            </tr>
                            <tr>
                                <td>Limit</td>
                                <td>{`${tier.limit} requests/day`}</td>
                            </tr>
                            <tr>
                                <td>Status</td>
                                <td>Online</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div className={styles.tierButtons}>
                    <Button>See other plans</Button>
                    <Button>Request upgrade</Button>
                </div>
            </div>
            <Button type="primary" onClick={onContinue} disabled={submitting}>
                Continue <ArrowRightOutlined />
            </Button>
            <div className={styles.footnote}>
                The tier information above will also be accessible in the dashboard so feel free to continue.
            </div>
        </div>
    );
}

export default OnboardTierProvisioned;
