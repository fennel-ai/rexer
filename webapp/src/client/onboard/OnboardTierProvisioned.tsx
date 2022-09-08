import axios, { AxiosResponse } from "axios";
import { Button, Badge } from "antd";
import { CopyOutlined, LoadingOutlined, LockFilled, ArrowRightOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";

import OnboardStepper from "./OnboardStepper";
import commonStyles from "./styles/Onboard.module.scss";
import styles from "./styles/OnboardTierProvisioned.module.scss";

export interface Tier {
    apiUrl:   string,
    limit:    number,
    location: string,
    plan: string,
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
    const [tier, setTier] = useState<Tier | undefined>(props.tier);
    const [submitting, setSubmitting] = useState(false);

    const queryTier = () => {
        if (loaded) {
            return;
        }
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
    useEffect(queryTier, [loaded]);

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

    const copyURL = async () => {
        const url: string = tier?.apiUrl || "";
        await navigator.clipboard.writeText(url);
    };

    // TODO(xiao) polish UI
    return (
        <div className={commonStyles.container}>
            <div className={commonStyles.logoAndName}>
                <img src="images/logo_name.svg" alt="logo" />
            </div>
            <OnboardStepper steps={3} activeStep={2} />
            {
                (loading || !tier)
                ? (
                    <div className={commonStyles.content}>
                        <LoadingOutlined spin />
                    </div>)
                :(
                    <>
                        <div className={commonStyles.content}>
                            <h4 className={commonStyles.title}>
                                Congrats, we got you a pre-provisioned tier 🎉
                            </h4>
                            <div>
                                As part of the {tier.plan} plan you’ve been pre-provisioned the following tier.
                            </div>
                            <div className={styles.tierContainer}>
                                <div className={styles.tierTitle}>
                                    <LockFilled />
                                    <span>{tier.plan} tier</span>
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
                                                <td>
                                                    <div className={styles.tierURLContainer}>
                                                        <span className={styles.tierURL}>
                                                            {tier.apiUrl}
                                                        </span>
                                                        <span className={styles.tierURLIcon} >
                                                            <CopyOutlined onClick={copyURL}/>
                                                        </span>
                                                    </div>
                                                </td>
                                            </tr>
                                            <tr>
                                                <td>Limit</td>
                                                <td>{`${tier.limit} requests/day`}</td>
                                            </tr>
                                            <tr>
                                                <td>Status</td>
                                                <td>
                                                    <span>
                                                        <Badge status="success" />
                                                        Online
                                                    </span>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                        <Button type="primary" onClick={onContinue} disabled={submitting}>
                            Continue <ArrowRightOutlined />
                        </Button>
                        <div className={styles.footnote}>
                            The tier information above will also be accessible in the dashboard so feel free to continue.
                        </div>
                    </>
                )
            }
        </div>
    );
}

export default OnboardTierProvisioned;
