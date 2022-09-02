import { Button } from "antd";
import { ArrowRightOutlined } from '@ant-design/icons';
import axios, { AxiosResponse } from "axios";
import { useState } from "react";

import commonStyles from "./styles/Onboard.module.scss";

interface Props {
    onOnboardStatusChange: (status: number) => void,
}

interface AboutYourselfResponse {
    onboardStatus: number,
}

function OnboardAboutYourself({onOnboardStatusChange}: Props) {
    const [submitting, setSubmitting] = useState(false);

    const onContinue = () => {
        setSubmitting(true);

        axios.post("/onboard/submit_questionnaire")
            .then((response: AxiosResponse<AboutYourselfResponse>) => {
                onOnboardStatusChange(response.data.onboardStatus);
                setSubmitting(false);
            })
            .catch(() => {
                // TODO(xiao) error handling
                setSubmitting(false);
            });
    };

    return (
        <div className={commonStyles.container}>
            <div className={commonStyles.logoAndName}>
                <img src="images/logo.svg" alt="logo" />
                Fennel AI
            </div>
            <h4 className={commonStyles.title}>Tell us a bit about yourself</h4>
            <div>
                To be built!
            </div>
            <Button type="primary" onClick={onContinue} disabled={submitting}>
                Continue <ArrowRightOutlined />
            </Button>
        </div>
    );
}

export default OnboardAboutYourself;
