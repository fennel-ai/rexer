import { Form, Input, Checkbox, Button } from "antd";
import { ArrowRightOutlined } from '@ant-design/icons';
import { CheckboxChangeEvent } from "antd/lib/checkbox";
import axios, { AxiosResponse } from "axios";
import { useState, ChangeEvent } from "react";

import OnboardStepper from "./OnboardStepper";
import commonStyles from "./styles/Onboard.module.scss";

interface Props {
    isPersonalDomain: boolean,
    user: User,
    onOnboardStatusChange: (status: number) => void,
}

interface User {
    firstName: string,
    email: string,
}

interface CreateTeamResponse {
    onboardStatus: number,
}

function OnboardCreateTeam({isPersonalDomain, user, onOnboardStatusChange}: Props) {
    const domain = user.email.substring(user.email.lastIndexOf("@") +1);
    const suggestedName = isPersonalDomain ? `${user.firstName}'s Team` : domain.substring(0, domain.indexOf("."));

    const [teamName, setTeamName] = useState<string>(suggestedName);
    const [allowAutoJoin, setAllowAutoJoin] = useState<boolean>(!isPersonalDomain);
    const [submitting, setSubmitting] = useState(false);

    const onContinue = () => {
        setSubmitting(true);

        axios.post("/onboard/create_team", {
            name: teamName,
            allowAutoJoin,
        }).then((response: AxiosResponse<CreateTeamResponse>) => {
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
                <img src="images/logo_name.svg" alt="logo" />
            </div>
            <OnboardStepper steps={3} activeStep={1} />

            <div className={commonStyles.content}>
                <h4 className={commonStyles.title}>Let’s set up your team</h4>
                <div>What is the name of your team?</div>
                <Form name="createTeamForm">
                    <Form.Item
                        name="teamName"
                        rules={[{ required: true, message: "team name can't be empty" }]}
                    >
                        <Input
                            autoComplete="off"
                            value={teamName}
                            defaultValue={teamName}
                            onChange={(e: ChangeEvent<HTMLInputElement>) => setTeamName(e.target.value)}
                        />
                    </Form.Item>
                    {
                        isPersonalDomain ? null : (
                            <Form.Item name="allowAutoJoin">
                                <Checkbox
                                    checked={allowAutoJoin}
                                    onChange={(e: CheckboxChangeEvent) => setAllowAutoJoin(e.target.value)}
                                >
                                    Users of @{domain} can automatically join
                                </Checkbox>
                            </Form.Item>
                        )
                    }
                </Form>
            </div>
            <Button type="primary" onClick={onContinue} disabled={submitting}>
                Continue <ArrowRightOutlined />
            </Button>
        </div>
    );
}

export default OnboardCreateTeam;
