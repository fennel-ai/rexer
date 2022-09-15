import { Form, Input, Checkbox, Button } from "antd";
import { ArrowRightOutlined } from '@ant-design/icons';
import axios, { AxiosResponse } from "axios";
import { useState } from "react";

import OnboardStepper from "./OnboardStepper";
import commonStyles from "./styles/Onboard.module.scss";
import styles from "./styles/OnboardCreateTeam.module.scss";

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

    const [submitting, setSubmitting] = useState(false);

    const onFinish = (values: { teamName: string, allowAutoJoin: boolean }) => {
        setSubmitting(true);

        axios.post("/onboard/create_team", {
            name: values.teamName,
            allowAutoJoin: values.allowAutoJoin,
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
                <Form
                    name="createTeamForm"
                    autoComplete="off"
                    onFinish={onFinish}
                    initialValues={{ teamName: suggestedName, allowAutoJoin: !isPersonalDomain }}>

                    <Form.Item
                        name="teamName"
                        rules={[{ required: true, message: "Team name can't be empty" }]}
                        className={styles.teamName}
                    >
                        <Input />
                    </Form.Item>
                    {
                        isPersonalDomain ? null : (
                            <Form.Item name="allowAutoJoin" valuePropName="checked" className={styles.autoJoin}>
                                <Checkbox>
                                    Users of @{domain} can automatically join
                                </Checkbox>
                            </Form.Item>
                        )
                    }
                    <Button htmlType="submit" type="primary" disabled={submitting} loading={submitting} className={styles.button}>
                        Continue <ArrowRightOutlined />
                    </Button>
                </Form>
            </div>
        </div>
    );
}

export default OnboardCreateTeam;
