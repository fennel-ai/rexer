import { Form, Input, Checkbox, Button } from "antd";
import { ArrowRightOutlined } from '@ant-design/icons';

import commonStyles from "./styles/Onboard.module.scss";

interface Props {
    isPersonalDomain: boolean,
    user: User,
}

interface User {
    firstName: string,
    email: string,
}

function OnboardCreateTeam({isPersonalDomain, user}: Props) {
    const domain = user.email.substring(user.email.lastIndexOf("@") +1);
    const suggestedName = isPersonalDomain ? `${user.firstName}'s Team` : domain.substring(0, domain.indexOf("."));

    return (
        <div className={commonStyles.container}>
            <div className={commonStyles.logoAndName}>
                <img src="images/logo.svg" alt="logo" />
                Fennel AI
            </div>
            <h4 className={commonStyles.title}>Let’s set up your team</h4>
            <div>What is the name of your team?</div>
            <Form name="updatePwdForm">
                <Form.Item
                    name="teamName"
                    rules={[{ required: true, message: "team name can't be empty" }]}
                >
                    <Input
                        autoComplete="off"
                        defaultValue={suggestedName}
                    />
                </Form.Item>
                <Form.Item name="allowAutoJoin">
                    <Checkbox defaultChecked={!isPersonalDomain}>
                        Users of @{domain} can automatically join
                    </Checkbox>
                </Form.Item>
            </Form>
            <Button type="primary">
                Continue <ArrowRightOutlined />
            </Button>
        </div>
    );
}

export default OnboardCreateTeam;
