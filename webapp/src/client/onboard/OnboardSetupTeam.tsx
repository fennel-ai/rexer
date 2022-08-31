import { Form, Input, Checkbox, Button } from "antd";
import { ArrowRightOutlined } from '@ant-design/icons';

function OnboardSetupTeam() {
    return (
        <div>
            <div>
                <img src="images/logo.svg" alt="logo" />
                Fennel AI
            </div>
            <h4>Let’s set up your team</h4>
            <div>What is the name of your team?</div>
            <Form name="updatePwdForm">
                <Form.Item
                    name="teamName"
                    rules={[{ required: true, message: "team name can't be empty" }]}
                >
                    <Input
                        autoComplete="off"
                        value="Xiao's team"
                    />
                </Form.Item>
            </Form>
            <Checkbox>Users of @company.com can automatically join</Checkbox>
            <Button>Continue <ArrowRightOutlined /></Button>
        </div>
    );
}

export default OnboardSetupTeam;
