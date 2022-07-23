import { Button, Form, Input } from "antd";
import styles from "../styles/signon/SignUp.module.scss";

function SignUp() {
    return (
        <div>
            <div className={styles.container}>
                <img src="images/logo.svg" alt="logo" className={styles.logo} />
                <hr className={styles.logoDivider} />
                <div className={styles.signUpHeader}>
                    <h4>Sign up</h4>
                    <div>Login Instead</div>
                </div>
                <SignUpForm />
            </div>
        </div>
    );
}

function SignUpForm() {
    const onFinish = () => {
        console.log("sign up");
    };

    return (
        <Form
            name="normal_login"
            className={styles.signUpForm}
            initialValues={{ remember: true }}
            onFinish={onFinish}
        >
            <Form.Item
                name="email"
                rules={[{ required: true, message: 'Please input your work email!' }]}
                className={styles.signUpFormItem}
            >
                <Input placeholder="Work email" />
            </Form.Item>
            <Form.Item
                name="password"
                rules={[{ required: true, message: 'Please input your Password!' }]}
                className={styles.signUpFormItem}
            >
            <Input
                type="password"
                placeholder="Password"
            />
            </Form.Item>
            <Form.Item className={styles.signUpFormItem}>
                <Button type="primary" htmlType="submit" className={styles.signUpFormButton}>
                    Sign Up
                </Button>
            </Form.Item>
      </Form>
    );
}

export default SignUp;