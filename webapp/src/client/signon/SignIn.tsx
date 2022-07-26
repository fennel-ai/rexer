import { Button, Form, Input } from "antd";
import styles from "../styles/signon/SignIn.module.scss";

function SignIn() {
    return (
        <div>
            <div className={styles.container}>
                <img src="images/logo.svg" alt="logo" className={styles.logo} />
                <hr className={styles.logoDivider} />
                <div className={styles.signInHeader}>
                    <h4>Sign In</h4>
                    <a href="#">Sign up instead?</a>
                </div>
                <SignInForm />
            </div>
        </div>
    );
}

function SignInForm() {
    const onFinish = () => {
        console.log("sign in");
    };

    return (
        <Form
            name="signin_form"
            className={styles.signInForm}
            initialValues={{ remember: true }}
            onFinish={onFinish}
        >
            <Form.Item
                name="email"
                rules={[
                    { required: true, message: "Please input your work email" },
                    { type: "email", message: "Please input a valid email address"},
                ]}
                className={styles.signInFormItem}
            >
                <Input placeholder="Work email" />
            </Form.Item>
            <Form.Item
                name="password"
                rules={[{ required: true, message: "Please input your password" }]}
                className={styles.signInFormItem}
            >
                <Input
                    type="password"
                    placeholder="Password"
                />
            </Form.Item>
            <Form.Item className={styles.signInFormItem}>
                <Button type="primary" htmlType="submit" className={styles.signInFormButton}>
                    Sign In
                </Button>
            </Form.Item>
            <a href="#">Forgot your password</a>
        </Form>
    );
}

export default SignIn;
