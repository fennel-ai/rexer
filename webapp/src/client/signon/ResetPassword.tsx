import { Form, Input, Button } from "antd";
import { LoadingOutlined } from "@ant-design/icons";
import { useState } from "react";

import Pancake from "./Pancake";
import styles from "../styles/signon/SignOn.module.scss";

function ResetPassword() {
    return (
        <div className={styles.page}>
            <Pancake />
            <div className={styles.container}>
                <img src="images/logo.svg" alt="logo" className={styles.logo} />
                <div className={styles.logoDivider} />
                <div className={styles.header}>
                    <h4 className={styles.headerTitle}>Forgot Password?</h4>
                    <a href="/signin" className={styles.headerAlt}>Sign in</a>
                </div>
                <ForgotForm />
            </div>
        </div>
    );
}

function ForgotForm() {
    const [submitting, setSubmitting] = useState(false);

    const onFinish = () => {
        setSubmitting(true);
    };

    return (
        <Form
            name="signin_form"
            className={styles.mainForm}
            initialValues={{ remember: true }}
            onFinish={onFinish}
        >
            <Form.Item
                name="email"
                rules={[
                    { required: true, message: "Please input your work email" },
                    { type: "email", message: "Please input a valid email address"},
                ]}
                className={styles.formItem}
            >
                <Input placeholder="Work email" />
            </Form.Item>
            <Form.Item className={styles.formItem}>
            <Button
                    type="primary"
                    htmlType="submit"
                    className={styles.formButton}
                    style={{background: styles.formButtonBackground}}
                    disabled={submitting}>

                    {submitting ? (<div> <LoadingOutlined spin /> Sending... </div>) : "Send reset password"}
                </Button>
            </Form.Item>
        </Form>
    );
}

export default ResetPassword;
