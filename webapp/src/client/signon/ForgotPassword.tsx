import { Form, Input, Button, notification } from "antd";
import { LoadingOutlined } from "@ant-design/icons";
import { useState } from "react";
import axios, { AxiosError } from "axios";

import Pancake from "./Pancake";
import styles from "./styles/SignOn.module.scss";

function ForgotPassword() {
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

    const onFinish = (values: {email: string}) => {
        setSubmitting(true);

        axios.post("/forgot_password", {
            email: values.email,
        })
        .then(function () {
            notification.success({
                message: "A link to reset your password has been sent!",
                description: (
                    <>
                        <p>Please check your email address and click on the link to reset your password.</p>
                        <p>Didn’t receive the link? Try clicking again on the button above.</p>
                    </>
                ),
                placement: "bottomRight",
                onClose: () => {
                    setSubmitting(false);
                },
            })
        })
        .catch((error: AxiosError<{error: string}>) => {
            notification.error({
                message: error.response?.data.error,
                placement: "bottomRight",
                onClose: () => {
                    setSubmitting(false);
                },
            });
        });
    };

    return (
        <Form
            name="forgot_password_form"
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
                    size="large"
                    disabled={submitting}>

                    {submitting ? (<> <LoadingOutlined spin /> Sending... </>) : "Send a link to reset your password"}
                </Button>
            </Form.Item>
        </Form>
    );
}

export default ForgotPassword;
