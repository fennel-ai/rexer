import { Form, Input, Button, notification } from "antd";
import { useState } from "react";
import axios, { AxiosError } from "axios";

import styles from "./styles/SignOn.module.scss";

function ForgotPassword() {
    return (
        <div className={styles.page}>
            <div className={styles.container}>
                <img src="/images/logo_name.svg" alt="logo" className={styles.logo} />
                <div className={styles.logoDivider} />
                <div className={styles.content}>
                    <div className={styles.header}>
                        <h4 className={styles.headerTitle}>Forgot Password?</h4>
                        <a href="/signin" className={styles.headerAlt}>Sign in</a>
                    </div>
                    <ForgotForm />
                </div>
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
        <div className={styles.mainForm}>
            <Form
                name="forgot_password_form"
                onFinish={onFinish}
                validateTrigger={ ["onSubmit"] }
            >
                <Form.Item
                    name="email"
                    rules={[
                        { required: true, message: "Please input your email" },
                        { type: "email", message: "Please input a valid email address" },
                    ]}
                    className={styles.formItem}
                >
                    <Input placeholder="Email" />
                </Form.Item>
                <Button
                    type="primary"
                    htmlType="submit"
                    className={styles.formButton}
                    size="large"
                    disabled={submitting}
                    loading={submitting}>

                    Send a link to reset your password
                </Button>
            </Form>
        </div>
    );
}

export default ForgotPassword;
