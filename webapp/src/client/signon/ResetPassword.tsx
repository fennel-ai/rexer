import { Form, Input, Button, notification } from "antd";
import { useState } from "react";
import axios, { AxiosError } from "axios";

import styles from "./styles/SignOn.module.scss";

function ResetPassword() {
    return (
        <div className={styles.page}>
            <div className={styles.container}>
                <img src="/images/logo_name.svg" alt="logo" className={styles.logo} />
                <div className={styles.logoDivider} />
                <div className={styles.content}>
                    <div className={styles.header}>
                        <h4>Reset your password</h4>
                        <a href="/signin" className={styles.headerAlt}>Sign in</a>
                    </div>
                    <ResetForm />
                </div>
            </div>
        </div>
    );
}

function readTokenFromURLParam(): string {
    const url = new URL(window.location.href);
    return url.searchParams.get("token") || "";
}

function ResetForm() {
    const [submitting, setSubmitting] = useState(false);
    const token = readTokenFromURLParam();

    const onFinish = (values: {password: string}) => {
        setSubmitting(true);
        axios.post("/reset_password", {
            token,
            password: values.password,
        })
        .then(() => {
            setSubmitting(false);
            notification.success({
                message: "Your password has been updated! You can now sign in.",
                placement: "bottomRight",
                duration: 2, // seconds
                onClose: () => {
                    window.location.href = "/signin";
                },
            });
        })
        .catch((error: AxiosError<{error: string}>) => {
            setSubmitting(false);
            notification.error({
                message: error.response?.data.error,
                placement: "bottomRight",
            });
        });
    };

    return (
        <div className={styles.mainForm}>
            <Form
                name="reset_password_form"
                onFinish={onFinish}
                validateTrigger={ ["onSubmit"] }
            >
                <Form.Item
                    name="password"
                    rules={[{ required: true, message: "Please input your password" }]}
                    className={styles.formItem}
                >
                    <Input
                        type="password"
                        placeholder="Enter new password"
                        autoComplete="off"
                    />
                </Form.Item>
                <Form.Item
                    name="confirm_password"
                    rules={[
                        { required: true, message: "Please re-enter your password" },
                        ({ getFieldValue }) => ({
                            validator(_, value) {
                                if (!value || getFieldValue("password") === value) {
                                    return Promise.resolve();
                                }
                                return Promise.reject(new Error('The two passwords do not match'));
                            },
                        }),
                    ]}
                    className={styles.formItem}
                >
                    <Input
                        type="password"
                        placeholder="Re-enter new password"
                        autoComplete="off"
                    />
                </Form.Item>
                <Button
                    type="primary"
                    htmlType="submit"
                    className={styles.formButton}
                    size="large"
                    disabled={submitting}
                    loading={submitting}>

                    Confirm Password Reset
                </Button>
            </Form>
        </div>
    );
}

export default ResetPassword;
