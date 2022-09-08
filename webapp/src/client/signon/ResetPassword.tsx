import { Form, Input, Button, notification } from "antd";
import { LoadingOutlined } from "@ant-design/icons";
import { useState } from "react";
import axios, { AxiosError } from "axios";

import Pancake from "./Pancake";
import styles from "./styles/SignOn.module.scss";

function ResetPassword() {
    return (
        <div className={styles.page}>
            <Pancake />
            <div className={styles.container}>
                <img src="images/logo.svg" alt="logo" className={styles.logo} />
                <div className={styles.logoDivider} />
                <div className={styles.header}>
                    <h4 className={styles.headerTitle}>Reset your password</h4>
                    <a href="/signin" className={styles.headerAlt}>Sign in</a>
                </div>
                <ResetForm />
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
        <Form
            name="reset_password_form"
            className={styles.mainForm}
            onFinish={onFinish}
        >
            <Form.Item
                name="password"
                rules={[{ required: true, message: "Please input your password" }]}
                className={styles.formItem}
            >
                <Input
                    type="password"
                    placeholder="Enter new password"
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
                />
            </Form.Item>
            <Form.Item className={styles.formItem}>
                <Button
                    type="primary"
                    htmlType="submit"
                    className={styles.formButton}
                    size="large"
                    disabled={submitting}>

                    {submitting ? (<div> <LoadingOutlined spin /> Sending... </div>) : "Confirm Password Reset"}
                </Button>
            </Form.Item>
        </Form>
    );
}

export default ResetPassword;
