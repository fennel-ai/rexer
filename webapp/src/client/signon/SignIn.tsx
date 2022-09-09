import { Button, Form, Input, notification } from "antd";
import { LoadingOutlined } from "@ant-design/icons";
import axios, { AxiosError } from "axios";
import { useState } from "react";

import styles from "./styles/SignOn.module.scss";

function SignIn() {
    return (
        <div className={styles.page}>
            <div className={styles.container}>
                <img src="/images/logo_name.svg" alt="logo" className={styles.logo} />
                <div className={styles.logoDivider} />
                <div className={styles.content}>
                    <div className={styles.header}>
                        <h4 className={styles.headerTitle}>Sign In</h4>
                        <a href="/signup" className={styles.headerAlt}>Sign up instead?</a>
                    </div>
                    <SignInForm />
                    <a href="/forgot_password">Forgot your password</a>
                </div>
            </div>
        </div>
    );
}

interface FormValues {
    email: string,
    password: string,
}

function SignInForm() {
    const [submitting, setSubmitting] = useState(false);

    const onFinish = (values: FormValues) => {
        setSubmitting(true);
        axios.post("/signin", {
            email: values.email,
            password: values.password,
        })
        .then(function () {
            setSubmitting(false);
            window.location.href = "/";
        })
        .catch(function (error: AxiosError<{error: string}>) {
            setSubmitting(false);
            notification.error({
                message: error.response?.data.error,
                placement: "bottomRight",
            })
        });
    };

    return (
        <div className={styles.mainForm}>
            <Form
                name="signin_form"

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
                <Form.Item
                    name="password"
                    rules={[{ required: true, message: "Please input your password" }]}
                    className={styles.formItem}
                >
                    <Input
                        type="password"
                        placeholder="Password"
                        autoComplete="off"
                    />
                </Form.Item>
                <Button
                    type="primary"
                    htmlType="submit"
                    className={styles.formButton}
                    size="large"
                    disabled={submitting}>

                    {submitting ? (<div> <LoadingOutlined spin /> Signing In... </div>) : "Sign In"}
                </Button>
            </Form>
        </div>
    );
}

export default SignIn;
