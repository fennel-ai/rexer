import { Button, Form, Input, notification } from "antd";
import { LoadingOutlined, CheckCircleOutlined } from '@ant-design/icons';
import axios, { AxiosError } from "axios";
import { useState } from "react";

import styles from "./styles/SignOn.module.scss";

function SignUp() {
    const [submitted, setSubmitted] = useState(false);
    const [submittedEmail, setSubmittedEmail] = useState("");

    const onSubmit = (email: string) => {
        setSubmitted(true);
        setSubmittedEmail(email);
    };

    return (
        <div className={styles.page}>
            <div className={styles.container}>
                <img src="/images/logo_name.svg" alt="logo" className={styles.logo} />
                <div className={styles.logoDivider} />
                {
                    submitted ? <ConfirmEmail email={submittedEmail} />
                        : <SignUpForm onSubmit={onSubmit} />
                }
            </div>
        </div>
    );
}

interface FormValues {
    email: string,
    password: string,
}

interface SignUpFormProps {
    onSubmit: (email: string) => void,
}

function SignUpForm({ onSubmit }: SignUpFormProps) {
    const [submitting, setSubmitting] = useState(false);

    const onFinish = (values: FormValues) => {
        setSubmitting(true);
        axios.post("/signup", {
            email: values.email,
            password: values.password,
        })
        .then(() => {
            setSubmitting(false);
            onSubmit(values.email);
        })
        .catch((error: AxiosError<{error: string}>) => {
            setSubmitting(false);
            notification.error({
                message: error.response?.data.error,
                placement: "bottomRight",
            })
        });
    };

    return (
        <>
            <div className={styles.header}>
                <h4 className={styles.headerTitle}>Sign up</h4>
                <a href="/signin" className={styles.headerAlt}>Login instead?</a>
            </div>
            <Form
                name="signup_form"
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
                <Form.Item
                    name="password"
                    rules={[{ required: true, message: "Please input your password" }]}
                    className={styles.formItem}
                >
                    <Input
                        type="password"
                        placeholder="Password"
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
                        placeholder="Re-enter password"
                    />
                </Form.Item>
                <Form.Item className={styles.formItem}>
                    <Button
                        type="primary"
                        htmlType="submit"
                        className={styles.formButton}
                        size="large"
                        disabled={submitting}>

                        {submitting ? (<div> <LoadingOutlined spin /> Signing Up... </div>) : "Sign Up"}
                    </Button>
                </Form.Item>
            </Form>
        </>
    );
}

function ConfirmEmail(props: {email: string}) {
    return (
        <div className={styles.confirmEmailContainer}>
            <CheckCircleOutlined
                style={{ fontSize:"24px", color: "#52C41A", marginTop: "4px"}}
            />
            <div className={styles.confirmEmailContent}>
                <h4 className={styles.headerTitle}>
                    You’re on your way! Please confirm your email.
                </h4>
                <p>
                    An email to confirm your email address has been sent. Please click the link to confirm it and sign in.
                </p>
                <p className={styles.missEmail}>
                    Didn’t get an email?
                </p>
                <ResendButton {...props} />
            </div>
        </div>
    )
}

function ResendButton({email}: {email: string}) {
    const [resent, setResent] = useState(false);

    const onFinish = () => {
        setResent(true);
        axios.post("/resend_confirmation_email", {
            email: email,
        })
        .then(() => {
            notification.success({
                message: "Confirmation email resent",
                description: "Please check your email.",
                placement: "bottomRight",
                onClose: () => {
                    setResent(false);
                },
            });
        })
        .catch((error: AxiosError<{error: string}>) => {
            notification.error({
                message: error.response?.data.error,
                placement: "bottomRight",
                onClose: () => {
                    setResent(false);
                },
            });
        });
    };
    return (
        <Form
            name="resend_confirmation_form"
            onFinish={onFinish}
        >
            <Button
                type="primary"
                htmlType="submit"
                className={styles.resendButton}
                size="large"
                disabled={resent}>

                Resend email
            </Button>
        </Form>
    );
}

export default SignUp;
