import { Avatar, Input, Modal, Form, notification } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import axios, { AxiosError, AxiosResponse } from "axios";
import { useState, useEffect, ChangeEvent, MouseEvent } from "react";

import styles from "./styles/AccountTab.module.scss";

interface User {
    email: string,
    firstName: string,
    lastName: string,
}

interface UserResponse {
    user: User,
}

function AccountTab() {
    const [user, setUser] = useState<User>();
    const [dirty, setDirty] = useState(false);
    const [loading, setLoading] = useState(false);
    const [showModal, setShowModal] = useState(false);

    const queryUser = () => {
        setLoading(true);
        axios.get("/user")
            .then((response: AxiosResponse<UserResponse>) => {
                setUser(response.data.user);
                setLoading(false);
            })
            .catch(() => {
                // TODO(xiao) error handling
                setLoading(false);
            });
    };
    const mutateUserNames = () => {
        if (!dirty) return;

        axios.patch("/user_names", {
            firstName: user?.firstName,
            lastName: user?.lastName,
        }).then(() => {
            notification.success({
                message: "Your name has been updated!",
                placement: "bottomRight",
            });
            setDirty(false);
        }).catch((e: AxiosError<{error: string}>) => {
            notification.error({
                message: e.response?.data.error || "Something went wrong",
                placement: "bottomRight",
            });
        });
    };

    useEffect(queryUser, []);

    if (loading) {
        return (<LoadingOutlined spin />);
    }
    if (!user) {
        return (<div />);
    }

    return (
        <div>
            <table className={styles.table}>
                <tbody>
                    <tr>
                        <td>Picture</td>
                        <td>
                            <Avatar shape="square" size={86}>
                                <div className={styles.avatarInitial}>
                                    {nameInitial(user)}
                                </div>
                            </Avatar>
                        </td>
                    </tr>
                    <tr>
                        <td>First Name</td>
                        <td>
                            <Input
                                value={user.firstName}
                                onChange={(e: ChangeEvent<HTMLInputElement>) => {
                                    setUser({
                                        ...user,
                                        firstName: e.target.value,
                                    });
                                    setDirty(true);
                                }}
                                onBlur={mutateUserNames}
                            />
                        </td>
                    </tr>
                    <tr>
                        <td>Last Name</td>
                        <td>
                            <Input
                                value={user.lastName}
                                onChange={(e: ChangeEvent<HTMLInputElement>) => {
                                    setUser({
                                        ...user,
                                        lastName: e.target.value,
                                    });
                                    setDirty(true);
                                }}
                                onBlur={mutateUserNames}
                            />
                        </td>
                    </tr>
                    <tr>
                        <td>Email</td>
                        <td>
                            <Input
                                value={user.email}
                                disabled
                            />
                        </td>
                    </tr>
                    <tr>
                        <td>Password</td>
                        <td>
                            <Input
                                type="password"
                                value="uselesspassword"
                                onClick={(e: MouseEvent<HTMLInputElement>) => {
                                    e.currentTarget.blur();
                                    setShowModal(true);
                                }}
                            />
                        </td>
                    </tr>
                </tbody>
            </table>
            {showModal ? (<UpdatePwdModal hideModal={() => setShowModal(false)} />) : null}
        </div>
    );
}

function UpdatePwdModal({hideModal}: {hideModal: () => void}) {
    const [currentPassword, setCurrentPassword] = useState("");
    const [newPassword, setNewPassword] = useState("");
    const [confirmNewPassword, setConfirmNewPassword] = useState("");

    const mutateUserPwd = () => {
        axios.patch("/user_password", {
            currentPassword,
            newPassword,
        }).then(() => {
            notification.success({
                message: "Your password has been updated!",
                placement: "bottomRight",
            });
            hideModal();
        }).catch((e: AxiosError<{error: string}>) => {
            notification.error({
                message: e.response?.data.error || "Something went wrong",
                placement: "bottomRight",
            });
        });
    };

    return (
        <Modal
            visible={true}
            title="Update password"
            okText="Update"
            onOk={mutateUserPwd}
            onCancel={hideModal}>

            <Form name="updatePwdForm">
                <table className={styles.pwdTable}>
                    <tbody>
                        <tr>
                            <td>
                                <label htmlFor="account-tab-current-password">
                                    Current password
                                </label>
                            </td>
                            <td>
                                <Form.Item
                                    name="currentPassword"
                                    rules={[{ required: true, message: "Please input your current password" }]}
                                    className={styles.pwdModalItem}
                                >
                                    <Input
                                        id="account-tab-current-password"
                                        type="password"
                                        placeholder="Current password"
                                        autoComplete="off"
                                        value={currentPassword}
                                        onChange={(e: ChangeEvent<HTMLInputElement>) => setCurrentPassword(e.target.value)}
                                    />
                                </Form.Item>
                            </td>
                        </tr>
                    </tbody>
                    <tbody>
                        <tr className={styles.pwdModalDividerTr}>
                            <td colSpan={2}>
                                <div className={styles.pwdModalDivider} />
                            </td>
                        </tr>
                    </tbody>
                    <tbody>
                        <tr>
                            <td>
                                <label htmlFor="account-tab-new-password">
                                    New password
                                </label>
                            </td>
                            <td>
                                <Form.Item
                                    name="newPassword"
                                    rules={[{ required: true, message: "Please input your new password" }]}
                                    className={styles.pwdModalItem}
                                >
                                    <Input
                                        id="account-tab-new-password"
                                        type="password"
                                        placeholder="New password"
                                        autoComplete="off"
                                        value={newPassword}
                                        onChange={(e: ChangeEvent<HTMLInputElement>) => setNewPassword(e.target.value)}
                                    />
                                </Form.Item>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label htmlFor="account-tab-confirm-new-password">
                                    Confirm new password
                                </label>
                            </td>
                            <td>
                                <Form.Item
                                    name="confirmNewPassword"
                                    rules={[
                                        { required: true, message: "Please re-entere your new password" },
                                        ({ getFieldValue }) => ({
                                            validator(_, value) {
                                                if (!value || getFieldValue("newPassword") === value) {
                                                    return Promise.resolve();
                                                }
                                                return Promise.reject(new Error('The two passwords do not match'));
                                            },
                                        }),
                                    ]}
                                    className={styles.pwdModalItem}
                                >
                                    <Input
                                        type="password"
                                        id="account-tab-confirm-new-password"
                                        placeholder="Confirm new password"
                                        autoComplete="off"
                                        value={confirmNewPassword}
                                        onChange={(e: ChangeEvent<HTMLInputElement>) => setConfirmNewPassword(e.target.value)}
                                    />
                                </Form.Item>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </Form>
        </Modal>
    );
}

function nameInitial(user: User): string {
    if (user.firstName) {
        return user.firstName[0];
    }
    return " ";
}

export default AccountTab;
