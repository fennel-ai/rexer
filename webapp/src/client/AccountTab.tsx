import { Avatar, Input, notification } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import axios, { AxiosError, AxiosResponse } from "axios";
import { useState, useEffect } from "react";

import styles from "./styles/AccountTab.module.scss";
import * as React from "react";

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
    const [dirty, setDirty] = useState<boolean>(false);
    const [loading, setLoading] = useState<boolean>(false);

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
                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
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
                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
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
                    <td>Work Email</td>
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
                        />
                    </td>
                </tr>
            </tbody>
        </table>
    );
}

function nameInitial(user: User): string {
    if (user.lastName) {
        return user.lastName[0];
    }
    return " ";
}

export default AccountTab;
