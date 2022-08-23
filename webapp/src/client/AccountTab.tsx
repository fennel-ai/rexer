import { Avatar, Input } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import axios, { AxiosResponse } from "axios";
import { useState, useEffect } from "react";

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

    useEffect(queryUser, []);

    if (loading) {
        return (<LoadingOutlined spin />);
    }
    if (!user) {
        return (<div />);
    }

    return (
        <table className={styles.table}>
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
                    />
                </td>
            </tr>
            <tr>
                <td>Last Name</td>
                <td>
                    <Input
                        value={user.lastName}
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
