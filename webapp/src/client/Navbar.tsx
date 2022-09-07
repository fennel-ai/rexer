import axios, { AxiosError } from "axios";

import styles from "./styles/Navbar.module.scss";
import { MenuProps, notification, Dropdown, Menu, Space, Avatar } from "antd";
import { DownOutlined, UserOutlined, TeamOutlined, LogoutOutlined } from '@ant-design/icons';

interface Props {
    page: string;
}

function Navbar({page}: Props) {
    const items: MenuProps["items"] = [
        {
            label: (<a href="/dashboard">Dashboard</a>),
            key: "dashboard",
        },
        {
            label: (<a href="/data">Data</a>),
            key: "data",
        },
    ];

    return (
        <nav>
            <div className={styles.container}>
                <div className={styles.leftNav}>
                    <div>
                        <img src="/images/logo.svg" alt="logo" />
                    </div>
                    <div className={styles.divider} />
                    <div>
                        <TierDropdown />
                    </div>
                    <div className={styles.divider} />
                    <div>
                        <Menu
                            mode="horizontal"
                            defaultSelectedKeys={[page]}
                            items={items}
                            className={styles.menu}
                        />
                    </div>
                </div>

                <div className={styles.rightNav}>
                    <div>
                        <a href="https://app.gitbook.com/o/ezMhZP7ASmi43q12NHfL/s/5DToQ2XCuEpPMMLC0Rwr/">Documentation</a>
                    </div>
                    <div className={styles.avatar}>
                        <AvatarDropdown />
                    </div>
                </div>
            </div>
        </nav>
    );
}

function AvatarDropdown() {
    const onLogout = () => {
        axios.post("/logout")
            .then(() => {
                window.location.href = "/signin";
            })
            .catch((e: AxiosError<{error: string}>) => {
                notification.error({
                    message: e.response?.data.error,
                    placement: "bottomRight",
                });
            });
    };
    const items: MenuProps["items"] = [
        {
            icon: <UserOutlined />,
            label: (<a href="/settings#account">Account</a>),
            key: "account",
        },
        {
            icon: <TeamOutlined />,
            label: (<a href="/settings#team">Team</a>),
            key: "team",
        },
        {
            type: "divider",
        },
        {
            icon: <LogoutOutlined />,
            label: "Log out",
            key: "logout",
            onClick: onLogout,
        },
    ];
    return (
        <Dropdown overlay={<Menu items={items} />} trigger={["click"]}>
            <Avatar size={24} icon={<UserOutlined />} />
        </Dropdown>
    );
}

function TierDropdown() {
    const menu = <Menu
        selectable
        defaultSelectedKeys={['1']}
        items={[
            {
                key: '1',
                label: 'Tier 1',
            },
            {
                key: '2',
                label: 'Tier 2',
            },
            {
                key: '3',
                label: 'Tier 3',
            },
            {
                type: 'divider',
            },
            {
                key: 'management',
                label: 'Tier Management',
            },
        ]}
    />;

    return (
        <Dropdown overlay={menu} trigger={["click"]}>
            <Space>
                Tier 1
                <DownOutlined />
            </Space>
        </Dropdown>
    );
}

export default Navbar;
