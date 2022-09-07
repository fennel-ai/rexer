import axios, { AxiosError } from "axios";
import { useParams, generatePath } from "react-router-dom";

import styles from "./styles/Navbar.module.scss";
import { MenuProps, notification, Dropdown, Menu, Space, Avatar } from "antd";
import { DownOutlined, UserOutlined, TeamOutlined, LogoutOutlined } from '@ant-design/icons';

export interface Tier {
    id: string,
}

interface Props {
    activeTab?: string,
    tiers: Tier[],
}

function Navbar({ activeTab, tiers }: Props) {
    const { tierID } = useParams();

    const items: MenuProps["items"] = [];
    if (tierID) {
        items.push(
            {
                label: (<a href={generatePath("/tier/:tierID/dashboard", {tierID})}>Dashboard</a>),
                key: "dashboard",
            },
            {
                label: (<a href={generatePath("/tier/:tierID/data", {tierID})}>Data</a>),
                key: "data",
            },
        );
    }

    return (
        <nav>
            <div className={styles.container}>
                <div className={styles.leftNav}>
                    <div>
                        <img src="/images/logo.svg" alt="logo" />
                    </div>
                    <div className={styles.divider} />
                    <div>
                        <TierDropdown tiers={tiers} />
                    </div>
                    <div className={styles.divider} />
                    <div>
                        <Menu
                            mode="horizontal"
                            defaultSelectedKeys={activeTab ? [activeTab] : []}
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

function TierDropdown({ tiers }: { tiers: Tier[] }) {
    const { tierID } = useParams();
    const items = tiers.map(tier => ({
        key: tier.id,
        label: (<a href={generatePath("/tier/:tierID", {tierID: tier.id})}>Tier {tier.id}</a>),
    }));

    const menu = <Menu
        selectable
        defaultSelectedKeys={tierID ? [ tierID ] : []}
        items={[
            ...items,
            {
                type: 'divider',
            },
            {
                key: 'management',
                label: (<a href="/tier_management">Tier Management</a>),
            },
        ]}
    />;

    return (
        <Dropdown overlay={menu} trigger={["click"]}>
            <Space>
                {tierID ? `Tier ${tierID}` : "Tier Management"}
                <DownOutlined />
            </Space>
        </Dropdown>
    );
}

export default Navbar;
