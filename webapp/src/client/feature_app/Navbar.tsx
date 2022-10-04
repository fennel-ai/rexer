import styles from "./styles/Navbar.module.scss";
import Logo from "../assets/logo_color.module.svg";
import { DownOutlined, UserOutlined, TeamOutlined, LogoutOutlined } from '@ant-design/icons';
import { MenuProps, Dropdown, Menu, Space, Avatar } from "antd";

export interface Tier {
    id: string,
}

interface Props {
    activeTab?: string,
}

function menuItems(tierID: string | undefined): MenuProps["items"] {
    if (!tierID) {
        return [];
    }
    const items: MenuProps["items"] = [
        {
            label: (<a href="#">Feature</a>), // TODO(xiao): real href
            key: "Feature",
        },
        {
            label: (<a href="#">Aggregate</a>), // TODO(xiao): real href
            key: "Aggregate",
        },
        {
            label: (<a href="#">Stream</a>), // TODO(xiao): real href
            key: "Stream",
        },
    ];
    return items;
}

function Navbar({ activeTab }: Props) {
    const tierID = "1001";
    const tiers = [{ id: "1001" }, { id: "1002" }]; // TODO(xiao): use real ones

    const items = menuItems(tierID);

    return (
        <nav>
            <div className={styles.container}>
                <div className={styles.leftNav}>
                    <div>
                        <a href="/">
                            <Logo className={styles.logo} />
                        </a>
                    </div>
                    <div className={styles.divider} />
                    <div>
                        <TierDropdown tiers={tiers} />
                    </div>
                    <div className={styles.divider} />
                    <div className={styles.menuContainer}>
                        <Menu
                            mode="horizontal"
                            defaultSelectedKeys={activeTab ? [activeTab] : []}
                            items={items}
                            className={styles.menu}
                        />
                    </div>
                </div>

                <div className={styles.rightNav}>
                    <a target="_blank" rel="noreferrer" className={styles.documentation} href="https://app.gitbook.com/o/ezMhZP7ASmi43q12NHfL/s/5DToQ2XCuEpPMMLC0Rwr/">Documentation</a>
                    <AvatarDropdown />
                </div>
            </div>
        </nav>
    );
}

function AvatarDropdown() {
    const user = {
        firstName: "Xiao",
    }; // TODO(xiao)
    const onLogout = () => {
        // TODO(xiao);
    };
    const itemStyle = { paddingTop: "6px", paddingBottom: "6px", paddingRight: "20px" }; // default is 12
    const items: MenuProps["items"] = [
        {
            icon: <UserOutlined />,
            label: (<a href="/settings#account">Account</a>),
            key: "account",
            style: itemStyle,
        },
        {
            icon: <TeamOutlined />,
            label: (<a href="/settings#team">Team</a>),
            key: "team",
            style: itemStyle,
        },
        {
            type: "divider",
        },
        {
            icon: <LogoutOutlined />,
            label: "Log out",
            key: "logout",
            onClick: onLogout,
            style: itemStyle,
        },
    ];
    return (
        <div className={styles.avatarContainer}>
            <Dropdown overlay={<Menu items={items} />} trigger={["click"]}>
                <Avatar style={{ width: "24px", height: "24px", lineHeight: "24px", fontSize: "12px" }}>
                    { user.firstName ? user.firstName[0] : " " }
                </Avatar>
            </Dropdown>
        </div>
    );
}

function TierDropdown({ tiers }: { tiers: Tier[] }) {
    const tierID = "1001"; // TODO(xiao): use real ones

    const items = tiers.map(tier => ({
        key: tier.id,
        label: (<a href="#">Tier {tier.id}</a>),
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
            <Space className={styles.activeTier}>
                {tierID ? `Tier ${tierID}` : "Tier Management"}
                <DownOutlined />
            </Space>
        </Dropdown>
    );
}

export default Navbar;
