import { Tabs } from "antd";
import { useState, useEffect } from "react";

import AccountTab from "./AccountTab";
import OrgTab from "./OrgTab";
import styles from "./styles/Page.module.scss";

function tabFromHash(): string {
    return window.location.hash && window.location.hash.substring(1);
}

function SettingsPage() {
    const [activeTab, setActiveTab] = useState<string>(tabFromHash() || "account");
    const onTabClick = (key: string) => {
        window.location.hash = key;
    };
    useEffect(() => {
        const onHashChange = () => {
            setActiveTab(tabFromHash);
        };
        window.addEventListener("hashchange", onHashChange);
        return () => {
            window.addEventListener("hashchange", onHashChange);
        };
    }, []);

    return (
        <div className={styles.container}>
            <h4 className={styles.title}>Settings</h4>
            <Tabs className={styles.tabs} activeKey={activeTab} onTabClick={onTabClick}>
                <Tabs.TabPane tab="Account" key="account">
                    <div className={styles.tabContent}>
                        <AccountTab />
                    </div>
                </Tabs.TabPane>
                <Tabs.TabPane tab="Organization" key="organization">
                    <div className={styles.tabContent}>
                        <OrgTab />
                    </div>
                </Tabs.TabPane>
            </Tabs>
        </div>
    );
}

export default SettingsPage;
