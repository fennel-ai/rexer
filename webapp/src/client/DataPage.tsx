import { Tabs } from "antd";
import { useState, useEffect } from "react";

import styles from "./styles/Page.module.scss";
import ProfilesTab from "./ProfilesTab";
import ActionsTab from "./ActionsTab";
import FeaturesTab from "./FeaturesTab";

function tabFromHash(): string {
    return window.location.hash && window.location.hash.substring(1);
}

function DataPage() {
    const [activeTab, setActiveTab] = useState<string>(tabFromHash() || "profiles");
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
            <h4 className={styles.title}>Data</h4>
            <Tabs activeKey={activeTab} onTabClick={onTabClick}>
                <Tabs.TabPane tab="Profiles" key="profiles">
                    <div className={styles.tabContent}>
                        <ProfilesTab />
                    </div>
                </Tabs.TabPane>
                <Tabs.TabPane tab="Actions" key="actions">
                    <div className={styles.tabContent}>
                        <ActionsTab />
                    </div>
                </Tabs.TabPane>
                <Tabs.TabPane tab="Features" key="features">
                    <div className={styles.tabContent}>
                        <FeaturesTab />
                    </div>
                </Tabs.TabPane>
            </Tabs>
        </div>
    );
}

export default DataPage;
