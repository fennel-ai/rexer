import { Tabs } from "antd";

import AccountTab from "./AccountTab";
import OrgTab from "./OrgTab";
import styles from "./styles/Page.module.scss";

function SettingsPage() {
    return (
        <div className={styles.container}>
            <h4 className={styles.title}>Settings</h4>
            <Tabs defaultActiveKey="account" className={styles.tabs}>
                <Tabs.TabPane tab="Account" key="account">
                    <div className={styles.tabContent}>
                        <AccountTab />
                    </div>
                </Tabs.TabPane>
                <Tabs.TabPane tab="Organization" key="org">
                    <div className={styles.tabContent}>
                        <OrgTab />
                    </div>
                </Tabs.TabPane>
            </Tabs>
        </div>
    );
}

export default SettingsPage;
