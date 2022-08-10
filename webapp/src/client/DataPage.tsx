import { Tabs } from "antd";
import styles from "./styles/DataPage.module.scss";
import ProfilesTab from "./ProfilesTab";
import ActionsTab from "./ActionsTab";

function DataPage(){
    return (
        <div className={styles.container}>
            <h4 className={styles.title}>Data</h4>
            <Tabs defaultActiveKey="profiles" className={styles.tabs}>
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
                <Tabs.TabPane tab="Features" disabled key="features" />
            </Tabs>
        </div>
    );
}

export default DataPage;
