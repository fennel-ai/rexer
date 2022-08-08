import { Tabs } from "antd";
import styles from "./styles/DataPage.module.scss";
import ProfilesTab from "./ProfilesTab";

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
                <Tabs.TabPane tab="Actions" disabled key="actions" />
                <Tabs.TabPane tab="Features" disabled key="features" />
            </Tabs>
        </div>
    );
}

export default DataPage;
