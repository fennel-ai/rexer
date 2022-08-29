import { Collapse } from "antd";

import styles from "./styles/DashboardPage.module.scss";

function DashboardPage() {
    return (
        <div className={styles.container}>
            <div className={styles.titleSection}>
                <h4 className={styles.title}>Dashboard</h4>
            </div>
            <Collapse defaultActiveKey="qps">
                <Collapse.Panel header="QPS" key="qps">
                </Collapse.Panel>
                <Collapse.Panel header="Backlog" key="backlog">
                </Collapse.Panel>
                <Collapse.Panel header="Latency" key="latency">
                </Collapse.Panel>
            </Collapse>
        </div>
    );
}

export default DashboardPage;
