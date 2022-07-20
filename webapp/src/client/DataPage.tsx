import { Tabs, Table } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import styles from "./styles/DataPage.module.scss";

function DataPage(){
    return (
        <div className={styles.container}>
            <h4>Data</h4>
            <Tabs defaultActiveKey="profiles" className={styles.tabs}>
                <Tabs.TabPane tab="Profiles" key="profiles">
                    <ProfilesTab />
                </Tabs.TabPane>
                <Tabs.TabPane tab="Actions" disabled key="actions" />
                <Tabs.TabPane tab="Features" disabled key="features" />
            </Tabs>
        </div>
    );
}

function ProfilesTab() {
    const dataSource = [
        {
            key: '1-genre',
            otype: 'movie',
            oid: 1,
            key_col: 'genre',
            last_updated: 1652296764,
            value: "Adventure|Animation|Children",
        },
        {
            key: '1-movie_title',
            otype: 'movie',
            oid: 1,
            key_col: 'movie_title',
            last_updated: 1652296764,
            value: "Toy Story"
        },
        {
            key: '1-release_year',
            otype: 'movie',
            oid: 1,
            key_col: 'release_year',
            last_updated: 1652296764,
            value: "1995",
        },
    ];
    const columns = [
        {
            title: 'otype',
            dataIndex: 'otype',
            key: 'otype',
        },
        {
            title: 'oid',
            dataIndex: 'oid',
            key: 'oid',
        },
        {
            title: 'key',
            dataIndex: 'key_col',
            key: 'key_col',
        },
        {
            title: "last_updated",
            dataIndex: 'last_updated',
            key: 'last_updated',
        },
        {
            title: "value",
            dataIndex: 'value',
            key: 'value',
        },
    ];
    const antIcon = <LoadingOutlined spin />;

    return (
        <div className={styles.profiles}>
            <Table dataSource={dataSource} columns={columns} loading={{"indicator": antIcon}} />
        </div>
    );
}

export default DataPage;
