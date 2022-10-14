import { Table, Badge, Space, notification } from "antd";
import { generatePath } from "react-router-dom";
import type { ColumnsType } from 'antd/es/table';
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";

import styles from "./styles/TierManagement.module.scss";
import commonStyles from "./styles/Page.module.scss";

const columns: ColumnsType<Tier> = [
    {
        title: "Name",
        key: "name",
        dataIndex: "id",
        render: (id: string) => `Tier ${id}`,
    },
    { title: "Plan", dataIndex: "plan", key: "plan" },
    { title: "Region", dataIndex: "location", key: "region" },
    { title: "URL", dataIndex: "apiUrl", key: "url" },
    {
        title: "Status",
        key: "status",
        render: () => (
            <span>
                <Badge status="success" />
                Online
            </span>
        ),
    },
    {
        title: "Actions",
        key: "actions",
        render: (_, row: Tier) => (
            <Space>
                <a onClick={async () => {
                    await navigator.clipboard.writeText(row.apiUrl);
                    notification.success({
                        message: "The URL has been successfully copied to your clipboard.",
                        placement: "bottomRight",
                    })
                }}>
                    Copy URL
                </a>
                <a href={generatePath("/tier/:tierID", {tierID: row.id})}>Open</a>
            </Space>
        ),
    },
];

interface TiersResponse {
    "tiers": Tier[],
}

interface Tier {
    apiUrl:   string,
    limit:    number,
    location: string,
    id: string,
}

function TierManagementPage() {
    const [loading, setLoading] = useState(false);
    const [dataSource, setDataSource]  = useState<Tier[]>([]);

    const queryTiers = () => {
        setLoading(true);

        axios.get("/tiers")
            .then((response: AxiosResponse<TiersResponse>) => {
                setLoading(false);
                setDataSource(response.data.tiers.map((tier, i) => ({
                    key: i,
                    ...tier,
                })));
            })
            .catch(() => {
                // TODO(xiao) error handling
                setLoading(false);
            });
    };
    useEffect(queryTiers, []);

    return (
        <div className={commonStyles.container}>
            <h4 className={commonStyles.title}>Tier Management</h4>

            <Table
                bordered
                dataSource={dataSource}
                columns={columns}
                loading={loading && {"indicator": <LoadingOutlined spin />}}
                pagination={false}
                className={styles.table}
            />
        </div>
    );
}

export default TierManagementPage;
