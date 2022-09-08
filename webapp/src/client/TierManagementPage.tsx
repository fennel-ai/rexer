import { Table, Badge } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";

import pageStyles from "./styles/Page.module.scss";

const columns = [
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
];

interface TiersResponse {
    "tiers": Tier[],
}

interface Tier {
    apiUrl:   string,
    limit:    number,
    location: string,
}

function TierManagementPage() {
    const [loading, setLoading] = useState(false);
    const [dataSource, setDataSource]  = useState<object[]>([]);

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
        <div className={pageStyles.container}>
            <h4 className={pageStyles.title}>Tier Management</h4>

            <Table
                bordered
                dataSource={dataSource}
                columns={columns}
                loading={loading && {"indicator": <LoadingOutlined spin />}}
                pagination={false}
            />
        </div>
    );
}

export default TierManagementPage;
