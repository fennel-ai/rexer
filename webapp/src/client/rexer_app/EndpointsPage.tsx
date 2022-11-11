import { Table } from "antd";
import { LoadingOutlined } from "@ant-design/icons";
import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import commonStyles from "./styles/Page.module.scss";
import axios, { AxiosResponse } from "axios";

const columns = [
    { title: "Name", dataIndex: "name", key: "name" },
    { title: "Description", dataIndex: "description", key: "description" },
    { title: "Last Updated", dataIndex: "updatedAt", key: "updatedAt" },
];

interface StoredQuery {
    id: number,
    name: string,
    description?: string,
    timestamp: number,
}

interface QueriesResponse {
    queries: StoredQuery[],
}

function EndpointsPage() {
    const [loading, setLoading] = useState(false);
    const [dataSource, setDataSource] = useState<object[]>([]);

    const { tierID } = useParams();

    const queryEndpoints = () => {
        setLoading(true);
        axios.get(`/tier/${tierID}/stored_queries`)
            .then((response: AxiosResponse<QueriesResponse>) => {
                const data = response.data.queries.map((query: StoredQuery) => ({
                    ...query,
                    key: query.id,
                    updatedAt: new Date(query.timestamp * 1000).toISOString(),
                }));
                setDataSource(data);
                setLoading(false);
            })
            .catch(() => {
                // TODO(xiao) error handling
                setLoading(false);
            });
    }
    useEffect(queryEndpoints, []);

    return (
        <div className={commonStyles.container}>
            <h4 className={commonStyles.title}>Endpoints</h4>
            <Table
                bordered
                dataSource={dataSource}
                columns={columns}
                loading={loading && {"indicator": (<LoadingOutlined spin />)}}
                pagination={{ position: ["bottomRight"], pageSize: 15 }}
            />
        </div>
    );
}

export default EndpointsPage;
