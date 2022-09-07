import { Table } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";
import { useParams } from "react-router-dom";

import styles from "./styles/Tab.module.scss";

interface Column {
    title: string,
    dataIndex: string,
    key: string,
}

function columnsFromNames(names: string[]): Column[] {
    return names.map(name => ({
        title: name,
        dataIndex: name,
        key: name,
    }));
}

const DEFAULT_COLUMNS = columnsFromNames([
    "candidate_oid", "candidate_otype", "context_oid",
    "context_otype", "model_name", "model_prediction",
    "model_version", "workflow",
    "request_id", "timestamp",
])

function FeaturesTab() {
    const [dataSource, setDataSource] = useState<object[]>([]);
    const [loading, setLoading] = useState<boolean>(false);
    const [columns, setColumns] = useState<{title: string, dataIndex: string, key:string}[]>(
        DEFAULT_COLUMNS,
    );

    const { tierID } = useParams();

    const queryFeatures = () => {
        setLoading(true);
        axios.get(`/tier/${tierID}/features`)
            .then((response: AxiosResponse<{features: object[]}>) => {
                const newData = response.data.features.map((feature: object, idx: number) => ({
                    key: idx,
                    ...feature,
                }));
                setDataSource(newData);
                setLoading(false);
                if (newData.length > 0) {
                    const names = Object.keys(response.data.features[0]);
                    setColumns(columnsFromNames(names));
                }
            })
            .catch(() => {
                // TODO(xiao) error handling
                setLoading(false);
            });
    };

    useEffect(queryFeatures, []);

    const antIcon = <LoadingOutlined spin />;
    return (
        <div className={styles.container}>
            <Table
                bordered
                dataSource={dataSource}
                columns={columns}
                loading={loading && {"indicator": antIcon}}
                pagination={{ position: ["bottomRight"]}}
            />
        </div>
    );
}

export default FeaturesTab;
