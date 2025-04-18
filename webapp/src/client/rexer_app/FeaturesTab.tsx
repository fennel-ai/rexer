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

const COLUMN_TITLE_MAP = new Map<string, string>([
    ["candidate_oid", "Candidate oid"],
    ["candidate_otype", "Candidate otype"],
    ["context_oid", "Context oid"],
    ["context_otype", "Context otype"],
    ["model_name", "Model Name"],
    ["model_prediction", "Model Prediction"],
    ["model_version", "Model Version"],
    ["workflow", "Workflow"],
    ["request_id", "Request ID"],
    ["timestamp", "Timestamp"],
]);

function columnTitle(name: string): string {
    return COLUMN_TITLE_MAP.get(name) || name;
}

function columnsFromNames(names: string[]): Column[] {
    return names.map(name => ({
        title: columnTitle(name),
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

// full date structure in lib/feature.go FeatureRow
interface Feature {
	timestamp: number,
}

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
            .then((response: AxiosResponse<{features: Feature[]}>) => {
                const newData = response.data.features.map((feature: Feature, idx: number) => ({
                    ...feature,
                    key: idx,
                    timestamp: new Date(feature.timestamp * 1000).toISOString(),
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
