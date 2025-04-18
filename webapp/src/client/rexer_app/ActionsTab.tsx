import { Table, Button, Input, Form, Space } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";
import { useParams } from "react-router-dom";

import styles from "./styles/Tab.module.scss";

const columns = [
    { title: "Action Type", dataIndex: "ActionType", key: "actionType" },
    { title: "Action ID", dataIndex: "ActionID", key: "actionId" },
    { title: "Actor Type", dataIndex: "ActorType", key: "actorType" },
    { title: "Actor ID", dataIndex: "ActorID", key: "actorId" },
    { title: "Target ID", dataIndex: "TargetID", key: "targetId" },
    { title: "Request ID", dataIndex: "RequestID", key: "requestId" },
    { title: "Metadata", dataIndex: "Metadata", key: "metadata" },
    { title: "Timestamp", dataIndex: "Timestamp", key: "timestamp" },
];

interface Action {
	ActionID: string,
	ActorID: string,
	ActorType: string,
	TargetID: string,
	TargetType: string,
	ActionType: string,
	RequestID: string,
	Metadata: string,
	Timestamp: number,
}

interface ActionResponse {
    actions: Action[],
}

function ActionsTab() {
    const [dataSource, setDataSource] = useState<object[]>([]);
    const [loading, setLoading] = useState<boolean>(false);
    const [actionType, setActionType] = useState<string>("");
    const [actorId, setActorId] = useState<string>("");
    const [actorType, setActorType] = useState<string>("");
    const [targetId, setTargetId] = useState<string>("");
    const [targetType, setTargetType] = useState<string>("");

    const { tierID } = useParams();

    const queryActions = () => {
        setLoading(true);
        const params = {
            action_type: actionType,
            actor_id: actorId,
            actor_type: actorType,
            target_id: targetId,
            target_type: targetType,
        };
        axios.get(`/tier/${tierID}/actions`, {
            params,
        }).then((response: AxiosResponse<ActionResponse>) => {
            const newData = response.data.actions.map((action: Action, idx: number) => ({
                ...action,
                key: idx,
                Timestamp: new Date(action.Timestamp * 1000).toISOString(),
            }));
            setDataSource(newData);
            setLoading(false);
        })
        .catch(() => {
            // TODO(xiao) error handling
            setLoading(false);
        });
    };
    useEffect(queryActions, []);

    const onReset = () => {
        setActionType("");
        setActorType("");
        setActorId("");
        setTargetType("");
        setTargetId("");
    };
    const antIcon = <LoadingOutlined spin />;

    return (
        <div className={styles.container}>
            <div className={styles.filtersContainer}>
                <Filter
                    name="Action Type"
                    value={actionType}
                    onChange={(newValue) => setActionType(newValue)}
                    onPressEnter={queryActions}
                />
                <Space size="small" align="start">
                    <Button
                        onClick={onReset}
                        disabled={loading}>
                        Reset
                    </Button>
                    <Button
                        type="primary"
                        disabled={loading}
                        onClick={queryActions}>
                        Query
                    </Button>
                </Space>
            </div>
            <Table
                bordered
                dataSource={dataSource}
                columns={columns}
                loading={loading && {"indicator": antIcon}}
                pagination={{ position: ["bottomRight"] }}
            />
        </div>
    );
}

interface FilterProp {
    name: string,
    value: string,
    onChange: (newValue: string) => void,
    onPressEnter: () => void,
}

function Filter({name, value, onChange, onPressEnter}: FilterProp) {
    return (
        <Form.Item label={name} className={styles.filter}>
            <Input
                placeholder="Enter value"
                value={value}
                onChange={(e) => onChange(e.target.value)}
                onPressEnter={onPressEnter}
            />
        </Form.Item>
    );
}

export default ActionsTab;
