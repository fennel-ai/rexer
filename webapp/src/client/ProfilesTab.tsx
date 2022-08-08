import { Table, Button, Input, Form, Space} from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios from "axios";
import styles from "./styles/ProfilesTab.module.scss";

function ProfilesTab() {
    const [dataSource, setDataSource] = useState([]);
    const [loading, setLoading] = useState(false);
    useEffect(() => {
        setLoading(true);
        axios.get("/profiles")
            .then((response) => {
                if (response.status === 200) {
                    setDataSource(response.data.profiles.map((profile: object, idx: number) => ({
                        key: idx,
                        ...profile,
                    })));
                }
                setLoading(false);
            })
            .catch((err) => {
                console.log(err);
            });
    }, []);
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
    const onQuery = (otype: string, oid: string) => {
        console.log(otype, oid);
    };

    return (
        <div className={styles.container}>
            <Filters onQuery={onQuery} />
            <Table dataSource={dataSource} columns={columns} loading={loading && {"indicator": antIcon}} />
        </div>
    );
}

interface FiltersProps {
    onQuery: (otype: string, oid: string) => void,
}

function Filters(props: FiltersProps) {
    const [otype, setOtype] = useState("");
    const [oid, setOid] = useState("");
    const onReset = () => {
        setOtype("");
        setOid("");
    };

    return (
        <div className={styles.filtersContainer}>
            <Form.Item label="otype" className={styles.filter}>
                <Input
                    placeholder="Enter value"
                    value={otype}
                    onChange={(e) => setOtype(e.target.value)}
                />
            </Form.Item>
            <Form.Item label="oid" className={styles.filter}>
                <Input
                    placeholder="Enter value"
                    value={oid}
                    onChange={(e) => setOid(e.target.value)}
                />
            </Form.Item>
            <Space size="small" align="start">
                <Button
                    onClick={onReset}>
                    Reset
                </Button>
                <Button
                    type="primary"
                    onClick={() => props.onQuery(otype, oid)}>
                    Query
                </Button>
            </Space>
        </div>
    );
}

export default ProfilesTab;
