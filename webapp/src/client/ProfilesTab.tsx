import { Table, Button, Input, Form, Space, Pagination} from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios from "axios";
import styles from "./styles/ProfilesTab.module.scss";

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

function ProfilesTab() {
    const [dataSource, setDataSource] = useState([]);
    const [loading, setLoading] = useState(false);
    const [otype, setOtype] = useState("");
    const [oid, setOid] = useState("");
    const [page, setPage] = useState(1);
    const [per, setPer] = useState(10);

    const queryProfiles = (page: number, per: number) => {
        setLoading(true);
        const params = {
            otype,
            oid,
            page,
            per,
        };
        axios.get("/profiles", {
            params: params,
        }).then((response) => {
                if (response.status === 200) {
                    setDataSource(response.data.profiles.map((profile: object, idx: number) => ({
                        key: idx,
                        ...profile,
                    })));
                }
                setLoading(false);
            })
            .catch(err => {
                setLoading(false);
                console.log(err);
            });
    };

    useEffect(() => queryProfiles(page, per), []);
    const antIcon = <LoadingOutlined spin />;
    return (
        <div className={styles.container}>
            <Filters
                otype={otype}
                oid={oid}
                onOidChange={(newOid) => setOid(newOid)}
                onOtypeChange={(newOtype) => setOtype(newOtype)}
                onQuery={() => queryProfiles(page, per)}
                buttonDisabled={loading}
            />
            <Table
                bordered
                dataSource={dataSource}
                columns={columns}
                loading={loading && {"indicator": antIcon}}
            />
            <Pagination
                className={styles.pagination}
                current={page}
                pageSize={per}
                onChange={(page, per) => {
                    setPage(page);
                    setPer(per);
                    queryProfiles(page, per);
                }}
                disabled={loading}
                total={100}
            />
        </div>
    );
}

interface FiltersProps {
    oid: string,
    otype: string,
    onQuery: () => void,
    onOtypeChange: (otype: string) => void,
    onOidChange: (oid: string) => void,
    buttonDisabled?: boolean,
}

function Filters(props: FiltersProps) {
    const {
        oid,
        otype,
        onQuery,
        onOtypeChange,
        onOidChange,
        buttonDisabled,
     } = props;
    const onReset = () => {
        onOidChange("");
        onOtypeChange("");
    };

    return (
        <div className={styles.filtersContainer}>
            <Form.Item label="otype" className={styles.filter}>
                <Input
                    placeholder="Enter value"
                    value={otype}
                    onChange={(e) => onOtypeChange(e.target.value)}
                />
            </Form.Item>
            <Form.Item label="oid" className={styles.filter}>
                <Input
                    placeholder="Enter value"
                    value={oid}
                    onChange={(e) => onOidChange(e.target.value)}
                />
            </Form.Item>
            <Space size="small" align="start">
                <Button
                    onClick={onReset}
                    disabled={buttonDisabled}>
                    Reset
                </Button>
                <Button
                    type="primary"
                    disabled={buttonDisabled}
                    onClick={onQuery}>
                    Query
                </Button>
            </Space>
        </div>
    );
}

export default ProfilesTab;
