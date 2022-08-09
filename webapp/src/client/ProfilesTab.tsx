import { Table, Button, Input, Form, Space, Pagination} from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";
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
        dataIndex: 'keyCol',
        key: 'keyCol',
    },
    {
        title: "updatedTime",
        dataIndex: 'updatedTime',
        key: 'updatedTime',
    },
    {
        title: "value",
        dataIndex: 'value',
        key: 'value',
    },
];

interface ProfileResponse {
    profiles: Array<Profile>,
}

interface Profile {
    OType: string,
    Oid: string,
    Key: string,
    Value: string,
    UpdateTime: number,
}

function ProfilesTab() {
    const [dataSource, setDataSource] = useState<object[]>([]);
    const [loading, setLoading] = useState<boolean>(false);
    const [otype, setOtype] = useState<string>("");
    const [oid, setOid] = useState<string>("");
    const [page, setPage] = useState<number>(1);
    const [per, setPer] = useState<number>(10);

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
        }).then((response: AxiosResponse<ProfileResponse>) => {
                if (response.status === 200) {
                    const newData = response.data.profiles.map((profile: Profile, idx: number) => ({
                        key: idx,
                        otype: profile.OType,
                        oid: profile.Oid,
                        keyCol: profile.Key,
                        updatedTime: profile.UpdateTime,
                    }));
                    setDataSource(newData);
                }
                setLoading(false);
            })
            .catch(() => {
                // TODO(xiao) error handling
                setLoading(false);
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
                onQuery={() => {
                    setPage(1); // reset to page 1
                    queryProfiles(1, per);
                }}
                buttonDisabled={loading}
            />
            <Table
                bordered
                dataSource={dataSource}
                columns={columns}
                loading={loading && {"indicator": antIcon}}
                pagination={false}
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
                    onPressEnter={onQuery}
                />
            </Form.Item>
            <Form.Item label="oid" className={styles.filter}>
                <Input
                    placeholder="Enter value"
                    value={oid}
                    onChange={(e) => onOidChange(e.target.value)}
                    onPressEnter={onQuery}
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
