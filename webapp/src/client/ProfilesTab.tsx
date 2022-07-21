import { Table } from "antd";
import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios from "axios";

function ProfilesTab() {
    const [dataSource, setDataSource] = useState([]);
    const [loading, setLoading] = useState(false);
    useEffect(() => {
        setLoading(true);
        axios.get("/profiles")
            .then(function (response) {
                if (response.status === 200) {
                    setDataSource(response.data.profiles.map((profile: any, idx: any) => ({
                        key: idx,
                        ...profile,
                    })));
                }
                setLoading(false);
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

    return (
        <Table dataSource={dataSource} columns={columns} loading={loading && {"indicator": antIcon}} />
    );
}

export default ProfilesTab;
