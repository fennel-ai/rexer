import { Table} from "antd";
import type { ColumnsType } from 'antd/es/table';

import { LoadingOutlined } from '@ant-design/icons';
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";

interface User {
    firstName: string,
    lastName: string,
    email: string,
}

interface OrgResponse {
    organization: {
        users: User[],
    },
}

interface DataType {
    key: React.Key,
    name: string,
    email: string,
}

const columns: ColumnsType<DataType> = [
    { title: "Name", dataIndex: "name", key: "name", sorter: (a, b) => (a.name < b.name ? -1 : (a.name === b.name ? 0 : 1))},
    { title: "Email", dataIndex: "email", key: "email", sorter: (a, b) => (a.email < b.email ? -1 : (a.email === b.email ? 0 : 1))},
]

function OrgTab() {
    const [loading, setLoading] = useState(false);
    const [dataSource, setDataSource] = useState<DataType[]>([]);

    const queryOrg = () => {
        setLoading(true);
        axios.get("/organization")
            .then((response: AxiosResponse<OrgResponse>) => {
                setDataSource(response.data.organization.users.map((user, idx) => ({
                    key: idx,
                    name: `${user.firstName} ${user.lastName}`,
                    email: user.email,
                })));
                setLoading(false);
            })
            .catch(() => {
                // TODO(xiao) error handling
                setLoading(false);
            });
    }
    useEffect(() => queryOrg(), []);

    return (
        <Table
            bordered
            dataSource={dataSource}
            columns={columns}
            loading={loading && {"indicator": <LoadingOutlined spin />}}
            pagination={{ position: ["bottomRight"] }}
        />
    );
}

export default OrgTab;
