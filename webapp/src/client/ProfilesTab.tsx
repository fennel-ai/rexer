import { Table } from "antd";
import { LoadingOutlined } from '@ant-design/icons';

function ProfilesTab() {
    const dataSource = [
        {
            key: '1-genre',
            otype: 'movie',
            oid: 1,
            key_col: 'genre',
            last_updated: 1652296764,
            value: "Adventure|Animation|Children",
        },
        {
            key: '1-movie_title',
            otype: 'movie',
            oid: 1,
            key_col: 'movie_title',
            last_updated: 1652296764,
            value: "Toy Story"
        },
        {
            key: '1-release_year',
            otype: 'movie',
            oid: 1,
            key_col: 'release_year',
            last_updated: 1652296764,
            value: "1995",
        },
    ];
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
        <Table dataSource={dataSource} columns={columns} loading={{"indicator": antIcon}} />
    );
}

export default ProfilesTab;
