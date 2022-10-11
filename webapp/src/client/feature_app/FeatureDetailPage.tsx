import { PicLeftOutlined, BranchesOutlined, DownOutlined } from "@ant-design/icons";
import { Dropdown, Menu, Space } from "antd";

import styles from "./styles/FeatureDetailPage.module.scss";
import commonStyles from "./styles/Page.module.scss";

function FeatureDetailPage(): JSX.Element {
    // TODO(xiao): change the icon and tags, real version
    // TODO(xiao): real aggregates and load more
    const aggregates = ["Aggregate 1", "Aggregate 2", "Aggregate 3", "Aggregate 4"];

    return (
        <div className={commonStyles.container}>
            <div>
                <div className={styles.titleLhs}>
                    <PicLeftOutlined size={24} className={styles.titleIcon} />
                    <h4 className={commonStyles.title}>Feature</h4>
                    <span className={styles.tags}>
                        <span className={styles.tag}>tag 1</span>
                        <span className={styles.tag}>tag 2</span>
                        <span className={styles.tag}>tag 3</span>
                    </span>
                </div>
            </div>

            <table className={styles.subtitle}>
                <tbody>
                    <tr>
                        <td>Version</td>
                        <td>
                            <VersionDropdown latest={4} current={3} />
                        </td>
                    </tr>
                    <tr>
                        <td>Aggregates</td>
                        <td>
                            <AggregateList aggregates={aggregates} />
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    );
}

function VersionDropdown({ latest, current }: { latest: number, current: number }): JSX.Element {
    const items = [];
    for (let i = latest; i >= 1; i--) {
        items.push({
            key: i.toString(),
            label: (<a href="#">{i}</a>),
        });
    }
    const menu = <Menu
        selectable
        defaultSelectedKeys={[current.toString()]}
        items={items}
    />;

    return (
        <Dropdown overlay={menu} trigger={["click"]}>
            <Space size={6} align="center">
                <span>{current}</span>
                <DownOutlined className={styles.versionDropdownIcon} />
            </Space>
        </Dropdown>
    );
}

function AggregateList({ aggregates }: { aggregates: string[] }): JSX.Element {
    // TODO(xiao): load more

    return (
        <div className={styles.aggregateList}>
            { aggregates.map(agg => (
                <Space key={agg} size={8} align="center">
                    <BranchesOutlined />
                    <span>{agg}</span>
                </Space>
            ))}
        </div>
    );
}

export default FeatureDetailPage;
