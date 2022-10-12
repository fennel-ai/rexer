import { PicLeftOutlined, BranchesOutlined, DownOutlined } from "@ant-design/icons";
import { Dropdown, Menu, Space, Collapse } from "antd";
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { atomOneLight } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import python from 'react-syntax-highlighter/dist/esm/languages/hljs/python';

import styles from "./styles/FeatureDetailPage.module.scss";
import commonStyles from "./styles/Page.module.scss";

SyntaxHighlighter.registerLanguage("python", python);

const CODE = `
    # Given a movie, users who rated the movie 5 stars
    @rex.aggregate(
        name='users_who_liked_movie', aggregate_type='list',
        action_types=['rating'], config={'durations': [14*DAY, 7*DAY, 1*DAY]},
    )
    def users_who_liked_movie(actions):
        top_rated_events = op.std.filter(actions, var='e', where=var('e').metadata == 5.0)
        with_key = op.std.set(top_rated_events, var='e', name='groupkey', value=var('e').target_id)
        return op.std.set(with_key, var='e', name='value', value=var('e').actor_id)


    # Given a user, all the movies they have rated with 5 stars
    @rex.aggregate(
        name='top_rated_movies_by_user', aggregate_type='list',
        action_types=['rating'], config={'durations': [14*DAY, 7*DAY, 1*DAY]},
    )
    def top_rated_movies_by_user(actions):
        top_rated_events = op.std.filter(actions, var='e', where=var('e').metadata == 5.0)
        with_key = op.std.set(top_rated_events, var='e', name='groupkey', value=var('e').actor_id)
        return op.std.set(with_key, var='e', name='value', value=var('e').target_id)
`;

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
            <Collapse>
                <Collapse.Panel header="Code" key="code">
                    <SyntaxHighlighter language="python" style={atomOneLight}>
                        {CODE}
                    </SyntaxHighlighter>
                </Collapse.Panel>
            </Collapse>
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
