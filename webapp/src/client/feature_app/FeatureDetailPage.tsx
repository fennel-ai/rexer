import { LoadingOutlined, PicLeftOutlined, BranchesOutlined, DownOutlined } from "@ant-design/icons";
import { Dropdown, Menu, Space, Collapse } from "antd";
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { atomOneLight } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import python from 'react-syntax-highlighter/dist/esm/languages/hljs/python';
import { useEffect, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import axios, { AxiosResponse } from "axios";

import { featureDetailAjaxPath, featureDetailPagePath } from "./route";
import styles from "./styles/FeatureDetailPage.module.scss";
import commonStyles from "./styles/Page.module.scss";


SyntaxHighlighter.registerLanguage("python", python);

interface Feature {
    id: string,
    name: string,
    version: number,
    latestVersion: number,
    tags: string[],
    code: string,
    aggregates: Aggregate[],
}

interface Aggregate {
    id: string,
    name: string,
}

function FeatureDetailPage(): JSX.Element {
    // TODO(xiao): change the icon and tags
    const { tierID, featureID } = useParams();
    const [searchParams] = useSearchParams();
    const version = searchParams.get("version");

    const [feature, setFeature] = useState<Feature>();
    const [loading, setLoading] = useState<boolean>();

    const queryFeature = () => {
        if (!tierID || !featureID) {
            return;
        }
        setLoading(true);
        axios.get(featureDetailAjaxPath({ tierID, featureID, version }))
            .then((response: AxiosResponse<{feature: Feature}>) => {
                setFeature(response.data.feature);
                setLoading(false);
            });
    };
    useEffect(() => {
        document.title = "Fennel | Features";
    }, []);
    useEffect(queryFeature, [tierID, featureID, version]);
    if (loading || !feature) {
        return <LoadingOutlined />;
    }

    return (
        <div className={commonStyles.container}>
            <div>
                <div className={styles.titleLhs}>
                    <PicLeftOutlined size={24} className={styles.titleIcon} />
                    <h4 className={commonStyles.title}>{feature.name}</h4>
                    <span className={styles.tags}>
                        {feature.tags.map((tag) => (
                            <span className={styles.tag} key={tag}>{tag}</span>
                        ))}
                    </span>
                </div>
            </div>

            <table className={styles.subtitle}>
                <tbody>
                    <tr>
                        <td>Version</td>
                        <td>
                            <VersionDropdown latest={feature.latestVersion} current={feature.version} />
                        </td>
                    </tr>
                    <tr>
                        <td>Aggregates</td>
                        <td>
                            <AggregateList aggregates={feature.aggregates} />
                        </td>
                    </tr>
                </tbody>
            </table>
            <Collapse defaultActiveKey="code">
                <Collapse.Panel header="Code" key="code">
                    <SyntaxHighlighter language="python" style={atomOneLight}>
                        {feature.code}
                    </SyntaxHighlighter>
                </Collapse.Panel>
            </Collapse>
        </div>
    );
}

function VersionDropdown({ latest, current }: { latest: number, current: number }): JSX.Element {
    const { tierID, featureID } = useParams();
    const items = [];
    if (tierID && featureID) {
        for (let i = latest; i >= 1; i--) {
            items.push({
                key: i.toString(),
                label: (<a href={featureDetailPagePath({ tierID, featureID, version: i })}>{i}</a>),
            });
        }
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

function AggregateList({ aggregates }: { aggregates: Aggregate[] }): JSX.Element {
    // TODO(xiao): load more

    return (
        <div className={styles.aggregateList}>
            { aggregates.map(agg => (
                <Space key={agg.id} size={8} align="center">
                    <BranchesOutlined />
                    <span>{agg.name}</span>
                </Space>
            ))}
        </div>
    );
}

export default FeatureDetailPage;
