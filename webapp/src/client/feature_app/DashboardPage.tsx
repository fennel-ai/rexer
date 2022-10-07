import { List } from "antd";
import React from "react";

import SearchBar from "./SearchBar";
import pageStyles from "./styles/Page.module.scss";
import styles from "./styles/DashboardPage.module.scss";

function DashboardPage(): JSX.Element {
    return (
        <div className={pageStyles.container}>
            <div>
                <h4 className={styles.title}>Dashboard</h4>
            </div>
            <SearchBar
                className={styles.search}
                placeholder="Search for a feature"
                filterOptions={[{ type: "tag", value: "awesome" }, { type: "tag", value: "great" }, { type: "name", value: "foo" }, { type: "name", value: "bar" }]}
            />
            <FeatureList />
        </div>
    );
}

function FeatureList(): JSX.Element {
    const data = [
        "Feature 1",
        "Feature 2",
        "Feature 3",
        "Feature 4",
    ];
    return (
        <List
            className={styles.featureList}
            dataSource={data}
            renderItem={item => (
                <List.Item>
                    {item}
                </List.Item>
            )}
        />
    );
}

export default DashboardPage;
