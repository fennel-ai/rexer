import axios, { AxiosResponse } from "axios";
import { ProfileOutlined, RightOutlined } from "@ant-design/icons";
import { useState, useEffect } from "react";

import SearchBar, { type FilterOption } from "./SearchBar";
import commonStyles from "./styles/Page.module.scss";
import styles from "./styles/DashboardPage.module.scss";

interface Feature {
    id: string,
    name: string,
    version: number,
}

function DashboardPage(): JSX.Element {
    const [features, setFeatures] = useState<Feature[]>([]);

    const queryFeatures = (filters: FilterOption[]) => {
        axios.post("/features", {
            filters,
        }).then((response: AxiosResponse<{features: Feature[]}>) => {
            setFeatures(response.data.features);
        });
    };

    useEffect(() => queryFeatures([]), []);
    const filterOptions = [
        { type: "tag", value: "good" },
        { type: "tag", value: "ok" },
        { type: "name", value: "bad" },
        { type: "name", value: "user_avg_rating" },
        { type: "name", value: "movie_avg_rating" },
        { type: "name", value: "user_likes_last_3days"},
        { type: "name", value: "movie_likes_last_3days"},
    ];
    return (
        <div className={commonStyles.container}>
            <div>
                <h4 className={styles.title}>Dashboard</h4>
            </div>
            <SearchBar
                className={styles.search}
                placeholder="Search for a feature"
                filterOptions={filterOptions}
                onFilterChange={queryFeatures}
            />
            <FeatureList features={features} />
        </div>
    );
}

function FeatureList({ features }: { features: Feature[] }): JSX.Element {
    return (
        <div className={commonStyles.list}>
            {features.map(f => (<SingleFeature key={f.name} feature={f} />))}
        </div>
    );
}

function SingleFeature({ feature }: { feature: Feature }): JSX.Element {
    return (
        <div className={commonStyles.listItem}>
            <div className={commonStyles.listItemLhs}>
                <ProfileOutlined size={18} />
                <span>{feature.name}</span>
            </div>
            <RightOutlined size={10} />
        </div>
    );
}

export default DashboardPage;
