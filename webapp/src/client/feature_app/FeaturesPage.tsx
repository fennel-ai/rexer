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

function FeaturesPage(): JSX.Element {
    const [features, setFeatures] = useState<Feature[]>([]);
    const [filterOptions, setFilterOptions] = useState<FilterOption[]>([]);

    const queryFeatures = (filters: FilterOption[], listFilterOptions: boolean) => {
        axios.post("/features", {
            filters,
            listFilterOptions,
        }).then((response: AxiosResponse<{features: Feature[], filterOptions?: FilterOption[]}>) => {
            setFeatures(response.data.features);
            if (listFilterOptions && response.data.filterOptions) {
                setFilterOptions(response.data.filterOptions);
            }
        });
    };

    useEffect(() => {
        queryFeatures([], true);
        document.title = "Fennel | Features";
    }, []);

    return (
        <div className={commonStyles.container}>
            <div>
                <h4 className={commonStyles.title}>Features</h4>
            </div>
            <SearchBar
                className={styles.search}
                placeholder="Search for a feature"
                filterOptions={filterOptions}
                onFilterChange={(filters) => queryFeatures(filters, false)}
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
    const navigateToDetail = () => {
        window.location.assign("/feature/101"); // TODO(xiao)
    };
    return (
        <div className={commonStyles.listItem} onClick={navigateToDetail}>
            <div className={commonStyles.listItemLhs}>
                <ProfileOutlined size={18} />
                <span>{feature.name}</span>
            </div>
            <RightOutlined size={10} />
        </div>
    );
}

export default FeaturesPage;
