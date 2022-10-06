import { List, Select } from "antd";

import pageStyles from "./styles/Page.module.scss";

function DashboardPage(): JSX.Element {
    return (
        <div className={pageStyles.container}>
            <div>
                Dashboard
            </div>
            <SearchBar />
            <FeatureList />
        </div>
    );
}

function SearchBar(): JSX.Element {
    const handleChange = (value: string[]) => {
        console.log(`selected ${value}`);
    };
    return (
        <Select
            mode="multiple"
            allowClear
            style={{ width: "100%" }}
            placeholder="Please select"
            defaultValue={["a"]}
            onChange={handleChange}
        >
            <Select.Option key="a">
                Tag: production
            </Select.Option>
            <Select.Option key="b">
                Tag: xgboost
            </Select.Option>
            <Select.Option key="c">
                Feature name: avg_user_rating
            </Select.Option>
        </Select>
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
