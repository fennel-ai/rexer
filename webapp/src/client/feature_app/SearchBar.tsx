import { SearchOutlined, CloseOutlined } from "@ant-design/icons"
import { useState } from "react";

import styles from "./styles/SearchBar.module.scss";

interface FilterOption {
    type: string,
    value: string,
}

interface Props extends React.HTMLAttributes<HTMLDivElement> {
    filterOptions: FilterOption[],
    placeholder?: string,
}

function SelectedFilter({ type, value } : FilterOption): JSX.Element {
    const name = `${type}: ${value}`;

    return (
        <span className={styles.selectedFilter}>
            {name}
            <CloseOutlined />
        </span>
    );
}

function SearchBar(props: Props): JSX.Element {
    const [selectedFilters] = useState<FilterOption[]>([{ type: "tag", value: "production" }, { type: "tag", value: "WIP" }]);

    return (
        <div className={props.className}>
            <span className="ant-input-affix-wrapper">
                <span className={styles.prefixContainer}>
                    <SearchOutlined />
                    {selectedFilters.map(f => (<SelectedFilter {...f} key={`${f.type}:${f.value}`} />))}
                </span>
                <span className={styles.inputContainer}>
                    <input type="text" value="userliked" placeholder={props.placeholder} />
                    <div className={styles.inputSuggestions}>
                        {props.filterOptions.map(f => (<div key={`${f.type}:${f.value}`}>{f.type}: {f.value}</div>))}
                    </div>
                </span>
            </span>

        </div>
    );
}

export default SearchBar;
