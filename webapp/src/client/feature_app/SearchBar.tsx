import { SearchOutlined, CloseOutlined } from "@ant-design/icons"
import React, { useState } from "react";

import styles from "./styles/SearchBar.module.scss";

interface FilterOption {
    type: string,
    value: string,
}

interface Props extends React.HTMLAttributes<HTMLDivElement> {
    filterOptions: FilterOption[],
    placeholder?: string,
    initialValue?: string,
}

function SearchBar(props: Props): JSX.Element {
    const [selectedFilters, setSelectedFilters] = useState<FilterOption[]>([{ type: "tag", value: "production" }, { type: "tag", value: "WIP" }]);
    const [value, setValue] = useState<string | undefined>(props.initialValue);

    const onSelectFilter = (f: FilterOption) => setSelectedFilters([...selectedFilters, f]);
    const onUnselect = (unf: FilterOption) => setSelectedFilters(selectedFilters.filter(f => f.type !== unf.type || f.value !== unf.value));

    const onChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setValue(e.target.value);
    };

    return (
        <div className={props.className}>
            <span className="ant-input-affix-wrapper">
                <span className={styles.prefixContainer}>
                    <SearchOutlined />
                    {selectedFilters.map(f => (
                        <SelectedFilter
                            key={`${f.type}:${f.value}`}
                            onUnselect={onUnselect}
                            {...f}
                        />
                    ))}
                </span>
                <span className={styles.inputContainer}>
                    <input
                        type="text"
                        value={value}
                        placeholder={props.placeholder}
                        onChange={onChange}
                    />
                    <div className={styles.inputSuggestions}>
                        {props.filterOptions.map(f => (
                            <div key={`${f.type}:${f.value}`} className={styles.suggestion} onClick={() => onSelectFilter(f)}>
                                {f.type}: {f.value}
                            </div>
                        ))}
                    </div>
                </span>
            </span>
        </div>
    );
}

type SelectedFilterProps = FilterOption & {
    onUnselect: (f: FilterOption) => void,
};

function SelectedFilter({ type, value, onUnselect } : SelectedFilterProps): JSX.Element {
    const name = `${type}: ${value}`;

    return (
        <span className={styles.selectedFilter}>
            <span>{name}</span>
            <CloseOutlined
                size={6}
                onClick={() => onUnselect({ type, value })}
                className={styles.unselectIcon}
            />
        </span>
    );
}

export default SearchBar;
