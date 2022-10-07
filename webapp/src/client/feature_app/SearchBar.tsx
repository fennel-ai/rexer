import classnames from "classnames";
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
    const [selectedFilters, setSelectedFilters] = useState<FilterOption[]>([]);
    const [value, setValue] = useState<string | undefined>(props.initialValue);
    const [focused, setFocused] = useState<boolean>(false);

    const onSelectFilter = (f: FilterOption) => {
        setSelectedFilters([...selectedFilters, f]);
        setValue("");
    }
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
                        value={value || ""}
                        placeholder={props.placeholder}
                        onChange={onChange}
                        onBlur={() => setFocused(false)}
                        onFocus={() => setFocused(true)}
                    />
                    <InputSuggestions
                        hidden={!focused}
                        allFilters={props.filterOptions}
                        selectedFilters={selectedFilters}
                        onSelectFilter={onSelectFilter}
                        text={value}
                    />
                </span>
            </span>
        </div>
    );
}

interface InputSuggestionsProps {
    hidden: boolean,
    allFilters: FilterOption[],
    selectedFilters: FilterOption[],
    text: string | undefined,
    onSelectFilter: (f: FilterOption) => void,
}

function InputSuggestions({ hidden, allFilters, text, selectedFilters, onSelectFilter }: InputSuggestionsProps): JSX.Element | null {
    let filters = allFilters.filter(f => !selectedFilters.some(sf => sf.type === f.type && sf.value === f.value));
    if (text) {
        filters = filters.filter(f => f.value.startsWith(text));
    }

    if (filters.length === 0) {
        return null;
    }

    return (
        <div className={classnames(styles.inputSuggestions, hidden && styles.hidden)}>
            {filters.map(f => (
                <div
                    key={`${f.type}:${f.value}`}
                    className={styles.suggestion}
                    onMouseDown={(e) => e.preventDefault()} // work around of the onclick, onblur triggering order
                    onClick={() => onSelectFilter(f)}>

                    {f.type}: {f.value}
                </div>
            ))}
        </div>
    );
}

type SelectedFilterProps = FilterOption & {
    onUnselect: (f: FilterOption) => void,
}

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
