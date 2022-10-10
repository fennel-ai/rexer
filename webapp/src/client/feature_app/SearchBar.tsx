import classnames from "classnames";
import { SearchOutlined, CloseOutlined } from "@ant-design/icons"
import React, { useState } from "react";

import styles from "./styles/SearchBar.module.scss";

export interface FilterOption {
    type: string,
    value: string,
}

interface Props extends React.HTMLAttributes<HTMLDivElement> {
    placeholder?: string,
    filterOptions: FilterOption[],
    onFilterChange: (filters: FilterOption[]) => void,
}

function SearchBar(props: Props): JSX.Element {
    const [selectedFilters, setSelectedFilters] = useState<FilterOption[]>([]);
    const [value, setValue] = useState<string | undefined>();
    const [focused, setFocused] = useState<boolean>(false);

    const onSelectFilter = (f: FilterOption) => {
        const filters = [...selectedFilters, f];
        setSelectedFilters(filters);
        setValue("");
        props.onFilterChange(filters)
    }
    const onUnselect = (unf: FilterOption) => {
        const filters = selectedFilters.filter(f => f.type !== unf.type || f.value !== unf.value);
        setSelectedFilters(filters);
        props.onFilterChange(filters)
    }

    const onTextChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setValue(e.target.value);
    };

    return (
        <div className={props.className}>
            <span className={classnames("ant-input-affix-wrapper", styles.container)}>
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
                        onChange={onTextChange}
                        onBlur={() => setFocused(false)}
                        onFocus={() => setFocused(true)}
                        onKeyDown={e => {
                            if (e.key === "Backspace" && !value && selectedFilters.length > 0) {
                                onUnselect(selectedFilters[selectedFilters.length - 1]);
                            }
                        }}
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
        filters = filters.filter(f => f.value.startsWith(text.trimStart()));
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
