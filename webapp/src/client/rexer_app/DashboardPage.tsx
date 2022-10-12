import { Collapse, Space, DatePicker } from "antd";
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";
import { Line, LineConfig } from '@ant-design/plots';
import humanFormat from "human-format";
import { useParams, generatePath } from "react-router-dom";
import classNames from "classnames";

import commonStyles from "./styles/Page.module.scss";
import styles from "./styles/DashboardPage.module.scss";

const DAY_MS = 24 * 3600 * 1000;
const WEEK_MS = DAY_MS * 7;
const MONTH_MS = DAY_MS * 30;

function DashboardPage() {
    const now = Date.now();
    const [startTime, setStartTime] = useState<number>(now - DAY_MS);
    const [endTime, setEndTime] = useState<number>(now);
    const [step, setStep] = useState<string>("1h");
    const [selected, setSelected] = useState<string>("Last day");

    const DateControlLink = ({ duration, step, text }: { duration: number, step: string, text: string }) => {
        return (
            <a
                className={classNames(styles.dateControl, (selected === text) && styles.selected)}
                onClick={e => {
                    e.preventDefault();
                    setStartTime(now - duration);
                    setEndTime(now);
                    setStep(step);
                    setSelected(text);
                }}>
                {text}
            </a>
        );
    };

    return (
        <div className={commonStyles.container}>
            <div className={styles.titleSection}>
                <h4>Dashboard</h4>
                <Space size={24}>
                    <DateControlLink
                        duration={DAY_MS}
                        step="1h"
                        text="Last day"
                    />
                    <DateControlLink
                        duration={WEEK_MS}
                        step="6h"
                        text="Last week"
                    />
                    <DateControlLink
                        duration={MONTH_MS}
                        step="24h"
                        text="Last month"
                    />
                    <DatePicker.RangePicker showTime onCalendarChange={(dates) => {
                        if (dates && dates[0] && dates[1] && dates[0] < dates[1]) {
                            const newStartTime = dates[0].toDate().getTime();
                            const newEndTime = dates[1].toDate().getTime();
                            setStartTime(newStartTime);
                            setEndTime(newEndTime);
                            if (newEndTime - newStartTime < DAY_MS) {
                                setStep("1h");
                            } else if (newEndTime - newStartTime < WEEK_MS) {
                                setStep("6h");
                            } else {
                                setStep("24h");
                            }
                            setSelected("Customized");
                        }
                    }}/>
                </Space>
            </div>
            <Collapse defaultActiveKey="qps">
                <Collapse.Panel header="QPS" key="qps">
                    <Graph
                        query='sum by (Namespace, path) (rate(http_requests_total{ContainerName=~"http-server|query-server", path=~"/query|/set_profile|/set_profile_multi|/log|/log_multi|/v1/actions|/v1/query"}[2m]))'
                        startTime={startTime}
                        endTime={endTime}
                        step={step}
                        unit="req/s"
                    />
                </Collapse.Panel>
                <Collapse.Panel header="Aggregate Lag" key="lag">
                    <Graph
                        query='sum by (Namespace, aggregate_name) (label_replace(aggregator_backlog{consumer_group!~"^locustfennel.*"}, "aggregate_name", "$1", "consumer_group", "(.*)"))'
                        startTime={startTime}
                        endTime={endTime}
                        step={step}
                    />
                </Collapse.Panel>
                <Collapse.Panel header="Latency (Median)" key="latency">
                    <Graph
                        query='MAX by (Namespace, path) (http_response_time_seconds{quantile="0.5", PodName=~"(http-server.*)|(query-server.*)"})'
                        startTime={startTime}
                        endTime={endTime}
                        step={step}
                        unit="s"
                    />
                </Collapse.Panel>
            </Collapse>
        </div>
    );
}

interface RangeVector {
    metric: Record<string, string>,
    values: [number, string][],
}

function generateSeriesName(metric: Record<string, string>): string {
    return Object.keys(metric)
        .filter(k => k !== "Namespace")
        .map(k => metric[k])
        .join(" - ");
}

interface GraphData {
    time: number,
    value: number,
    series: string,
}

interface GraphProps {
    query: string,
    startTime: number,
    endTime: number,
    step: string,
    unit?: string,
}

function Graph({ query, startTime, endTime, step, unit}: GraphProps) {
    const [data, setData] = useState<GraphData[]>([]);
    const params = {
        query,
        start: new Date(startTime).toISOString(),
        end: new Date(endTime).toISOString(),
        step,
    };
    const { tierID } = useParams();

    const queryMetrics = () => {
        axios.get(generatePath("/tier/:tierID/metrics/query_range", { tierID }), {
            params,
        }).then((response: AxiosResponse<RangeVector[]>) => {
            const newData = response.data.flatMap(rv => {
                const seriesName = generateSeriesName(rv.metric);
                return rv.values.map((scalar: [number, string]) => ({
                    time: scalar[0] * 1000,
                    value: Math.round(parseFloat(scalar[1]) * 10 + Number.EPSILON) / 10,
                    series: seriesName,
                }));
            });
            setData(newData);
        }).catch(()=> {
            // TODO(xiao) error handling
        });
    };

    useEffect(queryMetrics, [query, startTime, endTime, step]);

    const config: LineConfig = {
        data,
        xField: "time",
        yField: "value",
        seriesField: "series",
        xAxis: {
            type: "time",
        },
        yAxis: {
            label: {
                formatter: v => unit ? `${humanFormat(parseFloat(v))} ${unit}` : `${humanFormat(parseFloat(v))}`,
            },
        },
    };

    return <Line {...config} />;
}

export default DashboardPage;
