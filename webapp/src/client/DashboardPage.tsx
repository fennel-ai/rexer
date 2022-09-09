import { Collapse, Space } from "antd";
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";
import { Line, LineConfig } from '@ant-design/plots';

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

    return (
        <div className={commonStyles.container}>
            <div className={styles.titleSection}>
                <h4 className={styles.title}>Dashboard</h4>
                <Space size={24}>
                    <a className={styles.dateControl} onClick={e => {
                        e.preventDefault();
                        setStartTime(now - DAY_MS);
                        setStep("1h");
                    }}>
                        Last day
                    </a>
                    <a className={styles.dateControl} onClick={e => {
                        e.preventDefault();
                        setStartTime(now - WEEK_MS);
                        setStep("6h");
                    }}>
                        Last week
                    </a>
                    <a className={styles.dateControl} onClick={e => {
                        e.preventDefault();
                        setStartTime(now - MONTH_MS);
                        setStep("24h");
                    }}>
                        Last month
                    </a>
                </Space>
            </div>
            <Collapse defaultActiveKey="qps">
                <Collapse.Panel header="QPS" key="qps">
                    <Graph
                        query='sum by (Namespace, path) (rate(http_requests_total{ContainerName=~"http-server|query-server", path=~"/query|/set_profile|/set_profile_multi|/log|/log_multi"}[1h]))'
                        startTime={startTime}
                        endTime={endTime}
                        step={step}
                    />
                </Collapse.Panel>
                <Collapse.Panel header="Backlog" key="backlog">
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

function generateSeriesName(metric: Record<string, string>) {
    return Object.values(metric).join(" - ");
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
}

function Graph({ query, startTime, endTime, step }: GraphProps) {
    const [data, setData] = useState<GraphData[]>([]);
    const params = {
        query,
        start: new Date(startTime).toISOString(),
        end: new Date(endTime).toISOString(),
        step,
    };

    const queryMetrics = () => {
        axios.get("/metrics/query_range", {
            params,
        }).then((response: AxiosResponse<RangeVector[]>) => {
            const newData = response.data.flatMap(rv => {
                const seriesName = generateSeriesName(rv.metric);
                return rv.values.map((scalar: [number, string]) => ({
                    time: scalar[0],
                    value: parseFloat(scalar[1]),
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
        xField: 'time',
        yField: 'value',
        seriesField: 'series',
        xAxis: {
            label: {
                formatter: (t:string) => (new Date(parseFloat(t)*1000).toLocaleString("en-US")),
            },
        },
    };

    return <Line {...config} />;
}

export default DashboardPage;
