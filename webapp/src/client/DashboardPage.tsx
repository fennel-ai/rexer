import { Collapse } from "antd";
import { useState, useEffect } from "react";
import axios, { AxiosResponse } from "axios";
import { Line, LineConfig } from '@ant-design/plots';

import styles from "./styles/DashboardPage.module.scss";

function DashboardPage() {
    return (
        <div className={styles.container}>
            <div className={styles.titleSection}>
                <h4 className={styles.title}>Dashboard</h4>
            </div>
            <Collapse defaultActiveKey="qps">
                <Collapse.Panel header="QPS" key="qps">
                    <Graph query='sum by (Namespace, path) (rate(http_requests_total{ContainerName=~"http-server|query-server", path=~"/query|/set_profile|/set_profile_multi|/log|/log_multi"}[1h]))' />
                </Collapse.Panel>
                <Collapse.Panel header="Backlog" key="backlog">
                    <Graph query='sum by (Namespace, aggregate_name) (label_replace(aggregator_backlog{consumer_group!~"^locustfennel.*"}, "aggregate_name", "$1", "consumer_group", "(.*)"))' />
                </Collapse.Panel>
                <Collapse.Panel header="Latency (Median)" key="latency">
                    <Graph query='MAX by (Namespace, path) (http_response_time_seconds{quantile="0.5", PodName=~"(http-server.*)|(query-server.*)"})' />
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

function Graph({query}: {query: string}) {
    const [data, setData] = useState<GraphData[]>([]);
    const params = {
        query,
        start: "2022-08-28T00:00:00.00Z", // TODO(xiao)
        end: "2022-08-29T00:00:00.00Z",
        step: "3h",
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

    useEffect(() => queryMetrics(), []);

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
