import { Collapse } from "antd";
import { useState, useEffect } from "react";
import axios, { AxiosError, AxiosResponse } from "axios";
import { Line } from '@ant-design/plots';


import styles from "./styles/DashboardPage.module.scss";

function DashboardPage() {
    return (
        <div className={styles.container}>
            <div className={styles.titleSection}>
                <h4 className={styles.title}>Dashboard</h4>
            </div>
            <Collapse defaultActiveKey="qps">
                <Collapse.Panel header="QPS" key="qps">
                    <Graph query='sum by (path) (rate(http_requests_total{ContainerName="http-server", Namespace="t-107", path=~"(/set|/log)"}[1h]))' />
                </Collapse.Panel>
                <Collapse.Panel header="Backlog" key="backlog">
                </Collapse.Panel>
                <Collapse.Panel header="Latency" key="latency">
                </Collapse.Panel>
            </Collapse>
        </div>
    );
}

/*
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "path": "/log"
        },
        "values": [
          [
            1661299200,
            "0.3347457627118644"
          ],
*/

interface RangeVector {
    metric: Record<string, string>,
    values: [number, string][],
}

function generateSeriesName(metric: Record<string, string>) {
    let name = "";
    for (const label in metric) {
        name = name.concat(`-${metric[label]}`)
    }
    return name;
}

interface GraphData {
    time: number,
    value: number,
    series: string,
}

// TODO(xiao): cache data
function Graph({query}: {query: string}) {
    const [data, setData] = useState<GraphData[]>([]);
    const params = {
        query,
        start: "2022-08-28T00:00:00.00Z",
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

    const config = {
        data,
        xField: 'time',
        yField: 'value',
        seriesField: 'series',
    };

    return <Line {...config} />;
}

export default DashboardPage;
