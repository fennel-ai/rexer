import * as React from "react";
import "./../style.css";
import { styles } from "../styles";

const ConsoleResult = ({ data, metadata }) => {
  return (
    <div className="consoleResult">
      <h2 className="consoleResultHeader">Results</h2>
      <table style={styles.table}>
        <thead>
          <tr style={styles.tableRow}>
            <th style={styles.tableHeaderData}>Action ID</th>
            <th style={styles.tableHeaderData}>Action Type</th>
            <th style={styles.tableHeaderData}>Actor ID</th>
            <th style={styles.tableHeaderData}>Actor Type</th>
            <th style={styles.tableHeaderData}>Target ID</th>
            <th style={styles.tableHeaderData}>Target Type</th>
            <th style={styles.tableHeaderData}>Request ID</th>
            <th style={styles.tableHeaderData}>Timestamp</th>
            <th style={styles.tableHeaderData}>Metadata</th>
          </tr>
        </thead>
        <tbody>
          {data &&
            data.map((item) => <ActionRow data={item} key={item.ActionID} />)}
        </tbody>
      </table>
    </div>
  );
};

const formatDate = (ms) => {
  let t = new Date(ms);
  let timestamp = "";

  timestamp += String(t.getHours()).padStart(2, "0");
  timestamp += ":" + String(t.getMinutes()).padStart(2, "0");
  timestamp += ":" + String(t.getSeconds()).padStart(2, "0");
  timestamp += " " + t.toDateString().slice(4);

  return timestamp;
};

const ActionRow = ({ data }) => (
  <tr style={styles.tableRow}>
    <td style={styles.tableData}>{data.ActionID}</td>
    <td style={styles.tableData}>{data.ActionType}</td>
    <td style={styles.tableData}>{data.ActorID}</td>
    <td style={styles.tableData}>{data.ActorType}</td>
    <td style={styles.tableData}>{data.TargetID}</td>
    <td style={styles.tableData}>{data.TargetType}</td>
    <td style={styles.tableData}>{data.RequestID}</td>
    <td style={styles.tableData} className="timestamp">
      {formatDate(data.Timestamp)}
    </td>
    <td style={styles.tableData}>{data.Metadata}</td>
  </tr>
);

export { ConsoleResult };
