import * as React from "react";
import "./../style.css";
import { styles } from "../styles";

const ConsoleResult = ({ results }) => (
  <div className="consoleResult">
    <h2 className="consoleResultHeader">Results</h2>
    <table style={styles.table}>
      <thead>
        <tr style={styles.tableRow}>
          <th style={styles.tableHeaderData}>OType</th>
          <th style={styles.tableHeaderData}>OID</th>
          <th style={styles.tableHeaderData}>Key</th>
          <th style={styles.tableHeaderData}>Version</th>
        </tr>
      </thead>
      <tbody>
        {results.map((item) => (
          <ActionRow data={item} key={item.oId} />
        ))}
      </tbody>
    </table>
  </div>
);

const ActionRow = ({ data }) => (
  <tr style={styles.tableRow}>
    <td style={styles.tableData}>{data.oType}</td>
    <td style={styles.tableData}>{data.oId}</td>
    <td style={styles.tableData}>{data.key}</td>
    <td style={styles.tableData}>{data.version}</td>
  </tr>
);

export { ConsoleResult };
