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
          <th style={styles.tableHeaderData}>Value</th>
        </tr>
      </thead>
      <tbody>
        {results &&
          results.map((item) => <ActionRow data={item} key={[item.Oid, item.Key, item.Version]} />)}
      </tbody>
    </table>
  </div>
);

const ActionRow = ({ data }) => (
  <tr style={styles.tableRow}>
    <td style={styles.tableData}>{data.OType}</td>
    <td style={styles.tableData}>{data.Oid}</td>
    <td style={styles.tableData}>{data.Key}</td>
    <td style={styles.tableData}>{data.Version}</td>
    <td style={styles.tableData}>{data.Value}</td>
  </tr>
);

export { ConsoleResult };
