import * as React from 'react';
import './../style.css';

const ConsoleResult = ({ results }) => (
  <div className="consoleResult">
    <h2 className="consoleResultHeader">
      Results
    </h2>
    <table>
      <thead>
        <tr>
          <th>OType</th>
          <th>OID</th>
          <th>Key</th>
          <th>Version</th>
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
  <tr>
    <td>{data.oType}</td>
    <td>{data.oId}</td>
    <td>{data.key}</td>
    <td>{data.version}</td>
  </tr>
);

export { ConsoleResult };