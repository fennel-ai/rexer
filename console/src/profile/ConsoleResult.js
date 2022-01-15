import * as React from 'react';
import './../style.css';

const ConsoleResult = ({ updateData }) => {
  const [ results, setResults ] = React.useState([]);
  
  const updateResults = (data) => {
    setResults(data);
    
    console.log(data);
  };
  
  React.useEffect(() => {
    updateData.current = updateResults;
  });
  
  return (
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
            <ActionRow data={item} key={item.logId} />
          ))}
        </tbody>
      </table>
    </div>
  );
};

const ActionRow = ({ data }) => console.log(data) || console.log(typeof data.timestamp) || (
  <tr>
    <td>{data.oType}</td>
    <td>{data.oId}</td>
    <td>{data.key}</td>
    <td>{data.version}</td>
  </tr>
);

export { ConsoleResult };