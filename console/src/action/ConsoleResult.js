import * as React from 'react';
import './../style.css';

const processData = (data, metadata) => {
  data.map((item) => {
    item.actionType = metadata.actionTypes[item.actionType].text;
    item.actorType = metadata.actorTypes[item.actorType].text;
    item.targetType = metadata.targetTypes[item.targetType].text;
  });
};

const ConsoleResult = ({ updateData, metadata }) => {
  const [ results, setResults ] = React.useState([]);
  
  const updateResults = (data) => {
    processData(data, metadata);
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
            <th>Action ID</th>
            <th>Action Type</th>
            <th>Target ID</th>
            <th>Target Type</th>
            <th>Actor ID</th>
            <th>Actor Type</th>
            <th>Request ID</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {results.map((item) => (
            <ActionRow data={item} key={item.actionId} />
          ))}
        </tbody>
      </table>
    </div>
  );
};

const formatDate = (ms) => {
  let t = new Date(ms);
  let timestamp = '';

  timestamp += String(t.getHours()).padStart(2, '0');
  timestamp += ':' + String(t.getMinutes()).padStart(2, '0');
  timestamp += ':' + String(t.getSeconds()).padStart(2, '0');
  timestamp += ' ' + t.toDateString().slice(4);
  
  
  return timestamp;
};

const ActionRow = ({ data }) => console.log(data) || console.log(typeof data.timestamp) || (
  <tr>
    <td>{data.actionId}</td>
    <td>{data.actionType}</td>
    <td>{data.targetId}</td>
    <td>{data.targetType}</td>
    <td>{data.actorId}</td>
    <td>{data.actorType}</td>
    <td>{data.requestId}</td>
    <td className="timestamp">{formatDate(data.timestamp)}</td>
  </tr>
);

export { ConsoleResult };