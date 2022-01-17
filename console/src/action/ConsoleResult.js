import * as React from 'react';
import './../style.css';

const processData = (data, metadata) => {
  return data.map((item) => {
    const res = { ...item };
    
    res.actionType = metadata.actionTypes[item.actionType].text;
    res.actorType = metadata.actorTypes[item.actorType].text;
    res.targetType = metadata.targetTypes[item.targetType].text;
    
    return res;
  });
};

const ConsoleResult = ({ data, metadata }) => {
  const results = processData(data, metadata);
  
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
            <th>Action Value</th>
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

const ActionRow = ({ data }) => (
  <tr>
    <td>{data.actionId}</td>
    <td>{data.actionType}</td>
    <td>{data.actionValue}</td>
    <td>{data.targetId}</td>
    <td>{data.targetType}</td>
    <td>{data.actorId}</td>
    <td>{data.actorType}</td>
    <td>{data.requestId}</td>
    <td className="timestamp">{formatDate(data.timestamp)}</td>
  </tr>
);

export { ConsoleResult };