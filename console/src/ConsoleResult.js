import * as React from 'react';

import './style.css'

const ConsoleResult = ({ updateData }) => {
  const [ results, setResults ] = React.useState([]);
  
  const updateResults = (data) => {
    setResults(data);
    
    console.log(data);
  };
  
  React.useEffect(() => {
    updateData.current = updateResults;
  }, []);
  
  console.log("Rerendering..");
  console.log(results);
  
  return (
    <table>
      <thead>
        <tr>
          <th>Action Type</th>
          <th>Target ID</th>
          <th>Target Type</th>
          <th>Actor ID</th>
          <th>Actor Type</th>
        </tr>
      </thead>
      <tbody>
        {results.map((item) => (
          <ActionRow data={item} key={item.logId} />
        ))}
      </tbody>
    </table>
  );
};

const ActionRow = ({ data }) => console.log(data) || (
  <tr>
    <td>{data.actionType}</td>
    <td>{data.targetId}</td>
    <td>{data.targetType}</td>
    <td>{data.actorId}</td>
    <td>{data.actorType}</td>
  </tr>
);

export { ConsoleResult };