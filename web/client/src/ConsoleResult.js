import * as React from 'react';

const ConsoleResult = ({ updateData }) => {
  const [ results, setResults ] = React.useState({});
  
  const updateResults = (data) => {
    setResults(data);
    
    console.log(data);
  };
  
  React.useEffect(() => {
    updateData.current = updateResults;
  }, []);
  
  return (
    <>
    </>
  );
};

export { ConsoleResult };