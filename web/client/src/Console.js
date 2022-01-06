import * as React from 'react';

import { ConsoleForm } from './ConsoleForm';
import { ConsoleResult } from './ConsoleResult';

const Console = () => {
  const handleQuery = (event) => {
    
    console.log(event);
    
    // Handle API Call Here
    // Populate ConsoleResult
    
    event.preventDefault();
  }
  
  return (
    <>
      <ConsoleForm onQuerySubmit={handleQuery} />
      <ConsoleResult />
    </>
  );
};

export { Console };