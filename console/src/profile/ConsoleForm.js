import * as React from 'react';
import { ConsoleInput } from './../ConsoleInput';

import './../style.css'

const filters = {
  oType : {
    id: 'filterOType',
    label: 'OType: ',
  },
  oId : {
    id: 'filterOId',
    label: 'OID: ',
  },
  key : {
    id: 'filterKey',
    label: 'Key: ',
  },
  version : {
    id: 'filterVersion',
    label: 'Version: ',
  },
};

const ConsoleForm = ({ onQuerySubmit }) => {    
  return (
    <form onSubmit={onQuerySubmit} className="consoleForm">
      <ConsoleInput data={filters.oType} />
      <ConsoleInput data={filters.oId} />
      <ConsoleInput data={filters.key} />
      <ConsoleInput data={filters.version} />
      
      <div className="consoleFormSubmit">
        <button type="submit" className="consoleFormSubmitButton">Query</button>
      </div>
    </form>
  );
};

export { ConsoleForm };