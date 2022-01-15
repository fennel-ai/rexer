import * as React from 'react';
import { ConsoleInput } from './../ConsoleInput';
import './../style.css';

const filters = {
  oType : {
    id: 'filterOType',
    label: 'OType: ',
    type: 'number',
  },
  oId : {
    id: 'filterOId',
    label: 'OID: ',
    type: 'number',
  },
  key : {
    id: 'filterKey',
    label: 'Key: ',
    type: 'text',
  },
  version : {
    id: 'filterVersion',
    label: 'Version: ',
    type: 'number',
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