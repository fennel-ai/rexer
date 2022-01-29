import * as React from 'react';
import { ConsoleInput } from './../ConsoleInput';
import { ConsoleSelect } from './../ConsoleSelect';
import './../style.css';

const filters = {
  oType : {
    id: 'filterOType',
    label: 'OType: ',
    options: [
      {val: 'ANY', text: 'ANY'},
      {val: 'USER', text: 'USER'},
      {val: 'VIDEO', text: 'VIDEO'},
    ]
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
      <ConsoleSelect data={filters.oType} more={[]} />
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