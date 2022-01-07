import * as React from 'react';

import './style.css'

const ConsoleInput = ({ data }) => {
  return (
    <div className="consoleFormItem">
      <label htmlFor={data.id}>{data.label}</label>
      <input id={data.id} />
    </div>
  );
};

export { ConsoleInput };