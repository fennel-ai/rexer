import * as React from 'react';
import './style.css'

const ConsoleInput = ({ data }) => {
  return (
    <div className="consoleFormItem">
      <div className="consoleFormItemLabel">
        <label htmlFor={data.id}>{data.label}</label>
      </div>
      <div className="consoleFormItemInput">
        <input id={data.id} />
      </div>
    </div>
  );
};

export { ConsoleInput };