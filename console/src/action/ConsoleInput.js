import * as React from 'react';
import './style.css'

const ConsoleInput = ({ data }) => (
  <div className="consoleFormItem">
    <div className="consoleFormItemLabel">
      <label htmlFor={data.id}>{data.label}</label>
    </div>
    <div className="consoleFormItemInput">
      <input id={data.id} type={data.type} />
    </div>
  </div>
);

export { ConsoleInput };