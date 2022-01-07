import * as React from 'react';
import './style.css'

const ConsoleDateTime = ({ data }) => (
  <div className="consoleFormItem">
    <div className="consoleFormItemLabel">
      <label htmlFor={data.id}>{data.label}</label>
    </div>
    <div className="consoleFormItemInput">
      <input type="datetime-local" name={data.id} id={data.id} />
    </div>
  </div>
);

export { ConsoleDateTime };