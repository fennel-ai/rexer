import * as React from 'react';
import './style.css';

const ConsoleSelect = ({ data, more }) => ( 
  <div className="consoleFormItem">
    <div className="consoleFormItemLabel">
      <label htmlFor={data.id}>{data.label}</label>
    </div>
    <div className="consoleFormItemInput">
      <select name={data.id} id={data.id}>
        {data.options.concat(more).map((option) => (
          <option key={option.val} value={option.val}>{option.text}</option>
        ))}
      </select>
    </div>
  </div>
);

export { ConsoleSelect };