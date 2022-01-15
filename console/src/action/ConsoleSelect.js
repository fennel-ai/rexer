import * as React from 'react';
import './style.css'

const ConsoleSelect = ({ data }) => (
  <div className="consoleFormItem">
    <div className="consoleFormItemLabel">
      <label htmlFor={data.id}>{data.label}</label>
    </div>
    <div className="consoleFormItemInput">
      <select name={data.id} id={data.id}>
        {data.options.map((option) => (
          <option value={option.val} key={option}>{option.text}</option>
        ))}
      </select>
    </div>
  </div>
);

export { ConsoleSelect };