import * as React from 'react';

const ConsoleSelect = ({ data }) => {
  return (
    <div>
      <label htmlFor={data.id}>{data.label}</label>
      <select name={data.id} id={data.id}>
        {data.options.map((option) => (
          <option value={option} key={option}>{option}</option>
        ))}
      </select>
    </div>
  );
};

export { ConsoleSelect };