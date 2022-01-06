import * as React from 'react';

const ConsoleInput = ({ data }) => {
  return (
    <div>
      <label htmlFor={data.id}>{data.label}</label>
      <input id={data.id} />
    </div>
  );
};

export { ConsoleInput };