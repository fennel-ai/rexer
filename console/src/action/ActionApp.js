import * as React from 'react';
import { Console } from './Console'
import './../style.css'

const ActionApp = () => {
  return (
    <div className="container">
      <h1 className="title">Console</h1>
      <Console />
    </div>
  );
}

export default ActionApp;