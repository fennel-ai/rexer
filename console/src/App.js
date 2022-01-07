import * as React from 'react';

import Amplify from 'aws-amplify';
import awsconfig from './aws-exports';

import { Console } from './Console'

import './style.css'

Amplify.configure(awsconfig);

function App() {
  return (
    <div className="container">
      <h1>Console</h1>
      <Console />
    </div>
  );
}

export default App;