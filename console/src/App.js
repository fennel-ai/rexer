import './App.css';

import { Console } from './Console'

import Amplify from 'aws-amplify';
import awsconfig from './aws-exports';

Amplify.configure(awsconfig);

function App() {
  return (
    <>
      <h1>Console</h1>
      <hr />
      <Console />
    </>
  );
}

export default App;
