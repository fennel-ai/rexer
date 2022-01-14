import * as React from 'react';
import { Link } from 'react-router-dom';
import { ActionPage } from './action/ActionPage';

function App() {
    return(
        <>
          <Link to="/action">Action</Link>
        </>
    );
}

export default App;