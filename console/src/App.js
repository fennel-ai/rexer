import * as React from 'react';
import { Link } from 'react-router-dom';

const App = () => {
  return (
    <div>
      <Link to="/action">Actions</Link>
      <Link to="/profile">Profiles</Link>
    </div>
  );
}

export default App;