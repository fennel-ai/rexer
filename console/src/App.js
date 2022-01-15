import * as React from 'react';
import { Outlet, Link } from 'react-router-dom';
import './style.css';

const App = () => {
  return (
    <div className="container">
      <h1 className="title">Console</h1>
      <div className="links">
        <Link to="/action"><span className="link">Actions</span></Link>
        <Link to="/profile"><span className="link">Profiles</span></Link>
      </div>
      <Outlet />
    </div>
  );
};

export default App;