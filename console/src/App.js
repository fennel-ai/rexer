import * as React from 'react';
import { Outlet, NavLink } from 'react-router-dom';
import './style.css';

const App = () => {
  return (
    <div className="container">
      <h1 className="title">Console</h1>
      <div className="links">
        <NavLink to="/action"><span className="link">Actions</span></NavLink>
        <NavLink to="/profile"><span className="link">Profiles</span></NavLink>
      </div>
      <Outlet />
    </div>
  );
};

export default App;