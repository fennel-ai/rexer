import * as React from 'react';
import { Outlet, NavLink } from 'react-router-dom';
import './style.css';

const App = () => {
  return (
    <div className="container">
      <h1 className="title">Console</h1>
      <div className="links">
        <NavLink className={({isActive}) => isActive ? "curLink" : "link"} to="/action">Actions</NavLink>
        <NavLink className={({isActive}) => isActive ? "curLink" : "link"} to="/profile">Profiles</NavLink>
      </div>
      <Outlet />
    </div>
  );
};

export default App;