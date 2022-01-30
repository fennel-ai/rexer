import * as React from "react";
import { Outlet, NavLink } from "react-router-dom";
import "./style.css";
import { styles } from "./styles";

const Header = () => {
  return (
    <div className="container">
      <div style={styles.header} className="links">
        <h1 className="title">Console</h1>
        <NavLink
          className={({ isActive }) => (isActive ? "curLink" : "link")}
          to="/"
        >
          Actions
        </NavLink>
        <NavLink
          className={({ isActive }) => (isActive ? "curLink" : "link")}
          to="/profile"
        >
          Profiles
        </NavLink>
      </div>
      <Outlet />
    </div>
  );
};

export default Header;
