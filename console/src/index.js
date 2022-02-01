import React from "react";
import ReactDOM from "react-dom";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import reportWebVitals from "./reportWebVitals";
import Amplify from "aws-amplify";
import awsconfig from "./aws-exports";
import "./index.css";
import ActionApp from "./action/ActionApp";
import ProfileApp from "./profile/ProfileApp";
import { SignIn } from "./SignIn";
import { SignUp } from "./SignUp";

Amplify.configure(awsconfig);

ReactDOM.render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<SignIn />} />
        <Route path="/sign-up" element={<SignUp />} />
        <Route path="actions" element={<ActionApp />} />
        <Route path="profile" element={<ProfileApp />} />
        <Route path="*" element={"404 Not found"} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>,
  document.getElementById("root")
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
