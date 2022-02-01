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
import { ConfirmSignUp } from "./ConfirmSignUp";
const config = {
  aws_project_region: "us-west-2",
  aws_cognito_identity_pool_id: "us-west-2_fJKVSx1Dj",
  aws_cognito_region: "us-west-2",
  aws_user_pools_id: "us-west-2_fJKVSx1Dj",
  aws_user_pools_web_client_id: "4g5cf9uncg1pfupqq37233ccth",
  oauth: {},
  aws_cognito_username_attributes: ["EMAIL"],
  aws_cognito_social_providers: [],
  aws_cognito_signup_attributes: ["EMAIL"],
  aws_cognito_mfa_configuration: "OFF",
  aws_cognito_mfa_types: ["SMS"],
  aws_cognito_password_protection_settings: {
    passwordPolicyMinLength: 8,
    passwordPolicyCharacters: [],
  },
  aws_cognito_verification_mechanisms: ["EMAIL"],
  aws_cloud_logic_custom: [
    {
      name: "consoleBff",
      endpoint:
        "https://nizekl4xd9.execute-api.us-west-2.amazonaws.com/westdev",
      region: "us-west-2",
    },
  ],
};
Amplify.configure(config);

ReactDOM.render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<SignIn />} />
        <Route path="/verify" element={<ConfirmSignUp />} />
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
