import * as React from "react";
import { useState } from "react";
import { Auth } from "aws-amplify";
import { NavLink, useNavigate } from "react-router-dom";
import { styles } from "../styles";
import { loadAuthPage } from "../AuthFunctions";

export const SignUp = () => {
  const [password, setPassword] = useState("");
  const [email, setEmail] = useState("");
  const [error, setError] = useState("");
  const navigate = useNavigate();

  React.useEffect(() => {
    loadAuthPage(navigate);
  }, []);

  async function signUp() {
    try {
      await Auth.signUp({
        username: email,
        password,
        attributes: {
          email,
        },
      });
      navigate("/verify", { state: { email, password } });
    } catch (error) {
      console.log("error signing in", error);
      setError(error.message);
    }
  }
  return (
    <div style={styles.authContainer}>
      <h1>Sign Up</h1>
      <h2>Email</h2>
      <input
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        style={styles.inputContainer}
      />
      <h2>Password</h2>
      <input
        type={"password"}
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        style={styles.inputContainer}
      />
      <button onClick={signUp} style={styles.signInButton}>
        Sign Up
      </button>
      <div style={{ color: "red" }}>{error}</div>
      <NavLink to="/">Have an account? Sign in.</NavLink>
    </div>
  );
};
