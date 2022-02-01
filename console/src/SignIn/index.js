import { useState } from "react";
import { Auth } from "aws-amplify";
import { NavLink, useNavigate } from "react-router-dom";
import { styles } from "../styles";

export const SignIn = () => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const navigate = useNavigate();

  async function signIn() {
    try {
      await Auth.signIn(username, password);
      navigate("/");
    } catch (error) {
      console.log("error signing in", error);
      setError(error.message);
    }
  }
  return (
    <div style={styles.authContainer}>
      <h1>Sign In</h1>
      <h2>Username</h2>
      <input
        value={username}
        onChange={(e) => setUsername(e.target.value)}
        style={styles.inputContainer}
      />
      <h2>Password</h2>
      <input
        type={"password"}
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        style={styles.inputContainer}
      />
      <button onClick={signIn} style={styles.signInButton}>
        Sign In
      </button>
      <div style={{ color: "red" }}>{error}</div>
      <NavLink to="/sign-up">Don't have an account? Sign up.</NavLink>
    </div>
  );
};
