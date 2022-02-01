import { useState } from "react";
import { Auth } from "aws-amplify";
import { NavLink, useNavigate } from "react-router-dom";
import { styles } from "../styles";

export const SignUp = () => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [email, setEmail] = useState("");
  const [error, setError] = useState("");
  const navigate = useNavigate();

  async function signUp() {
    try {
      await Auth.signUp({
        username,
        password,
        attributes: {
          email,
        },
      });
      await Auth.signIn(username, password);
      navigate("/");
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
      <NavLink to="/actions">
        <button onClick={signUp} style={styles.signInButton}>
          Sign Up
        </button>
      </NavLink>
      <div style={{ color: "red" }}>{error}</div>
      <NavLink to="/">Have an account? Sign in.</NavLink>
    </div>
  );
};
