import { useState } from "react";
import { styles } from "../styles";
import { useLocation, useNavigate } from "react-router-dom";
import { Auth } from "aws-amplify";

export const ConfirmSignUp = () => {
  const [verification, setVerification] = useState("");
  const location = useLocation();
  const navigate = useNavigate();
  const email = location.state.email;
  const password = location.state.password;
  const onSubmit = async () => {
    await Auth.confirmSignUp(email, verification);
    await Auth.signIn(email, password);
    navigate("/actions");
  };
  return (
    <div style={styles.authContainer}>
      <h1>Enter Confirmation Code</h1>
      <input
        value={verification}
        onChange={(e) => setVerification(e.target.value)}
        style={styles.inputContainer}
      />
      <button onClick={onSubmit} style={styles.signInButton}>
        Verify
      </button>
    </div>
  );
};
