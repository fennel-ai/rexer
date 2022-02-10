import { Auth } from "aws-amplify";

export const loadLoggedInPage = async (setUsername, navigate) => {
  const user = await Auth.currentAuthenticatedUser();
  if (user && user.attributes && user.attributes.email)
    setUsername(user.attributes.email);
  else navigate("/");
};

export const loadAuthPage = async (navigate) => {
  const user = await Auth.currentAuthenticatedUser();
  if (user) navigate("/actions");
};
