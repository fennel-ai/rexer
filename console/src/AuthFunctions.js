import { Auth } from "aws-amplify";

export const loadLoggedInPage = async (setUsername, navigate) => {
  const user = await Auth.currentAuthenticatedUser();
  if (user) setUsername(user.username);
  else navigate("/");
};
