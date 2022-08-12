import SignUp from "./SignUp";
import SignIn from "./SignIn";
import ForgotPassword from "./ForgotPassword";
import "../styles/signon/SignOnApp.css";

interface Props {
    page: string,
}

const SIGNUP_PAGE = "signup";
const SIGNIN_PAGE = "signin";
const FORGOT_PASSWORD_PAGE = "forgot_password";

function SignOnApp({page}: Props) {
    return (
        <Route page={page} />
    );
}

function Route({page}: Props) {
    switch (page) {
        case SIGNUP_PAGE:
            return <SignUp />;
        case SIGNIN_PAGE:
            return <SignIn />;
        case FORGOT_PASSWORD_PAGE:
            return <ForgotPassword />;
    }
    return <SignIn />;
}

export default SignOnApp;
