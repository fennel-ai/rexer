import SignUp from "./SignUp";
import SignIn from "./SignIn";
import ForgotPassword from "./ForgotPassword";
import ResetPassword from "./ResetPassword";
import "./styles/SignOnApp.less";

interface Props {
    page: string,
}

const SIGNUP_PAGE = "signup";
const SIGNIN_PAGE = "signin";
const FORGOT_PASSWORD_PAGE = "forgot_password";
const RESET_PASSWORD_PAGE = "reset_password";

function SignOnApp({page}: Props) {
    return (
        <Route page={page} />
    );
}

function Route({ page }: Props) {
    switch (page) {
        case SIGNUP_PAGE:
            return <SignUp />;
        case SIGNIN_PAGE:
            return <SignIn />;
        case FORGOT_PASSWORD_PAGE:
            return <ForgotPassword />;
        case RESET_PASSWORD_PAGE:
            return <ResetPassword />;
    }
    return <SignIn />;
}

export default SignOnApp;
