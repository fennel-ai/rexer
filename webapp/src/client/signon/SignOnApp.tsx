import SignUp from "./SignUp";
import SignIn from "./SignIn";
import ResetPassword from "./ResetPassword";
import "../styles/signon/SignOnApp.css";

interface Props {
    page: string,
}

const SIGNUP_PAGE = "signup";
const SIGNIN_PAGE = "signin";
const RESET_PASSWORD_PAGE = "resetpassword";

function SignOnApp({page}: Props) {
    return (
        <div>
            <Route page={page} />
        </div>
    )
}

function Route({page}: Props) {
    switch (page) {
        case SIGNUP_PAGE:
            return <SignUp />;
        case SIGNIN_PAGE:
            return <SignIn />;
        case RESET_PASSWORD_PAGE:
            return <ResetPassword />;
    }
    return <SignIn />;
}

export default SignOnApp;
