import {
    BrowserRouter,
    Routes,
    Route,
} from "react-router-dom";

import SignUp from "./SignUp";
import SignIn from "./SignIn";
import ForgotPassword from "./ForgotPassword";
import ResetPassword from "./ResetPassword";
import "./styles/SignOnApp.less";

function SignOnApp(): JSX.Element {
    return (
        <BrowserRouter>
            <Routes>
                <Route
                    path="/signup"
                    element={<SignUp />}
                />
                <Route
                    path="/"
                    element={<SignIn />}
                />
                <Route
                    path="/signin"
                    element={<SignIn />}
                />
                <Route
                    path="/forgot_password"
                    element={<ForgotPassword />}
                />
                <Route
                    path="/reset_password"
                    element={<ResetPassword />}
                />
            </Routes>
        </BrowserRouter>
    );
}

export default SignOnApp;
