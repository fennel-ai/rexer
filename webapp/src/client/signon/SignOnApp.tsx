import SignUp from "./SignUp";
import SignIn from "./SignIn";

interface Props {
    page: string,
}

const SIGNUP_PAGE = "signup";
const SIGNIN_PAGE = "signin";

function SignOnApp(props: Props) {
    const {page} = props;

    return (
        <div>
            <Route page={page} />
        </div>
    )
}

function Route(props: Props) {
    const {page} = props;

    switch (page) {
        case SIGNUP_PAGE:
            return <SignUp />
        case SIGNIN_PAGE:
            return <SignIn />
    }
    return <SignIn />;
}

export default SignOnApp;
