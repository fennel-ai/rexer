import "./styles/App.less";
import Navbar from "./Navbar";

interface User {
    email: string,
    firstName: string,
    lastName: string,
    onboardStatus: number,
}

interface Props {
    user: User,
}

function App({ user }: Props) {
    return (
        <div>
            <Navbar user={user} />
            Hello World
        </div>
    );
}

export default App;
