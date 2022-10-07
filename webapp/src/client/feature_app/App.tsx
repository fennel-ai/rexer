import "./styles/App.less";
import Navbar from "./Navbar";
import DashboardPage from "./DashboardPage";

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
            <DashboardPage />
        </div>
    );
}

export default App;
