import {
    BrowserRouter,
    Routes,
    Route,
} from "react-router-dom";

import "./styles/App.less";
import Navbar from "./Navbar";
import DashboardPage from "./DashboardPage";
import FeatureDetailPage from "./FeatureDetailPage";
import OnboardPage from "../onboard/OnboardPage";

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
        <BrowserRouter>
            <Routes>
                <Route
                    path="/"
                    element={(
                        <>
                            <Navbar user={user} />
                            <DashboardPage />
                        </>
                    )}
                />
                <Route
                    path="/feature/:featureID"
                    element={(
                        <>
                            <Navbar user={user} />
                            <FeatureDetailPage />
                        </>
                    )}
                />
                <Route
                    path="/onboard"
                    element={(
                        <div>
                            <OnboardPage user={user} />
                        </div>
                    )}
                />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
