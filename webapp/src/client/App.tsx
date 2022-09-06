import {
    BrowserRouter,
    Routes,
    Route,
  } from "react-router-dom";

import "./styles/App.less";
import Navbar from "./Navbar";
import DataPage from "./DataPage";
import SettingsPage from "./SettingsPage";
import DashboardPage from "./DashboardPage";
import OnboardPage from "./onboard/OnboardPage";
import TierManagementPage from "./TierManagementPage";

interface User {
    email: string,
    firstName: string,
    lastName: string,
    onboardStatus: number,
}

interface Props {
    page: string;
    user: User,
}

function App({page, user}: Props) {
    return (
        <BrowserRouter>
            <Routes>
                <Route
                    path="/"
                    element={(
                        <div>
                            <Navbar page={page} />
                            <TierManagementPage />
                        </div>
                    )}
                />
                <Route
                    path="/data"
                    element={(
                        <div>
                            <Navbar page={page} />
                            <DataPage />
                        </div>
                    )}
                />
                <Route
                    path="/dashboard"
                    element={(
                        <div>
                            <Navbar page={page} />
                            <DashboardPage />
                        </div>
                    )}
                />
                <Route
                    path="/settings"
                    element={(
                        <div>
                            <Navbar page={page} />
                            <SettingsPage />
                        </div>
                    )}
                />
                <Route
                    path="/onboard"
                    element={(
                        <div>
                            <Navbar page={page} />
                            <OnboardPage user={user} />
                        </div>
                    )}
                />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
