import {
    BrowserRouter,
    Routes,
    Route,
  } from "react-router-dom";

import "./styles/App.less";
import Navbar, { type Tier } from "./Navbar";
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
    tiers: Tier[],
    user: User,
}

function App({user, tiers}: Props) {
    return (
        <BrowserRouter>
            <Routes>
                <Route
                    path="/tier_management"
                    element={(
                        <div>
                            <Navbar tiers={tiers} />
                            <TierManagementPage />
                        </div>
                    )}
                />
                <Route
                    path="/"
                    element={(
                        <div>
                            <Navbar tiers={tiers} />
                            <TierManagementPage />
                        </div>
                    )}
                />
                <Route path="/tier/:tierID">
                    <Route
                        path="data"
                        element={(
                            <div>
                                <Navbar tiers={tiers} activeTab="data" />
                                <DataPage />
                            </div>
                        )}
                    />
                    <Route
                        path="dashboard"
                        element={(
                            <div>
                                <Navbar tiers={tiers} activeTab="dashboard" />
                                <DashboardPage />
                            </div>
                        )}
                    />
                    <Route
                        index
                        element={(
                            <div>
                                <Navbar tiers={tiers} activeTab="dashboard" />
                                <DashboardPage />
                            </div>
                        )}
                    />
                </Route>
                <Route
                    path="/settings"
                    element={(
                        <div>
                            <Navbar tiers={tiers} />
                            <SettingsPage />
                        </div>
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
