import {
    BrowserRouter,
    Routes,
    Route,
} from "react-router-dom";

import "./styles/App.less";
import Navbar, { type Tier } from "./Navbar";
import FeaturesPage from "./FeaturesPage";
import FeatureDetailPage from "./FeatureDetailPage";
import TierManagementPage from "./TierManagementPage";
import OnboardPage from "../onboard/OnboardPage";

interface User {
    email: string,
    firstName: string,
    lastName: string,
    onboardStatus: number,
}

interface Props {
    user: User,
    tiers: Tier[],
}

function App({ user, tiers }: Props) {
    return (
        <BrowserRouter>
            <Routes>
                <Route
                    path="/tier_management"
                    element={(
                        <div>
                            <Navbar tiers={tiers} user={user} />
                            <TierManagementPage />
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
                <Route path="/tier/:tierID">
                    <Route
                        index
                        element={(
                            <>
                                <Navbar user={user} tiers={tiers} />
                                <FeaturesPage />
                            </>
                        )}
                    />
                    <Route
                        path="features"
                        element={(
                            <>
                                <Navbar user={user} tiers={tiers} />
                                <FeaturesPage />
                            </>
                        )}
                    />
                    <Route
                        path="feature/:featureID"
                        element={(
                            <>
                                <Navbar user={user} tiers={tiers} />
                                <FeatureDetailPage />
                            </>
                        )}
                    />
                </Route>
            </Routes>
        </BrowserRouter>
    );
}

export default App;
