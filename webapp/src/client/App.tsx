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

const DASHBOARD_PAGE = "dashboard";
const DATA_PAGE = "data";
const SETTINGS_PAGE = "settings";
const ONBOARD_PAGE = "onboard";

function App({page, user}: Props) {
    switch (page) {
        case DATA_PAGE:
            return (
                <div>
                    <Navbar page={page} />
                    <DataPage />
                </div>
            );
        case DASHBOARD_PAGE:
            return (
                <div>
                    <Navbar page={page} />
                    <DashboardPage />
                </div>
            );
        case SETTINGS_PAGE:
            return (
                <div>
                    <Navbar page={page} />
                    <SettingsPage />
                </div>
            );
        case ONBOARD_PAGE:
            return <OnboardPage user={user} />;
        default:
            return (
                <div>
                    <Navbar page={page} />
                    <TierManagementPage />
                </div>
            );
    }
}

export default App;
