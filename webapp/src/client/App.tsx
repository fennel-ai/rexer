import "./styles/App.less";
import Navbar from "./Navbar";
import DataPage from "./DataPage";
import SettingsPage from "./SettingsPage";
import DashboardPage from "./DashboardPage";
import OnboardPage from "./OnboardPage";

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

function WithNavBar({page, mainComponent}: {page: string, mainComponent: JSX.Element}) {
    return (
        <div>
            <Navbar page={page} />
            {mainComponent}
        </div>
    );
}

function App({page, user}: Props) {
    switch (page) {
        case DATA_PAGE:
            return <WithNavBar page={page} mainComponent={<DataPage />} />;
        case DASHBOARD_PAGE:
            return <WithNavBar page={page} mainComponent={<DashboardPage />} />;
        case SETTINGS_PAGE:
            return <WithNavBar page={page} mainComponent={<SettingsPage />} />;
        case ONBOARD_PAGE:
            return <OnboardPage user={user} />;
    }
    return <DashboardPage />;
}

export default App;
