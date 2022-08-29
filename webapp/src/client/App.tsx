import "./styles/App.less";
import Navbar from "./Navbar";
import DataPage from "./DataPage";
import SettingsPage from "./SettingsPage";
import DashboardPage from "./DashboardPage";

interface Props {
    page: string | null;
}

const DASHBOARD_PAGE = "dashboard";
const DATA_PAGE = "data";
const SETTINGS_PAGE = "settings";

function App(props: Props) {
    const page = props.page || DASHBOARD_PAGE
    return (
        <div>
            <Navbar page={page} />
            <Route page={page} />
        </div>
    );
}

function Route(props: Props) {
    switch (props.page) {
        case DATA_PAGE:
            return <DataPage />;
        case DASHBOARD_PAGE:
            return <DashboardPage />;
        case SETTINGS_PAGE:
            return <SettingsPage />;
    }
    return <DashboardPage />;
}

export default App;
