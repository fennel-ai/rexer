import "./styles/App.css";
import Navbar from "./Navbar";

interface Props {
    page: string | null;
}

const DASHBOARD_PAGE = "dashboard";
const DATA_PAGE = "data";

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
    }
    return <DashboardPage />;
}

function DataPage() {
    return (<h1> Data </h1>);
}

function DashboardPage() {
    return (<h1> Dashboard </h1>);
}

export default App;
