import "./styles/App.css";
import styles from "./styles/App.module.scss";
import Navbar from "./Navbar";
import { Button, DatePicker } from "antd";

function App() {
    return (
        <div>
            <Navbar />
            <h1 className={styles.foo}>
                Hello world!
            </h1>
            <DatePicker />
            <Button type="primary" style={{ marginLeft: 8 }}>
                Primary Button
            </Button>
        </div>
    );
}

export default App;
