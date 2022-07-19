import styles from "./styles/index.module.scss";
import Navbar from "./Navbar";

function App() {
    return (
        <div>
            <Navbar />
            <h1 className={styles.foo}>
                Hello world!
            </h1>
        </div>
    );
}

export default App;
