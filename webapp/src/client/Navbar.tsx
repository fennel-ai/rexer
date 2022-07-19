import styles from "./styles/Navbar.module.scss";

function Navbar() {
    return (
        <nav>
            <div className={styles.container}>
                <div className={styles.leftNav}>
                    <div className="logo">
                        <img src="images/logo.svg" alt="logo" />
                    </div>
                    <div>
                        Tier 1
                    </div>
                    <div>
                        Dashboard
                    </div>
                    <div>
                        Data
                    </div>
                </div>

                <div>
                    <div>
                        Documentation
                    </div>
                </div>
            </div>
        </nav>
    );
}

export default Navbar;
