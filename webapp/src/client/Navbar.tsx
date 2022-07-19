import styles from "./styles/Navbar.module.scss";

function Navbar() {
    return (
        <nav>
            <div className={styles.container}>
                <div className={styles.leftNav}>
                    <div>
                        <img src="images/logo.svg" alt="logo" />
                    </div>
                    <div className={styles.divider} />
                    <div>
                        Tier 1
                    </div>
                    <div className={styles.divider} />
                </div>

                <div className={styles.rightNav}>
                    <div>
                        Documentation
                    </div>
                </div>
            </div>
        </nav>
    );
}

export default Navbar;
