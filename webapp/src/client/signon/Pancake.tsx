import styles from "./styles/Pancake.module.scss";

function Pancake() {
    return (
        <div className={styles.container}>
            <div className={styles.banner}>
                <span className={styles.jump}>Jump start ML</span> in Your Organization
            </div>
            <img src="images/pancake.svg" className={styles.pancake} />
        </div>
    );
}

export default Pancake;
