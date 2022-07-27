import styles from "../styles/signon/Pancake.module.scss";

function Pancake() {
    return (
        <div className={styles.container}>
            <div className={styles.banner}>
                Jump start ML in Your Organization
            </div>
            <img src="images/pancake.svg" className={styles.pancake} />
        </div>
    );
}

export default Pancake;
