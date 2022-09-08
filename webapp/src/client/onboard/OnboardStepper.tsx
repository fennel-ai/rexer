import styles from "./styles/OnboardStepper.module.scss";

interface Props {
    steps: number,
    activeStep: number,
}

function OnboardStepper({ steps, activeStep }: Props) {
    const comps = [];
    for (let i = 1; i <= steps; i++) {
        const classes = [styles.step];
        if (i <= activeStep) {
            classes.push(styles.activeStep)
        }
        comps.push(
            <div key={i} className={classes.join(" ")} />
        );
    }
    return (
        <div className={styles.container}>
            {comps}
        </div>
    );
}

export default OnboardStepper;
