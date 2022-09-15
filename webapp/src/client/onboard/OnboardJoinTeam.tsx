import { Avatar, Button } from "antd";
import { ArrowRightOutlined } from '@ant-design/icons';
import axios, { AxiosResponse } from "axios";
import { useState } from "react";

import OnboardStepper from "./OnboardStepper";
import commonStyles from "./styles/Onboard.module.scss";
import styles from "./styles/OnboardJoinTeam.module.scss";

interface TeamMember {
    lastName: string,
}

interface Team {
    id: number,
    name: string,
    users: TeamMember[],
}

interface Props {
    team: Team,
    onOnboardStatusChange: (status: number) => void,
}

interface JoinTeamResponse {
    onboardStatus: number,
}

function OnboardJoinTeam({ team, onOnboardStatusChange }: Props) {
    const [submitting, setSubmitting] = useState(false);

    const onContinue = () => {
        setSubmitting(true);

        axios.post("/onboard/join_team", {
            teamID: team.id,
        }).then((response: AxiosResponse<JoinTeamResponse>) => {
            setSubmitting(false);
            onOnboardStatusChange(response.data.onboardStatus);
        })
        .catch(() => {
            // TODO(xiao) error handling
            setSubmitting(false);
        });
    };
    const avatarStyle = {
        backgroundColor: "#EFECF2",
        color: "#222124",
    };

    return (
        <div className={commonStyles.container}>
            <div className={commonStyles.logoAndName}>
                <img src="images/logo_name.svg" alt="logo" />
            </div>
            <OnboardStepper steps={2} activeStep={1} />

            <div className={commonStyles.content}>
                <h4 className={commonStyles.title}>Your team is already on Fennel!</h4>

                <div className={styles.teamContainer}>
                    <div className={styles.nameAndCount}>
                        <div>{team.name}</div>
                        <div className={styles.membersCount}>{`${team.users.length} members`}</div>
                    </div>
                    <Avatar.Group maxCount={3} maxStyle={avatarStyle}>
                        {team.users.map((u, i) => (<Avatar key={i} style={avatarStyle}>{u.lastName[0]}</Avatar>))}
                    </Avatar.Group>
                </div>
            </div>
            <Button type="primary" onClick={onContinue} disabled={submitting} loading={submitting}>
                Continue <ArrowRightOutlined />
            </Button>
        </div>
    );
}

export default OnboardJoinTeam;
