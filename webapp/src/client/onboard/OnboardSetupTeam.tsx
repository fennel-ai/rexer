import { LoadingOutlined } from '@ant-design/icons';
import axios, { AxiosResponse } from "axios";
import { useState, useEffect } from "react";

import OnboardCreateTeam from "./OnboardCreateTeam";
import OnboardJoinTeam from "./OnboardJoinTeam";

interface TeamMember {
    lastName: string,
}

interface Team {
    users: TeamMember[],
}

interface TeamMatchResponse {
    matched: boolean,
    team?: Team,
    isPersonalDomain: boolean,
}

interface User {
    email: string,
    firstName: string,
}

interface Props {
    user: User,
    onOnboardStatusChange: (status: number) => void,
}

function OnboardSetupTeam({user, onOnboardStatusChange}: Props) {
    const [loading, setLoading] = useState(false);
    const [matched, setMatched] = useState(false);
    const [isPersonalDomain, setIsPersonalDomain] = useState(false);
    const [team, setTeam] = useState<Team>();

    const queryTeamMatch = () => {
        setLoading(true);

        axios.get("/onboard/team_match")
            .then((response: AxiosResponse<TeamMatchResponse>) => {
                const { matched, team, isPersonalDomain } = response.data;
                if (matched) {
                    setTeam(team);
                }
                setIsPersonalDomain(isPersonalDomain);
                setMatched(matched);
                setLoading(false);
            })
            .catch(() => {
                // TODO(xiao) error handling
            });
    };

    useEffect(() => queryTeamMatch(), []);

    if (loading) {
        return <LoadingOutlined spin />
    }

    if (matched) {
        return <OnboardJoinTeam />;
    }
    return (
        <OnboardCreateTeam
            user={user}
            isPersonalDomain={isPersonalDomain}
            onOnboardStatusChange={onOnboardStatusChange}
        />
    );
}

export default OnboardSetupTeam;
