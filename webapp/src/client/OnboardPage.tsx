interface User {
    onboardStatus: number,
}

function OnboardPage({user}: {user: User}) {
    return (<div>{user.onboardStatus}</div>);
}

export default OnboardPage;
