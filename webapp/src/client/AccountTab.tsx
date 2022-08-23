import { Avatar, Input } from "antd";

import styles from "./styles/AccountTab.module.scss";

function AccountTab() {
    return (
        <table className={styles.table}>
            <tr>
                <td>Picture</td>
                <td>
                    <Avatar shape="square" size={86}>
                        <div className={styles.avatarInitial}>J</div>
                    </Avatar>
                </td>
            </tr>
            <tr>
                <td>First Name</td>
                <td>
                    <Input
                        value="Xiao"
                    />
                </td>
            </tr>
            <tr>
                <td>Last Name</td>
                <td>
                    <Input
                        value="Jiang"
                    />
                </td>
            </tr>
            <tr>
                <td>Work Email</td>
                <td>
                    <Input
                        value="xiao@fennel.ai"
                        disabled
                    />
                </td>
            </tr>
            <tr>
                <td>Password</td>
                <td>
                    <Input
                        type="password"
                        value="uselesspassword"
                    />
                </td>
            </tr>
        </table>
    );
}

export default AccountTab;
