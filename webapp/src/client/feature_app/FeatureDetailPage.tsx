import { PicLeftOutlined } from "@ant-design/icons";

import styles from "./styles/FeatureDetailPage.module.scss";
import commonStyles from "./styles/Page.module.scss";

function FeatureDetailPage(): JSX.Element {
    // TODO(xiao): change the icon and tags
    return (
        <div className={commonStyles.container}>
            <div>
                <div className={styles.titleLhs}>
                    <PicLeftOutlined size={24} className={styles.titleIcon} />
                    <h4 className={commonStyles.title}>Feature</h4>
                    <span className={styles.tags}>
                        <span className={styles.tag}>tag 1</span>
                        <span className={styles.tag}>tag 2</span>
                        <span className={styles.tag}>tag 3</span>
                    </span>
                </div>
            </div>
        </div>
    );
}

export default FeatureDetailPage;
