import SignOnApp from "./SignOnApp";
import { createRoot } from "react-dom/client";
import { notification } from "antd";

const container = document.getElementById("root")!;  // eslint-disable-line @typescript-eslint/no-non-null-assertion
const props = container.dataset;

const root = createRoot(container);
root.render(<SignOnApp />);

const flashMsgType = props.flashMsgType;
const flashMsgContent = props.flashMsgContent;

if (flashMsgType && flashMsgContent) {
    switch (flashMsgType) {
        case "success":
            notification.success({message: flashMsgContent, placement: "bottomRight"});
            break;
        case "error":
            notification.error({message: flashMsgContent, placement: "bottomRight"});
            break;
    }
}
