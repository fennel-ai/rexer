import SignOnApp from "./SignOnApp";
import { createRoot } from "react-dom/client";

const container = document.getElementById("root")!;  // eslint-disable-line @typescript-eslint/no-non-null-assertion
const props = container.dataset;

const root = createRoot(container);
root.render(<SignOnApp page={props.page!} />); // eslint-disable-line @typescript-eslint/no-non-null-assertion
