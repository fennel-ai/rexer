import App from "./App";
import { createRoot } from "react-dom/client";

const container = document.getElementById("root");
const root = createRoot(container!); // eslint-disable-line @typescript-eslint/no-non-null-assertion
root.render(<App />);
