import App from "./App";
import { createRoot } from "react-dom/client";

const container = document.getElementById("root")!;  // eslint-disable-line @typescript-eslint/no-non-null-assertion
const { page, user } = container.dataset;
const parsedUser = JSON.parse(user || "{}");

const root = createRoot(container);
root.render(<App page={page || null} user={parsedUser} />);
