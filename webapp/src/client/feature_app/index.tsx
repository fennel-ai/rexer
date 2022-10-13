import App from "./App";
import { createRoot } from "react-dom/client";

const container = document.getElementById("root")!;  // eslint-disable-line @typescript-eslint/no-non-null-assertion
const { tiers, user } = container.dataset;
const parsedUser = user ? JSON.parse(user) : {};
const parsedTiers = JSON.parse(tiers || "{}")

const root = createRoot(container);
root.render(<App user={parsedUser} tiers={parsedTiers} />);
