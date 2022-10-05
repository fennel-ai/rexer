import { generatePath } from "react-router-dom";

export function dashboardTabPath(tierID: string): string {
    return generatePath("/tier/:tierID/dashboard", { tierID });
}
