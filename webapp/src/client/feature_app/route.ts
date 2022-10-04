import { generatePath } from "react-router-dom";

export function featureTabPath(tierID: string): string {
    return generatePath("/tier/:tierID/features", { tierID });
}
