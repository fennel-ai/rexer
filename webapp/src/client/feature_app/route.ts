import { generatePath } from "react-router-dom";

export function featuresTabPath(tierID: string): string {
    return generatePath("/tier/:tierID/features", { tierID });
}

export function featuresSearchPath(tierID: string): string {
    return generatePath("/tier/:tierID/features", { tierID });
}

export function featureDetailPath(tierID: string, featureID: string): string {
    return generatePath("/tier/:tierID/feature/:featureID", { tierID, featureID });
}
