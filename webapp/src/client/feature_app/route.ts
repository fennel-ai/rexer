import { generatePath } from "react-router-dom";

export function tierPagePath(tierID: string): string {
    return generatePath("/tier/:tierID", { tierID });
}

export function featuresPagePath(tierID: string): string {
    return generatePath("/tier/:tierID/features", { tierID });
}

export function featuresSearchPath(tierID: string): string {
    return generatePath("/tier/:tierID/features", { tierID });
}

export function featureDetailPagePath({tierID, featureID, version} : { tierID: string, featureID: string, version?: number}): string {
    if (version) {
        return generatePath("/tier/:tierID/feature/:featureID?version=:version", { tierID, featureID, version: version.toString() });
    }
    return generatePath("/tier/:tierID/feature/:featureID", { tierID, featureID });
}

export function featureDetailAjaxPath({ tierID, featureID, version} : { tierID: string, featureID: string, version?: number | string | null}): string {
    if (version) {
        return generatePath("/tier/:tierID/feature/:featureID/detail?version=:version", { tierID, featureID, version: version.toString()});
    }
    return generatePath("/tier/:tierID/feature/:featureID/detail", { tierID, featureID });
}
