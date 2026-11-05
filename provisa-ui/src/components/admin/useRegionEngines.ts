// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: whether Admin > Federation is a tab per region, and the grant those tabs show. A
// region's engine is the store the platform assigned the org there, so the platform administrator
// reads and saves it through the org's grant; anyone else reads the grant the org holds. With no
// regions -- none declared by the platform, or none granted to the org -- the page is unchanged.

import { useEffect, useState } from "react";
import { useQuery } from "@apollo/client/react";
import { isDemoSandbox } from "../../api/demoSandbox";
import {
  fetchOrgGrant,
  fetchOrgRegions,
  problemOf,
  saveOrgGrant,
  type Grant,
  type GrantView,
} from "../../api/regions";
import { useAuth } from "../../context/AuthContext";
import { RegionChoices } from "../../hooks/admin.graphql";

export type RegionEngines =
  | { status: "loading" }
  | { status: "none" }
  | { status: "error"; text: string }
  | {
      status: "regions";
      grant: GrantView;
      /** Present only for the platform administrator: saves the org's whole grant. */
      save: ((grant: Grant) => Promise<void>) | null;
      sandbox: boolean;
    };

export function useRegionEngines(): RegionEngines {
  const { capabilities, activeOrgId } = useAuth();
  const choices = useQuery<{ regionChoices: { regions: string[] } }>(RegionChoices);
  const declared = (choices.data?.regionChoices.regions.length ?? 0) > 0;
  const sandbox = isDemoSandbox(activeOrgId);
  // The grant route is the platform administrator's: `platform_settings` and `cross_org` together
  // (require_deployment_settings). A sandbox never writes.
  const platformOrg =
    !sandbox &&
    activeOrgId !== null &&
    capabilities.includes("platform_settings") &&
    capabilities.includes("cross_org")
      ? activeOrgId
      : null;
  const [grant, setGrant] = useState<GrantView | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!declared) return;
    (platformOrg !== null ? fetchOrgGrant(platformOrg) : fetchOrgRegions())
      .then(setGrant)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, [declared, platformOrg]);

  if (choices.error) return { status: "error", text: choices.error.message };
  if (error) return { status: "error", text: error };
  if (choices.loading) return { status: "loading" };
  if (!declared) return { status: "none" };
  if (grant === null) return { status: "loading" };
  if (grant.regions.length === 0) return { status: "none" };
  return {
    status: "regions",
    grant,
    save:
      platformOrg === null
        ? null
        : async (next) => setGrant(await saveOrgGrant(platformOrg, next, false)),
    sandbox,
  };
}
