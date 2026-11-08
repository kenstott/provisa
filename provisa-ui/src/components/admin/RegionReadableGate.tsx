// Copyright (c) 2026 Kenneth Stott
// Canary: 8f3a1c47-2e90-4b6d-a5c8-7d10e4f9b263
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922, REQ-1327: a platform page that also shows the org what the platform assigned it in each
// region. Where the org has regions, the page's platform right opens it, `regionsCapability` opens
// it to read, and a demo sandbox's visitor reads it; with none it is the platform's page alone. The
// page itself shows nothing it could change to a reader, and the server refuses a reader's write.

import type { ReactNode } from "react";
import { isDemoSandbox } from "../../api/demoSandbox";
import { useAuth } from "../../context/AuthContext";
import { useRegionChoices } from "../../hooks/useRegionQueries";
import { meetsRequirement, type CapabilityRequirement } from "../../lib/capabilities";
import type { Capability } from "../../types/auth";
import { CapabilityGate } from "../CapabilityGate";
import { CredentialCheck } from "../CredentialCheck";

export function RegionReadableGate({
  requirement,
  regionsCapability,
  fallback,
  navigable,
  children,
}: {
  requirement: CapabilityRequirement;
  regionsCapability: Capability;
  fallback?: ReactNode;
  navigable?: boolean;
  children: ReactNode;
}) {
  const { activeOrgId, capabilities } = useAuth();
  const { regions, loading } = useRegionChoices();
  if (loading) return fallback === undefined ? null : <CredentialCheck />;
  const hasRegions = regions.length > 0;
  if (hasRegions && (isDemoSandbox(activeOrgId) || meetsRequirement(capabilities, requirement))) {
    return <>{children}</>;
  }
  const gate = hasRegions ? { capability: regionsCapability } : requirement;
  return (
    <CapabilityGate {...gate} fallback={fallback} navigable={navigable}>
      {children}
    </CapabilityGate>
  );
}
