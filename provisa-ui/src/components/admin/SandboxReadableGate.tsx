// Copyright (c) 2026 Kenneth Stott
// Canary: c795d096-0e84-4f8c-8867-bf9d231f7eae
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: a page a demo sandbox's visitor may open to read, though its capability is not theirs
// -- the Platform page's regions, shown to explain regions. Everywhere else the capability gates it.
// The page itself shows nothing it could change there, and the server refuses a sandbox's write.

import type { ReactNode } from "react";
import { isDemoSandbox } from "../../api/demoSandbox";
import { useAuth } from "../../context/AuthContext";
import type { Capability } from "../../types/auth";
import { CapabilityGate } from "../CapabilityGate";

export function SandboxReadableGate({
  capability,
  fallback,
  navigable,
  children,
}: {
  capability: Capability;
  fallback?: ReactNode;
  navigable?: boolean;
  children: ReactNode;
}) {
  const { activeOrgId } = useAuth();
  if (isDemoSandbox(activeOrgId)) return <>{children}</>;
  return (
    <CapabilityGate capability={capability} fallback={fallback} navigable={navigable}>
      {children}
    </CapabilityGate>
  );
}
