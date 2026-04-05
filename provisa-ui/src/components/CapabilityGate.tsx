// Copyright (c) 2026 Kenneth Stott
// Canary: ed6e7a3c-eeee-4b5d-9e48-a8043b84a6fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ReactNode } from "react";
import { useCapability } from "../hooks/useCapability";
import type { Capability } from "../types/auth";

interface Props {
  capability: Capability;
  children: ReactNode;
  fallback?: ReactNode;
}

/** Only render children if current role has the required capability. */
export function CapabilityGate({ capability, children, fallback }: Props) {
  const allowed = useCapability(capability);
  if (!allowed) return fallback ?? null;
  return <>{children}</>;
}
