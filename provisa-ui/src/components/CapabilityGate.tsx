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
import { CredentialCheck } from "./CredentialCheck";
import { useCapability } from "../hooks/useCapability";
import { useAuth } from "../context/AuthContext";
import type { Capability } from "../types/auth";

interface Props {
  capability: Capability;
  children: ReactNode;
  fallback?: ReactNode;
}

/**
 * REQ-1430: a bootstrap in flight has no capabilities yet, which is not a denial. Rendering the
 * denial fallback while `loading` is true told the user they had lost access for as long as
 * /auth/me took to answer (tens of seconds against a cold coordinator), then swapped to the real
 * page when the roles landed — reading as a spontaneous reload. Say what is actually happening.
 */
export function CapabilityGate({ capability, children, fallback }: Props) {
  const { loading } = useAuth();
  const allowed = useCapability(capability);
  if (loading) return <CredentialCheck />;
  if (!allowed) return fallback ?? null;
  return <>{children}</>;
}
