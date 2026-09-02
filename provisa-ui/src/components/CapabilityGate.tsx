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
import { useAuth } from "../context/AuthContext";
import { DemonstratedFeature } from "./DemonstratedFeature";
import { isDemonstrated, meetsRequirement, type CapabilityRequirement } from "../lib/capabilities";

interface Props extends CapabilityRequirement {
  children: ReactNode;
  fallback?: ReactNode;
  /**
   * REQ-1602: this gate wraps a LINK into the region it guards, not the region itself. A right the
   * role is only shown leaves the link clickable, because the page it opens carries the
   * demonstration -- a nav entry that refuses its own click demonstrates nothing.
   */
  navigable?: boolean;
}

/**
 * REQ-1430: a bootstrap in flight has no capabilities yet, which is not a denial. Rendering the
 * denial fallback while `loading` is true told the user they had lost access for as long as
 * /auth/me took to answer (tens of seconds against a cold coordinator), then swapped to the real
 * page when the roles landed — reading as a spontaneous reload. Say what is actually happening.
 */
export function CapabilityGate({
  capability,
  strict,
  orCapability,
  children,
  fallback,
  navigable,
}: Props) {
  const { loading, capabilities, demonstrated } = useAuth();
  const req = { capability, strict, orCapability };
  const allowed = meetsRequirement(capabilities, req);
  // A gate with a fallback owns a whole region — a route body — and has to show something there.
  // A gate without one contributes a nav link or a button to a larger layout: it renders nothing
  // when denied, so it renders nothing while loading too. Otherwise every inline gate on the page
  // paints its own centred 60vh spinner and the layout fills with them.
  if (loading) return fallback === undefined ? null : <CredentialCheck />;
  // REQ-1602: a right the role is shown rather than given keeps its surface on the page, inert and
  // badged. The `fallback` distinction carries over -- a gate that owns a region demonstrates a
  // region, an inline gate demonstrates a control.
  if (!allowed && isDemonstrated(demonstrated, req)) {
    return (
      <DemonstratedFeature
        block={fallback !== undefined}
        navigable={navigable}
        capability={capability}
      >
        {children}
      </DemonstratedFeature>
    );
  }
  if (!allowed) return fallback ?? null;
  return <>{children}</>;
}
