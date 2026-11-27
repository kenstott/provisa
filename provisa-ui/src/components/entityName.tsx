// Copyright (c) 2026 Kenneth Stott
// Canary: ec8b2492-d65c-4fd8-a9be-cda2d2ba4a72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ReactNode } from "react";
import type { EntityKind } from "../lib/entityHref";
import { EntityLink } from "./EntityLink";

/**
 * REQ-2277 rule 6: the object `kind` `id` named by its id, as its link; null where none is set, so a
 * detail field shows its one empty mark (DetailFields) and a list cell its own.
 */
export function entityName(
  kind: EntityKind,
  id: string | null | undefined,
  testId?: string,
): ReactNode {
  if (id === null || id === undefined || id === "") return null;
  return (
    <EntityLink kind={kind} id={id} testId={testId}>
      {id}
    </EntityLink>
  );
}
