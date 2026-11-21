// Copyright (c) 2026 Kenneth Stott
// Canary: 9b49081f-7932-46bc-8256-55d7af6d42fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { Anchor } from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { entityLink, type EntityKind } from "../lib/entityHref";

/** REQ-2259: a link to another page's thing, drawn only when the viewer holds that page's right;
 * otherwise its name as plain text. */
export function EntityLink({
  kind,
  id,
  table,
  children,
  testId,
}: {
  kind: EntityKind;
  id: string | number;
  table?: number;
  children: ReactNode;
  testId?: string;
}) {
  const { capabilities } = useAuth();
  const href = entityLink(capabilities, kind, id, table);
  if (href === undefined) return <span data-testid={testId}>{children}</span>;
  return (
    <Anchor component={Link} to={href} data-testid={testId}>
      {children}
    </Anchor>
  );
}
