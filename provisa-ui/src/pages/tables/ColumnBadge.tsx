// Copyright (c) 2026 Kenneth Stott
// Canary: c7d4f84c-f3bc-4341-a086-69d120615bed
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ReactNode } from "react";
import { Badge } from "@mantine/core";

/** A small monospace badge beside a column's name in the table editor (PK/FK/AK, path/query
 * parameter, implicit measure/dimension). */
export function ColumnBadge({ color, children }: { color: string; children: ReactNode }) {
  return (
    <Badge ml={6} size="xs" variant="light" color={color} style={{ fontFamily: "monospace" }}>
      {children}
    </Badge>
  );
}
