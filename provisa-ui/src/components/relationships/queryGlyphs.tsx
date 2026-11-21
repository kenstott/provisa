// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6d8b23-a947-4c5e-b302-8e7a14c9d6f0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252: each query surface's glyph, as every query link draws it (a relationship's and a
// table's).

import type { ReactNode } from "react";
import {
  Braces,
  Cable,
  FileJson,
  Globe,
  Share2,
  Table as TableIcon,
  Waypoints,
} from "lucide-react";
import type { RelationshipSurface } from "./relationshipQueries";

export const GLYPH: Record<RelationshipSurface, ReactNode> = {
  cypher: <Share2 size={14} />,
  sql: <TableIcon size={14} />,
  sparql: <Waypoints size={14} />,
  graphql: <Braces size={14} />,
  rest: <Globe size={14} />,
  grpc: <Cable size={14} />,
  jsonapi: <FileJson size={14} />,
};
