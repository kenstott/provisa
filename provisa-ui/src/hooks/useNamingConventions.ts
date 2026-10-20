// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-416: the GraphQL naming conventions a table, a source or the deployment may name. The
// server is their authority: the admin offers what it lists, and keeps no list of its own.

import { useQuery } from "@apollo/client/react";
import type { TFunction } from "i18next";
import { NamingConventions } from "./admin.graphql";

/** The conventions the server takes, in the order it gives them; [] until they are read. */
export function useNamingConventions(): string[] {
  const { data } = useQuery<{ namingConventions: string[] }>(NamingConventions, {
    fetchPolicy: "cache-first",
  });
  return data?.namingConventions ?? [];
}

const LABELS: Record<string, string> = {
  snake: "adminPage.namingConventionSnake",
  hasura_graphql: "adminPage.namingConventionHasura",
  apollo_graphql: "adminPage.namingConventionApollo",
};

/** How a convention the server names reads; one the admin has no words for reads as named. */
export function namingConventionLabel(t: TFunction, convention: string): string {
  return convention in LABELS ? t(LABELS[convention]) : convention;
}
