// Copyright (c) 2026 Kenneth Stott
// Canary: 38e05168-4e29-4410-a532-c6a6502bf6b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2191: a Generic SQL source's setup (the engine its capabilities are set for, the SQL dialects
// it may be written in) and its Detect checks, which run only when the admin presses Detect.

import { gql } from "@apollo/client";
import { useMutation, useQuery } from "@apollo/client/react";

const GENERIC_SQL_SETUP = gql`
  query GenericSqlSetup {
    genericSqlSetup {
      engine
      sqlDialects
      capabilities
    }
  }
`;

const DETECT_GENERIC_SQL = gql`
  mutation DetectGenericSql($input: GenericSqlDetectInput!) {
    detectGenericSql(input: $input) {
      engine
      checks {
        capability
        passed
        reason
      }
    }
  }
`;

export interface GenericSqlSetup {
  engine: string;
  sqlDialects: string[];
  capabilities: string[];
}

export interface GenericSqlCheck {
  capability: string;
  passed: boolean;
  reason: string;
}

export interface GenericSqlDetection {
  engine: string;
  checks: GenericSqlCheck[];
}

export interface GenericSqlDetectInput {
  url: string;
  sqlDialect: string;
  username: string;
  password: string;
  jdbcDriverJar: string;
  jdbcDriverClass: string;
}

export function useGenericSqlSetup(skip: boolean) {
  const { data, error } = useQuery<{ genericSqlSetup: GenericSqlSetup }>(GENERIC_SQL_SETUP, {
    skip,
  });
  return { setup: data?.genericSqlSetup ?? null, error };
}

export function useDetectGenericSql() {
  const [detect, { loading }] = useMutation<{ detectGenericSql: GenericSqlDetection }>(
    DETECT_GENERIC_SQL,
  );
  return {
    detect: async (input: GenericSqlDetectInput) => {
      const result = await detect({ variables: { input } });
      if (!result.data) throw new Error("detectGenericSql answered nothing");
      return result.data.detectGenericSql;
    },
    loading,
  };
}
