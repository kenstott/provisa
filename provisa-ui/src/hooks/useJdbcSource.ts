// Copyright (c) 2026 Kenneth Stott
// Canary: 38e05168-4e29-4410-a532-c6a6502bf6b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2191: a JDBC source's setup (the engine its capabilities are set for, the standard drivers,
// the SQL dialects it may be written in) and its Detect checks, which run only when the admin
// presses Detect.

import { gql } from "@apollo/client";
import { useMutation, useQuery } from "@apollo/client/react";

const JDBC_SOURCE_SETUP = gql`
  query JdbcSourceSetup {
    jdbcSourceSetup {
      engine
      drivers {
        backend
        sourceType
        scheme
        file
        attach
      }
      sqlDialects
      capabilities
    }
  }
`;

const DETECT_JDBC_SOURCE = gql`
  mutation DetectJdbcSource($input: JdbcSourceDetectInput!) {
    detectJdbcSource(input: $input) {
      engine
      checks {
        capability
        passed
        reason
      }
    }
  }
`;

/** A standard driver the form offers (provisa/api/admin/jdbc_source_admin.py). */
export interface JdbcDriver {
  backend: string;
  /** The built-in source type of the same database, which names it. */
  sourceType: string;
  /** The URL scheme the form writes for it. */
  scheme: string;
  /** Whether its URL names a file rather than a host. */
  file: boolean;
  /** Whether the active engine attaches the built-in type, so live attach is offered. */
  attach: boolean;
}

export interface JdbcSourceSetup {
  engine: string;
  drivers: JdbcDriver[];
  sqlDialects: string[];
  capabilities: string[];
}

export interface JdbcSourceCheck {
  capability: string;
  passed: boolean;
  reason: string;
}

export interface JdbcSourceDetection {
  engine: string;
  checks: JdbcSourceCheck[];
}

export interface JdbcSourceDetectInput {
  url: string;
  sqlDialect: string;
  username: string;
  password: string;
  jdbcDriverJar: string;
  jdbcDriverClass: string;
}

export function useJdbcSourceSetup(skip: boolean) {
  const { data, error } = useQuery<{ jdbcSourceSetup: JdbcSourceSetup }>(JDBC_SOURCE_SETUP, {
    skip,
  });
  return { setup: data?.jdbcSourceSetup ?? null, error };
}

export function useDetectJdbcSource() {
  const [detect, { loading }] = useMutation<{ detectJdbcSource: JdbcSourceDetection }>(
    DETECT_JDBC_SOURCE,
  );
  return {
    detect: async (input: JdbcSourceDetectInput) => {
      const result = await detect({ variables: { input } });
      if (!result.data) throw new Error("detectJdbcSource answered nothing");
      return result.data.detectJdbcSource;
    },
    loading,
  };
}
