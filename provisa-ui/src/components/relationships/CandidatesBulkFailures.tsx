// Copyright (c) 2026 Kenneth Stott
// Canary: b6d1e9a3-2c58-4f70-8e4b-0a9c7f3d5e12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Alert, Text } from "@mantine/core";
import type { BulkFailures } from "./useCandidatesAtOnce";

/** REQ-413: the suggestions an Accept all or Reject all could not handle, each named. */
export function CandidatesBulkFailures({ failures }: { failures: BulkFailures | null }) {
  if (!failures) return null;
  return (
    <Alert color="red" mb="md" data-testid="rels-bulk-failures">
      <Text fz="0.875rem">{failures.heading}</Text>
      {failures.lines.map((line) => (
        <Text key={line} fz="0.875rem">
          {line}
        </Text>
      ))}
    </Alert>
  );
}
