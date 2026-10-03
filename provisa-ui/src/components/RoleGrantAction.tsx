// Copyright (c) 2026 Kenneth Stott
// Canary: 504a4cae-9e7b-4442-89df-bad55f9b8eea
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1918: what a role's delete dialog offers on each dependent it can deal with in place —
// a table's column grants, a metric's, command's or webhook's assigned roles, an assignment.
// Whatever needs a replacement chosen (a data product's owner, a domain's steward, a parent
// role) keeps only its page link.

import { useState } from "react";
import { Button, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { Dependent } from "../lib/dependents";
import type { GrantKind } from "../hooks/useSecurityQueries";
import { serverMessage } from "../i18n/serverMessage";
import type { MutationResult } from "../types/admin";

const OBJECT_KINDS: Record<string, GrantKind> = {
  metric: "METRIC",
  command: "COMMAND",
  webhook: "WEBHOOK",
};

export interface RoleGrantRevokers {
  revokeFromTable: (roleId: string, tableId: number) => Promise<MutationResult>;
  revokeFromObject: (roleId: string, kind: GrantKind, name: string) => Promise<MutationResult>;
  removeAssignment: (userId: string, assignmentId: number) => Promise<void>;
}

interface Props extends RoleGrantRevokers {
  roleId: string;
  dependent: Dependent;
  all: Dependent[];
  handled: (gone: (d: Dependent) => boolean) => void;
}

/** The plan for one dependent: what it does and which listed dependents it settles, or null. */
function planFor(
  props: Props,
): { label: string; run: () => Promise<MutationResult | void>; settles: (d: Dependent) => boolean } | null {
  const { roleId, dependent: d, all } = props;
  if (d.kind === "column" && d.table_id !== undefined) {
    // One action per table: shown on its first column grant, settling every one of them.
    const first = all.find((x) => x.kind === "column" && x.table_id === d.table_id);
    if (first !== d) return null;
    const tableId = d.table_id;
    return {
      label: "dependentsDialog.removeTableGrants",
      run: () => props.revokeFromTable(roleId, tableId),
      settles: (x) => x.kind === "column" && x.table_id === tableId,
    };
  }
  const kind = OBJECT_KINDS[d.kind];
  if (kind) {
    return {
      label: "dependentsDialog.removeGrant",
      run: () => props.revokeFromObject(roleId, kind, String(d.id)),
      settles: (x) => x.kind === d.kind && x.id === d.id,
    };
  }
  if (d.kind === "role_assignment" && d.user_id !== undefined) {
    const userId = d.user_id;
    return {
      label: "dependentsDialog.removeAssignment",
      run: () => props.removeAssignment(userId, Number(d.id)),
      settles: (x) => x.kind === d.kind && x.id === d.id,
    };
  }
  return null;
}

export function RoleGrantAction(props: Props) {
  const { t } = useTranslation();
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState<string | null>(null);
  const plan = planFor(props);
  if (plan === null) return null;
  const go = async () => {
    setBusy(true);
    setFailed(null);
    try {
      const result = await plan.run();
      if (result && !result.success) {
        setFailed(serverMessage(result, result.message));
        return;
      }
      props.handled(plan.settles);
    } catch (e) {
      setFailed(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      <Button size="compact-xs" variant="light" loading={busy} onClick={go}>
        {t(plan.label)}
      </Button>
      {failed && (
        <Text size="xs" c="red">
          {failed}
        </Text>
      )}
    </>
  );
}
