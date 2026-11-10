// Copyright (c) 2026 Kenneth Stott
// Canary: d37115df-b0f8-44a6-9f18-65aa43a7fb06
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: a person's role grant on the Team page: each list by kind with its origin (the
// identity provider, read-only there; set in Provisa; or the role's default), superseded lists
// greyed, ended entries struck, and end dates; set and ended with a reason through the one write
// API. Where every list comes from the identity provider, the view is read-only and says why.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { Pencil, Plus, X } from "lucide-react";
import {
  endList,
  fetchEffective,
  fetchHeldLists,
  type AccessView,
  type EntitlementListView,
  type HeldLists,
  type PendingGrant,
} from "../../api/entitlements";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";
import { EntitlementListForm } from "./EntitlementListForm";
import { isKind, type EntitlementKind } from "./kinds";

const SHOWN_ENTRIES = 8;

type Mode =
  | { type: "add" }
  | { type: "edit"; kind: EntitlementKind }
  | { type: "end"; kind: string };

function isAccessView(r: AccessView | PendingGrant): r is AccessView {
  return "lists" in r;
}

/** The roles whose grant has lists Provisa holds. */
function heldRolesOf(held: HeldLists): string[] {
  return [...new Set(held.lists.map((l) => l.role_id))].sort();
}

/** The day (YYYY-MM-DD) of the rule that will end `kind`'s list, if one is still to come. */
function endsOn(view: AccessView, kind: string): string | null {
  const rule = view.expiries.find((e) => e.list_kind === kind && !e.passed);
  return rule ? rule.ends_at.slice(0, 10) : null;
}

export function PersonEntitlements({
  principal,
  roleIds,
}: {
  principal: string;
  roleIds: string[];
}) {
  const { t } = useTranslation();
  const [heldRoles, setHeldRoles] = useState<string[]>([]);
  const [roleId, setRoleId] = useState<string | null>(null);
  const [view, setView] = useState<AccessView | null>(null);
  const [mode, setMode] = useState<Mode | null>(null);
  const [endReason, setEndReason] = useState("");
  const [error, setError] = useState<string | null>(null);

  const report = useCallback(
    (e: unknown) => setError(e instanceof Error ? e.message : String(e)),
    [],
  );

  const loadHeld = useCallback(
    () =>
      fetchHeldLists(principal)
        .then((held) => setHeldRoles(heldRolesOf(held)))
        .catch(report),
    [principal, report],
  );

  const selectRole = useCallback(
    (next: string | null) => {
      setRoleId(next);
      setView(null);
      setMode(null);
      if (next !== null) fetchEffective(principal, next).then(setView).catch(report);
    },
    [principal, report],
  );

  // A person's panel is mounted afresh for each person, so nothing of the last one's is kept. It
  // opens on the first role grant with lists set in Provisa.
  useEffect(() => {
    fetchHeldLists(principal)
      .then((held) => {
        const roles = heldRolesOf(held);
        setHeldRoles(roles);
        if (roles.length > 0) selectRole(roles[0]);
      })
      .catch(report);
  }, [principal, selectRole, report]);

  const saved = (result: AccessView | PendingGrant) => {
    if (isAccessView(result)) setView(result);
    setMode(null);
    setEndReason("");
    setError(null);
    loadHeld();
  };

  const end = async (kind: string) => {
    if (roleId === null) return;
    try {
      saved(await endList({ grantee: { principal }, roleId, kind, reason: endReason.trim() }));
    } catch (e) {
      report(e);
    }
  };

  const allFromIdp =
    view !== null && view.lists.length > 0 && view.lists.every((l) => l.origin === "idp");
  const editable = view !== null && !allFromIdp;
  const taken = new Set((view?.lists ?? []).filter((l) => l.origin !== "role").map((l) => l.kind));
  // Roles with lists Provisa holds come first; any role can be looked at.
  const roleOptions = [
    ...heldRoles.map((r) => ({
      value: r,
      label: t("entitlements.person.roleWithLists", { role: r }),
    })),
    ...[...roleIds]
      .sort()
      .filter((r) => !heldRoles.includes(r))
      .map((r) => ({ value: r, label: r })),
  ];

  return (
    <Stack gap="sm" data-testid="person-entitlements">
      <Text size="sm" c="dimmed">
        {t("entitlements.person.help")}
      </Text>
      {error && (
        <Alert
          color="red"
          withCloseButton
          onClose={() => setError(null)}
          data-testid="person-entitlements-error"
        >
          {error}
        </Alert>
      )}
      <FormLayout>
        <FormField size="medium" hint={t("entitlements.person.roleHint")}>
          <Select
            label={t("entitlements.person.roleLabel")}
            placeholder={t("entitlements.person.rolePlaceholder")}
            data={roleOptions}
            value={roleId}
            onChange={selectRole}
            searchable
            data-testid="person-entitlements-role"
          />
        </FormField>
      </FormLayout>

      {view?.grant_ended && (
        <Alert color="gray" data-testid="person-entitlements-grant-ended">
          {t("entitlements.person.grantEnded")}
        </Alert>
      )}
      {allFromIdp && (
        <Alert color="blue" data-testid="person-entitlements-read-only">
          {t("entitlements.person.readOnly")}
        </Alert>
      )}

      {view !== null && (
        <Table withRowBorders data-testid="person-entitlements-lists">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("entitlements.person.colKind")}</Table.Th>
              <Table.Th>{t("entitlements.person.colEntries")}</Table.Th>
              <Table.Th>{t("entitlements.person.colOrigin")}</Table.Th>
              <Table.Th>{t("entitlements.person.colEnds")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {view.lists.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={5}>
                  <Text size="sm" c="dimmed">
                    {t("entitlements.person.noLists")}
                  </Text>
                </Table.Td>
              </Table.Tr>
            )}
            {view.lists.map((l) => (
              <ListRow
                key={l.kind}
                list={l}
                endsOn={endsOn(view, l.kind)}
                editable={editable && l.origin !== "idp" && isKind(l.kind)}
                endable={editable && l.origin === "provisa"}
                onEdit={() => isKind(l.kind) && setMode({ type: "edit", kind: l.kind })}
                onEnd={() => {
                  setEndReason("");
                  setMode({ type: "end", kind: l.kind });
                }}
              />
            ))}
          </Table.Tbody>
        </Table>
      )}

      {view !== null && view.findings.length > 0 && (
        <Alert color="yellow" data-testid="person-entitlements-findings">
          <Stack gap={2}>
            {view.findings.map((f) => (
              <Text size="sm" key={`${f.kind}:${f.identifier}:${f.code}`}>
                {t(`entitlements.finding.${f.code}`, {
                  kind: t(`entitlements.kind.${f.kind}`),
                  identifier: f.identifier,
                })}
              </Text>
            ))}
          </Stack>
        </Alert>
      )}

      {editable && roleId !== null && mode?.type === "edit" && (
        <EntitlementListForm
          key={`edit-${mode.kind}`}
          grantee={{ principal }}
          roleId={roleId}
          kind={mode.kind}
          identifiers={view.lists.find((l) => l.kind === mode.kind)?.identifiers ?? []}
          endsOn={endsOn(view, mode.kind)}
          roleIds={roleIds}
          onSaved={saved}
          onCancel={() => setMode(null)}
          onError={report}
          testId="person-entitlements-form"
        />
      )}
      {editable && roleId !== null && mode?.type === "add" && (
        <EntitlementListForm
          key="add"
          grantee={{ principal }}
          roleId={roleId}
          takenKinds={taken}
          roleIds={roleIds}
          onSaved={saved}
          onCancel={() => setMode(null)}
          onError={report}
          testId="person-entitlements-form"
        />
      )}
      {editable && mode?.type === "end" && (
        <FormLayout data-testid="person-entitlements-end">
          <Text size="sm">
            {t("entitlements.person.endPrompt", { kind: t(`entitlements.kind.${mode.kind}`) })}
          </Text>
          <FormField size="long" hint={t("entitlements.form.reasonHint")}>
            <TextInput
              label={t("entitlements.form.reasonLabel")}
              required
              value={endReason}
              onChange={(e) => setEndReason(e.currentTarget.value)}
              data-testid="person-entitlements-end-reason"
            />
          </FormField>
          <FormFooter
            onCancel={() => setMode(null)}
            cancelProps={{ "data-testid": "person-entitlements-end-cancel" }}
            action={
              <Button
                color="red"
                disabled={endReason.trim() === ""}
                onClick={() => end(mode.kind)}
                data-testid="person-entitlements-end-submit"
              >
                {t("entitlements.person.endButton")}
              </Button>
            }
          />
        </FormLayout>
      )}
      {editable && mode === null && (
        <Group>
          <Button
            variant="default"
            size="xs"
            leftSection={<Plus size={14} />}
            onClick={() => setMode({ type: "add" })}
            data-testid="person-entitlements-add"
          >
            {t("entitlements.person.addList")}
          </Button>
        </Group>
      )}
    </Stack>
  );
}

const ORIGIN_COLOR: Record<string, string> = { idp: "violet", provisa: "blue", role: "gray" };

function ListRow({
  list,
  endsOn: ends,
  editable,
  endable,
  onEdit,
  onEnd,
}: {
  list: EntitlementListView;
  endsOn: string | null;
  editable: boolean;
  endable: boolean;
  onEdit: () => void;
  onEnd: () => void;
}) {
  const { t } = useTranslation();
  const kindLabel = t(`entitlements.kind.${list.kind}`);
  const shown = list.identifiers.slice(0, SHOWN_ENTRIES);
  const more = list.identifiers.length - shown.length;
  return (
    <Table.Tr data-testid={`person-list-${list.kind}`}>
      <Table.Td>
        <Text size="sm">{kindLabel}</Text>
      </Table.Td>
      <Table.Td>
        <Stack gap={4}>
          <Group gap={4}>
            {list.identifiers.length === 0 && (
              <Text size="sm" c="dimmed">
                {t("entitlements.person.nothing")}
              </Text>
            )}
            {shown.map((i) => (
              <Badge key={i} variant="light" tt="none" ff="monospace">
                {i === "*" ? t("entitlements.person.everything") : i}
              </Badge>
            ))}
            {more > 0 && (
              <Tip text={list.identifiers.slice(SHOWN_ENTRIES).join(", ")}>
                <Text size="xs" c="dimmed">
                  {t("entitlements.person.more", { count: more })}
                </Text>
              </Tip>
            )}
            {list.ended.map((i) => (
              <Badge
                key={`ended-${i}`}
                variant="outline"
                color="gray"
                tt="none"
                ff="monospace"
                td="line-through"
                title={t("entitlements.person.endedHint")}
                data-testid={`person-list-${list.kind}-ended-${i}`}
              >
                {i}
              </Badge>
            ))}
          </Group>
          {list.superseded !== null && (
            <Text
              size="xs"
              c="dimmed"
              td="line-through"
              title={t("entitlements.person.supersededHint")}
              data-testid={`person-list-${list.kind}-superseded`}
            >
              {t("entitlements.person.superseded", {
                entries:
                  list.superseded.length === 0
                    ? t("entitlements.person.nothing")
                    : list.superseded.join(", "),
              })}
            </Text>
          )}
          {list.reason && (
            <Text size="xs" c="dimmed">
              {t("entitlements.person.setBy", { who: list.set_by ?? "—", reason: list.reason })}
            </Text>
          )}
        </Stack>
      </Table.Td>
      <Table.Td>
        <Badge
          variant="light"
          color={ORIGIN_COLOR[list.origin]}
          data-testid={`person-list-${list.kind}-origin`}
        >
          {t(`entitlements.origin.${list.origin}`)}
        </Badge>
      </Table.Td>
      <Table.Td>
        <Text size="sm">{ends ?? "—"}</Text>
      </Table.Td>
      <Table.Td>
        <Group gap={4} wrap="nowrap">
          {editable && (
            <ActionIcon
              variant="subtle"
              aria-label={t("entitlements.person.editList", { kind: kindLabel })}
              onClick={onEdit}
              data-testid={`person-list-${list.kind}-edit`}
            >
              <Pencil size={14} />
            </ActionIcon>
          )}
          {endable && (
            <ActionIcon
              variant="subtle"
              color="red"
              aria-label={t("entitlements.person.endList", { kind: kindLabel })}
              onClick={onEnd}
              data-testid={`person-list-${list.kind}-end`}
            >
              <X size={14} />
            </ActionIcon>
          )}
        </Group>
      </Table.Td>
    </Table.Tr>
  );
}
