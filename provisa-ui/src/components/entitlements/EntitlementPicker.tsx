// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the one shared picker for the values of an entitlement list. Domains, tables and
// columns are picked from a tree of the names table, where checking a node includes its children;
// roles from the org's roles; the other kinds are typed. * means everything of the kind, including
// what is added later. A listed value the tree does not hold (a pattern, or a name the model no
// longer has) is kept and shown, never dropped.

import { useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Checkbox,
  Group,
  Input,
  ScrollArea,
  Stack,
  TagsInput,
  Text,
  TextInput,
} from "@mantine/core";
import { ChevronDown, ChevronRight, X } from "lucide-react";
import { useDomains } from "../../hooks/useAdminQueries";
import { useTables } from "../../hooks/useTableQueries";
import { TREE_DEPTH, WILDCARD, type EntitlementKind } from "./kinds";
import {
  allIds,
  buildNamesTree,
  checkNode,
  covers,
  uncheckNode,
  type NamesNode,
} from "./namesTree";

export interface EntitlementPickerProps {
  kind: EntitlementKind;
  value: string[];
  onChange: (value: string[]) => void;
  /** The org's role ids, for a roles list. */
  roleIds: string[];
  /** Given by the FormField that places the picker. */
  label?: ReactNode;
  description?: ReactNode;
  testId?: string;
}

export function EntitlementPicker({
  kind,
  value,
  onChange,
  roleIds,
  label,
  description,
  testId = "entitlement-picker",
}: EntitlementPickerProps) {
  const { t } = useTranslation();
  const everything = value.includes(WILDCARD);
  const depth = TREE_DEPTH[kind];

  return (
    <Input.Wrapper
      label={label}
      description={description}
      inputWrapperOrder={["label", "input", "description"]}
    >
      <Stack gap="xs" data-testid={testId}>
        <Checkbox
          label={t("entitlements.picker.everything")}
          checked={everything}
          onChange={(e) => onChange(e.currentTarget.checked ? [WILDCARD] : [])}
          data-testid={`${testId}-everything`}
        />
        {!everything && depth !== undefined && (
          <NamesTreePicker depth={depth} value={value} onChange={onChange} testId={testId} />
        )}
        {!everything && kind === "roles" && (
          <RolesPicker roleIds={roleIds} value={value} onChange={onChange} testId={testId} />
        )}
        {!everything && depth === undefined && kind !== "roles" && (
          <TagsInput
            aria-label={t("entitlements.picker.typedLabel")}
            placeholder={t("entitlements.picker.typedPlaceholder")}
            value={value}
            onChange={onChange}
            data-testid={`${testId}-typed`}
          />
        )}
      </Stack>
    </Input.Wrapper>
  );
}

function OtherValues({
  values,
  onRemove,
  testId,
}: {
  values: string[];
  onRemove: (v: string) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  if (values.length === 0) return null;
  return (
    <Stack gap={4} data-testid={`${testId}-other`}>
      <Text size="xs" c="dimmed">
        {t("entitlements.picker.otherValues")}
      </Text>
      <Group gap={4}>
        {values.map((v) => (
          <Badge
            key={v}
            variant="light"
            color="gray"
            tt="none"
            rightSection={
              <ActionIcon
                size="xs"
                variant="transparent"
                aria-label={t("entitlements.picker.removeValue", { value: v })}
                onClick={() => onRemove(v)}
              >
                <X size={10} />
              </ActionIcon>
            }
          >
            {v}
          </Badge>
        ))}
      </Group>
    </Stack>
  );
}

function NamesTreePicker({
  depth,
  value,
  onChange,
  testId,
}: {
  depth: 1 | 2 | 3;
  value: string[];
  onChange: (v: string[]) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const { domains } = useDomains();
  const { tables } = useTables({ inPage: true });
  const roots = useMemo(() => buildNamesTree(domains, tables, depth), [domains, tables, depth]);
  const known = useMemo(() => allIds(roots), [roots]);
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState<Set<string>>(new Set());

  const toggleOpen = (id: string) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  const needle = filter.trim().toLowerCase();
  const shows = (n: NamesNode): boolean =>
    needle === "" || n.id.toLowerCase().includes(needle) || n.children.some(shows);

  const render = (nodes: NamesNode[], level: number) =>
    nodes.filter(shows).map((n) => {
      const listed = value.some((v) => covers(v, n.id));
      const byAncestor = listed && !value.some((v) => v.toLowerCase() === n.id.toLowerCase());
      const partly = !listed && value.some((v) => covers(n.id, v));
      const expanded = needle !== "" || open.has(n.id);
      return (
        <div key={n.id}>
          <Group gap={4} wrap="nowrap" style={{ paddingInlineStart: level * 20 }}>
            {n.children.length > 0 ? (
              <ActionIcon
                size="sm"
                variant="subtle"
                color="gray"
                aria-label={t(
                  expanded ? "entitlements.picker.collapse" : "entitlements.picker.expand",
                  {
                    name: n.label,
                  },
                )}
                aria-expanded={expanded}
                onClick={() => toggleOpen(n.id)}
                data-testid={`${testId}-toggle-${n.id}`}
              >
                {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              </ActionIcon>
            ) : (
              <span style={{ width: 22, display: "inline-block" }} />
            )}
            <Checkbox
              size="xs"
              label={n.label}
              checked={listed}
              indeterminate={partly}
              title={byAncestor ? t("entitlements.picker.includedByParent") : undefined}
              onChange={(e) =>
                onChange(
                  e.currentTarget.checked ? checkNode(value, n) : uncheckNode(value, n, roots),
                )
              }
              data-testid={`${testId}-node-${n.id}`}
            />
          </Group>
          {expanded && n.children.length > 0 && render(n.children, level + 1)}
        </div>
      );
    });

  const other = value.filter((v) => v !== WILDCARD && !known.has(v.toLowerCase()));

  return (
    <Stack gap="xs">
      <TextInput
        size="xs"
        aria-label={t("entitlements.picker.filterLabel")}
        placeholder={t("entitlements.picker.filterPlaceholder")}
        value={filter}
        onChange={(e) => setFilter(e.currentTarget.value)}
        data-testid={`${testId}-filter`}
      />
      <ScrollArea.Autosize mah={280} type="auto">
        {roots.length === 0 ? (
          <Text size="sm" c="dimmed">
            {t("entitlements.picker.noObjects")}
          </Text>
        ) : (
          render(roots, 0)
        )}
      </ScrollArea.Autosize>
      <OtherValues
        values={other}
        onRemove={(v) => onChange(value.filter((x) => x !== v))}
        testId={testId}
      />
    </Stack>
  );
}

function RolesPicker({
  roleIds,
  value,
  onChange,
  testId,
}: {
  roleIds: string[];
  value: string[];
  onChange: (v: string[]) => void;
  testId: string;
}) {
  const known = new Set(roleIds);
  const other = value.filter((v) => !known.has(v));
  return (
    <Stack gap="xs">
      <Checkbox.Group
        value={value.filter((v) => known.has(v))}
        onChange={(v) => onChange([...v, ...other])}
      >
        <Stack gap={4}>
          {[...roleIds].sort().map((id) => (
            <Checkbox
              key={id}
              size="xs"
              value={id}
              label={id}
              data-testid={`${testId}-role-${id}`}
            />
          ))}
        </Stack>
      </Checkbox.Group>
      <OtherValues
        values={other}
        onRemove={(v) => onChange(value.filter((x) => x !== v))}
        testId={testId}
      />
    </Stack>
  );
}
