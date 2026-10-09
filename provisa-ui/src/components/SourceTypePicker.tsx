// Copyright (c) 2026 Kenneth Stott
// Canary: 7d4710c2-c345-438f-893e-6f33b502539f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1938: the source-type picker. The add-source form's type list is the same grouped list the
// select shows (its reach labels and gating included), laid out as a fluid set of category columns
// that fill the dialog's width, with a search box and category chips that filter in place.
import { useMemo, useRef, useState } from "react";
import { Chip, Group, Modal, Stack, Text, TextInput, UnstyledButton } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Ban, Copy, Zap } from "lucide-react";
import { SourceLogo } from "./SourceLogo";

export interface PickerItem {
  value: string;
  label: string;
  disabled?: boolean;
  /** The plain name, shown with ``reach`` as an icon; ``label`` carries the select's text suffix. */
  name?: string;
  reach?: { tag: "live" | "replica" | "unreachable"; liveEngines: string[] };
}

export interface PickerGroup {
  group: string;
  items: PickerItem[];
}

const RECENT_KEY = "provisa.sourceTypePicker.recent";
const RECENT_MAX = 6;

// Other names a reader may type for a source type.
const ALIASES: Record<string, string> = {
  postgresql: "postgres pg",
  sqlserver: "mssql microsoft azure sql",
  bigquery: "google gcp",
  redshift: "amazon aws",
  synapse: "microsoft azure",
  fabric: "microsoft onelake",
  cassandra: "apache",
  kafka: "apache",
  druid: "apache",
  pinot: "apache",
  hive: "apache",
  hiveserver2: "apache hive",
  google_sheets: "spreadsheet",
  // REQ-1923: what a person looking for their mail types.
  google_workspace: "gmail google mail email calendar tasks g suite gsuite",
  microsoft_365: "exchange outlook office 365 o365 microsoft mail email calendar tasks to do",
  openapi: "rest swagger",
  elasticsearch: "opensearch",
  saphana: "sap hana",
};

function readRecent(): string[] {
  try {
    const raw = window.localStorage.getItem(RECENT_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter((v): v is string => typeof v === "string") : [];
  } catch {
    // A per-viewer convenience only: blocked or cleared storage just means no recent row.
    return [];
  }
}

function writeRecent(value: string): void {
  try {
    const next = [value, ...readRecent().filter((v) => v !== value)].slice(0, RECENT_MAX);
    window.localStorage.setItem(RECENT_KEY, JSON.stringify(next));
  } catch {
    // Same: remembering the pick is a convenience and never blocks it.
  }
}

function matches(item: PickerItem, query: string): boolean {
  if (!query) return true;
  const hay = `${item.label} ${item.value} ${ALIASES[item.value] ?? ""}`.toLowerCase();
  return query
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .every((term) => hay.includes(term));
}

interface PickerProps {
  opened: boolean;
  onClose: () => void;
  groups: PickerGroup[];
  value: string;
  onPick: (value: string) => void;
}

export function SourceTypePicker(props: PickerProps) {
  const { t } = useTranslation();
  return (
    <Modal
      opened={props.opened}
      onClose={props.onClose}
      size="90%"
      title={t("sourceTypePicker.title")}
      closeButtonProps={{ "data-testid": "source-type-picker-close" } as object}
      data-testid="source-type-picker"
    >
      {/* Mounted only while open, so every opening starts with an empty search and "All". */}
      {props.opened && <PickerBody {...props} />}
    </Modal>
  );
}

function PickerBody({ onClose, groups, value, onPick }: PickerProps) {
  const { t } = useTranslation();
  const reachText = (r: NonNullable<PickerItem["reach"]>) =>
    r.tag === "live"
      ? t("sourceTypePicker.reachLive")
      : r.tag === "replica"
        ? t("sourceTypePicker.reachReplica")
        : r.liveEngines.length
          ? t("sourceTypePicker.reachLiveOn", { engines: r.liveEngines.join(", ") })
          : t("sourceTypePicker.reachNone");
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState<string>("__all__");
  const [recent] = useState<string[]>(readRecent);
  const bodyRef = useRef<HTMLDivElement>(null);

  const categoryOf = useMemo(() => {
    const out: Record<string, string> = {};
    for (const g of groups) for (const i of g.items) out[i.value] = g.group;
    return out;
  }, [groups]);

  const visible = useMemo(
    () =>
      groups
        .filter((g) => category === "__all__" || g.group === category)
        .map((g) => ({ ...g, items: g.items.filter((i) => matches(i, query)) }))
        .filter((g) => g.items.length > 0),
    [groups, category, query],
  );

  const recentItems = useMemo(() => {
    if (query || category !== "__all__") return [];
    const byValue: Record<string, PickerItem> = {};
    for (const g of groups) for (const i of g.items) byValue[i.value] = i;
    return recent.map((v) => byValue[v]).filter((i): i is PickerItem => !!i && !i.disabled);
  }, [recent, groups, query, category]);

  const pick = (item: PickerItem) => {
    if (item.disabled) return;
    writeRecent(item.value);
    onPick(item.value);
    onClose();
  };

  // Up/Down move between rows in reading order; Enter picks (the rows are buttons).
  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
    const rows = Array.from(
      bodyRef.current?.querySelectorAll<HTMLButtonElement>(
        "button[data-picker-row]:not([disabled])",
      ) ?? [],
    );
    if (rows.length === 0) return;
    e.preventDefault();
    const at = rows.indexOf(document.activeElement as HTMLButtonElement);
    const next = e.key === "ArrowDown" ? Math.min(at + 1, rows.length - 1) : Math.max(at - 1, 0);
    rows[at === -1 ? 0 : next].focus();
  };

  const row = (item: PickerItem, group: string) => (
    <UnstyledButton
      key={`${group}:${item.value}`}
      data-picker-row
      data-testid={`source-type-option-${item.value}`}
      disabled={item.disabled}
      onClick={() => pick(item)}
      title={`${item.name ?? item.label}${item.reach ? ` — ${reachText(item.reach)}` : ""}`}
      aria-current={item.value === value ? "true" : undefined}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        width: "100%",
        padding: "4px 8px",
        borderRadius: 6,
        opacity: item.disabled ? 0.45 : 1,
        cursor: item.disabled ? "not-allowed" : "pointer",
        background: item.value === value ? "var(--mantine-color-blue-light)" : undefined,
      }}
      className="source-type-picker-row"
    >
      <SourceLogo
        type={item.value}
        label={item.name ?? item.label}
        category={categoryOf[item.value] ?? group}
      />
      <Text size="sm" truncate style={{ flex: 1 }}>
        {item.name ?? item.label}
      </Text>
      {item.reach && <ReachIcon reach={item.reach} label={reachText(item.reach)} />}
    </UnstyledButton>
  );

  return (
    <Stack gap="sm" onKeyDown={onKeyDown}>
      <TextInput
        data-autofocus
        value={query}
        onChange={(e) => setQuery(e.currentTarget.value)}
        placeholder={t("sourceTypePicker.searchPlaceholder")}
        aria-label={t("sourceTypePicker.searchPlaceholder")}
        data-testid="source-type-picker-search"
      />
      <Chip.Group multiple={false} value={category} onChange={(v) => setCategory(v as string)}>
        <Group gap={6}>
          <Chip size="xs" value="__all__" data-testid="source-type-picker-cat-all">
            {t("sourceTypePicker.all")}
          </Chip>
          {groups.map((g) => (
            <Chip
              size="xs"
              key={g.group}
              value={g.group}
              data-testid={`source-type-picker-cat-${g.group}`}
            >
              {g.group} ({g.items.length})
            </Chip>
          ))}
        </Group>
      </Chip.Group>
      <Group gap={14} data-testid="source-type-picker-legend">
        <Group gap={4}>
          <Zap size={13} aria-hidden="true" color="var(--mantine-color-green-6)" />
          <Text size="xs" c="dimmed">
            {t("sourceTypePicker.reachLive")}
          </Text>
        </Group>
        <Group gap={4}>
          <Copy size={13} aria-hidden="true" color="var(--mantine-color-blue-6)" />
          <Text size="xs" c="dimmed">
            {t("sourceTypePicker.reachReplica")}
          </Text>
        </Group>
      </Group>
      <div ref={bodyRef} data-testid="source-type-picker-body">
        {recentItems.length > 0 && (
          <div style={{ marginBottom: 12 }}>
            <Text size="xs" fw={700} c="dimmed" tt="uppercase" mb={4}>
              {t("sourceTypePicker.recent")}
            </Text>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
                gap: 2,
              }}
            >
              {recentItems.map((i) => row(i, "recent"))}
            </div>
          </div>
        )}
        {visible.length === 0 ? (
          <Text size="sm" c="dimmed" data-testid="source-type-picker-empty">
            {t("sourceTypePicker.noMatch")}
          </Text>
        ) : (
          <div style={{ columnWidth: 220, columnGap: 24 }}>
            {visible.map((g) => (
              <section
                key={g.group}
                style={{ breakInside: "avoid", marginBottom: 12 }}
                data-testid={`source-type-picker-section-${g.group}`}
              >
                <Text size="xs" fw={700} c="dimmed" tt="uppercase" mb={4}>
                  {g.group}
                </Text>
                {g.items.map((i) => row(i, g.group))}
              </section>
            ))}
          </div>
        )}
      </div>
    </Stack>
  );
}

// REQ-1938: how the current engine reaches a type, as a small icon (its text is the tooltip and
// the accessible name): live in place, through a replica, or not from this engine.
function ReachIcon({ reach, label }: { reach: NonNullable<PickerItem["reach"]>; label: string }) {
  const common = { size: 13, "aria-label": label, role: "img" } as const;
  const testId = `source-reach-${reach.tag}`;
  if (reach.tag === "live")
    return <Zap {...common} data-testid={testId} color="var(--mantine-color-green-6)" />;
  if (reach.tag === "replica")
    return <Copy {...common} data-testid={testId} color="var(--mantine-color-blue-6)" />;
  return <Ban {...common} data-testid={testId} color="var(--mantine-color-gray-6)" />;
}
