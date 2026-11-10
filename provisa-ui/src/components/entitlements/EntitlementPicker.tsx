// Copyright (c) 2026 Kenneth Stott
// Canary: c0bab01e-642f-42a2-b2a5-4f9ef8f83e6b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the one shared picker for the values of an entitlement list. Domains, tables and
// columns are picked in cascading searchable pickers, one per level of the kind's names tree:
// domains; the tables of the chosen domains (of all domains when none is chosen); the columns of
// the chosen tables. A domain or table picked with nothing chosen beneath it stands for all of it.
// Roles are picked from the org's roles; the other kinds are typed. * means everything of the
// kind, including what is added later: it is ticked on the label's line, and the pickers beneath
// it are then disabled. A listed value the tree does not hold (a pattern, or a name the model no
// longer has) is kept and shown flagged, never dropped.

import { useId, useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Checkbox,
  CheckIcon,
  Combobox,
  Group,
  Input,
  Pill,
  ScrollArea,
  Stack,
  TagsInput,
  Text,
  TextInput,
  useCombobox,
} from "@mantine/core";
import { TriangleAlert, X } from "lucide-react";
import type { KindInfo } from "../../api/entitlements";
import { Tip } from "../Tip";
import {
  choicesAt,
  dropUnknown,
  filterChoices,
  indexNames,
  levelOfUnknown,
  selectionOf,
  toggle,
  valueOf,
  type ChoiceGroup,
  type NamesIndex,
  type Selection,
} from "./cascade";
import { WILDCARD } from "./kinds";
import { toDepth, type NamesNode } from "./namesTree";

/** The most choices a picker's list draws at once; a search narrows to the rest. */
export const PICKER_LIMIT = 200;

const LEVELS = ["domains", "tables", "columns"] as const;

export interface EntitlementPickerProps {
  /** The kind, as the server defines it (GET /admin/entitlements/kinds). */
  kind: KindInfo;
  /** The server's names tree; null before a model is built. */
  names: NamesNode[] | null;
  value: string[];
  onChange: (value: string[]) => void;
  /** The org's role ids, for a roles list. */
  roleIds: string[];
  /** Given by the FormField that places the picker. */
  label?: ReactNode;
  description?: ReactNode;
  "aria-labelledby"?: string;
  "aria-description"?: string;
  testId?: string;
}

export function EntitlementPicker({
  kind,
  names,
  value,
  onChange,
  roleIds,
  label,
  description,
  "aria-labelledby": labelledBy,
  "aria-description": ariaDescription,
  testId = "entitlement-picker",
}: EntitlementPickerProps) {
  const { t } = useTranslation();
  const everything = value.includes(WILDCARD);
  const depth = kind.tree_depth;
  const labelId = useId();
  // What the names pickers held when * was ticked, so unticking it gives the choice back.
  const [held, setHeld] = useState<string[]>([]);

  return (
    <Stack
      gap={4}
      role="group"
      aria-labelledby={labelledBy ?? labelId}
      aria-description={ariaDescription}
      data-testid={testId}
    >
      {/* The label's line: the label (and its (?)) at the start, everything at the end, so the
          first picker is level with the controls beside the field. */}
      <Group justify="space-between" wrap="nowrap" gap="sm">
        <span className="form-label-standalone" id={labelId}>
          {label}
        </span>
        <Checkbox
          size="xs"
          label={t("entitlements.picker.everything")}
          checked={everything}
          onChange={(e) => {
            if (e.currentTarget.checked) {
              setHeld(value);
              onChange([WILDCARD]);
            } else {
              onChange(held);
            }
          }}
          data-testid={`${testId}-everything`}
        />
      </Group>
      {depth !== null && (
        <NamesPickers
          names={names}
          depth={depth}
          value={everything ? held : value}
          onChange={onChange}
          disabled={everything}
          testId={testId}
        />
      )}
      {kind.kind === "roles" && (
        <RolesPicker
          roleIds={roleIds}
          value={everything ? held : value}
          onChange={onChange}
          disabled={everything}
          testId={testId}
        />
      )}
      {depth === null && kind.kind !== "roles" && (
        <TagsInput
          aria-label={t("entitlements.picker.typedLabel")}
          placeholder={t("entitlements.picker.typedPlaceholder")}
          value={everything ? held : value}
          onChange={onChange}
          disabled={everything}
          data-testid={`${testId}-typed`}
        />
      )}
      {description !== undefined && <Input.Description>{description}</Input.Description>}
    </Stack>
  );
}

function OtherValues({
  values,
  onRemove,
  disabled,
  testId,
}: {
  values: string[];
  onRemove: (v: string) => void;
  disabled: boolean;
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
                disabled={disabled}
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

const sameList = (a: readonly string[], b: readonly string[]) =>
  a.length === b.length && a.every((x, i) => x === b[i]);

/** The cascading pickers of a domains, tables or columns list: one per level of the kind. */
function NamesPickers({
  names,
  depth,
  value,
  onChange,
  disabled,
  testId,
}: {
  names: NamesNode[] | null;
  depth: 1 | 2 | 3;
  value: string[];
  onChange: (v: string[]) => void;
  disabled: boolean;
  testId: string;
}) {
  const { t } = useTranslation();
  const roots = useMemo(() => toDepth(names ?? [], depth), [names, depth]);
  const index = useMemo(() => indexNames(roots), [roots]);
  // The pickers keep their own choice: a table picked with no domain chosen leaves the domains
  // empty, so the tables stay searched across every domain. A list handed in from outside (the
  // form reset, * unticked) is taken up as the pickers' choice.
  const [sel, setSel] = useState<Selection>(() => selectionOf(value, index, depth));
  const [seen, setSeen] = useState<{ value: string[]; index: NamesIndex }>({ value, index });
  if (seen.value !== value || seen.index !== index) {
    setSeen({ value, index });
    if (seen.index !== index || !sameList(value, valueOf(sel, index))) {
      setSel(selectionOf(value, index, depth));
      // Drawn again at once with the choice taken up.
      return null;
    }
  }
  const change = (next: Selection) => {
    setSel(next);
    onChange(valueOf(next, index));
  };

  if (roots.length === 0) {
    const unknown = selectionOf(value, index, depth).unknown;
    return (
      <Stack gap={4}>
        <Text size="sm" c="dimmed">
          {t("entitlements.picker.noObjects")}
        </Text>
        <OtherValues
          values={unknown}
          onRemove={(v) => onChange(value.filter((x) => x !== v))}
          disabled={disabled}
          testId={testId}
        />
      </Stack>
    );
  }

  return (
    <Stack gap="xs">
      {sel.levels.map((_, level) => (
        <LevelPicker
          key={LEVELS[level]}
          level={level}
          groups={choicesAt(level, roots, sel, index)}
          chosen={sel.levels[level]}
          unknown={sel.unknown.filter((v) => levelOfUnknown(v, depth) === level)}
          noneAbove={level > 0 && sel.levels[level - 1].length === 0}
          disabled={disabled}
          onToggle={(id) => change(toggle(sel, id, index))}
          onDropUnknown={(v) => change(dropUnknown(sel, v))}
          testId={`${testId}-${LEVELS[level]}`}
        />
      ))}
    </Stack>
  );
}

/** One level's picker: a search that lists the level's choices (grouped by what they belong to,
 *  at most PICKER_LIMIT drawn), and the chosen ones as removable pills under it. */
function LevelPicker({
  level,
  groups,
  chosen,
  unknown,
  noneAbove,
  disabled,
  onToggle,
  onDropUnknown,
  testId,
}: {
  level: number;
  groups: ChoiceGroup[];
  chosen: string[];
  unknown: string[];
  noneAbove: boolean;
  disabled: boolean;
  onToggle: (id: string) => void;
  onDropUnknown: (v: string) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const inputId = useId();
  const [search, setSearch] = useState("");
  const combobox = useCombobox({
    onDropdownClose: () => {
      combobox.resetSelectedOption();
      setSearch("");
    },
  });
  const name = LEVELS[level];
  const listed = filterChoices(groups, search, PICKER_LIMIT);
  // Columns are listed only for chosen tables: with none chosen there is nothing to search.
  const nothingToPick = groups.length === 0;
  const placeholder = nothingToPick
    ? t(`entitlements.picker.${name}.chooseAbove`)
    : t(`entitlements.picker.${name}.${noneAbove ? "searchAll" : "search"}`);
  const isChosen = new Set(chosen);

  return (
    <div className="entitlement-level" data-testid={testId}>
      <Combobox store={combobox} onOptionSubmit={(id) => onToggle(id)} withinPortal>
        <Combobox.Target>
          <TextInput
            id={inputId}
            label={t(`entitlements.picker.${name}.label`)}
            size="sm"
            value={search}
            placeholder={placeholder}
            disabled={disabled || nothingToPick}
            rightSection={<Combobox.Chevron />}
            rightSectionPointerEvents="none"
            onChange={(e) => {
              setSearch(e.currentTarget.value);
              combobox.openDropdown();
              combobox.updateSelectedOptionIndex();
            }}
            onClick={() => combobox.openDropdown()}
            onFocus={() => combobox.openDropdown()}
            onBlur={() => combobox.closeDropdown()}
            data-testid={`${testId}-search`}
          />
        </Combobox.Target>
        <Combobox.Dropdown>
          <Combobox.Options>
            <ScrollArea.Autosize mah={260} type="auto">
              {listed.total === 0 ? (
                <Combobox.Empty>{t("entitlements.picker.noMatch")}</Combobox.Empty>
              ) : (
                listed.groups.map((g) => {
                  const options = g.options.map((o) => (
                    <Combobox.Option
                      key={o.id}
                      value={o.id}
                      active={isChosen.has(o.id)}
                      aria-selected={isChosen.has(o.id)}
                      data-testid={`${testId}-option-${o.id}`}
                    >
                      <Group gap={6} wrap="nowrap">
                        {isChosen.has(o.id) ? (
                          <CheckIcon size={10} style={{ flex: "none" }} />
                        ) : (
                          <span style={{ width: 10, flex: "none" }} />
                        )}
                        <span>{o.label}</span>
                      </Group>
                    </Combobox.Option>
                  ));
                  return g.parent === null ? (
                    options
                  ) : (
                    <Combobox.Group key={g.parent} label={g.parent}>
                      {options}
                    </Combobox.Group>
                  );
                })
              )}
            </ScrollArea.Autosize>
          </Combobox.Options>
          {listed.shown < listed.total && (
            <Combobox.Footer>
              <Text size="xs" c="dimmed" data-testid={`${testId}-more`}>
                {t("entitlements.picker.more", { shown: listed.shown, total: listed.total })}
              </Text>
            </Combobox.Footer>
          )}
        </Combobox.Dropdown>
      </Combobox>
      {(chosen.length > 0 || unknown.length > 0) && (
        <ScrollArea.Autosize mah={96} type="auto" mt={6}>
          <Pill.Group data-testid={`${testId}-chosen`}>
            {chosen.map((id) => (
              <Pill
                key={id}
                withRemoveButton={!disabled}
                onRemove={() => onToggle(id)}
                removeButtonProps={{
                  // Mantine hides a pill's remove from the keyboard and assistive technology.
                  "aria-hidden": false,
                  tabIndex: 0,
                  "aria-label": t("entitlements.picker.removeValue", { value: id }),
                }}
                data-testid={`${testId}-pill-${id}`}
              >
                {id}
              </Pill>
            ))}
            {unknown.map((v) => (
              <Tip key={v} text={t("entitlements.picker.unknownValue")}>
                <Pill
                  withRemoveButton={!disabled}
                  onRemove={() => onDropUnknown(v)}
                  removeButtonProps={{
                    "aria-hidden": false,
                    tabIndex: 0,
                    "aria-label": t("entitlements.picker.removeValue", { value: v }),
                  }}
                  bg="var(--mantine-color-yellow-light)"
                  data-testid={`${testId}-unknown-${v}`}
                >
                  <Group gap={4} wrap="nowrap" component="span">
                    <TriangleAlert size={12} aria-hidden />
                    {v}
                  </Group>
                </Pill>
              </Tip>
            ))}
          </Pill.Group>
        </ScrollArea.Autosize>
      )}
    </div>
  );
}

function RolesPicker({
  roleIds,
  value,
  onChange,
  disabled,
  testId,
}: {
  roleIds: string[];
  value: string[];
  onChange: (v: string[]) => void;
  disabled: boolean;
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
              disabled={disabled}
              data-testid={`${testId}-role-${id}`}
            />
          ))}
        </Stack>
      </Checkbox.Group>
      <OtherValues
        values={other}
        onRemove={(v) => onChange(value.filter((x) => x !== v))}
        disabled={disabled}
        testId={testId}
      />
    </Stack>
  );
}
