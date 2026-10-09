// Copyright (c) 2026 Kenneth Stott
// Canary: 37808e82-5eba-44a3-aee0-b2d02633bf9a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: a column's typed filter menu — operators for its kind, the operands, and a checklist of
// the distinct values the grid holds (with counts and a search box). The header's box keeps the
// quick syntax; this is the structured way to the same filters.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Button,
  Checkbox,
  Group,
  Popover,
  ScrollArea,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { Filter } from "lucide-react";
import {
  EMPTY_VALUE,
  NO_OPERAND,
  OPS_BY_KIND,
  TWO_OPERANDS,
  type ColumnKind,
  type FilterOp,
  type FilterSpec,
} from "./columnFilter";

interface Props {
  col: string;
  kind: ColumnKind;
  /** The filter in force on this column, if any. */
  current: FilterSpec | null;
  values: { value: string; count: number }[];
  /** True when the grid holds only part of the result, so the value list covers the rows loaded. */
  valuesFromPage: boolean;
  rowsHeld: number;
  onApply: (spec: FilterSpec | null) => void;
}

export function ColumnFilterMenu({
  col,
  kind,
  current,
  values,
  valuesFromPage,
  rowsHeld,
  onApply,
}: Props) {
  const { t } = useTranslation();
  const [opened, setOpened] = useState(false);
  const ops = OPS_BY_KIND[kind];
  const [op, setOp] = useState<FilterOp>(current?.op ?? ops[0]);
  const [a, setA] = useState(current?.a === "ci" ? "" : (current?.a ?? ""));
  const [b, setB] = useState(current?.b ?? "");
  const [ticked, setTicked] = useState<Set<string>>(new Set(current?.values ?? []));
  const [search, setSearch] = useState("");

  const open = () => {
    // Start from what is in force, so the menu shows the filter the chip names.
    setOp(current?.op ?? ops[0]);
    setA(current?.a === "ci" ? "" : (current?.a ?? ""));
    setB(current?.b ?? "");
    setTicked(new Set(current?.values ?? []));
    setSearch("");
    setOpened(true);
  };

  // The list offered: the values of the rows held, and before them any value that is ticked but
  // is not among those rows -- one typed by hand, or ticked from another page of a longer
  // relation. It has no count: none of the rows held carries it.
  const offered = useMemo(() => {
    const held = new Set(values.map((v) => v.value));
    const extra = [...ticked].filter((v) => !held.has(v)).map((value) => ({ value, count: null }));
    return [...extra, ...values] as { value: string; count: number | null }[];
  }, [values, ticked]);

  const shown = useMemo(() => {
    const q = search.trim().toLowerCase();
    return q === ""
      ? offered
      : offered.filter((v) =>
          (v.value === EMPTY_VALUE ? t("columnFilter.emptyValue") : v.value)
            .toLowerCase()
            .includes(q),
        );
  }, [offered, search, t]);

  // What is typed in the search box, when it is not a value already offered: it can be added to
  // the checklist as typed. The rows held are not all the values there are.
  const typed = search.trim();
  const canAddTyped =
    typed !== "" && !offered.some((v) => v.value === typed) && !typed.includes("\u0000");

  const apply = () => {
    if (op === "in") onApply(ticked.size > 0 ? { op, values: [...ticked] } : null);
    else if (NO_OPERAND.has(op)) onApply({ op });
    else if (TWO_OPERANDS.has(op)) onApply(a !== "" && b !== "" ? { op, a, b } : null);
    else onApply(a !== "" ? { op, a } : null);
    setOpened(false);
  };

  const operandType =
    kind === "number" || op === "lastDays" ? "number" : kind === "date" ? "date" : "text";
  const id = `col-filter-${col}`;

  return (
    <Popover
      opened={opened}
      onChange={(o) => {
        if (!o) setOpened(false);
      }}
      withinPortal
      position="bottom-start"
      width={280}
      shadow="md"
    >
      <Popover.Target>
        <ActionIcon
          variant={current ? "filled" : "subtle"}
          size="xs"
          aria-label={t("columnFilter.open", { column: col })}
          title={t("columnFilter.open", { column: col })}
          data-testid={`${id}-btn`}
          onClick={(e) => {
            // A controlled Popover does not toggle itself from its target.
            e.stopPropagation();
            if (opened) setOpened(false);
            else open();
          }}
        >
          <Filter size={11} />
        </ActionIcon>
      </Popover.Target>
      <Popover.Dropdown onClick={(e) => e.stopPropagation()} data-testid={`${id}-menu`}>
        <Stack gap="xs">
          <Select
            size="xs"
            label={t("columnFilter.operator")}
            data={ops.map((o) => ({ value: o, label: t(`columnFilter.op.${o}`) }))}
            value={op}
            onChange={(v) => v && setOp(v as FilterOp)}
            allowDeselect={false}
            comboboxProps={{ withinPortal: false }}
            data-testid={`${id}-op`}
          />
          {op === "in" ? (
            <>
              <TextInput
                size="xs"
                placeholder={t("columnFilter.searchValues")}
                value={search}
                onChange={(e) => setSearch(e.currentTarget.value)}
                data-testid={`${id}-search`}
              />
              <Group gap="xs">
                <Button
                  size="compact-xs"
                  variant="default"
                  onClick={() => setTicked(new Set([...ticked, ...shown.map((v) => v.value)]))}
                  data-testid={`${id}-select-shown`}
                >
                  {t("columnFilter.selectShown")}
                </Button>
                <Button size="compact-xs" variant="default" onClick={() => setTicked(new Set())}>
                  {t("columnFilter.clearSelection")}
                </Button>
              </Group>
              {canAddTyped && (
                <Button
                  size="compact-xs"
                  variant="light"
                  onClick={() => {
                    setTicked(new Set([...ticked, typed]));
                    setSearch("");
                  }}
                  data-testid={`${id}-add-typed`}
                >
                  {t("columnFilter.addTyped", { value: typed })}
                </Button>
              )}
              <ScrollArea.Autosize mah={180}>
                <Stack gap={4}>
                  {shown.length === 0 && (
                    <Text size="xs" c="dimmed">
                      {t("columnFilter.noValues")}
                    </Text>
                  )}
                  {shown.map((v) => (
                    <Checkbox
                      key={v.value}
                      size="xs"
                      checked={ticked.has(v.value)}
                      onChange={(e) => {
                        const on = e.currentTarget.checked;
                        setTicked((prev) => {
                          const next = new Set(prev);
                          if (on) next.add(v.value);
                          else next.delete(v.value);
                          return next;
                        });
                      }}
                      label={
                        (v.value === EMPTY_VALUE ? t("columnFilter.emptyValue") : v.value) +
                        (v.count === null ? "" : ` (${v.count})`)
                      }
                      data-testid={`${id}-value-${v.value === EMPTY_VALUE ? "empty" : v.value}`}
                    />
                  ))}
                </Stack>
              </ScrollArea.Autosize>
              {valuesFromPage && (
                <Text size="xs" c="dimmed">
                  {t("columnFilter.valuesFromLoaded", { count: rowsHeld })}
                </Text>
              )}
            </>
          ) : NO_OPERAND.has(op) ? null : (
            <Group gap="xs" wrap="nowrap">
              <TextInput
                size="xs"
                type={operandType}
                style={{ flex: 1 }}
                aria-label={t(
                  TWO_OPERANDS.has(op)
                    ? "columnFilter.from"
                    : op === "lastDays"
                      ? "columnFilter.days"
                      : "columnFilter.value",
                )}
                placeholder={t(
                  TWO_OPERANDS.has(op)
                    ? "columnFilter.from"
                    : op === "lastDays"
                      ? "columnFilter.days"
                      : "columnFilter.value",
                )}
                value={a}
                onChange={(e) => setA(e.currentTarget.value)}
                data-testid={`${id}-a`}
              />
              {TWO_OPERANDS.has(op) && (
                <TextInput
                  size="xs"
                  type={operandType}
                  style={{ flex: 1 }}
                  aria-label={t("columnFilter.to")}
                  placeholder={t("columnFilter.to")}
                  value={b}
                  onChange={(e) => setB(e.currentTarget.value)}
                  data-testid={`${id}-b`}
                />
              )}
            </Group>
          )}
          {kind === "date" && (
            <Text size="xs" c="dimmed">
              {t("columnFilter.timeZoneNote")}
            </Text>
          )}
          <Group justify="space-between">
            <Button
              size="compact-xs"
              variant="default"
              onClick={() => {
                onApply(null);
                setOpened(false);
              }}
              data-testid={`${id}-clear`}
            >
              {t("columnFilter.clear")}
            </Button>
            <Button size="compact-xs" onClick={apply} data-testid={`${id}-apply`}>
              {t("columnFilter.apply")}
            </Button>
          </Group>
        </Stack>
      </Popover.Dropdown>
    </Popover>
  );
}
