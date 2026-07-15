// Copyright (c) 2026 Kenneth Stott
// Canary: 91caf8b6-bf56-42b1-92b2-77bfbaa804d1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useRef, useEffect, useLayoutEffect } from "react";
import {
  Button,
  Group,
  Modal,
  NumberInput,
  SegmentedControl,
  Select,
  Slider,
  Stack,
  Text,
  TextInput,
  UnstyledButton,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import { labelColor, PALETTE } from "./graph-model";
import type { RelLineOverride } from "./graph-model";

// ── Context menu state ──────────────────────────────────────────────────────────
export interface ContextMenuState {
  x: number;
  y: number;
  compoundLabel: string;
  tableLabel: string;
  properties: string[];
  numericProperties: string[];
}

export interface RelContextMenuState {
  x: number;
  y: number;
  type: string;
}

// ── Node context menu ───────────────────────────────────────────────────────────
interface NodeContextMenuProps {
  menu: ContextMenuState;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  sizeByProperty: Record<string, string>;
  sizeMultiplier: Record<string, number>;
  onColorChange: (label: string, color: string) => void;
  onSizeChange: (label: string, size: number) => void;
  onLabelPropertyChange: (label: string, prop: string) => void;
  onSizeByPropertyChange: (label: string, prop: string) => void;
  onSizeMultiplierChange: (label: string, multiplier: number) => void;
  onClose: () => void;
}

export function NodeContextMenu({
  menu,
  colorOverrides,
  sizeOverrides,
  labelProperty,
  sizeByProperty,
  sizeMultiplier,
  onColorChange,
  onSizeChange,
  onLabelPropertyChange,
  onSizeByPropertyChange,
  onSizeMultiplierChange,
  onClose,
}: NodeContextMenuProps) {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  const currentColor = colorOverrides[menu.compoundLabel] ?? labelColor(menu.compoundLabel);
  const currentSize = sizeOverrides[menu.compoundLabel] ?? 44;
  const currentProp = labelProperty[menu.compoundLabel] ?? "";
  const currentSizeByProp = sizeByProperty[menu.compoundLabel] ?? "";
  const currentMultiplier = sizeMultiplier[menu.compoundLabel] ?? 3;

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    let left = menu.x;
    let top = menu.y;
    if (r.right > window.innerWidth) left -= r.right - window.innerWidth;
    if (left < 0) left = 0;
    if (r.bottom > window.innerHeight) top -= r.height;
    if (top < 0) top = 0;
    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
    el.style.visibility = "visible";
  }, [menu.x, menu.y]);

  return (
    <div
      ref={ref}
      className="node-ctx-menu"
      style={{ left: menu.x, top: menu.y, visibility: "hidden" }}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <div className="node-ctx-title">{menu.tableLabel}</div>
      <Stack gap={4}>
        <Text className="node-ctx-section-label">{t("graphContextMenus.color")}</Text>
        <Group gap={4} wrap="wrap" role="group" aria-label={t("graphContextMenus.color")}>
          {PALETTE.map((c) => (
            <UnstyledButton
              key={c}
              className={`node-ctx-swatch${currentColor === c ? " active" : ""}`}
              style={{ background: c }}
              aria-label={t("graphContextMenus.colorSwatchLabel", { color: c })}
              aria-current={currentColor === c}
              onClick={() => {
                onColorChange(menu.compoundLabel, c);
                onClose();
              }}
            />
          ))}
        </Group>

        <Text className="node-ctx-section-label">{t("graphContextMenus.size")}</Text>
        <Group className="node-ctx-size-row" gap="sm" wrap="nowrap">
          <Slider
            aria-label={t("graphContextMenus.size")}
            style={{ flex: 1 }}
            min={20}
            max={120}
            value={currentSize}
            onChange={(v) => onSizeChange(menu.compoundLabel, v)}
            label={null}
          />
          <span>{currentSize}px</span>
        </Group>

        {menu.properties.length > 0 && (
          <>
            <Text className="node-ctx-section-label">{t("graphContextMenus.labelBy")}</Text>
            <Select
              aria-label={t("graphContextMenus.labelBy")}
              data={[
                { value: "", label: t("graphContextMenus.labelByDefault") },
                ...menu.properties.map((p) => ({ value: p, label: p })),
              ]}
              value={currentProp}
              onChange={(v) => {
                onLabelPropertyChange(menu.compoundLabel, v ?? "");
                onClose();
              }}
              allowDeselect={false}
              comboboxProps={{ withinPortal: false }}
            />
          </>
        )}

        {menu.numericProperties.length > 0 && (
          <>
            <Group justify="space-between" align="center">
              <Text className="node-ctx-section-label" style={{ margin: 0 }}>
                {t("graphContextMenus.sizeBy")}
              </Text>
              {currentSizeByProp && (
                <NumberInput
                  aria-label={t("graphContextMenus.sizeMultiplier")}
                  min={1}
                  max={10}
                  step={0.5}
                  value={currentMultiplier}
                  onChange={(v) => {
                    const num = typeof v === "number" ? v : parseFloat(v);
                    if (!isNaN(num) && num >= 1) onSizeMultiplierChange(menu.compoundLabel, num);
                  }}
                  className="node-ctx-select"
                  style={{ width: 68 }}
                  size="xs"
                />
              )}
            </Group>
            <Select
              aria-label={t("graphContextMenus.sizeBy")}
              data={[
                { value: "", label: t("graphContextMenus.sizeByFixed") },
                ...menu.numericProperties.map((p) => ({ value: p, label: p })),
              ]}
              value={currentSizeByProp}
              onChange={(v) => {
                onSizeByPropertyChange(menu.compoundLabel, v ?? "");
                onClose();
              }}
              allowDeselect={false}
              comboboxProps={{ withinPortal: false }}
            />
          </>
        )}
      </Stack>
    </div>
  );
}

// ── Relationship context menu ─────────────────────────────────────────────────
interface RelContextMenuProps {
  menu: RelContextMenuState;
  relLineOverrides: Record<string, RelLineOverride>;
  onRelLineChange: (type: string, override: RelLineOverride) => void;
  onClose: () => void;
}

const LINE_STYLES: Array<RelLineOverride["style"]> = ["solid", "dashed", "dotted"];

export function RelContextMenu({
  menu,
  relLineOverrides,
  onRelLineChange,
  onClose,
}: RelContextMenuProps) {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  const current = relLineOverrides[menu.type] ?? { width: 1.5, style: "solid" as const };

  const lineStyleLabels: Record<RelLineOverride["style"], string> = {
    solid: t("graphContextMenus.lineStyleSolid"),
    dashed: t("graphContextMenus.lineStyleDashed"),
    dotted: t("graphContextMenus.lineStyleDotted"),
  };

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    let left = menu.x;
    let top = menu.y;
    if (r.right > window.innerWidth) left -= r.right - window.innerWidth;
    if (left < 0) left = 0;
    if (r.bottom > window.innerHeight) top -= r.height;
    if (top < 0) top = 0;
    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
    el.style.visibility = "visible";
  }, [menu.x, menu.y]);

  return (
    <div
      ref={ref}
      className="node-ctx-menu"
      style={{ left: menu.x, top: menu.y, visibility: "hidden" }}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <div className="node-ctx-title">{menu.type}</div>
      <Stack gap={4}>
        <Text className="node-ctx-section-label">{t("graphContextMenus.lineStyle")}</Text>
        <SegmentedControl
          aria-label={t("graphContextMenus.lineStyle")}
          value={current.style}
          onChange={(v) => onRelLineChange(menu.type, { ...current, style: v as RelLineOverride["style"] })}
          data={LINE_STYLES.map((s) => ({ value: s, label: lineStyleLabels[s] }))}
        />

        <Text className="node-ctx-section-label">{t("graphContextMenus.lineWidth")}</Text>
        <Group className="node-ctx-size-row" gap="sm" wrap="nowrap">
          <Slider
            aria-label={t("graphContextMenus.lineWidth")}
            style={{ flex: 1 }}
            min={0.5}
            max={8}
            step={0.5}
            value={current.width}
            onChange={(v) => onRelLineChange(menu.type, { ...current, width: v })}
            label={null}
          />
          <span>{current.width}px</span>
        </Group>
      </Stack>
    </div>
  );
}

// ── Native filter modal ───────────────────────────────────────────────────────
interface NativeFilterModalProps {
  label: string;
  filterColumns: { name: string; type: string }[];
  onConfirm: (params: Record<string, string>) => void;
  onCancel: () => void;
}

export function NativeFilterModal({
  label,
  filterColumns,
  onConfirm,
  onCancel,
}: NativeFilterModalProps) {
  const { t } = useTranslation();
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(filterColumns.map((c) => [c.name, ""])),
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onConfirm(values);
  };

  return (
    <Modal
      opened
      onClose={onCancel}
      title={t("graphContextMenus.filterParametersTitle", { label })}
      centered
    >
      <form onSubmit={handleSubmit}>
        <Stack gap="sm">
          {filterColumns.map((col) => (
            <TextInput
              key={col.name}
              label={col.name}
              value={values[col.name]}
              onChange={(e) => setValues((v) => ({ ...v, [col.name]: e.target.value }))}
              placeholder={col.name}
              autoFocus={filterColumns[0].name === col.name}
            />
          ))}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={onCancel}>
              {t("graphContextMenus.cancel")}
            </Button>
            <Button type="submit">{t("graphContextMenus.run")}</Button>
          </Group>
        </Stack>
      </form>
    </Modal>
  );
}
