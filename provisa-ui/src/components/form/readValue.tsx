// Copyright (c) 2026 Kenneth Stott
// Canary: 37d37549-084d-47c0-839a-7c9ad52cabdc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A form's control read as a value: what a read-only form shows beside the control's label.

import { Children, isValidElement, type ReactElement, type ReactNode } from "react";
import {
  Checkbox,
  FileInput,
  MultiSelect,
  PasswordInput,
  Radio,
  SegmentedControl,
  Select,
  Switch,
} from "@mantine/core";
import type { TFunction } from "i18next";

/** A control of its own making says how it reads: `Control.readValue = (props, t) => value`. */
export interface ReadableControl {
  readValue?: (props: Record<string, unknown>, t: TFunction) => ReactNode;
}

type Option = string | { value: string; label?: string } | { group: string; items: Option[] };

function optionLabel(data: Option[] | undefined, value: string): string {
  for (const o of data ?? []) {
    if (typeof o === "string") {
      if (o === value) return o;
    } else if ("group" in o) {
      const found = optionLabel(o.items, value);
      if (found !== value) return found;
    } else if (o.value === value) {
      return o.label ?? o.value;
    }
  }
  // A stored value no option offers is still what is stored: shown as it is.
  return value;
}

/** The labels of the choices (Radio or Checkbox) among `children` whose values are `values`. */
function choiceLabels(children: ReactNode, values: string[]): ReactNode[] {
  const found: ReactNode[] = [];
  Children.forEach(children, (c) => {
    if (!isValidElement<{ value?: string; label?: ReactNode; children?: ReactNode }>(c)) return;
    if (c.props.value !== undefined && values.includes(c.props.value)) {
      found.push(c.props.label ?? c.props.value);
    } else if (c.props.children !== undefined) {
      found.push(...choiceLabels(c.props.children, values));
    }
  });
  return found;
}

function joined(parts: ReactNode[]): ReactNode {
  if (parts.length === 0) return null;
  return parts.flatMap((p, i) => (i === 0 ? [p] : [", ", p]));
}

/** The value a control holds, as the read view shows it; null, undefined or "" is empty. */
export function readValue(control: ReactElement, t: TFunction): ReactNode {
  const p = control.props as Record<string, unknown>;
  const own = (control.type as ReadableControl).readValue;
  if (typeof own === "function") return own(p, t);
  if (control.type === PasswordInput) return p.value ? t("detailFields.set") : null;
  // A file input reads as the file chosen, else as what it keeps from before (its `data-kept`):
  // an archive already uploaded holds no File, only its name and hash.
  if (control.type === FileInput) {
    return (
      (p.value as File | null | undefined)?.name ?? (p["data-kept"] as string | undefined) ?? null
    );
  }
  if (control.type === Checkbox || control.type === Switch) {
    return t(p.checked ? "detailFields.yes" : "detailFields.no");
  }
  if (control.type === Select || control.type === SegmentedControl) {
    // An option may stand for "" (Inherit): it reads as that option; "" with none is empty.
    return p.value == null ? null : optionLabel(p.data as Option[], String(p.value));
  }
  if (control.type === MultiSelect) {
    return joined((p.value as string[]).map((v) => optionLabel(p.data as Option[], v)));
  }
  if (control.type === Radio.Group) {
    return p.value ? joined(choiceLabels(p.children as ReactNode, [String(p.value)])) : null;
  }
  if (control.type === Checkbox.Group) {
    return joined(choiceLabels(p.children as ReactNode, p.value as string[]));
  }
  // A list of choices (the shared MultiSelect names its options `{ id, label }`).
  if (Array.isArray(p.value)) {
    const options = (p.options as { id: string; label: string }[] | undefined)?.map((o) => ({
      value: o.id,
      label: o.label,
    }));
    return joined(
      (p.value as unknown[]).map((v) => optionLabel(options ?? (p.data as Option[]), String(v))),
    );
  }
  return p.value as ReactNode;
}
