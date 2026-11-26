// Copyright (c) 2026 Kenneth Stott
// Canary: f9f3b7e3-f06b-4f20-b309-9675e1f97e4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968 (amended 2026-11-22): a missing field never disables a form's primary action. The
// action validates: each missing field is marked inline and focus moves to the first. A required
// field carries its marker from the start (the same `required` the check reads), turns red once
// left empty (blur) or after an attempt, and from then on follows its value: filled clears the red,
// emptied brings it back. An untouched field is not red when the form opens.

import { useCallback, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

/** Empty: nothing typed (or only spaces), nothing picked, an empty list. */
function isEmpty(value: unknown): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "string") return value.trim() === "";
  if (Array.isArray(value)) return value.length === 0;
  return false;
}

/** What a Mantine input takes: its marker, its message, when it starts showing, and its focus. */
export interface RequiredFieldProps {
  required: true;
  error: string | undefined;
  onBlur: () => void;
  ref: (el: HTMLElement | null) => void;
}

/** What a native `<select className="form-select">` takes; its label draws the marker and its
 *  message is `errorOf`. */
export interface RequiredNativeProps {
  required: true;
  "aria-invalid": boolean;
  onBlur: () => void;
  ref: (el: HTMLElement | null) => void;
}

/**
 * The required fields of one form, by name, with each one's current value. Only the fields the
 * form shows are passed: a field that is not on screen does not block.
 */
export function useRequiredFields<K extends string>(values: Partial<Record<K, unknown>>) {
  const { t } = useTranslation();
  const [shown, setShown] = useState<ReadonlySet<K>>(() => new Set());
  const elements = useRef<Partial<Record<K, HTMLElement | null>>>({});
  const names = Object.keys(values) as K[];

  const show = useCallback((name: K) => {
    setShown((prev) => (prev.has(name) ? prev : new Set(prev).add(name)));
  }, []);

  const missing = (name: K) => name in values && isEmpty(values[name]);

  const errorOf = (name: K): string | undefined =>
    shown.has(name) && missing(name) ? t("common.required") : undefined;

  const common = (name: K) => ({
    required: true as const,
    onBlur: () => show(name),
    ref: (el: HTMLElement | null) => {
      elements.current[name] = el;
    },
  });

  const field = (name: K): RequiredFieldProps => ({ ...common(name), error: errorOf(name) });

  const native = (name: K): RequiredNativeProps => ({
    ...common(name),
    "aria-invalid": errorOf(name) !== undefined,
  });

  /** On the primary action: every field is marked from now on; true when none is missing, else
   *  focus moves to the first missing one. An action that needs only some of the form's fields
   *  (a preview that needs the target but not the message) names them in `only`. */
  const validate = (only?: readonly K[]): boolean => {
    const checked = only ?? names;
    setShown((prev) => new Set([...prev, ...checked]));
    const empty = checked.filter(missing);
    if (empty.length === 0) return true;
    // The first on screen, whatever order the form named them in.
    const placed = empty
      .map((name) => elements.current[name])
      .filter((el): el is HTMLElement => el !== null && el !== undefined)
      .sort((a, b) => (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1));
    placed[0]?.focus();
    return false;
  };

  /** After the action succeeds and the form is cleared for the next entry: the cleared fields are
   *  not red until they are left empty or pressed again, as when the form first opened. */
  const reset = useCallback(() => setShown(new Set()), []);

  return { field, native, errorOf, validate, reset };
}

export type RequiredFields<K extends string = string> = ReturnType<typeof useRequiredFields<K>>;
