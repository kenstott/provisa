// Copyright (c) 2026 Kenneth Stott
// Canary: a32e88a8-09cc-4b49-8ee2-8eaa90024e4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useState } from "react";
import type { ReactNode } from "react";
import { DependentsDialog } from "../components/DependentsDialog";
import type { MutationResult } from "../types/admin";
import { dependentsOf } from "../lib/dependents";
import type { Dependent } from "../lib/dependents";

/** For a page with a delete action: `refused(result, subject)` opens the dialog and returns
 *  true when the delete was refused for dependents; `dialog` is rendered once in the page. */
export function useDependentsDialog(
  /** An action the page offers on one dependent of ``subject``; ``handled`` takes the dependents
   *  it dealt with off the list. */
  itemAction?: (
    subject: string,
    dependent: Dependent,
    all: Dependent[],
    handled: (gone: (d: Dependent) => boolean) => void,
  ) => ReactNode,
): {
  refused: (result: MutationResult | null | undefined, subject: string) => boolean;
  dialog: ReactNode;
} {
  const [shown, setShown] = useState<{ subject: string; dependents: Dependent[] } | null>(null);
  const handled = useCallback((gone: (d: Dependent) => boolean) => {
    setShown((current) =>
      current ? { ...current, dependents: current.dependents.filter((d) => !gone(d)) } : null,
    );
  }, []);
  const refused = useCallback(
    (result: MutationResult | null | undefined, subject: string) => {
      const dependents = dependentsOf(result);
      if (dependents === null) return false;
      setShown({ subject, dependents });
      return true;
    },
    [],
  );
  const dialog = shown ? (
    <DependentsDialog
      subject={shown.subject}
      dependents={shown.dependents}
      onClose={() => setShown(null)}
      itemAction={
        itemAction
          ? (d) => itemAction(shown.subject, d, shown.dependents, handled)
          : undefined
      }
    />
  ) : null;
  return { refused, dialog };
}
