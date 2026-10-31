// Copyright (c) 2026 Kenneth Stott
// Canary: 154765ce-1e79-4785-bf31-0b39e7dbf0d1
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useLayoutEffect } from "react";
import { X } from "lucide-react";
import { ActionIcon, Group, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { CopyButton } from "../CopyButton";
import { getStored, removeStored, setStored } from "../../lib/browserStorage";

// What a list's search box last held, per viewer, per page: a list reopens filtered as it was left.
// Remembering is a convenience only, so blocked or unavailable storage means nothing is remembered.
function readRemembered(key: string): string | null {
  try {
    return getStored(key);
  } catch {
    return null;
  }
}

function writeRemembered(key: string, value: string): void {
  try {
    if (value) setStored(key, value);
    else removeStored(key);
  } catch {
    // Blocked storage: the filter holds while the page is open and is not kept.
  }
}

export function FilterInput({
  value,
  onChange,
  placeholder,
  testId = "filter-input",
  remember,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  /** Overridden where a page's tests already address its filter by another name. */
  testId?: string;
  /** The name it is remembered under on its page, where two of a page's boxes share a test id;
   * false where the page keeps its own search. */
  remember?: string | false;
}) {
  const { t } = useTranslation();
  const key =
    remember === false ? null : `filter.${window.location.pathname}.${remember ?? testId}`;
  // Only the reader's own changes are saved. A deep link -- a value the page opened with, or any
  // other query in the URL, which may name a row this filter would hide -- decides that visit and
  // leaves what is remembered alone. Before paint, so a remembered filter never flashes empty.
  useLayoutEffect(() => {
    if (key === null || value !== "" || window.location.search !== "") return;
    const remembered = readRemembered(key);
    if (remembered) onChange(remembered);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- restored once, as the page opens; the scope gate remounts the page when the scope changes
  }, []);
  const change = (v: string) => {
    onChange(v);
    if (key !== null) writeRemembered(key, v);
  };
  return (
    <div className="search-wrap">
      <TextInput
        type="search"
        aria-label={placeholder ?? t("filterInput.filter")}
        placeholder={placeholder}
        value={value}
        onChange={(e) => change(e.target.value)}
        data-testid={testId}
        rightSection={
          <Group gap={2} wrap="nowrap">
            <CopyButton text={value} size={11} />
            <ActionIcon
              type="button"
              variant="transparent"
              aria-label={t("filterInput.clear")}
              data-testid="filter-input-clear"
              onClick={() => change("")}
            >
              <X size={11} />
            </ActionIcon>
          </Group>
        }
        rightSectionWidth={54}
      />
    </div>
  );
}
