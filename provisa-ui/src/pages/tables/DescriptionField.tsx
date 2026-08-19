// Copyright (c) 2026 Kenneth Stott
// Canary: 9e1b4c67-2d5f-4a3e-8b0d-5c7f2a9e3b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useState } from "react";
import { Sparkles, X } from "lucide-react";
import { ActionIcon, Group, Stack, Textarea, Tooltip } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { CopyButton } from "../../components/CopyButton";

const EXPANDED_HEIGHT = 300;

/** Top of the region the editor may occupy: the nearest scrolling ancestor's inner
 *  edge, or the viewport when the page itself is what scrolls. Anything above that
 *  edge is clipped, so it is what decides whether 300px fits above the field. */
function visibleTop(el: HTMLElement): number {
  for (let node = el.parentElement; node !== null; node = node.parentElement) {
    const { overflowY } = getComputedStyle(node);
    if (overflowY === "auto" || overflowY === "scroll") return node.getBoundingClientRect().top;
  }
  return 0;
}

export function DescriptionField({
  value,
  onChange,
  placeholder,
  rows = 2,
  onGenerate,
  generating,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  rows?: number;
  onGenerate?: () => void;
  generating?: boolean;
}) {
  const { t } = useTranslation();
  const [focused, setFocused] = useState(false);
  // Which edge the expanded editor is pinned to. Bottom-anchored by default so the
  // caret stays where the user clicked; flipped to top when the collapsed field sits
  // too close to the top of the viewport for 300px to fit above it, which would push
  // the editor's top edge off-screen.
  const [anchor, setAnchor] = useState<"bottom" | "top">("bottom");
  // The focused editor is 300px tall; growing it in flow moved every sibling laid out
  // below or beside it — including an adjacent Save button, which collapse-on-blur then
  // yanked out from under the pointer between mousedown and mouseup, so no click event
  // ever fired and the edit could not be saved. The slot keeps the collapsed height it
  // measured on first paint and the focused editor is lifted out of flow to overlay the
  // page, so nothing around it moves.
  const [slotHeight, setSlotHeight] = useState<number | null>(null);
  const measureSlot = useCallback(
    (el: HTMLDivElement | null) => {
      if (el !== null && slotHeight === null) setSlotHeight(el.offsetHeight);
    },
    [slotHeight],
  );
  return (
    <Stack gap={4} className="desc-field">
      <div
        ref={measureSlot}
        style={{ position: "relative", height: slotHeight ?? undefined }}
      >
        <Textarea
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          rows={rows}
          onFocus={(e) => {
            const slot = e.currentTarget.getBoundingClientRect();
            const top = visibleTop(e.currentTarget);
            setAnchor(slot.bottom - EXPANDED_HEIGHT < top ? "top" : "bottom");
            setFocused(true);
          }}
          onBlur={() => setFocused(false)}
          styles={{
            root: focused
              ? {
                  position: "absolute",
                  left: 0,
                  right: 0,
                  [anchor]: 0,
                  zIndex: 5,
                }
              : {},
            input: focused
              ? { height: EXPANDED_HEIGHT, transition: "height 0.15s ease" }
              : { transition: "height 0.15s ease" },
          }}
        />
      </div>
      <Group gap={4} justify="flex-end" className="desc-field-toolbar">
        <CopyButton text={value} size={11} />
        {onGenerate && (
          <Tooltip label={t("descriptionField.generateWithAi")}>
            <ActionIcon
              type="button"
              variant="transparent"
              aria-label={t("descriptionField.generateWithAi")}
              data-testid="description-field-generate"
              onClick={onGenerate}
              disabled={generating}
            >
              <Sparkles size={11} />
            </ActionIcon>
          </Tooltip>
        )}
        <Tooltip label={t("descriptionField.clear")}>
          <ActionIcon
            type="button"
            variant="transparent"
            aria-label={t("descriptionField.clear")}
            data-testid="description-field-clear"
            onClick={() => onChange("")}
          >
            <X size={11} />
          </ActionIcon>
        </Tooltip>
      </Group>
    </Stack>
  );
}
