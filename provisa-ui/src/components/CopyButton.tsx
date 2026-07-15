// Copyright (c) 2026 Kenneth Stott
// Canary: 1689684b-e41a-4cb1-ad73-87872bf0c920
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback } from "react";
import { Copy, Check } from "lucide-react";
import { ActionIcon, Tooltip } from "@mantine/core";
import { useTranslation } from "react-i18next";

interface CopyButtonProps {
  text: string;
  size?: number;
  className?: string;
  title?: string;
  children?: React.ReactNode;
}

export function CopyButton({ text, size = 11, className, title, children }: CopyButtonProps) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const handleClick = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);

  const label = copied ? t("copyButton.copied") : (title ?? t("copyButton.copy"));

  return (
    <Tooltip label={label}>
      <ActionIcon
        type="button"
        variant="transparent"
        className={className}
        aria-label={label}
        data-testid="copy-button"
        onClick={handleClick}
      >
        {children ?? (copied ? <Check size={size} /> : <Copy size={size} />)}
      </ActionIcon>
    </Tooltip>
  );
}

export function CopySymbolButton({ text, className, title }: { text: string; className?: string; title?: string }) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const handleClick = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);

  const label = copied ? t("copyButton.copied") : (title ?? t("copyButton.copy"));

  return (
    <Tooltip label={label}>
      <ActionIcon
        type="button"
        variant="transparent"
        className={className}
        aria-label={label}
        data-testid="copy-symbol-button"
        onClick={handleClick}
      >
        {copied ? "✓" : "⎘"}
      </ActionIcon>
    </Tooltip>
  );
}
