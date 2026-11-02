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
import { ActionIcon } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Tip } from "./Tip";

interface CopyButtonProps {
  text: string;
  size?: number;
  className?: string;
  hint?: string;
  children?: React.ReactNode;
}

export function CopyButton({ text, size = 11, className, hint, children }: CopyButtonProps) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const handleClick = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);

  const label = copied ? t("copyButton.copied") : (hint ?? t("copyButton.copy"));

  return (
    <Tip text={label} named>
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
    </Tip>
  );
}

export function CopySymbolButton({
  text,
  className,
  hint,
}: {
  text: string;
  className?: string;
  hint?: string;
}) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const handleClick = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);

  const label = copied ? t("copyButton.copied") : (hint ?? t("copyButton.copy"));

  return (
    <Tip text={label} named>
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
    </Tip>
  );
}
