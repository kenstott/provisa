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

interface CopyButtonProps {
  text: string;
  size?: number;
  className?: string;
  title?: string;
  children?: React.ReactNode;
}

export function CopyButton({ text, size = 11, className, title = "Copy", children }: CopyButtonProps) {
  const [copied, setCopied] = useState(false);

  const handleClick = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);

  return (
    <button type="button" title={copied ? "Copied!" : title} className={className} onClick={handleClick}>
      {children ?? (copied ? <Check size={size} /> : <Copy size={size} />)}
    </button>
  );
}

export function CopySymbolButton({ text, className, title = "Copy" }: { text: string; className?: string; title?: string }) {
  const [copied, setCopied] = useState(false);

  const handleClick = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);

  return (
    <button type="button" title={copied ? "Copied!" : title} className={className} onClick={handleClick}>
      {copied ? "✓" : "⎘"}
    </button>
  );
}
