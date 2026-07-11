// Copyright (c) 2026 Kenneth Stott
// Canary: 9e1b4c67-2d5f-4a3e-8b0d-5c7f2a9e3b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { Sparkles, X } from "lucide-react";
import { CopyButton } from "../../components/CopyButton";

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
  const [focused, setFocused] = useState(false);
  return (
    <div className="desc-field">
      <textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        rows={rows}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        style={
          focused
            ? { height: 300, transition: "height 0.15s ease" }
            : { transition: "height 0.15s ease" }
        }
      />
      <div className="desc-field-toolbar">
        <CopyButton text={value} size={11} />
        {onGenerate && (
          <button type="button" title="Generate with AI" onClick={onGenerate} disabled={generating}>
            <Sparkles size={11} />
          </button>
        )}
        <button type="button" title="Clear" onClick={() => onChange("")}>
          <X size={11} />
        </button>
      </div>
    </div>
  );
}
