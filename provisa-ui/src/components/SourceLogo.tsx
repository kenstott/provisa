// Copyright (c) 2026 Kenneth Stott
// Canary: 122d7c74-3a07-4b33-8124-eb537a1a62d6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1938: a source type's logo in the source-type picker. Brand marks come from simple-icons
// (CC0 icon data; each mark remains its owner's trademark, shown only to name the source). A type
// with no published mark — some vendors withhold theirs — is drawn as a lettered tile in its
// category's colour, so every row has the same shape. A mark simple-icons does not carry, and
// that its owner lets us show, is a local file under assets/source-logos (its source and terms are
// in the README there), bundled with the UI.
import { CATEGORY_TONES, LOGO_FILES, MARKS, initials } from "./sourceLogoMarks";

export function SourceLogo({
  type,
  label,
  category,
  size = 20,
}: {
  type: string;
  label: string;
  category: string;
  size?: number;
}) {
  const file = LOGO_FILES[type];
  if (file) {
    return (
      <img
        src={file}
        alt=""
        aria-hidden="true"
        width={size}
        height={size}
        data-testid={`source-logo-${type}`}
        data-logo="file"
        style={{ flexShrink: 0 }}
      />
    );
  }
  const mark = MARKS[type];
  if (mark) {
    return (
      <svg
        role="img"
        aria-hidden="true"
        viewBox="0 0 24 24"
        width={size}
        height={size}
        fill={`#${mark.hex}`}
        data-testid={`source-logo-${type}`}
        data-logo="mark"
        style={{ flexShrink: 0 }}
      >
        <path d={mark.path} />
      </svg>
    );
  }
  const tone = CATEGORY_TONES[category] ?? "#6b7280";
  return (
    <span
      aria-hidden="true"
      data-testid={`source-logo-${type}`}
      data-logo="tile"
      style={{
        width: size,
        height: size,
        borderRadius: 4,
        background: tone,
        color: "#fff",
        fontSize: Math.round(size * 0.45),
        fontWeight: 700,
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        flexShrink: 0,
        lineHeight: 1,
      }}
    >
      {initials(label)}
    </span>
  );
}
