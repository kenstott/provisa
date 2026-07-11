// Copyright (c) 2026 Kenneth Stott
// Canary: 27629eaa-91f7-4758-a270-7be638d0f5b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.


interface ErrorsPanelProps {
  errors: string[];
}

export function ErrorsPanel({ errors }: ErrorsPanelProps) {
  if (errors.length === 0) {
    return (
      <div
        style={{
          padding: "1.5rem",
          textAlign: "center",
          color: "var(--text-muted)",
          fontSize: "0.85rem",
        }}
      >
        No unsupported conditions.
      </div>
    );
  }

  return (
    <div style={{ padding: "0.75rem" }}>
      <p
        style={{
          color: "var(--destructive)",
          fontSize: "0.8rem",
          fontWeight: 600,
          marginBottom: "0.5rem",
        }}
      >
        Unsupported ON conditions — simplify using a view:
      </p>
      <ul
        style={{
          margin: 0,
          paddingLeft: "1.25rem",
          display: "flex",
          flexDirection: "column",
          gap: "0.3rem",
        }}
      >
        {errors.map((e, i) => (
          <li
            key={i}
            style={{
              fontSize: "0.8rem",
              color: "var(--destructive)",
              fontFamily: "monospace",
            }}
          >
            {e}
          </li>
        ))}
      </ul>
    </div>
  );
}
