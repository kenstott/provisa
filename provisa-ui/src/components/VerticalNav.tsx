// Copyright (c) 2026 Kenneth Stott
// Canary: 2b47053f-15d5-4d6a-8d27-9b0d635645c0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The one vertical menu: the Admin rail's look (.vertical-nav in App.css) for a menu that picks what
// a page or a dialog shows beside it. The Admin rail itself is the same class over route links.

export interface VerticalNavItem {
  value: string;
  label: string;
  testId?: string;
}

export function VerticalNav({
  items,
  value,
  onChange,
  label,
  className,
  testId,
}: {
  items: VerticalNavItem[];
  value: string;
  onChange: (value: string) => void;
  /** The menu's accessible name. */
  label: string;
  className?: string;
  testId?: string;
}) {
  return (
    <nav
      className={className ? `vertical-nav ${className}` : "vertical-nav"}
      aria-label={label}
      data-testid={testId}
    >
      {items.map((item) => {
        const active = item.value === value;
        return (
          <button
            key={item.value}
            type="button"
            className={active ? "active" : undefined}
            aria-current={active ? "true" : undefined}
            onClick={() => onChange(item.value)}
            data-testid={item.testId}
          >
            {item.label}
          </button>
        );
      })}
    </nav>
  );
}
