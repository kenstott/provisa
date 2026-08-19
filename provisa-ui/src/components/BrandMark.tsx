// Copyright (c) 2026 Kenneth Stott
// Canary: 3d1a90c7-6b24-4f52-9a0e-8c5b7d21e4af
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The product mark. One definition for every surface that carries the brand — the navbar, the
 * signed-out landing page, and the tour offer — so the three cannot drift apart.
 *
 * The glyph takes its ink from `currentColor`; the caller sets the color and the size.
 */
export function BrandMark({ size = 24, className }: { size?: number; className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 100 100"
      width={size}
      height={size}
      role="img"
      aria-hidden="true"
    >
      <g fill="currentColor">
        <rect x="30" y="18" width="15" height="64" rx="7" />
        <circle cx="52" cy="35" r="22" />
      </g>
      <circle cx="52" cy="35" r="10.5" fill="var(--surface)" />
      <circle cx="52" cy="35" r="4.5" fill="#10B981" />
    </svg>
  );
}
