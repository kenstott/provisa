// Copyright (c) 2026 Kenneth Stott
// Canary: f3a8c2d1-74b5-4e6f-a9c2-18d3f7e4b5a2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

interface IconProps {
  size?: number;
}

export function DatabaseIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="8" cy="3" r="1.8" fill="currentColor" stroke="none"/>
      <circle cx="3" cy="12.5" r="1.8" fill="currentColor" stroke="none"/>
      <circle cx="13" cy="12.5" r="1.8" fill="currentColor" stroke="none"/>
      <line x1="8" y1="4.8" x2="3.9" y2="10.8"/>
      <line x1="8" y1="4.8" x2="12.1" y2="10.8"/>
      <line x1="4.8" y1="12.5" x2="11.2" y2="12.5"/>
    </svg>
  );
}

export function HistoryIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round">
      <circle cx="8" cy="8" r="5.5"/>
      <polyline points="8,5 8,8.5 10.5,10"/>
    </svg>
  );
}

export function StarIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="currentColor">
      <polygon points="8,1.5 9.7,6 14.5,6 10.6,8.9 12.1,13.5 8,10.5 3.9,13.5 5.4,8.9 1.5,6 6.3,6"/>
    </svg>
  );
}

export function GraphViewIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" strokeLinecap="round">
      <circle cx="8" cy="8" r="2" fill="currentColor"/>
      <circle cx="2.5" cy="4" r="1.6" fill="currentColor"/>
      <circle cx="13.5" cy="4" r="1.6" fill="currentColor"/>
      <circle cx="2.5" cy="12" r="1.6" fill="currentColor"/>
      <circle cx="13.5" cy="12" r="1.6" fill="currentColor"/>
      <line x1="6.2" y1="6.8" x2="4.1" y2="5.1" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="9.8" y1="6.8" x2="11.9" y2="5.1" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="6.2" y1="9.2" x2="4.1" y2="10.9" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="9.8" y1="9.2" x2="11.9" y2="10.9" stroke="currentColor" strokeWidth="1.2"/>
    </svg>
  );
}

export function TableViewIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round">
      <rect x="1.5" y="2.5" width="13" height="11" rx="1.5"/>
      <line x1="1.5" y1="6" x2="14.5" y2="6"/>
      <line x1="1.5" y1="9.5" x2="14.5" y2="9.5"/>
      <line x1="5.5" y1="6" x2="5.5" y2="13.5"/>
      <line x1="10.5" y1="6" x2="10.5" y2="13.5"/>
    </svg>
  );
}

export function TextViewIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round">
      <line x1="2" y1="4" x2="14" y2="4"/>
      <line x1="2" y1="7" x2="14" y2="7"/>
      <line x1="2" y1="10" x2="10" y2="10"/>
      <line x1="2" y1="13" x2="12" y2="13"/>
    </svg>
  );
}

export function JsonViewIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
      <path d="M5 3 C3.5 3 3 3.8 3 5 L3 7 C3 8.1 2.3 8.5 2 8.5 C2.3 8.5 3 9 3 10 L3 12 C3 13.2 3.5 14 5 14"/>
      <path d="M11 3 C12.5 3 13 3.8 13 5 L13 7 C13 8.1 13.7 8.5 14 8.5 C13.7 8.5 13 9 13 10 L13 12 C13 13.2 12.5 14 11 14"/>
      <circle cx="6.5" cy="8.5" r="0.9" fill="currentColor" stroke="none"/>
      <circle cx="9.5" cy="8.5" r="0.9" fill="currentColor" stroke="none"/>
    </svg>
  );
}

export function StatsViewIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="currentColor">
      <rect x="1.5" y="9" width="3" height="5" rx="0.5"/>
      <rect x="6.5" y="5.5" width="3" height="8.5" rx="0.5"/>
      <rect x="11.5" y="2" width="3" height="12" rx="0.5"/>
    </svg>
  );
}

export function CodeViewIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="5.5,4.5 1.5,8 5.5,11.5"/>
      <polyline points="10.5,4.5 14.5,8 10.5,11.5"/>
      <line x1="9" y1="3" x2="7" y2="13"/>
    </svg>
  );
}

export function ExpandModalIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="9.5,1.5 14.5,1.5 14.5,6.5"/>
      <polyline points="6.5,14.5 1.5,14.5 1.5,9.5"/>
      <line x1="14.5" y1="1.5" x2="9" y2="7"/>
      <line x1="1.5" y1="14.5" x2="7" y2="9"/>
    </svg>
  );
}

export function CollapseModalIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="7,2.5 7,7 2.5,7"/>
      <polyline points="9,13.5 9,9 13.5,9"/>
      <line x1="7" y1="7" x2="1.5" y2="1.5"/>
      <line x1="9" y1="9" x2="14.5" y2="14.5"/>
    </svg>
  );
}

export function CollapseQueryIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="3,10 8,5 13,10"/>
    </svg>
  );
}

export function ExpandQueryIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="3,6 8,11 13,6"/>
    </svg>
  );
}

export function PushPinIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size}>
      {/* rotate -45°: cap upper-left, needle lower-right */}
      <g transform="rotate(-45, 8, 8)" fill="currentColor">
        {/* round dome cap */}
        <circle cx="8" cy="3.2" r="2.6"/>
        {/* flat rim disc */}
        <ellipse cx="8" cy="6.8" rx="3.4" ry="1.1"/>
        {/* thin needle */}
        <line x1="8" y1="7.9" x2="8" y2="14" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" fill="none"/>
      </g>
    </svg>
  );
}

export function ZoomInIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round">
      <circle cx="6.5" cy="6.5" r="4.5"/>
      <line x1="4.5" y1="6.5" x2="8.5" y2="6.5"/>
      <line x1="6.5" y1="4.5" x2="6.5" y2="8.5"/>
      <line x1="9.9" y1="9.9" x2="14" y2="14"/>
    </svg>
  );
}

export function ZoomOutIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round">
      <circle cx="6.5" cy="6.5" r="4.5"/>
      <line x1="4.5" y1="6.5" x2="8.5" y2="6.5"/>
      <line x1="9.9" y1="9.9" x2="14" y2="14"/>
    </svg>
  );
}

export function FitScreenIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="1.5,5.5 1.5,1.5 5.5,1.5"/>
      <polyline points="10.5,1.5 14.5,1.5 14.5,5.5"/>
      <polyline points="14.5,10.5 14.5,14.5 10.5,14.5"/>
      <polyline points="5.5,14.5 1.5,14.5 1.5,10.5"/>
    </svg>
  );
}

export function ExportIcon({ size = 16 }: IconProps) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="8" y1="2" x2="8" y2="10.5"/>
      <polyline points="5,7.5 8,11 11,7.5"/>
      <line x1="2" y1="14" x2="14" y2="14"/>
    </svg>
  );
}
