// Copyright (c) 2026 Kenneth Stott
// Canary: c03ff5ba-89de-4e38-aa5d-b7381f483340
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface ErdPalette {
  bg: string;
  panelBorder: string;
  text: string;
  textMuted: string;
  textFaint: string;
  accent: string;
  tooltipBg: string;
  tooltipBorder: string;
  tableBg: string;
  relLine: string;
  relLineFaint: string;
  // REQ-1588: junction nodes and the legs through them, in the grape the VIA badge on the
  // relationships table already uses.
  junction: string;
  junctionBg: string;
}

const DARK: ErdPalette = {
  bg: "#0f172a",
  panelBorder: "#1e293b",
  text: "#e2e8f0",
  textMuted: "#94a3b8",
  textFaint: "#475569",
  accent: "#60a5fa",
  tooltipBg: "#1e293b",
  tooltipBorder: "#334155",
  tableBg: "#1e293b",
  relLine: "#475569",
  relLineFaint: "#334155",
  junction: "#cc5de8",
  junctionBg: "#2b1733",
};

const LIGHT: ErdPalette = {
  bg: "#f8fafc",
  panelBorder: "#e2e8f0",
  text: "#1e293b",
  textMuted: "#475569",
  textFaint: "#94a3b8",
  accent: "#2563eb",
  tooltipBg: "#ffffff",
  tooltipBorder: "#cbd5e1",
  tableBg: "#ffffff",
  relLine: "#94a3b8",
  relLineFaint: "#cbd5e1",
  junction: "#be4bdb",
  junctionBg: "#f8effc",
};

export function getErdPalette(isDark: boolean): ErdPalette {
  return isDark ? DARK : LIGHT;
}
