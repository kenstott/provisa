// Copyright (c) 2026 Kenneth Stott
// Canary: 9de299a9-a74d-4814-a02f-f9d7a2f7fb95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Local types for cytoscape — avoids import-resolution issues with the package's
// legacy export= declarations under moduleResolution "bundler".
export type CyLayoutOptions = { name: string; [key: string]: unknown };
export type CyElementDefinition = {
  group?: "nodes" | "edges";
  data: Record<string, unknown>;
  [key: string]: unknown;
};
// Aliases kept for callers that reference the old names.
export type CyElementDef = CyElementDefinition;
export interface CyElement {
  id(): string;
  data(key: string): unknown;
  data(key: string, value: unknown): CyElement;
  lock(): CyElement;
  unlock(): CyElement;
  locked(): boolean;
  select(): CyElement;
  unselect(): CyElement;
  style(name: string, value: unknown): CyElement;
  style(props: Record<string, unknown>): CyElement;
  removeStyle(names: string): CyElement;
  addClass(cls: string): CyElement;
  removeClass(cls: string): CyElement;
  position(): { x: number; y: number };
  position(dimension: string): number;
  position(pos: { x: number; y: number }): CyElement;
  source(): CyElement;
  target(): CyElement;
  neighborhood(): CyCollection;
  children(): CyNodeCollection;
  renderedPosition(): { x: number; y: number };
  renderedWidth(): number;
  width(): number;
  height(): number;
  boundingBox(opts?: object): { x1: number; x2: number; y1: number; y2: number; w: number; h: number };
  renderedBoundingBox(opts?: object): { x1: number; x2: number; y1: number; y2: number; w: number; h: number };
  degree(includeLoops: boolean): number;
  empty(): boolean;
}
export interface CyCollection {
  length: number;
  [index: number]: CyElement;
  forEach(fn: (ele: CyElement, i: number) => void): this;
  map<T>(fn: (ele: CyElement, i: number) => T): T[];
  filter(fn: ((ele: CyElement) => boolean) | string): this;
  not(selector: string): this;
  select(): this;
  unselect(): this;
  remove(): this;
  position(): { x: number; y: number };
  position(pos: { x: number; y: number }): this;
  lock(): this;
  unlock(): this;
  locked(): boolean;
  addClass(cls: string): this;
  removeClass(cls: string): this;
  neighborhood(selector?: string): CyCollection;
  children(): CyNodeCollection;
  data(key: string): unknown;
  id(): string;
  style(name: string, value: unknown): this;
  style(props: Record<string, unknown>): this;
  removeStyle(names: string): this;
  empty(): boolean;
  boundingBox(opts?: object): { x1: number; x2: number; y1: number; y2: number; w: number; h: number };
  renderedBoundingBox(opts?: object): { x1: number; x2: number; y1: number; y2: number; w: number; h: number };
  degree(includeLoops: boolean): number;
  width(): number;
  height(): number;
}
export interface CyNodeCollection extends CyCollection {
  forEach(fn: (ele: CyElement, i: number) => void): this;
}
export interface CyEvent {
  target: CyElement & CyInstance;
  position: { x: number; y: number };
  renderedPosition?: { x: number; y: number };
}
export interface CyInstance {
  $(selector: string): CyCollection;
  $id(id: string): CyCollection;
  nodes(selector?: string): CyNodeCollection;
  edges(selector?: string): CyCollection;
  elements(selector?: string): CyCollection;
  add(eles: CyElementDef | CyElementDef[] | CyCollection): CyCollection;
  remove(eles: CyCollection | string): CyCollection;
  batch(fn: () => void): void;
  layout(options: CyLayoutOptions): {
    run(): void;
    stop(): void;
    one(evt: string, fn: () => void): void;
  };
  fit(eles?: CyCollection, padding?: number): void;
  zoom(): number;
  zoom(level: number): void;
  pan(): { x: number; y: number };
  pan(pos: { x: number; y: number }): void;
  on(events: string, fn: (e: CyEvent) => void): void;
  on(events: string, selector: string, fn: (e: CyEvent) => void): void;
  off(events: string, fn?: (e: CyEvent) => void): void;
  png(options?: Record<string, unknown>): string;
  jpg(options?: Record<string, unknown>): string;
  svg(options?: Record<string, unknown>): string;
  destroy(): void;
  style(sheet?: unknown): void;
  container(): HTMLElement;
  getElementById(id: string): CyCollection;
  forceRender(): void;
}
