// ESM-compatible shim for cytoscape.
// Used via tsconfig.app.json "paths" to replace the package's export= declarations.
// cytoscape ships ESM but its index.d.ts uses legacy export= / export as namespace,
// which is incompatible with moduleResolution "bundler" + verbatimModuleSyntax.

export type Position = { x: number; y: number };
export type Selector = string;
export type Ext = (cy: CytoscapeStatic) => void;

export interface EventObject {
  target: SingularElementReturnValue & CytoscapeInstance;
  position: Position;
  renderedPosition?: Position;
  [key: string]: unknown;
}

export interface ElementDefinition {
  group?: "nodes" | "edges";
  data: Record<string, unknown>;
  [key: string]: unknown;
}

export interface SingularElement {
  id(): string;
  data(key: string): unknown;
  data(key: string, value: unknown): this;
  lock(): this;
  unlock(): this;
  select(): this;
  unselect(): this;
  style(name: string, value: unknown): this;
  style(props: Record<string, unknown>): this;
  addClass(cls: string): this;
  removeClass(cls: string): this;
  position(): Position;
  position(pos: Position): this;
  [key: string]: unknown;
}

export interface NodeSingular extends SingularElement {
  isNode(): true;
  isEdge(): false;
  connectedEdges(): Collection;
  neighborhood(): Collection;
  children(): NodeCollection;
  parent(): NodeCollection;
  outgoers(): Collection;
  incomers(): Collection;
  ancestors(): NodeCollection;
  descendants(): NodeCollection;
}

export interface EdgeSingular extends SingularElement {
  isEdge(): true;
  isNode(): false;
  source(): NodeSingular;
  target(): NodeSingular;
}

export type SingularElementReturnValue = NodeSingular & EdgeSingular;

export interface Collection {
  length: number;
  [index: number]: SingularElementReturnValue;
  forEach(fn: (ele: SingularElementReturnValue, i: number, arr: this) => void): this;
  filter(selector: Selector | ((ele: SingularElementReturnValue) => boolean)): this;
  map<T>(fn: (ele: SingularElementReturnValue, i: number, arr: this) => T): T[];
  includes(ele: SingularElementReturnValue): boolean;
  toArray(): SingularElementReturnValue[];
}

export interface NodeCollection extends Collection {
  forEach(fn: (ele: NodeSingular, i: number, arr: this) => void): this;
  map<T>(fn: (ele: NodeSingular, i: number, arr: this) => T): T[];
}

export interface BaseLayoutOptions {
  name: string;
  animate?: boolean;
  fit?: boolean;
  padding?: number;
  ready?: () => void;
  stop?: () => void;
  [key: string]: unknown;
}

export type LayoutOptions = BaseLayoutOptions;

export interface Layouts {
  run(): this;
  stop(): this;
  on(event: string, fn: (e: EventObject) => void): this;
  one(event: string, fn: (e: EventObject) => void): this;
  off(event: string, fn?: (e: EventObject) => void): this;
}

export interface Core {
  // Selection
  $(selector: Selector): Collection;
  $id(id: string): SingularElementReturnValue;
  nodes(selector?: Selector): NodeCollection;
  edges(selector?: Selector): Collection;
  elements(selector?: Selector): Collection;

  // Manipulation
  add(eles: ElementDefinition | ElementDefinition[] | Collection): Collection;
  remove(eles: Collection | Selector): Collection;
  batch(fn: () => void): this;

  // Layout
  layout(options: LayoutOptions): Layouts;
  makeLayout(options: LayoutOptions): Layouts;

  // Viewport
  fit(eles?: Collection, padding?: number): this;
  zoom(): number;
  zoom(level: number): this;
  zoom(opts: { level: number; position?: Position }): this;
  pan(): Position;
  pan(pos: Position): this;
  center(eles?: Collection): this;
  reset(): this;

  // Events
  on(events: string, fn: (e: EventObject) => void): this;
  on(events: string, selector: Selector, fn: (e: EventObject) => void): this;
  off(events: string, fn?: (e: EventObject) => void): this;
  off(events: string, selector: Selector, fn?: (e: EventObject) => void): this;

  // Export
  png(options?: Record<string, unknown>): string;
  jpg(options?: Record<string, unknown>): string;
  svg(options?: Record<string, unknown>): string;

  // Style
  style(styleSheet?: unknown): this;

  // Destroy
  destroy(): void;
}

export interface CytoscapeStatic {
  (options: {
    container: Element | null;
    elements?: ElementDefinition[];
    style?: unknown[];
    layout?: LayoutOptions;
    minZoom?: number;
    maxZoom?: number;
    [key: string]: unknown;
  }): Core;
  use(ext: Ext): void;
  warnings(condition: boolean): void;
}

// Alias used in GraphFrame.tsx
export type CytoscapeInstance = Core;

declare const cytoscape: CytoscapeStatic;
export default cytoscape;
