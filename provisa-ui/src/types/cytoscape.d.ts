// Shim for cytoscape under moduleResolution "bundler" + verbatimModuleSyntax.
//
// cytoscape ships an ESM entry (`import: ./dist/cytoscape.esm.mjs`) but its
// type declarations use the legacy `export =` / `export as namespace` pattern.
// Under bundler resolution TypeScript resolves types via the package exports map
// "types" field, giving `typeof import("cytoscape")` a shape that:
//   • is not callable (no call signatures survive the synthetic-default lift)
//   • does not expose namespace members as named exports
//
// This ambient module augmentation provides ESM-compatible declarations so that:
//   import cytoscape from "cytoscape"         → callable factory + .use()
//   import type { Core, … } from "cytoscape"  → resolved named types
//
// All concrete type bodies are intentionally minimal — we delegate the actual
// structural checking to the existing @types/cytoscape package via skipLibCheck.

declare module "cytoscape" {
  // ── Primitive aliases used across the types ───────────────────────────────
  type Position = { x: number; y: number };
  type Selector = string;

  // ── Extension hook ────────────────────────────────────────────────────────
  type Ext = (cy: CytoscapeStatic) => void;

  // ── Event object ──────────────────────────────────────────────────────────
  interface EventObject {
    target: SingularElementReturnValue & CytoscapeInstance;
    position: Position;
    renderedPosition?: Position;
    [key: string]: unknown;
  }

  // ── Element types ─────────────────────────────────────────────────────────
  interface ElementDefinition {
    group?: "nodes" | "edges";
    data: Record<string, unknown>;
    [key: string]: unknown;
  }

  interface SingularElement {
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

  interface NodeSingular extends SingularElement {
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

  interface EdgeSingular extends SingularElement {
    isEdge(): true;
    isNode(): false;
    source(): NodeSingular;
    target(): NodeSingular;
  }

  type SingularElementReturnValue = NodeSingular & EdgeSingular;

  interface Collection {
    length: number;
    [index: number]: SingularElementReturnValue;
    forEach(fn: (ele: SingularElementReturnValue, i: number, arr: this) => void): this;
    filter(selector: Selector | ((ele: SingularElementReturnValue) => boolean)): this;
    map<T>(fn: (ele: SingularElementReturnValue, i: number, arr: this) => T): T[];
    includes(ele: SingularElementReturnValue): boolean;
    toArray(): SingularElementReturnValue[];
  }

  interface NodeCollection extends Collection {
    forEach(fn: (ele: NodeSingular, i: number, arr: this) => void): this;
    map<T>(fn: (ele: NodeSingular, i: number, arr: this) => T): T[];
  }

  // ── Layout ────────────────────────────────────────────────────────────────
  interface BaseLayoutOptions {
    name: string;
    animate?: boolean;
    fit?: boolean;
    padding?: number;
    ready?: () => void;
    stop?: () => void;
    [key: string]: unknown;
  }

  type LayoutOptions = BaseLayoutOptions;

  interface Layouts {
    run(): this;
    stop(): this;
    on(event: string, fn: (e: EventObject) => void): this;
    one(event: string, fn: (e: EventObject) => void): this;
    off(event: string, fn?: (e: EventObject) => void): this;
  }

  // ── Core (cytoscape instance) ─────────────────────────────────────────────
  interface Core {
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

  // ── cytoscape static / factory ────────────────────────────────────────────
  interface CytoscapeStatic {
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

  // Alias used in a few places in GraphFrame.tsx
  type CytoscapeInstance = Core;

  const cytoscape: CytoscapeStatic;
  export default cytoscape;
  export type {
    Core,
    NodeSingular,
    EdgeSingular,
    NodeCollection,
    Collection,
    SingularElementReturnValue,
    ElementDefinition,
    LayoutOptions,
    Layouts,
    BaseLayoutOptions,
    EventObject,
    Position,
    Ext,
  };
}
