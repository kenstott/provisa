# ProvisaGraph: the one 2D graph component (REQ-2239)

Every 2D graph view (the ERD, the graph explorer's lane diagram, the ontology graph and any later
one) is drawn by `ProvisaGraph`. It is the only code that imports cytoscape or a cytoscape
extension (`src/components/__tests__/graphGuard.test.ts` holds this). The 3D view and text
diagrams (mermaid in Analyze) are not 2D graph views and keep their own.

## What a view passes

- `nodes`: `{ id, type, name, kind?, parent?, lines?, status?, data? }`. `type` is a key of
  `NODE_TYPES` (tokens.ts). `lines` is a body under the name (an ERD table's columns); a line may
  be `key` (shown at detail "key"). `kind: "more"` is a fold node standing for nodes not drawn
  ("+N columns"); clicking it is the view's `onMore`.
- `edges`: `{ id, source, target, label?, arrow?, dashed?, tone?, data? }`.
- `groups`: `{ id, label, description?, collapsed? }`.
- `preset`: `"layered"` (ELK layered, groups packed), `"lanes"` (ELK layered in fixed lane
  partitions, left to right), or `"force"` (fCoSE).

A view sets no colour, shape, size, font or control of its own. `data` carries the view's own
identifiers (a table id, a relationship id) back to it on a click; it is never styled.

## Tokens (tokens.ts), taken from the detail cards

The detail cards (DetailCard, the application's collapsible) are a 1px border in Mantine
`gray-3` / `dark-4`, `radius sm`, a heading in `font-size sm` weight 500, a filled body in `gray-0`
/ `dark-6`. The graph takes the same values; colours are Mantine's own (DEFAULT_THEME) and the
brand scale (theme.ts).

| Token | Light | Dark |
|---|---|---|
| canvas | `gray-0` | `dark-7` |
| dot grid | `gray-3` | `dark-5` |
| node fill | white | `dark-6` |
| node border | `gray-4` | `dark-4` |
| node radius | 4px (`radius sm`) | 4px |
| node shadow | 1px under-glow at 6% black (`shadow xs`, as cytoscape draws it) | 12% black |
| text | `gray-9` | `dark-0` |
| text, second line | `gray-6` | `dark-2` |
| edge | `gray-5` | `dark-3` |
| accent (hover, selection, focus) | brand-6 | brand-4 |

Sizes: a node is 180px wide (an ERD table with columns grows in height, never in width), 44px
tall with two lines. The label is 11px, weight 500 (cytoscape draws one font per label: the name
and the type's line share it; the type's line is uppercase). Edges are 1px. Group label 10px
uppercase, weight 700.

## Nodes

- One shape: a rounded rectangle, radius 4, a 1px border.
- A band on its leading (left) edge, 6px wide, in the type's colour. The type colour is Mantine's
  shade 6 of the type's hue (`NODE_TYPES`).
- An icon, the type's (lucide's), drawn as the node's background image at the band's right, in
  the type's colour.
- A two-line label: the name, then the type's name in uppercase. A node with `lines` lists them
  under a rule, left-aligned, in monospace.
- A fold node (`kind: "more"`) is the same box, dashed, with no band.
- A term's status: draft italic, deprecated at 55% opacity, retired at 35%.

## Edges

- One weight (1px), one curve (bezier). A view's reader may route an ERD's lines orthogonally
  (taxi); nothing else changes.
- Arrowheads only where direction means something: `arrow: "target" | "source" | "both"`
  (ontology relationships, lineage, an ERD's cardinality). The default is none.
- `dashed`: pending or indirect (a term link, a line to a collapsed group, a fold node's line).
- `tone: "via"`: the legs through an ERD junction, in the junction's grape.
- A label is 9px, on a canvas-coloured backing, never rotated.

## Groups

Dashed, lightly tinted boxes: a 1.5px dashed border in the group's hue at 55%, a 6% fill of the
hue, the label uppercase on the top border, on a canvas-coloured backing. The hue is derived from
the group's id (one hue per domain, everywhere). A collapsed group keeps its box, empty, at its
least size; lines to its members end on it, dashed. Under the lanes preset each lane is boxed and
titled the same way.

## Canvas

- A dot grid (CSS on the stage, 16px pitch), in the canvas token.
- Controls bottom-left, one vertical bar, always the same order: zoom in, zoom out, fit, full
  screen, download (SVG, PNG, JPEG, and JSON where the view gives it), then copy as text where
  the view gives it. Shown on hover or keyboard focus.
- One legend, top-right (clear of the application's floating buttons at the bottom-right): the
  types drawn, each a band swatch, icon and name.
- Top-left: what the view lays over the canvas (the ERD's tables new since a save), then the hint.
- One hover state: the node hovered and what it brings forward lit (accent), the rest faded to
  20%. What a node brings forward is its own lines and their ends, unless the view says more
  (the lane diagram's paths along the lanes).
- One selection state: a 2px accent border, a soft accent halo.
- Focus (a view's named nodes): the selection's look, and fitted into view, never zoomed past 1.5.
- A filter (a view's matches): matches in accent, the rest dimmed to 15%.

## Layout presets

- `layered`: ELK layered, upward, groups laid out with their children; then nodes without edges
  placed in a grid under their group, groups packed to the pane's aspect, related nodes kept
  apart (the ERD's arrangement).
- `lanes`: ELK layered left to right in fixed lane partitions; one lane may be a compound laid out
  top-down by its rank edges; a second pass orders each lane by the first's placement; arcs
  laid over the placed nodes; each lane's title above it.
- `force`: fCoSE.

## Owned by the component

Cytoscape setup and extension registration, the stylesheet, the presets, the controls, the
legend, hover and selection, focus and filter marks, saved places (read and stand), group
collapse and resize, grid snap, orphan placement, and SVG/PNG/JPEG export.
