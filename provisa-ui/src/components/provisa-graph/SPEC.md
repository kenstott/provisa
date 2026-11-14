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
- `preset`: `"layered"` (ELK layered, groups packed), `"lanes"` (fixed lanes left to right,
  sorted and placed by the component, no layout library), or `"force"` (fCoSE).

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
| node fill | white | `dark-6` |
| node border | `gray-4` | `dark-4` |
| node radius | 4px (`radius sm`) | 4px |
| node shadow | 1px under-glow at 6% black (`shadow xs`, as cytoscape draws it) | 12% black |
| text | `gray-9` | `dark-0` |
| text, second line | `gray-6` | `dark-2` |
| edge | `gray-5` | `dark-3` |
| accent (hover, selection, focus) | brand-6 | brand-4 |

Sizes: a node is at least 180px wide and as wide as its name and type's line need, up to 360px
(model.ts `boxWidth`); past that its name wraps and the node grows in height. An ERD table's
columns wrap inside that width and grow it in height. A node is 44px tall with two lines. The label is 11px, weight 500 (cytoscape draws one font per label: the name
and the type's line share it; the type's line is uppercase). Edges are 1px. Group label 10px
uppercase, weight 700.

## Nodes

- One shape (save the junction's diamond, below): a rounded rectangle, radius 4, a 1px border.
- A band on its leading (left) edge, 6px wide, in the type's colour. The type colour is Mantine's
  shade 6 of the type's hue (`NODE_TYPES`).
- An icon, the type's (lucide's), drawn as the node's background image at the band's right, in
  the type's colour.
- A two-line label: the name, then the type's name in uppercase. The label runs from 8px past the
  icon to 10px short of the right edge (`TEXT_LEFT`, `TEXT_RIGHT`), never over the icon: its
  room is the node's own (`text-max-width` from the node's width), and a label longer than the room
  wraps at its spaces, or anywhere when one of its words (a dotted name with no space in it) is
  wider than the room (`labelWrap`, `text-overflow-wrap`). A node with `lines` lists them
  under a rule, left-aligned, in monospace.
- A fold node (`kind: "more"`) is the same box, dashed, with no band.
- **The one exception: a junction is a diamond** (REQ-2239, amended 2026-11-11, A JUNCTION IS A
  DIAMOND). A junction table (an associative table that exists to connect others, REQ-1588) is
  drawn as Chen's relationship diamond, the form readers know from ER and UML. It keeps its band's
  colour (grape: the border, and the fill a tint of it), its icon above its name and type, no body,
  and its legs (`tone: "via"`). It is `NODE_TYPES.junction.shape`; the stylesheet's only diamond rule
  is `node[shape = "diamond"]`, and no other type may take a shape. The ERD marks a junction from
  erd-model's `erd-junction` tag. The diamond is 150 × 90, has no under-glow (cytoscape draws none
  for a diamond), and its selection is its border at 2.5px with no halo. The legend draws its
  swatch as a small diamond.
- A term's status: draft italic, deprecated at 55% opacity, retired at 35%.

## Edges

- One weight (1px), one curve (bezier). A view's reader may route an ERD's lines orthogonally
  (taxi); nothing else changes.
- Arrowheads only where direction means something: `arrow: "target" | "source" | "both"`
  (ontology relationships, lineage, an ERD's cardinality). The default is none.
- `dashed`: pending or indirect (a line to a collapsed group, a line to a fold node).
- `tone: "via"`: the legs through an ERD junction, in the junction's grape.
- Lines whose direction carries meaning (the lane diagram's term links, lineage, ontology
  relationships) are solid with arrows; dashed means pending or indirect, never direction.
- A label is 9px, on a canvas-coloured backing, never rotated.

## Groups

Dashed, lightly tinted boxes: a 1.5px dashed border in the group's hue at 55%, a 6% fill of the
hue, the label uppercase on the top border, on a canvas-coloured backing. The hue is derived from
the group's id (one hue per domain, everywhere). A collapsed group keeps its box, empty, at its
least size; lines to its members end on it, dashed. Under the lanes preset each lane is boxed and
titled the same way.

## Canvas

- A plain canvas in the canvas token: no dot grid (maintainer, 2026-11-11).
- How a graph opens, and Reset view, fit by the diagram's drawn extent, below the legend
  (`fitClear`). The whole diagram is fitted while the
  zoom stays at 0.8 or more (`READABLE_ZOOM`); a bigger diagram is drawn at the readable zoom from
  its top (and, wider than the canvas, its left) and scrolls: the wheel scrolls it down, Shift with
  the wheel or a trackpad's sideways swipe across (at its end the page scrolls); a pinch or the
  wheel with Ctrl or Cmd held zooms about the pointer.
- A graph's focus is the node it is about, given as `opensOn`. A term's graph has its term as
  focus. A graph with a focus opens at the fit's zoom and is then moved so the focus is in view.
  Reset view does the same. `showFocus` makes the move and `focusShift` is its rule (REQ-2239).
  The rule works per axis and then on both together:
  1. Along an axis on which the diagram fits the canvas, the diagram does not move. Along one it
     runs past, it moves no further than it can scroll.
  2. The move keeps the focus inside the canvas, with the fit's margin round it and below the
     legend. A focus larger than the canvas is held at the canvas's start.
  3. Among such moves, the chosen one shows the most nodes the focus joins directly. A node
     counts only when it is shown whole, across and down.
  4. Among those that tie, the chosen one is the least move from the fit.

  A diagram that fits opens as fitted. So does a graph with no focus, or with a focus it does not
  draw.
- The fit control fits the entire image, below the legend, however small that makes it
  (`fitWhole`; REQ-2239, FIT MEANS THE WHOLE IMAGE): the readable floor is not applied.
- A graph being laid out says so (REQ-2239, amended, A GRAPH BEING LAID OUT SAYS SO): from the
  moment a layout starts (on open, and on any change of what is drawn or how: preset, filter,
  depth, saved places, Reset view's relayout) until every node has its place and the opening fit
  has run, the core's div is held hidden, so the frame of unplaced nodes stacked at one point is
  never painted, and the canvas shows the graph views' loader (ReadState's `Loader`) with
  `provisaGraph.layingOut`. The layout starts once that frame has been painted, so a placement
  that runs as one long task (a large lane diagram's) is seen to be under way. The controls and legend stay; the controls, downloads, the camera and
  a save act only on a placed drawing. A layout that throws as it starts, that fails after it
  started (ELK's answer rejected: `elkLayout.ts` says it as `layoutfail`), or whose placement
  throws once it answers, ends the laying out and is thrown to the page's error boundary. There
  is no time limit: a slow layout is not a failed one.
- The hint (top-left) of a view whose wheel scrolls says so: `provisaGraph.hint`.
- Controls bottom-left, one vertical bar, always the same order: zoom in, zoom out, fit, full
  screen, download (SVG, PNG, JPEG, and JSON where the view gives it), then copy as text where
  the view gives it. Shown on hover or keyboard focus. Each control's tooltip is drawn in place
  (so it shows in full screen), on one line beside its button: an in-place Tip is as wide as its
  text (`max-content`, up to the tip's 340px), not as wide as the bar.
- The bar is the one stack every graph's canvas has, 2D and 3D (`CanvasControls.tsx`:
  `CanvasControls`, `CanvasButton`, `FullscreenButton`; `canvas-controls.css`). A 3D view
  (`profile3d/Stage3D.tsx`: the ontology graph and the table profile views) holds, in this order,
  Reset view, Rotate, Grid (where its scene draws a floor grid), full screen and download (image
  or a recorded turn, `ExportMenu.tsx`). Rotate and Grid are toggles: pressed (`aria-pressed`,
  the light variant) while on, read from and written to the view's remembered settings. What is
  drawn and how it is arranged stays in the view's toolbar; Rotate, Grid and Reset view are not
  toolbar buttons in 3D. A 3D view's legend stands beside the stack, not under it. The ontology
  explorer's lane diagram keeps its toolbar Reset view: it folds every table back as well as
  fitting (`fitClear`), where the stack's fit fits the whole image (`fitWhole`).
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

- `layered`: ELK layered (`elkLayout.ts`, Provisa's adapter around elkjs, registered as
  `provisa-elk`), upward, groups laid out with their children; then nodes without edges
  placed in a grid under their group, groups packed to the pane's aspect, related nodes kept
  apart (the ERD's arrangement).
- `lanes` (REQ-2239, amended 2026-11-11; REQ-2174, EACH SIBLING TERM ON ITS OWN ROW): no layout
  library places the diagram. Every lane but the term (`beside`) lane is sorted left to right by a
  sort, not a search (`sortLanes`): the first lane keeps its order; each lane after it by the mean
  place of what its nodes join on their left, nodes tied on that key (the tables of one product, or
  of none) next to the one reaching the most of the same nodes two lanes on (tables sharing terms),
  ties in the order given (the sort reads those lanes spread evenly over the tallest one's height).
  Those lanes stand side by side, each as wide as its widest node (`placeLanes`). (Amended
  2026-11-12, COLUMNS ARE FIXED; TABLES, PRODUCTS AND TERMS ALIGN TO THEM.) The lane just left of the
  term lane (the columns) is placed first and is fixed: packed in its sorted order from the top,
  `NODE_GAP` between rows, a table's columns one contiguous run. Every other lane is aligned to it, as
  a rule of the preset and not of named lanes: a node sits at the midpoint of the top and bottom
  node it reaches in the fixed lane through the lanes between (a table its columns, a folded table
  its one more-columns node, a product its tables' columns). Nodes of one lane whose midpoints fall
  closer than the node gap keep their order and move apart by the least (the summed square of the
  moves). A node reaching nothing in the fixed lane pulls on nothing: it keeps its place in its
  lane's order, a node gap below the node before it (above the node after it when it is first); a
  lane where nothing reaches the fixed lane is packed from the top. The term lane is placed, not sorted
  (`placeRows`), scored by the angle of its lines to the columns (the lanes a fixed distance apart,
  the summed vertical run): a term bound to one column sits level with it; a term bound to several
  sits level with the median of them (two tied: fewer crossings, then the higher); the children of
  one parent, and a parent bound to no column with them, are a block on adjacent rows, in the order
  of their columns, placed where its lines' summed run is least. Units are placed one at a time,
  single-column terms first, then multi-column terms, then blocks, then terms bound to nothing
  (nearest the terms they link to, else below the rest); none takes a row another has taken, so a
  block moves round the terms already level. Every term has its own row, `NODE_GAP` apart. The
  lines between two terms (rank lines and the arcs laid over them) run beside the lane, on its
  right, as right-angled segments, never through a term's row (`besideRoutes`). Each such line's
  name (J1227) is drawn right of its run down its track, growing away from the lane, centred on
  the middle of that run (`besideNames`); a name never lies on a node, another name or any line's
  route: it moves along its own run a name's height at a time (nearest the middle first, above
  before below) to the first clear place, and where its run has none it moves right past what it
  meets. Names are placed from the track nearest the lane outward, each track top to bottom. Other
  lines keep the generic edge label. Crossings that no placement removes are accepted. The same
  graph is always drawn the same way. Each lane's box bounds its own nodes (and the term lane's
  lines beside it and their names), its title above them, the boxes `LANE_GAP` apart.
- `force`: fCoSE.

## Owned by the component

Cytoscape setup and extension registration, the stylesheet, the presets, the controls, the
legend, hover and selection, focus and filter marks, saved places (read and stand), group
collapse and resize, grid snap, orphan placement, and SVG/PNG/JPEG export.
