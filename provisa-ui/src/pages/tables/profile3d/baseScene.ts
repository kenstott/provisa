// Copyright (c) 2026 Kenneth Stott
// Canary: 97c4d914-20bf-45ae-bb85-e42f387d5b06
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: what every 3D profile view shares — renderer, orbit camera, lights, HTML labels pinned
// to scene points, pointer picking and camera moves. Each view's scene extends it and is imported
// only through a dynamic import(), so three.js loads the first time a 3D view is shown.

import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";
import { FrameLoop } from "./frameLoop";
import { TurnPause } from "./turnPause";
import { bestView, type View, type ViewNode } from "./bestView";

/** How often a stage that only turns is drawn (20 frames a second). */
const TURN_FRAME_MS = 50;
/** How fast a stage turns: a full turn in 100 s. */
const TURN_RAD_PER_MS = (Math.PI * 2) / 100_000;

export interface SceneTheme {
  canvas: string;
  ground: string;
  grid: string;
  ramp: [string, string, string, string, string];
  wallA: string;
  wallB: string;
  wallLine: string;
  nul: string;
  qTail: string;
  qMid: string;
  hl: string;
  accent: string;
  warm: string;
  cool: string;
  family: Record<"numeric" | "temporal" | "text" | "boolean" | "other" | "foreign", string>;
  dark: boolean;
}

export interface SceneCallbacks<H> {
  onHover: (h: H | null, x: number, y: number) => void;
  onSelect: (h: H) => void;
  onUserMove: () => void;
}

interface PinnedLabel {
  el: HTMLDivElement;
  at: () => THREE.Vector3;
  rotate: number;
  alignEnd: boolean;
  /** Where the last frame put it on the stage, in CSS pixels; null while it is behind the camera. */
  placed: { x: number; y: number } | null;
}

/** A pinned label as the last frame drew it: what an export paints in place of the DOM layer. */
export interface PlacedLabel {
  el: HTMLElement;
  x: number;
  y: number;
  rotate: number;
  alignEnd: boolean;
}

/** One frame of a full orbit, at ``fraction`` of the way round (0 is the view it started from). */
export interface Orbit {
  frame: (fraction: number, draw: (src: HTMLCanvasElement) => void) => void;
  end: () => void;
}

/** A scene projected flat, for an SVG: what the stage shows, in CSS pixels. Each shape is a drawn
 * mesh's silhouette (the outline of its projected vertices) in its material's colour, listed back
 * to front, so later shapes cover earlier ones as they do on the stage. */
export interface VectorScene {
  width: number;
  height: number;
  /** Line segments drawn under everything else (the floor grid). */
  floor: { x1: number; y1: number; x2: number; y2: number; color: string; opacity: number }[];
  shapes: { points: [number, number][]; color: string; opacity: number }[];
}

const UP_AXIS = new THREE.Vector3(0, 1, 0);
/** Looking straight at the z = 0 plane: a 2D view. */
const FACE_ON: View = {
  dir: new THREE.Vector3(0, 0, 1),
  up: new THREE.Vector3(0, 1, 0),
  axis: new THREE.Vector3(0, 0, 1),
};

export abstract class BaseScene<H> {
  protected renderer: THREE.WebGLRenderer;
  protected scene = new THREE.Scene();
  /** The 3D camera, and the 2D one: face-on, orthographic, panned and zoomed only (setFlat). */
  protected persp = new THREE.PerspectiveCamera(38, 1, 0.1, 600);
  protected ortho = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 600);
  /** The camera the stage is drawn through. */
  protected camera: THREE.PerspectiveCamera | THREE.OrthographicCamera = this.persp;
  /** Whether the stage is in 2D (setFlat). */
  protected flat = false;
  /** REQ-2174: the content lies in the z = 0 plane (a layered layout), so even the 3D view opens
   * face-on, turning about the plane's normal. */
  protected planar = false;
  /** REQ-2174: the view that shows the most of the content, once computed (settleView). */
  protected best: View | null = null;
  protected controls: OrbitControls;
  protected hemi = new THREE.HemisphereLight(0xffffff, 0x444444, 0.7);
  protected sun = new THREE.DirectionalLight(0xffffff, 0.85);
  protected theme: SceneTheme;
  protected ramp: THREE.Color[] = [];
  protected readonly reduced: boolean;
  protected canvas: HTMLCanvasElement;
  protected labelsEl: HTMLElement;
  protected callbacks: SceneCallbacks<H>;
  private labels: PinnedLabel[] = [];
  private tween: {
    p0: THREE.Vector3;
    p1: THREE.Vector3;
    t0: THREE.Vector3;
    t1: THREE.Vector3;
    u0: THREE.Vector3;
    /** Turns u0 to the view's up. */
    turn: THREE.Quaternion;
    start: number;
    dur: number;
  } | null = null;
  /** Frames drawn on demand, and none while the stage cannot be seen (frameLoop.ts). */
  private frames: FrameLoop;
  private seen: IntersectionObserver;
  private ray = new THREE.Raycaster();
  private down: { x: number; y: number } | null = null;
  private dragging = false;
  private resizeObserver: ResizeObserver;
  private disposers: (() => void)[] = [];
  /** The floor grid under the content, as last laid, and whether it is drawn (setGrid). */
  private floor: THREE.GridHelper | null = null;
  private floorShown = true;
  /** Set while an export drives the camera (orbit()): the loop then draws nothing of its own. */
  private held = false;
  /** When the loop last drew: a stage that only turns is drawn at TURN_FRAME_MS, not every frame. */
  private drawnAt = 0;
  /** Set while the loop updates the orbit: the change it reports is the loop's own. */
  private updating = false;
  /** Whether the stage is asked to turn (setRotate); it turns while the page is in use. */
  private turnAsked = false;
  /** Stops the turn while nobody uses the page (turnPause.ts). */
  private turnPause: TurnPause;
  /** Whether the stage is turning now: asked to, in 3D, motion not reduced, the page in use. */
  private turning = false;
  /** What the turn goes about: the best view's axis, so the spread plane keeps facing the viewer. */
  private turnAxis = UP_AXIS.clone();
  /** When the turn last moved the camera; 0 while it is not turning. */
  private turnedAt = 0;
  /** The stage's width over its height. */
  private aspectRatio = 1;
  /** Half the 2D camera's height in world units, before its zoom: set to fit by the scene. */
  protected orthoHalf = 10;

  constructor(
    canvas: HTMLCanvasElement,
    labelsEl: HTMLElement,
    callbacks: SceneCallbacks<H>,
    theme: SceneTheme,
  ) {
    this.canvas = canvas;
    this.labelsEl = labelsEl;
    this.callbacks = callbacks;
    this.theme = theme;
    this.reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    this.renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.renderer.shadowMap.enabled = true;
    this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    // The light and the content hold still in the world while the camera moves (a drag, the
    // stage turning), so the shadow map is drawn again only when what is drawn changes (loop()):
    // a full extra pass per frame otherwise, the larger part of a turning stage's cost.
    this.renderer.shadowMap.autoUpdate = false;
    // The theme's colours are sRGB (CSS tokens), which three.js takes into linear space; the output
    // is converted back, or every colour lands far darker than its token and the stage reads black.
    this.renderer.outputColorSpace = THREE.SRGBColorSpace;
    this.controls = new OrbitControls(this.camera, canvas);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.08;
    this.controls.screenSpacePanning = true;
    this.controls.minDistance = 2;
    this.controls.maxDistance = 120;
    this.frames = new FrameLoop(this.loop);
    this.turnPause = new TurnPause(() => this.applyTurn());
    // The view moved by hand (a drag, the wheel): draw it. The loop's own update() (the turn, the
    // orbit settling) is a move it already draws, not a change to draw again.
    this.controls.addEventListener("change", () => {
      if (!this.updating) this.frames.invalidate();
    });
    this.scene.add(this.hemi);
    this.sun.position.set(-9, 15, 11);
    this.sun.castShadow = true;
    this.sun.shadow.mapSize.set(2048, 2048);
    this.sun.shadow.bias = -0.0008;
    this.scene.add(this.sun);
    this.applyBaseTheme();
    this.bindPointer();
    this.resizeObserver = new ResizeObserver(() => this.resize());
    this.resizeObserver.observe(canvas.parentElement as HTMLElement);
    this.resize();
    // Nothing is drawn while the stage is off screen or the page is hidden.
    let inView = true;
    const shown = () => this.frames.setShown(inView && !document.hidden);
    this.seen = new IntersectionObserver((entries) => {
      inView = entries.some((e) => e.isIntersecting);
      shown();
    });
    this.seen.observe(canvas);
    document.addEventListener("visibilitychange", shown);
    this.disposers.push(() => document.removeEventListener("visibilitychange", shown));
  }

  // ---------- for subclasses ----------

  /** The thing under the pointer, from a ray already cast through it. */
  protected abstract pickHit(ray: THREE.Raycaster): H | null;
  /** Rebuild whatever is drawn in the theme's colours. */
  protected abstract applyTheme(): void;
  /** Free everything the subclass built. */
  protected abstract disposeContent(): void;
  /** Called once per frame drawn, before rendering. */
  protected onFrame(): void {}
  /** Whether the scene's own animation (onFrame) still moves: frames are drawn while it does. */
  protected animating(): boolean {
    return false;
  }

  /** What is drawn changed: draw it on the next frame. Every change to the scene calls it. */
  invalidate() {
    this.frames.invalidate();
  }
  /** Called once per frame after the labels are placed, before rendering (REQ-2128: a scene may
   * hide labels that would collide). */
  protected onLabelsPlaced(): void {}

  protected rampColor(t: number): THREE.Color {
    const x = Math.min(1, Math.max(0, t)) * 4;
    const i = Math.min(3, Math.floor(x));
    return this.ramp[i].clone().lerp(this.ramp[i + 1], x - i);
  }

  protected pin(
    text: string,
    cls: string,
    at: () => THREE.Vector3,
    rotate = 0,
    alignEnd = false,
  ): HTMLDivElement {
    const el = document.createElement("div");
    el.className = cls;
    el.textContent = text;
    this.labelsEl.appendChild(el);
    this.labels.push({ el, at, rotate, alignEnd, placed: null });
    return el;
  }

  protected clearPins() {
    this.labelsEl.textContent = "";
    this.labels = [];
  }

  protected fitShadow(extent: number) {
    const sh = this.sun.shadow.camera;
    sh.left = -extent;
    sh.right = extent;
    sh.top = extent;
    sh.bottom = -extent;
    sh.near = 1;
    sh.far = extent * 4 + 20;
    sh.updateProjectionMatrix();
  }

  /** Moves the camera to look at ``tgt`` from ``pos``, ``up`` up on screen: at once when
   * ``instant`` or motion is reduced, else eased there. */
  protected flyTo(pos: THREE.Vector3, tgt: THREE.Vector3, instant: boolean, up = UP_AXIS) {
    if (instant || this.reduced) {
      this.camera.position.copy(pos);
      this.controls.target.copy(tgt);
      this.setUp(up);
      this.camera.lookAt(tgt);
      this.tween = null;
      this.invalidate();
      return;
    }
    const u0 = this.camera.up.clone();
    this.tween = {
      p0: this.camera.position.clone(),
      p1: pos,
      t0: this.controls.target.clone(),
      t1: tgt,
      u0,
      turn: new THREE.Quaternion().setFromUnitVectors(u0, up.clone().normalize()),
      start: performance.now(),
      dur: 750,
    };
    this.invalidate();
  }

  protected get aspect(): number {
    return this.aspectRatio;
  }

  /** The content a best view is computed for (REQ-2174): its points, their radii and label widths
   * in world units, and its edges; null keeps the scene's own view (a surface, a cloud). */
  protected viewContent(): { nodes: ViewNode[]; edges: [number, number][] } | null {
    return null;
  }

  /** Computes the content's best view (REQ-2174), the axis a turn goes about, and returns it; in
   * 2D the view is face-on. A scene calls it once its content is laid out. */
  protected settleView(): View | null {
    const content = this.viewContent();
    this.best =
      this.flat || this.planar ? FACE_ON : content ? bestView(content.nodes, content.edges) : null;
    this.turnAxis.copy(this.best ? this.best.axis : UP_AXIS);
    this.alignFloor();
    return this.best;
  }

  /** The floor grid square to the best view's up, a margin under the content, so it reads as the
   * ground whichever way the view stands; in 2D, behind the content, facing the camera. A scene
   * whose own view is kept (no best view) keeps its floor as laid. */
  protected alignFloor() {
    const content = this.viewContent();
    if (!this.floor || !this.best || !content || content.nodes.length === 0) return;
    const normal = this.flat || this.planar ? FACE_ON.dir : this.best.up;
    const lowest = Math.min(...content.nodes.map((n) => n.at.dot(normal) - n.radius));
    this.floor.quaternion.setFromUnitVectors(UP_AXIS, normal);
    this.floor.position.copy(normal).multiplyScalar(lowest - 1.5);
  }

  /** The scene's resting view: where it opens, where Reset puts it, and where it eases back to
   * when a turn pauses on idle. */
  abstract resetView(instant?: boolean): void;

  /** Lays a size×size floor grid at height ``y`` in ``world``, drawn or not as setGrid last said. */
  protected layFloor(world: THREE.Object3D, size: number, y: number, mat: THREE.LineBasicMaterial) {
    const grid = new THREE.GridHelper(size, size, this.theme.grid, this.theme.grid);
    (grid.material as THREE.Material).dispose();
    grid.material = mat;
    grid.position.y = y;
    grid.visible = this.floorShown;
    world.add(grid);
    this.floor = grid;
  }

  protected disposeTree(root: THREE.Object3D) {
    root.traverse((n) => {
      const mesh = n as THREE.Mesh;
      if (mesh.geometry) mesh.geometry.dispose();
    });
  }

  // ---------- public ----------

  setTheme(theme: SceneTheme) {
    this.theme = theme;
    this.applyBaseTheme();
    this.applyTheme();
    this.invalidate();
  }

  /** Draws the floor grid, or leaves it out. */
  setGrid(shown: boolean) {
    this.floorShown = shown;
    if (this.floor) this.floor.visible = shown;
    this.invalidate();
  }

  setRotate(on: boolean) {
    this.turnAsked = on;
    this.applyTurn();
  }

  /** 2D (REQ-2174): the face-on orthographic camera, panned and zoomed only, never turning; or
   * back to the 3D camera. The scene lays its content out flat and resets its view after. */
  setFlat(on: boolean) {
    if (on === this.flat) return;
    this.flat = on;
    const from = this.camera;
    this.camera = on ? this.ortho : this.persp;
    this.camera.position.copy(from.position);
    this.camera.up.copy(from.up);
    this.controls.object = this.camera;
    this.controls.enableRotate = !on;
    // In 2D a drag pans, as a map does; in 3D it orbits.
    this.controls.mouseButtons.LEFT = on ? THREE.MOUSE.PAN : THREE.MOUSE.ROTATE;
    this.controls.touches.ONE = on ? THREE.TOUCH.PAN : THREE.TOUCH.ROTATE;
    this.resize();
    this.applyTurn();
  }

  /** The stage's background, in the theme: what an export fills behind the frame. */
  get background(): string {
    return this.theme.canvas;
  }

  /** The labels as the last frame placed them, the ones in view. */
  placedLabels(): PlacedLabel[] {
    return this.labels.flatMap((l) =>
      l.placed
        ? [{ el: l.el, x: l.placed.x, y: l.placed.y, rotate: l.rotate, alignEnd: l.alignEnd }]
        : [],
    );
  }

  /**
   * Renders one frame of the current view at ``scale`` times the stage's CSS size and hands the
   * canvas to ``draw`` in the same task, while its drawing buffer still holds the frame -- so the
   * renderer never needs preserveDrawingBuffer. The usual size is put back after.
   */
  snapshot(scale: number, draw: (src: HTMLCanvasElement) => void) {
    const usual = this.renderer.getPixelRatio();
    this.renderer.setPixelRatio(scale);
    this.resize();
    this.placeLabels();
    this.renderer.render(this.scene, this.camera);
    try {
      draw(this.renderer.domElement);
    } finally {
      this.renderer.setPixelRatio(usual);
      this.resize();
      this.renderer.render(this.scene, this.camera);
    }
  }

  /**
   * A full turn about the view's target, for a recording: the loop stops moving the camera, each
   * frame is drawn when asked for, and end() puts the camera back where it was. The orbit starts
   * from the current view and turns at an even step, so the same view records the same video.
   */
  orbit(scale: number): Orbit {
    const position = this.camera.position.clone();
    const target = this.controls.target.clone();
    const up = this.camera.up.clone();
    const usual = this.renderer.getPixelRatio();
    const offset = position.clone().sub(target);
    const axis = this.turnAxis.clone();
    this.held = true;
    this.tween = null;
    this.renderer.setPixelRatio(scale);
    this.resize();
    return {
      frame: (fraction, draw) => {
        const angle = fraction * Math.PI * 2;
        this.camera.position.copy(target).add(offset.clone().applyAxisAngle(axis, angle));
        this.camera.up.copy(up).applyAxisAngle(axis, angle);
        this.camera.lookAt(target);
        this.onFrame();
        this.placeLabels();
        this.renderer.render(this.scene, this.camera);
        draw(this.renderer.domElement);
      },
      end: () => {
        this.camera.position.copy(position);
        this.controls.target.copy(target);
        this.camera.up.copy(up);
        this.camera.lookAt(target);
        this.renderer.setPixelRatio(usual);
        this.resize();
        this.held = false;
        this.applyTurn();
      },
    };
  }

  /** The scene projected flat for an SVG, or null where it cannot be drawn faithfully that way
   * (a surface, a cloud): those scenes offer raster images only. */
  vector(): VectorScene | null {
    return null;
  }

  /** Whether vector() draws this scene: what decides if an SVG is offered. */
  get canVector(): boolean {
    return false;
  }

  /** A world point on the stage, in CSS pixels, and its depth (larger is further). */
  protected toStage(p: THREE.Vector3): { x: number; y: number; z: number } {
    const parent = this.canvas.parentElement as HTMLElement;
    const v = p.clone().project(this.camera);
    return {
      x: ((v.x + 1) / 2) * parent.clientWidth,
      y: ((1 - v.y) / 2) * parent.clientHeight,
      z: v.z,
    };
  }

  /** ``meshes`` as their silhouettes, back to front, over the floor grid: a scene's vector() for
   * content made of solid meshes (nodes, edges, arrowheads). Labels are the export's own layer. */
  protected vectorOf(meshes: THREE.Mesh[]): VectorScene {
    // The camera as it is now, though no frame has been rendered since it moved.
    this.camera.updateMatrixWorld();
    const shapes = meshes
      .flatMap((m) => {
        const s = this.silhouette(m);
        return s ? [s] : [];
      })
      .sort((a, b) => b.depth - a.depth)
      .map(({ points, color, opacity }) => ({ points, color, opacity }));
    return { ...this.stageSize, floor: this.floorLines(), shapes };
  }

  /** A mesh as the camera sees it: the outline of its projected vertices, in its material's
   * colour, with its depth for ordering; null while it is behind the camera. */
  protected silhouette(
    mesh: THREE.Mesh,
  ): { points: [number, number][]; color: string; opacity: number; depth: number } | null {
    mesh.updateMatrixWorld();
    const pos = mesh.geometry.getAttribute("position");
    const pts: [number, number][] = [];
    let depth = 0;
    for (let i = 0; i < pos.count; i++) {
      const p = this.toStage(
        new THREE.Vector3().fromBufferAttribute(pos, i).applyMatrix4(mesh.matrixWorld),
      );
      if (p.z > 1) return null;
      pts.push([p.x, p.y]);
      depth += p.z;
    }
    const mat = mesh.material as THREE.MeshStandardMaterial;
    return {
      points: hull(pts),
      color: `#${mat.color.getHexString()}`,
      opacity: mat.transparent ? mat.opacity : 1,
      depth: depth / pos.count,
    };
  }

  /** The stage's size in CSS pixels. */
  protected get stageSize(): { width: number; height: number } {
    const parent = this.canvas.parentElement as HTMLElement;
    return { width: parent.clientWidth, height: parent.clientHeight };
  }

  /** The floor grid's lines on the stage, while it is drawn. */
  protected floorLines(): VectorScene["floor"] {
    const grid = this.floor;
    if (!grid || !grid.visible) return [];
    grid.updateMatrixWorld();
    const pos = grid.geometry.getAttribute("position");
    const mat = grid.material as THREE.LineBasicMaterial;
    const out: VectorScene["floor"] = [];
    for (let i = 0; i + 1 < pos.count; i += 2) {
      const a = this.toStage(
        new THREE.Vector3().fromBufferAttribute(pos, i).applyMatrix4(grid.matrixWorld),
      );
      const b = this.toStage(
        new THREE.Vector3().fromBufferAttribute(pos, i + 1).applyMatrix4(grid.matrixWorld),
      );
      if (a.z > 1 || b.z > 1) continue;
      out.push({
        x1: a.x,
        y1: a.y,
        x2: b.x,
        y2: b.y,
        color: `#${mat.color.getHexString()}`,
        opacity: mat.opacity,
      });
    }
    return out;
  }

  dispose() {
    this.frames.dispose();
    this.turnPause.dispose();
    this.seen.disconnect();
    this.resizeObserver.disconnect();
    this.disposers.forEach((d) => d());
    this.disposeContent();
    this.clearPins();
    this.controls.dispose();
    this.renderer.dispose();
  }

  // ---------- internals ----------

  /** Turns while asked to, in 3D, motion is not reduced and the page is in use; an export holds
   * it. A turn paused on idle eases back to the resting view (REQ-2174). */
  private applyTurn() {
    if (this.held) return;
    const was = this.turning;
    this.turning = this.turnAsked && !this.flat && !this.reduced && !this.turnPause.paused;
    if (was && !this.turning && this.turnPause.paused) this.resetView(false);
    this.invalidate();
  }

  /** Up on screen is ``up``, and a drag orbits about it: OrbitControls keeps the up it was made
   * with in a quaternion of its own (three 0.170's _quat and _quatInverse), set here to match. */
  private setUp(up: THREE.Vector3) {
    this.camera.up.copy(up).normalize();
    const orbit = this.controls as unknown as {
      _quat: THREE.Quaternion;
      _quatInverse: THREE.Quaternion;
    };
    orbit._quat.setFromUnitVectors(this.camera.up, UP_AXIS);
    orbit._quatInverse.copy(orbit._quat).invert();
  }

  /** The camera turned ``angle`` about turnAxis through the target, up on screen with it: the
   * spread plane moves within itself and keeps its face to the viewer. */
  private turnBy(angle: number) {
    const target = this.controls.target;
    const offset = this.camera.position.clone().sub(target).applyAxisAngle(this.turnAxis, angle);
    this.camera.position.copy(target).add(offset);
    this.camera.up.applyAxisAngle(this.turnAxis, angle);
    this.camera.lookAt(target);
  }

  private applyBaseTheme() {
    const th = this.theme;
    this.ramp = th.ramp.map((c) => new THREE.Color(c));
    this.renderer.setClearColor(th.canvas);
    this.hemi.color.set(th.dark ? 0xcfe0ff : 0xffffff);
    this.hemi.groundColor.set(th.ground);
    this.hemi.intensity = th.dark ? 0.85 : 0.8;
    this.sun.intensity = th.dark ? 1.1 : 0.85;
  }

  private pick(cx: number, cy: number): H | null {
    const r = this.canvas.getBoundingClientRect();
    const ndc = new THREE.Vector2(
      ((cx - r.left) / r.width) * 2 - 1,
      -((cy - r.top) / r.height) * 2 + 1,
    );
    this.ray.setFromCamera(ndc, this.camera);
    return this.pickHit(this.ray);
  }

  private bindPointer() {
    const c = this.canvas;
    const local = (e: PointerEvent) => {
      const r = c.getBoundingClientRect();
      return [e.clientX - r.left, e.clientY - r.top] as const;
    };
    const onDown = (e: PointerEvent) => {
      this.down = { x: e.clientX, y: e.clientY };
      this.dragging = true;
      this.tween = null;
      // A drag orbits about up as the turn left it.
      this.setUp(this.camera.up);
      this.callbacks.onUserMove();
    };
    const onUp = (e: PointerEvent) => {
      this.dragging = false;
      if (
        this.down &&
        Math.hypot(e.clientX - this.down.x, e.clientY - this.down.y) < 6 &&
        e.target === c
      ) {
        const h = this.pick(e.clientX, e.clientY);
        this.callbacks.onHover(h, ...local(e));
        if (h) this.callbacks.onSelect(h);
      }
      this.down = null;
    };
    const onMove = (e: PointerEvent) => {
      if (this.dragging || e.pointerType === "touch") return;
      this.callbacks.onHover(this.pick(e.clientX, e.clientY), ...local(e));
    };
    const onLeave = () => {
      if (!this.dragging) this.callbacks.onHover(null, 0, 0);
    };
    const onWheel = () => this.callbacks.onUserMove();
    c.addEventListener("pointerdown", onDown);
    window.addEventListener("pointerup", onUp);
    c.addEventListener("pointermove", onMove);
    c.addEventListener("pointerleave", onLeave);
    c.addEventListener("wheel", onWheel, { passive: true });
    this.disposers.push(() => {
      c.removeEventListener("pointerdown", onDown);
      window.removeEventListener("pointerup", onUp);
      c.removeEventListener("pointermove", onMove);
      c.removeEventListener("pointerleave", onLeave);
      c.removeEventListener("wheel", onWheel);
    });
  }

  private resize() {
    const parent = this.canvas.parentElement as HTMLElement;
    const w = parent.clientWidth;
    const h = parent.clientHeight;
    if (!w || !h) return;
    this.renderer.setSize(w, h, false);
    this.aspectRatio = w / h;
    this.persp.aspect = w / h;
    this.persp.updateProjectionMatrix();
    this.fitOrtho();
    this.frames.invalidate();
  }

  /** The 2D camera's frustum: orthoHalf high, as wide as the stage's aspect makes it. */
  protected fitOrtho() {
    this.ortho.left = -this.orthoHalf * this.aspectRatio;
    this.ortho.right = this.orthoHalf * this.aspectRatio;
    this.ortho.top = this.orthoHalf;
    this.ortho.bottom = -this.orthoHalf;
    this.ortho.updateProjectionMatrix();
  }

  /** One frame: advance what moves, and draw when it moved or ``changed``. Answers whether
   * anything still moves (a tween, the orbit settling or turning, the scene's own animation). */
  private loop = (now: number, changed: boolean): boolean => {
    if (this.held) return false; // an export drives the frames (orbit())
    let moving = false;
    if (this.tween) {
      const tw = this.tween;
      const k = Math.min(1, (now - tw.start) / tw.dur);
      const e = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
      this.camera.position.lerpVectors(tw.p0, tw.p1, e);
      this.controls.target.lerpVectors(tw.t0, tw.t1, e);
      const up = tw.u0
        .clone()
        .applyQuaternion(
          new THREE.Quaternion().slerpQuaternions(new THREE.Quaternion(), tw.turn, e),
        );
      if (k >= 1) {
        this.tween = null;
        this.setUp(up);
      } else this.camera.up.copy(up);
      this.camera.lookAt(this.controls.target);
      moving = true;
    }
    if (this.turning && !this.dragging && !this.tween) {
      // The first frame of a turn only sets the clock; a frame long delayed moves by at most 0.1 s.
      if (this.turnedAt) this.turnBy(TURN_RAD_PER_MS * Math.min(100, now - this.turnedAt));
      this.turnedAt = now;
      moving = true;
    } else this.turnedAt = 0;
    this.updating = true;
    if (this.controls.update()) moving = true;
    this.updating = false;
    const animating = this.animating();
    if (animating) moving = true;
    if (changed || animating) this.renderer.shadowMap.needsUpdate = true;
    // Only turning: the orbit still advances every frame, but it is drawn at a slower rate --
    // a turn reads as smooth at it, at a third of the cost.
    const turningOnly = moving && !changed && !animating && !this.tween && !this.dragging;
    if (turningOnly && now - this.drawnAt < TURN_FRAME_MS) return moving;
    if (moving || changed) {
      this.drawnAt = now;
      this.onFrame();
      this.placeLabels();
      this.renderer.render(this.scene, this.camera);
    }
    return moving;
  };

  /** Puts every pinned label where the camera now sees its point. */
  private placeLabels() {
    const parent = this.canvas.parentElement as HTMLElement;
    const w = parent.clientWidth;
    const h = parent.clientHeight;
    for (const l of this.labels) {
      const p = l.at().project(this.camera);
      if (p.z > 1) {
        l.el.style.visibility = "hidden";
        l.placed = null;
        continue;
      }
      l.el.style.visibility = "visible";
      const x = ((p.x + 1) / 2) * w;
      const y = ((1 - p.y) / 2) * h;
      l.placed = { x, y };
      l.el.style.transform =
        `translate(${x.toFixed(1)}px,${y.toFixed(1)}px)` +
        (l.rotate ? ` rotate(${l.rotate}deg)` : "") +
        (l.alignEnd ? " translate(-100%,-50%)" : "");
    }
    this.onLabelsPlaced();
  }
}

/** The convex outline of a set of points (Andrew's monotone chain), counter-clockwise. */
function hull(points: [number, number][]): [number, number][] {
  const p = [...points].sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  if (p.length < 3) return p;
  const cross = (o: [number, number], a: [number, number], b: [number, number]) =>
    (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  const lower: [number, number][] = [];
  for (const q of p) {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], q) <= 0)
      lower.pop();
    lower.push(q);
  }
  const upper: [number, number][] = [];
  for (const q of [...p].reverse()) {
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], q) <= 0)
      upper.pop();
    upper.push(q);
  }
  return lower.slice(0, -1).concat(upper.slice(0, -1));
}
