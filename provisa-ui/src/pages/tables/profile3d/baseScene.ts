// Copyright (c) 2026 Kenneth Stott
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
}

export abstract class BaseScene<H> {
  protected renderer: THREE.WebGLRenderer;
  protected scene = new THREE.Scene();
  protected camera = new THREE.PerspectiveCamera(38, 1, 0.1, 600);
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
    start: number;
    dur: number;
  } | null = null;
  private frame = 0;
  private ray = new THREE.Raycaster();
  private down: { x: number; y: number } | null = null;
  private dragging = false;
  private resizeObserver: ResizeObserver;
  private disposers: (() => void)[] = [];

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
    this.renderer.outputColorSpace = THREE.LinearSRGBColorSpace;
    this.controls = new OrbitControls(this.camera, canvas);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.08;
    this.controls.screenSpacePanning = true;
    this.controls.minDistance = 2;
    this.controls.maxDistance = 120;
    this.controls.autoRotateSpeed = 0.6;
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
    this.frame = requestAnimationFrame(this.loop);
  }

  // ---------- for subclasses ----------

  /** The thing under the pointer, from a ray already cast through it. */
  protected abstract pickHit(ray: THREE.Raycaster): H | null;
  /** Rebuild whatever is drawn in the theme's colours. */
  protected abstract applyTheme(): void;
  /** Free everything the subclass built. */
  protected abstract disposeContent(): void;
  /** Called once per frame before rendering. */
  protected onFrame(): void {}

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
    this.labels.push({ el, at, rotate, alignEnd });
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

  protected flyTo(pos: THREE.Vector3, tgt: THREE.Vector3, instant: boolean) {
    if (instant || this.reduced) {
      this.camera.position.copy(pos);
      this.controls.target.copy(tgt);
      this.tween = null;
      return;
    }
    this.tween = {
      p0: this.camera.position.clone(),
      p1: pos,
      t0: this.controls.target.clone(),
      t1: tgt,
      start: performance.now(),
      dur: 750,
    };
  }

  protected get aspect(): number {
    return this.camera.aspect;
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
  }

  setRotate(on: boolean) {
    this.controls.autoRotate = on && !this.reduced;
  }

  dispose() {
    cancelAnimationFrame(this.frame);
    this.resizeObserver.disconnect();
    this.disposers.forEach((d) => d());
    this.disposeContent();
    this.clearPins();
    this.controls.dispose();
    this.renderer.dispose();
  }

  // ---------- internals ----------

  private applyBaseTheme() {
    const th = this.theme;
    this.ramp = th.ramp.map((c) => new THREE.Color(c));
    this.renderer.setClearColor(th.canvas);
    this.hemi.color.set(th.dark ? 0xcfe0ff : 0xffffff);
    this.hemi.groundColor.set(th.ground);
    this.hemi.intensity = th.dark ? 0.55 : 0.75;
    this.sun.intensity = th.dark ? 0.95 : 0.8;
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
    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();
  }

  private loop = (now: number) => {
    this.frame = requestAnimationFrame(this.loop);
    if (this.tween) {
      const tw = this.tween;
      const k = Math.min(1, (now - tw.start) / tw.dur);
      const e = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
      this.camera.position.lerpVectors(tw.p0, tw.p1, e);
      this.controls.target.lerpVectors(tw.t0, tw.t1, e);
      if (k >= 1) this.tween = null;
    }
    this.controls.update();
    this.onFrame();
    const parent = this.canvas.parentElement as HTMLElement;
    const w = parent.clientWidth;
    const h = parent.clientHeight;
    for (const l of this.labels) {
      const p = l.at().project(this.camera);
      if (p.z > 1) {
        l.el.style.visibility = "hidden";
        continue;
      }
      l.el.style.visibility = "visible";
      const x = ((p.x + 1) / 2) * w;
      const y = ((1 - p.y) / 2) * h;
      l.el.style.transform =
        `translate(${x.toFixed(1)}px,${y.toFixed(1)}px)` +
        (l.rotate ? ` rotate(${l.rotate}deg)` : "") +
        (l.alignEnd ? " translate(-100%,-50%)" : "");
    }
    this.renderer.render(this.scene, this.camera);
  };
}
