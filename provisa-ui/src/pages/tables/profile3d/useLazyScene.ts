// Copyright (c) 2026 Kenneth Stott
// Canary: a50973c3-e61c-4220-8a5a-660a46239c92
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: a 3D profile scene, created once from a dynamically imported module and disposed on
// unmount, its colours following the Mantine colour scheme.

import { useEffect, useRef, useState, type RefObject } from "react";
import { useComputedColorScheme } from "@mantine/core";
import type { SceneTheme } from "./baseScene";
import { readSceneTheme } from "./sceneTheme";

export interface StageRefs {
  wrapRef: RefObject<HTMLDivElement | null>;
  canvasRef: RefObject<HTMLCanvasElement | null>;
  labelsRef: RefObject<HTMLDivElement | null>;
}

interface Themed {
  setTheme: (t: SceneTheme) => void;
  dispose: () => void;
}

/** ``create`` runs once, after the stage mounts; it is handed the canvas, the label layer and the
 * current theme. */
export function useLazyScene<S extends Themed>(
  create: (canvas: HTMLCanvasElement, labels: HTMLDivElement, theme: SceneTheme) => Promise<S>,
): StageRefs & { scene: S | null; error: string | null } {
  const scheme = useComputedColorScheme("dark");
  const wrap = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const labels = useRef<HTMLDivElement>(null);
  const [scene, setScene] = useState<S | null>(null);
  const [error, setError] = useState<string | null>(null);
  const first = useRef({ create, scheme });

  useEffect(() => {
    let cancelled = false;
    let made: S | null = null;
    const { create: make, scheme: initial } = first.current;
    make(
      canvas.current as HTMLCanvasElement,
      labels.current as HTMLDivElement,
      readSceneTheme(wrap.current as HTMLDivElement, initial === "dark"),
    )
      .then((s) => {
        if (cancelled) {
          s.dispose();
          return;
        }
        made = s;
        setScene(s);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
      made?.dispose();
    };
  }, []);

  const themed = useRef(scheme);
  useEffect(() => {
    if (!scene || !wrap.current || themed.current === scheme) return;
    themed.current = scheme;
    scene.setTheme(readSceneTheme(wrap.current, scheme === "dark"));
  }, [scene, scheme]);

  return { wrapRef: wrap, canvasRef: canvas, labelsRef: labels, scene, error };
}
