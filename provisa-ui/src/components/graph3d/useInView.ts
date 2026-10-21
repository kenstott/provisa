// Copyright (c) 2026 Kenneth Stott
// Canary: 1b8f4d62-3e7a-4c95-a2d0-6f9e1c3b5a27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: whether an element has come into view — once it has, it stays so, so what it gates
// (three.js, loaded on first sight) is loaded once.

import { useEffect, useRef, useState } from "react";

export function useInView<E extends HTMLElement>() {
  const ref = useRef<E>(null);
  const [seen, setSeen] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el || seen) return;
    const io = new IntersectionObserver((entries) => {
      if (entries.some((e) => e.isIntersecting)) {
        setSeen(true);
        io.disconnect();
      }
    });
    io.observe(el);
    return () => io.disconnect();
  }, [seen]);
  return { ref, seen };
}
