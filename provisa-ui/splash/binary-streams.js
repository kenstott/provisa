// Copyright (c) 2026 Kenneth Stott
// Canary: 904c1f55-ffe7-4a99-acad-da86684ea402
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/*
 * Binary streams converging on the Provisa mark: byte groups of 0s and 1s drift along a curl
 * flow field into the logo, bits flipping as they go, fading in at the edge and out near the
 * glow and over the text. One canvas; the glyphs come from one offscreen sprite (0 and 1 in a
 * few tones at a few alphas) copied with drawImage, so no text is drawn per frame. Under
 * reduced motion it draws one static frame, redrawn only on resize.
 *
 * The one source for both pages that show it: the app's splash (provisa-ui/index.html, inlined
 * at build by vite.config.ts) and the SaaS front door's waking page
 * (terraform/gcp-saas/front-door/proxy.py, which reads it from beside itself, where terraform
 * writes it). Plain ES5 with no imports: both pages run it before anything else loads.
 *
 * provisaBinaryStreams(canvas, options) -> { stop }
 *   options.center()  the point the streams converge on, in viewport coordinates
 *   options.avoid()   an element the glyphs fade out over (its text stays readable), or null
 *   options.reduce    true for the static frame
 */
(function (root) {
  "use strict";
  var CW = 8;
  var CH = 13;
  var TONES = 4;
  var ALPHAS = [0.1, 0.18, 0.27, 0.36];

  function provisaBinaryStreams(canvas, options) {
    var ctx = canvas.getContext("2d");
    var dpr = Math.min(root.devicePixelRatio || 1, 2);
    var W = 0;
    var H = 0;
    var cx = 0;
    var cy = 0;
    var spawnR = 0;
    var coreR = 0;
    var box = null;
    var raf = 0;

    var sprite = document.createElement("canvas");
    sprite.width = CW * dpr * ALPHAS.length * 2;
    sprite.height = CH * dpr * TONES;
    var sc = sprite.getContext("2d");
    sc.scale(dpr, dpr);
    sc.font = "11px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace";
    sc.textAlign = "center";
    sc.textBaseline = "middle";
    for (var ti = 0; ti < TONES; ti++) {
      var tt = ti / (TONES - 1);
      var rgb =
        Math.round(99 + (16 - 99) * tt) +
        "," +
        Math.round(102 + (185 - 102) * tt) +
        "," +
        Math.round(241 + (129 - 241) * tt);
      for (var ai = 0; ai < ALPHAS.length; ai++) {
        for (var b = 0; b < 2; b++) {
          sc.fillStyle = "rgba(" + rgb + "," + ALPHAS[ai] + ")";
          sc.fillText(b ? "1" : "0", (ai * 2 + b) * CW + CW / 2, ti * CH + CH / 2);
        }
      }
    }

    function anchor() {
      var c = options.center();
      cx = c.x;
      cy = c.y;
      var el = options.avoid();
      box = el ? el.getBoundingClientRect() : null;
    }

    function resize() {
      W = canvas.clientWidth;
      H = canvas.clientHeight;
      canvas.width = W * dpr;
      canvas.height = H * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      anchor();
      spawnR = Math.max(W, H) * 0.72;
      coreR = Math.max(56, Math.min(W, H) * 0.13);
    }
    resize();

    var N = Math.max(50, Math.min(110, Math.round((W * H) / 12000)));
    var S = [];

    function spawn(s) {
      var a = Math.random() * Math.PI * 2;
      var r = spawnR * (0.7 + Math.random() * 0.5);
      s.x = cx + Math.cos(a) * r;
      s.y = cy + Math.sin(a) * r * 0.82;
      s.spd = 0.6 + Math.random() * 1.1;
      s.life = 0;
      // 2 to 3 groups of 4 to 8 bits, a blank slot (-1) between groups.
      s.bits = [];
      var groups = 2 + Math.floor(Math.random() * 2);
      for (var g = 0; g < groups; g++) {
        if (g) s.bits.push(-1);
        var len = 4 + Math.floor(Math.random() * 5);
        for (var k = 0; k < len; k++) s.bits.push(Math.random() < 0.5 ? 0 : 1);
      }
      // Trail points, CW apart along the path the head has travelled.
      s.tx = [s.x];
      s.ty = [s.y];
    }

    function step(s) {
      var dx = cx - s.x;
      var dy = cy - s.y;
      var d = Math.sqrt(dx * dx + dy * dy) || 1;
      var curl = Math.sin(s.x * 0.012 + s.y * 0.01) * 0.9;
      var ang = Math.atan2(dy, dx) + curl * (d / spawnR);
      var v = s.spd * (0.6 + (1 - d / spawnR) * 2.2);
      s.x += Math.cos(ang) * v;
      s.y += Math.sin(ang) * v;
      s.life++;
      var hx = s.x - s.tx[0];
      var hy = s.y - s.ty[0];
      if (hx * hx + hy * hy >= CW * CW) {
        s.tx.unshift(s.x);
        s.ty.unshift(s.y);
        if (s.tx.length > s.bits.length + 1) {
          s.tx.pop();
          s.ty.pop();
        }
      }
      if (Math.random() < 0.12) {
        var i = Math.floor(Math.random() * s.bits.length);
        if (s.bits[i] >= 0) s.bits[i] ^= 1;
      }
      var n = s.tx.length - 1;
      var ex = s.tx[n] - cx;
      var ey = s.ty[n] - cy;
      if ((n === s.bits.length && ex * ex + ey * ey < coreR * coreR) || s.life > 1400) spawn(s);
    }

    function draw(s) {
      var fin = Math.min(1, s.life / 90);
      var hx = s.x - s.tx[0];
      var hy = s.y - s.ty[0];
      var f = Math.sqrt(hx * hx + hy * hy) / CW;
      for (var k = 0; k < s.bits.length; k++) {
        var b = s.bits[k];
        if (b < 0) continue;
        var x;
        var y;
        if (k === 0) {
          x = s.x;
          y = s.y;
        } else {
          if (k >= s.tx.length) break;
          var u = 1 - f;
          x = s.tx[k - 1] + (s.tx[k] - s.tx[k - 1]) * u;
          y = s.ty[k - 1] + (s.ty[k] - s.ty[k - 1]) * u;
        }
        var dx = x - cx;
        var dy = y - cy;
        var d = Math.sqrt(dx * dx + dy * dy);
        var fout = Math.max(0, Math.min(1, (d - coreR * 1.1) / (coreR * 1.8)));
        if (box) {
          var ox = Math.max(box.left - x, x - box.right, 0);
          var oy = Math.max(box.top - y, y - box.bottom, 0);
          fout *= Math.min(1, Math.sqrt(ox * ox + oy * oy) / 40);
        }
        var a = fin * fout;
        if (a < 0.08) continue;
        var level = Math.min(ALPHAS.length - 1, Math.floor(a * ALPHAS.length));
        var t = Math.max(0, Math.min(1, 1 - d / (spawnR * 0.55)));
        var tone = Math.round(t * (TONES - 1));
        ctx.drawImage(
          sprite,
          (level * 2 + b) * CW * dpr,
          tone * CH * dpr,
          CW * dpr,
          CH * dpr,
          Math.round(x - CW / 2),
          Math.round(y - CH / 2),
          CW,
          CH,
        );
      }
    }

    function glow(pulse) {
      var g = ctx.createRadialGradient(cx, cy, 0, cx, cy, coreR * 1.3);
      g.addColorStop(0, "rgba(16,185,129," + 0.16 * pulse + ")");
      g.addColorStop(0.5, "rgba(52,120,180," + 0.06 * pulse + ")");
      g.addColorStop(1, "rgba(16,185,129,0)");
      ctx.fillStyle = g;
      ctx.beginPath();
      ctx.arc(cx, cy, coreR * 1.3, 0, Math.PI * 2);
      ctx.fill();
    }

    function render(pulse) {
      ctx.clearRect(0, 0, W, H);
      for (var i = 0; i < S.length; i++) draw(S[i]);
      glow(pulse);
    }

    // Each stream starts part-way along its path, so the first frame is already flowing.
    for (var i = 0; i < N; i++) {
      var s = {};
      spawn(s);
      for (var w = Math.floor(Math.random() * 700); w > 0; w--) step(s);
      S.push(s);
    }

    if (options.reduce) {
      render(1);
      root.addEventListener("resize", function () {
        resize();
        render(1);
      });
      return { stop: function () {} };
    }

    root.addEventListener("resize", resize);
    function frame() {
      // The page may reflow the mark (a status line appearing, the entrance animation), so
      // the convergence point follows it every frame.
      anchor();
      for (var i = 0; i < S.length; i++) step(S[i]);
      render(0.55 + 0.45 * Math.sin(Date.now() * 0.002));
      raf = root.requestAnimationFrame(frame);
    }
    raf = root.requestAnimationFrame(frame);
    return {
      stop: function () {
        if (raf) root.cancelAnimationFrame(raf);
      },
    };
  }

  root.provisaBinaryStreams = provisaBinaryStreams;
})(window);
