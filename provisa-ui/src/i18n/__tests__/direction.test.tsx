// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8b5a2f-6c41-4d97-a0e8-9f5d3b7c1a24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, expect, it, afterEach } from "vitest";
import { act, render, cleanup } from "@testing-library/react";
import { DirectionProvider, MantineProvider } from "@mantine/core";
import { I18nextProvider } from "react-i18next";
import i18n from "../index";
import { DirectionSync, dirForLanguage } from "../direction";

describe("dirForLanguage", () => {
  it("maps RTL base subtags including regional variants", () => {
    expect(dirForLanguage("he")).toBe("rtl");
    expect(dirForLanguage("he-IL")).toBe("rtl");
    expect(dirForLanguage("ar")).toBe("rtl");
    expect(dirForLanguage("en")).toBe("ltr");
    expect(dirForLanguage("zh-HK")).toBe("ltr");
  });
});

describe("DirectionSync", () => {
  afterEach(async () => {
    cleanup();
    await act(() => i18n.changeLanguage("en"));
    document.documentElement.setAttribute("dir", "ltr");
  });

  function mount() {
    return render(
      <DirectionProvider>
        <MantineProvider defaultColorScheme="dark">
          <I18nextProvider i18n={i18n}>
            <DirectionSync />
          </I18nextProvider>
        </MantineProvider>
      </DirectionProvider>,
    );
  }

  it("sets html dir/lang to rtl when switching to Hebrew and back", async () => {
    mount();
    await act(() => i18n.changeLanguage("he"));
    expect(document.documentElement.getAttribute("dir")).toBe("rtl");
    expect(document.documentElement.getAttribute("lang")).toBe("he");

    await act(() => i18n.changeLanguage("fr"));
    expect(document.documentElement.getAttribute("dir")).toBe("ltr");
    expect(document.documentElement.getAttribute("lang")).toBe("fr");
  });
});
