// Copyright (c) 2026 Kenneth Stott
// Canary: e570de1b-4398-4d17-8da7-1439a1dc60d3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A page header's command buttons (div.page-header: title, filter, actions). When the header is too
// narrow for every button, the buttons move from the end, in order, into a (...) menu, so the row
// never crowds or clips. The filter gives first: the header's grid lets its track shrink to its
// minimum (App.css, --page-filter-min) before the actions track is short of room. The page's one
// (?) help is its title's (PageTitle), so nothing that must stay visible stands in this row.
// Each button is a PageCommand: its description and documentation open on hover in the row, and
// stand beneath it in the menu (maintainer 2026-11-28).

import {
  Children,
  cloneElement,
  isValidElement,
  useLayoutEffect,
  useRef,
  useState,
  type MouseEvent,
  type ReactElement,
  type ReactNode,
} from "react";
import {
  ActionIcon,
  Combobox,
  MantineThemeProvider,
  Menu,
  Popover,
  Stack,
  createTheme,
} from "@mantine/core";
import { Ellipsis } from "lucide-react";
import { useTranslation } from "react-i18next";
import { CapabilityGate } from "./CapabilityGate";
import { PageCommand, type PageCommandProps } from "./PageCommand";

/** Inside the (...) menu a button's own menu or picker opens in place, not in a portal: the outer
 * menu then reads a click in it as a click inside, and stays open for it. */
const IN_MENU = createTheme({
  components: {
    Menu: Menu.extend({ defaultProps: { withinPortal: false } }),
    Popover: Popover.extend({ defaultProps: { withinPortal: false } }),
    Combobox: Combobox.extend({ defaultProps: { withinPortal: false } }),
  },
});

/** Whether the header holding `row` is wider than it is drawn: its content overflows it. */
function overflows(row: HTMLElement): boolean {
  const header = row.parentElement;
  if (header === null) return false;
  return header.scrollWidth > header.clientWidth;
}

/** A command the row holds: a PageCommand, or a CapabilityGate around one (a command only some
 * roles have; the gate draws nothing, or the command inert and badged, for the others). */
type Command =
  ReactElement<PageCommandProps> | ReactElement<{ children: ReactElement<PageCommandProps> }>;

const isCommand = (node: unknown): node is ReactElement<PageCommandProps> =>
  isValidElement(node) && node.type === PageCommand;

/** The row's commands. Anything else is a page built wrong: toolbarGuard fails it statically, and
 * this throws for one that reaches the browser, rather than draw a command with no help. */
function commandsOf(children: ReactNode): Command[] {
  return Children.toArray(children)
    .filter(isValidElement)
    .map((item) => {
      if (isCommand(item)) return item;
      const inner = (item.props as { children?: unknown }).children;
      if (item.type === CapabilityGate && isCommand(inner))
        return item as ReactElement<{ children: ReactElement<PageCommandProps> }>;
      throw new Error("PageActions holds only PageCommand children (description and docs)");
    });
}

/** The command as the (...) menu draws it: its description and link stand beneath it. */
function inMenu(item: Command): ReactElement {
  if (isCommand(item)) return cloneElement(item, { inMenu: true });
  const gated = item as ReactElement<{ children: ReactElement<PageCommandProps> }>;
  return cloneElement(gated, {}, cloneElement(gated.props.children, { inMenu: true }));
}

export function PageActions({ children }: { children: ReactNode }) {
  const { t } = useTranslation();
  const items = commandsOf(children);
  const total = items.length;
  const rowRef = useRef<HTMLDivElement>(null);
  // How many buttons stand in the row, for the number of buttons the page now has: a page that
  // gains or loses a button lays the row out again from all of them.
  const [fit, setFit] = useState<{ total: number; shown: number }>({ total, shown: total });
  const shown = fit.total === total ? fit.shown : total;
  // Set when the header grew: the row then tries one more button at a time until one overflows.
  const growing = useRef(false);
  const lastWidth = useRef(0);
  const [opened, setOpened] = useState(false);

  // Measured before the first paint and after every render (a button appears, a label changes),
  // then again whenever the header changes size. Each pass moves one button at most, so the row
  // settles in a few passes before the paint.
  useLayoutEffect(() => {
    const row = rowRef.current;
    const header = row?.parentElement;
    if (!row || !header) return;
    const decide = () => {
      const width = header.clientWidth;
      if (width > lastWidth.current) growing.current = true;
      lastWidth.current = width;
      if (overflows(row)) {
        growing.current = false;
        if (shown > 0) setFit({ total, shown: shown - 1 });
      } else if (growing.current) {
        if (shown < total) setFit({ total, shown: shown + 1 });
        else growing.current = false;
      } else if (fit.total !== total) {
        setFit({ total, shown });
      }
    };
    decide();
    const observer = new ResizeObserver(decide);
    observer.observe(header);
    return () => observer.disconnect();
  });

  // A click on a button in the menu runs it and closes the menu; one that opens a menu of its own
  // keeps the outer one open for it.
  const closeOnCommand = (e: MouseEvent<HTMLDivElement>) => {
    const hit = (e.target as HTMLElement).closest("button, [role='menuitem'], a");
    if (hit !== null && !hit.hasAttribute("aria-haspopup")) setOpened(false);
  };

  const hidden = items.slice(shown).map(inMenu);
  return (
    <div className="page-actions" ref={rowRef} data-testid="page-actions">
      {items.slice(0, shown)}
      {hidden.length > 0 && (
        <Menu
          opened={opened}
          onChange={setOpened}
          position="bottom-end"
          withinPortal
          keepMounted
          closeOnItemClick={false}
          transitionProps={{ duration: 0 }}
        >
          <Menu.Target>
            <ActionIcon
              variant="subtle"
              size="lg"
              aria-label={t("pageActions.more")}
              data-testid="page-actions-more"
            >
              <Ellipsis size={18} aria-hidden />
            </ActionIcon>
          </Menu.Target>
          <Menu.Dropdown data-testid="page-actions-menu" onClick={closeOnCommand}>
            <MantineThemeProvider theme={IN_MENU}>
              <Stack gap="sm" p={4} align="flex-start">
                {hidden}
              </Stack>
            </MantineThemeProvider>
          </Menu.Dropdown>
        </Menu>
      )}
    </div>
  );
}
