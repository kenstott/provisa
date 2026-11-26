// Copyright (c) 2026 Kenneth Stott
// Canary: ac301480-5f36-4957-a1dd-f0ae10a2fc0e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import {
  Children,
  createContext,
  Fragment,
  isValidElement,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactElement,
  type ReactNode,
} from "react";
import { useTranslation } from "react-i18next";
import { Accordion, ActionIcon, Group } from "@mantine/core";
import { ChevronsDownUp, ChevronsUpDown } from "lucide-react";
import { Tip } from "../Tip";
import { readCollapsibleStates, writeCollapsibleState } from "./collapsibleState";
import "./collapsible.css";

/**
 * The one collapsible of the application: every collapsible panel is an item of a `Collapsibles`,
 * so all look alike open and closed (collapsible.css) and each opens as this viewer left it.
 */
export interface CollapsibleItemProps {
  /** The item's id within its `Collapsibles`: with the group's id, the panel's stable id. */
  value: string;
  /** The heading the item opens and closes by. */
  title: ReactNode;
  /** Open when this viewer has never opened or closed it. */
  defaultOpen?: boolean;
  /** Held open whatever was remembered: the panel holds something that needs attention. */
  forceOpen?: boolean;
  testId?: string;
  controlTestId?: string;
  /** Its content is mounted only while it is open: a panel whose content reads something when
   *  mounted (a deployment's configuration, a key's state) reads it only when looked at. */
  mountWhenOpen?: boolean;
  /** Draw the expand-all and collapse-all icons of the enclosing `CollapsibleScope` beside the
   *  heading while the item is open: it is the heading of the section the scope holds. */
  scopeIcons?: boolean;
  children: ReactNode;
}

/** A group of this many items or more is given expand-all and collapse-all icons. */
const EXPAND_ALL_FROM = 3;

interface ScopeCommand {
  open: boolean;
  n: number;
}
interface Scope {
  id: string;
  command: ScopeCommand;
  send: (open: boolean) => void;
}
const ScopeContext = createContext<Scope | null>(null);

/**
 * Collapsibles that are drawn apart (one card each, in columns) but are one section: icons placed
 * in the section's heading (`CollapseAllIcons`) open or close every group inside the scope.
 */
export function CollapsibleScope({ id, children }: { id: string; children: ReactNode }) {
  const [command, setCommand] = useState<ScopeCommand>({ open: true, n: 0 });
  const send = useCallback((open: boolean) => setCommand((held) => ({ open, n: held.n + 1 })), []);
  return <ScopeContext.Provider value={{ id, command, send }}>{children}</ScopeContext.Provider>;
}

/**
 * The expand-all and collapse-all icon buttons. Drawn by `Collapsibles` for a group of three or
 * more, and by a section's heading for the groups of its `CollapsibleScope`. ``inHeading`` draws
 * them as spans, for use inside a collapsible's heading button.
 */
export function CollapseAllIcons({
  id,
  onSet,
  inHeading = false,
}: {
  /** Names the test ids: the group's id, or the scope's when none is given. */
  id?: string;
  /** Opens or closes everything; with none, the enclosing `CollapsibleScope`'s. */
  onSet?: (open: boolean) => void;
  inHeading?: boolean;
}) {
  const { t } = useTranslation();
  const scope = useContext(ScopeContext);
  const name = id ?? scope?.id;
  const set = onSet ?? scope?.send;
  if (name === undefined || set === undefined) {
    throw new Error("CollapseAllIcons needs a group id and handler, or a CollapsibleScope");
  }
  const button = (open: boolean) => {
    const label = t(open ? "collapsible.expandAll" : "collapsible.collapseAll");
    const Icon = open ? ChevronsUpDown : ChevronsDownUp;
    return (
      <Tip text={label}>
        <ActionIcon
          component={inHeading ? "span" : "button"}
          variant="subtle"
          color="gray"
          size="sm"
          aria-label={label}
          data-testid={`${name}-${open ? "expand" : "collapse"}-all`}
          {...(inHeading ? { role: "button", tabIndex: 0 } : {})}
          onClick={(e: { stopPropagation: () => void }) => {
            // In a heading, using the icon must not also open or close that collapsible.
            e.stopPropagation();
            set(open);
          }}
          onKeyDown={(e: { key: string; stopPropagation: () => void }) => {
            if (inHeading) {
              e.stopPropagation();
              if (e.key === "Enter" || e.key === " ") set(open);
            }
          }}
        >
          <Icon size={16} aria-hidden />
        </ActionIcon>
      </Tip>
    );
  };
  return (
    <Group gap={2} wrap="nowrap" component={inHeading ? "span" : "div"}>
      {button(true)}
      {button(false)}
    </Group>
  );
}

/** The items of the `Collapsibles` around an item that are open. */
const OpenItems = createContext<ReadonlySet<string>>(new Set());

/** One panel of a `Collapsibles`: its heading, then its content while open. */
export function CollapsibleItem({
  value,
  title,
  testId,
  controlTestId,
  mountWhenOpen = false,
  scopeIcons = false,
  children,
}: CollapsibleItemProps) {
  const open = useContext(OpenItems);
  return (
    <Accordion.Item value={value} data-testid={testId}>
      <Accordion.Control data-testid={controlTestId ?? `${value}-toggle`}>
        <Group gap="xs" wrap="nowrap" component="span">
          {title}
          {scopeIcons && open.has(value) && <CollapseAllIcons inHeading />}
        </Group>
      </Accordion.Control>
      <Accordion.Panel>{mountWhenOpen && !open.has(value) ? null : children}</Accordion.Panel>
    </Accordion.Item>
  );
}

CollapsibleItem.collapsibleItem = true;

/** The `CollapsibleItem`s among ``children``, through fragments and lists. */
function items(children: ReactNode): ReactElement<CollapsibleItemProps>[] {
  const found: ReactElement<CollapsibleItemProps>[] = [];
  Children.forEach(children, (child) => {
    if (!isValidElement(child)) return;
    if (child.type === Fragment) {
      found.push(...items((child.props as { children?: ReactNode }).children));
    } else if ((child.type as { collapsibleItem?: boolean }).collapsibleItem) {
      // A CollapsibleItem, or a component that renders one (FormFold): its props name the item, and
      // a static ``defaultOpen`` on the component is its default where the props give none.
      found.push(child as ReactElement<CollapsibleItemProps>);
    }
  });
  return found;
}

/**
 * Collapsible panels, each open or closed on its own. ``id`` names the group where it is (a page
 * and its panel, e.g. ``admin.ai-models``): with each item's value it is the key the viewer's open
 * and closed state is kept under, so the panels open again as they were left.
 */
export function Collapsibles({
  id,
  reveal,
  children,
  testId,
}: {
  id: string;
  /** An item to open, when a link asks for it (``?section=``): opened, and remembered so. */
  reveal?: string | null;
  children: ReactNode;
  testId?: string;
}) {
  const [remembered, setRemembered] = useState<Record<string, boolean>>(() => {
    const held = readCollapsibleStates()[id] ?? {};
    return reveal ? { ...held, [reveal]: true } : held;
  });
  const [revealed, setRevealed] = useState(reveal);
  if (reveal !== revealed) {
    // A link that asks for another item opens it too.
    setRevealed(reveal);
    if (reveal) setRemembered((held) => ({ ...held, [reveal]: true }));
  }
  const found = items(children);
  const scope = useContext(ScopeContext);
  const setAll = (value: boolean) => {
    const next: Record<string, boolean> = {};
    for (const item of found) next[item.props.value] = value;
    setRemembered((held) => ({ ...held, ...next }));
    writeCollapsibleState(id, next);
  };
  const commanded = scope?.command.n ?? 0;
  const [applied, setApplied] = useState(commanded);
  // The group whose heading holds the icons is the section itself: they act on what is inside.
  const obeys = scope !== null && !found.some((item) => item.props.scopeIcons);
  if (applied !== commanded) {
    // A command sent to the scope after this group was drawn; a group drawn later starts as left.
    setApplied(commanded);
    if (obeys) {
      const next: Record<string, boolean> = {};
      for (const item of found) next[item.props.value] = scope.command.open;
      setRemembered((held) => ({ ...held, ...next }));
    }
  }
  useEffect(() => {
    if (!obeys || commanded === 0) return;
    const next: Record<string, boolean> = {};
    for (const item of found) next[item.props.value] = scope.command.open;
    writeCollapsibleState(id, next);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- written once per command
  }, [commanded]);
  const open = found
    .filter(
      (item) =>
        item.props.forceOpen ||
        (remembered[item.props.value] ??
          item.props.defaultOpen ??
          (item.type as { defaultOpen?: boolean }).defaultOpen ??
          true),
    )
    .map((item) => item.props.value);
  const onChange = (values: string[]) => {
    const next: Record<string, boolean> = {};
    for (const item of found) next[item.props.value] = values.includes(item.props.value);
    setRemembered((held) => ({ ...held, ...next }));
    writeCollapsibleState(id, next);
  };
  return (
    <>
      {found.length >= EXPAND_ALL_FROM && (
        <Group justify="flex-end" mb={4}>
          <CollapseAllIcons id={id} onSet={setAll} />
        </Group>
      )}
      <Accordion
        multiple
        variant="contained"
        className="provisa-collapsible"
        value={open}
        onChange={onChange}
        data-testid={testId}
        data-collapsible-id={id}
      >
        <OpenItems.Provider value={new Set(open)}>{children}</OpenItems.Provider>
      </Accordion>
    </>
  );
}
