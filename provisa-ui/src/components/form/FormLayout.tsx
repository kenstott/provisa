// Copyright (c) 2026 Kenneth Stott
// Canary: f421e7f1-870e-475c-bbe1-9938199a2c82
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968: the one layout every form uses. Fields flow in a wrapping row and take as many columns
// as the container allows, so no form carries breakpoints of its own; the DOM order is the visual
// order and the keyboard order. The widths live in App.css (`.form-layout`, `.form-field--*`).

import {
  Children,
  cloneElement,
  createContext,
  createElement,
  isValidElement,
  useContext,
  useId,
  useLayoutEffect,
  useState,
  type CSSProperties,
  type FormEventHandler,
  type HTMLAttributes,
  type ReactNode,
} from "react";
import { Button, Divider, Loader, Text } from "@mantine/core";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { Check, CircleHelp, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import { createPortal } from "react-dom";
import { PanelActionsContext, PanelNoticeContext } from "../list/panelActions";
import { PanelHeadStyle } from "../list/PanelHeadStyle";
import { useHelpNaming } from "./useHelpNaming";
import { Tip } from "../Tip";
import { readValue } from "./readValue";
import type { DetailField } from "../detail/DetailFields";
import {
  FormReadContext,
  FormSectionIdContext,
  FormSectionsContext,
  useFormRead,
  useFormSectionInView,
  type FormSectionsState,
} from "./formRead";

const FormLayoutContext = createContext(false);

/**
 * A form drawn read-only: the same form, so the read view lists exactly the fields the form has
 * for what it shows, under the same labels. Each field is its label and its value on the shared
 * detail grid ("—" when empty, "Set" for a secret that holds something); the layout around the
 * fields, the notes and the buttons are not drawn, and the form's Save and Cancel are not offered.
 */
export function FormRead({
  children,
  derived = [],
  testId,
}: {
  children: ReactNode;
  /** Values the form does not set but the object has, read after its fields. */
  derived?: DetailField[];
  testId?: string;
}) {
  return (
    <FormReadContext.Provider value={true}>
      <FormLayoutContext.Provider value={true}>
        <dl className="detail-fields form-read" data-testid={testId}>
          {children}
          {derived.map((f) => (
            <ReadRow key={f.label} label={f.label} value={f.value} testId={f.testId} />
          ))}
        </dl>
      </FormLayoutContext.Provider>
    </FormReadContext.Provider>
  );
}

/**
 * One field of a form read: its label beside its value ("—" when empty). A control that labels
 * itself draws this when `useFormRead()` says the form is read.
 */
export function ReadRow({
  label,
  value,
  testId,
}: {
  label: ReactNode;
  value: ReactNode;
  testId?: string;
}) {
  const { t } = useTranslation();
  return (
    <>
      <Text component="dt" c="dimmed" fw={500} size="sm">
        {label}
      </Text>
      <Text component="dd" m={0} size="sm" data-testid={testId}>
        {value === null || value === undefined || value === "" ? t("detailFields.empty") : value}
      </Text>
    </>
  );
}

/** A field read: its control's label beside the value the control holds. */
function ReadField({ children }: { children: ReactNode }) {
  const { t } = useTranslation();
  const only = Children.toArray(children);
  const control = only.length === 1 ? only[0] : null;
  const props = isValidElement<Explainable>(control) ? control.props : null;
  const label = props ? (props.label ?? props["aria-label"]) : undefined;
  // A rule that names the fields after it heads them.
  if (isValidElement(control) && control.type === Divider && label !== undefined) {
    return (
      <div role="heading" aria-level={4} className="form-read__heading">
        {label}
      </div>
    );
  }
  // A field whose child is no labelled control (a section of further fields) reads as those.
  if (!isValidElement(control) || label === undefined) return <>{children}</>;
  return <ReadRow label={label} value={readValue(control, t)} />;
}

function useInsideFormLayout(component: string): void {
  if (!useContext(FormLayoutContext)) {
    throw new Error(`<${component}> must be rendered inside a <FormLayout> (REQ-1968)`);
  }
}

function classes(...names: (string | undefined)[]): string {
  return names.filter(Boolean).join(" ");
}

type LayoutElementProps = Omit<HTMLAttributes<HTMLElement>, "onSubmit"> & {
  onSubmit?: FormEventHandler<HTMLFormElement>;
};

export function FormLayout({
  component = "div",
  className,
  children,
  ...rest
}: LayoutElementProps & { component?: "div" | "form" | "fieldset" }) {
  if (useFormRead()) return <>{children}</>;
  return (
    <FormLayoutContext.Provider value={true}>
      {createElement(
        component,
        { ...rest, className: classes("form-layout", className) },
        children,
      )}
    </FormLayoutContext.Provider>
  );
}

/**
 * short: numbers, ports, durations, switches, short choices. medium: names, hosts. long: addresses,
 * paths, one-line descriptions. full: multi-line text, editors, tables and sections only.
 */
export type FormFieldSize = "short" | "medium" | "long" | "full";

/**
 * A label with its longer explanation: the text, the required mark right after the text, then a
 * (?) opened by hover, focus or touch. The one help glyph of every form.
 *
 * The control is NAMED by the text alone and DESCRIBED by the help: its owner points
 * `aria-labelledby` at `textId` and gives the help as its description, so the (?) inside the
 * label is not read as part of the name.
 */
export function LabelWithHelp({
  label,
  help,
  required = false,
  textId,
}: {
  label: ReactNode;
  help: string;
  required?: boolean;
  textId?: string;
}) {
  return (
    <span className="form-label">
      <span>
        <span id={textId}>{label}</span>
        {required && (
          <span className="form-required" aria-hidden>
            {" *"}
          </span>
        )}
      </span>
      {/* The one tooltip of the application. It opens at once: the (?) exists only to be asked,
          so nothing is gained by waiting. Its name already is the help, so it is not also its
          description. */}
      <Tip text={help} openDelay={0} named>
        <span className="form-help" role="img" tabIndex={0} aria-label={help}>
          <CircleHelp size={14} aria-hidden />
        </span>
      </Tip>
    </span>
  );
}

type Explainable = {
  label?: ReactNode;
  description?: ReactNode;
  required?: boolean;
  withAsterisk?: boolean;
  "aria-label"?: string;
};

/**
 * One field of a form. `hint` is a brief explanation (one short sentence) shown BELOW the control;
 * `help` is a longer one, shown only behind a (?) immediately after the label. Nothing is ever
 * rendered between the label and the control, so the controls of a row share a baseline.
 */
export function FormField({
  size,
  hint,
  help,
  section,
  className,
  style,
  children,
  ...rest
}: Omit<HTMLAttributes<HTMLDivElement>, "style"> & {
  size: FormFieldSize;
  hint?: string;
  help?: string;
  style?: CSSProperties;
  /** The section of a sectioned form (`FormSections`) the field is in; its `FormSection`'s, else
   *  the form's first, when not given. */
  section?: string;
  children: ReactNode;
}) {
  useInsideFormLayout("FormField");
  const { textId, naming } = useHelpNaming(help);
  const inView = useFormSectionInView(section);
  const read = useFormRead();
  if (!inView) return null;
  // The fields inside a field (a fold of further fields) are in its section too.
  const within = (node: ReactNode) =>
    section ? (
      <FormSectionIdContext.Provider value={section}>{node}</FormSectionIdContext.Provider>
    ) : (
      node
    );
  if (read) return within(<ReadField>{children}</ReadField>);
  let content = children;
  if (hint !== undefined || help !== undefined) {
    const only = Children.toArray(children);
    const control = only.length === 1 ? only[0] : null;
    if (!isValidElement<Explainable>(control)) {
      throw new Error(
        "<FormField hint|help> needs the field's control as its only child (REQ-1968)",
      );
    }
    content = cloneElement(control, {
      ...(hint !== undefined ? { description: hint } : {}),
      ...(help !== undefined
        ? {
            // The mark is drawn with the text, so it comes before the (?); a required control
            // keeps `required` and only stops drawing a second mark after the icon. Only a
            // required control draws one: a tick or a switch takes no `withAsterisk`, and given
            // one it lands on the DOM input as an unknown attribute.
            label: (
              <LabelWithHelp
                label={control.props.label}
                help={help}
                required={control.props.required === true}
                textId={textId}
              />
            ),
            // Only an input that draws the mark takes the prop: a Checkbox or Switch passes it on
            // to its DOM element, where React warns of an unknown attribute.
            ...(control.props.required === true ? { withAsterisk: false } : {}),
            // A control that states its own accessible name keeps it; the help is still its
            // description.
            ...(control.props["aria-label"] === undefined ? naming : { "aria-description": help }),
          }
        : {}),
    });
  }
  return within(
    <div
      {...rest}
      style={style}
      className={classes("form-field", `form-field--${size}`, className)}
    >
      {content}
    </div>,
  );
}

/**
 * The ticks and switches of a form, together on a line of their own beneath the fields they
 * belong with. Each is its label and an optional (?); no paragraph sits beside a box.
 *
 * `column`: a checklist — one tick to a line, where the ticks ARE the form (a list of things to
 * accept or leave out) rather than a few settings beside its fields.
 */
export function FormChecks({
  column = false,
  children,
  ...rest
}: HTMLAttributes<HTMLDivElement> & { column?: boolean }) {
  useInsideFormLayout("FormChecks");
  if (useFormRead()) return <>{children}</>;
  return (
    <div
      {...rest}
      className={classes("form-checks", column ? "form-checks--column" : undefined, rest.className)}
    >
      {children}
    </div>
  );
}

/**
 * Fields that belong together (host and port, precision and scale): they wrap as one. With `row`
 * the group takes a line to itself: one entry of a list the form edits.
 */
export function FieldGroup({
  row = false,
  children,
  ...rest
}: HTMLAttributes<HTMLDivElement> & { row?: boolean }) {
  useInsideFormLayout("FieldGroup");
  if (useFormRead()) return <>{children}</>;
  return (
    <div
      {...rest}
      className={classes(
        "form-field-group",
        row ? "form-field-group--row" : undefined,
        rest.className,
      )}
    >
      {children}
    </div>
  );
}

/**
 * One entry of a list the form edits that takes more than one row (a column, then who sees it and
 * how it is masked): its rows kept together, and set apart from the next entry by a rule, so each
 * entry reads as one group. An entry of a single row is a `FieldGroup row` instead.
 */
export function FormEntry({ children, className, ...rest }: LayoutElementProps) {
  useInsideFormLayout("FormEntry");
  if (useFormRead()) return <>{children}</>;
  return (
    <FormLayout {...rest} className={classes("form-entry", className)}>
      {children}
    </FormLayout>
  );
}

/**
 * A form whose fields are in sections, one in view: a field shows when its section is ``active``.
 * A field names its section (`FormField section`), or is in a `FormSection`, or is in the form's
 * first, ``fallback``. ``onSections`` hears, after each render, the sections the form has for
 * what it shows (a section none of whose fields applies is not among them), so the tabs can hide
 * the others. The same in a form read (`FormRead`).
 */
export function FormSections({
  active,
  fallback,
  onSections,
  children,
}: {
  active: string;
  fallback: string;
  onSections: (sections: string[]) => void;
  children: ReactNode;
}) {
  const state: FormSectionsState = { active, fallback, seen: new Set<string>() };
  // The fields note their sections as they render, before this effect runs.
  useLayoutEffect(() => onSections([...state.seen]));
  return <FormSectionsContext.Provider value={state}>{children}</FormSectionsContext.Provider>;
}

/** Fields of one section of a `FormSections` form, shown while it is the section in view. */
export function FormSection({ id, children }: { id: string; children: ReactNode }) {
  const inView = useFormSectionInView(id);
  return (
    <FormSectionIdContext.Provider value={id}>
      {inView ? children : null}
    </FormSectionIdContext.Provider>
  );
}

/**
 * Fields folded away under headings until they are wanted (settings that all have defaults), as
 * the application's one collapsible (`Collapsibles`); ``id`` names the folds where they are, so
 * each opens as this viewer left it. Read, every fold is open: its heading heads its fields.
 */
export function FormFolds({ id, children }: { id: string; children: ReactNode }) {
  useInsideFormLayout("FormFolds");
  if (useFormRead()) return <>{children}</>;
  return (
    <Collapsibles id={id} testId={id}>
      {children}
    </Collapsibles>
  );
}

/** One fold of a `FormFolds`: its heading, an optional note under it, and its fields; closed
 *  until this viewer opens it, unless ``defaultOpen``. */
export function FormFold({
  value,
  title,
  note,
  defaultOpen = false,
  children,
}: {
  value: string;
  title: ReactNode;
  note?: ReactNode;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  if (useFormRead()) {
    return (
      <>
        <div role="heading" aria-level={4} className="form-read__heading">
          {title}
        </div>
        {children}
      </>
    );
  }
  return (
    <CollapsibleItem value={value} title={title} defaultOpen={defaultOpen}>
      {note && (
        <Text size="sm" c="dimmed" mb="sm">
          {note}
        </Text>
      )}
      <FormLayout>{children}</FormLayout>
    </CollapsibleItem>
  );
}
FormFold.collapsibleItem = true;
FormFold.defaultOpen = false;

/** The fields that name the object. They sit above the flow, in a section of their own. */
export function FormIdentifiers({ children, ...rest }: HTMLAttributes<HTMLElement>) {
  useInsideFormLayout("FormIdentifiers");
  const { t } = useTranslation();
  const headingId = useId();
  const read = useFormRead();
  // The fields that name the object are in the form's first section: not drawn in another.
  const inView = useFormSectionInView();
  if (!inView) return null;
  if (read) return <>{children}</>;
  return (
    <section
      {...rest}
      aria-labelledby={headingId}
      className={classes("form-identifiers", rest.className)}
    >
      <h3 id={headingId} className="form-identifiers__heading">
        {t("common.identity")}
      </h3>
      {children}
    </section>
  );
}

type ButtonExtras = { "data-testid"?: string; disabled?: boolean };

/**
 * A form's or dialog's action row: at the bottom, at the start of the line (left in a left-to-right
 * language, right in a right-to-left one). It owns the form's Save and Cancel, so every form has
 * the same two: the check and the word "Save", then the cross and the word "Cancel".
 *
 * From the press until the save settles Save shows a spinner in place of its icon and both buttons
 * are disabled. A form says it is saving in one of two ways: `onSave` returns the save's promise,
 * or the form passes `saving` (a form that submits through `<form onSubmit>` sets `submit`).
 * Other actions of the row go in as children, after the two. A form whose first action is not a
 * save (Generate, Run, Delete) passes it as `action`: it takes Save's place, before Cancel, and
 * keeps its own word.
 *
 * `panel`: the form is a detail panel's content, and these are the panel's actions while it is
 * open. In a panel (`SplitPane`) the same two buttons then sit in its pinned head beside the
 * name -- borderless and compact, so they cost no height -- in place of a row at the form's foot;
 * outside a panel a form that says `panel` keeps the row at its foot. One implementation, the
 * same test ids either way. `problem` is what the form has to say of a save that did not go
 * through: in a panel a line under its head, else a line above the row.
 *
 * `form`: the id of the <form> a `submit` Save belongs to. In a panel's head Save sits outside
 * the form's own element, where a submit button submits nothing; naming the form makes it that
 * form's submit wherever it sits, so the form's required-field checks and its onSubmit run.
 */
export function FormFooter({
  onSave,
  onCancel,
  submit = false,
  saving = false,
  saveProps,
  cancelProps,
  action,
  panel = false,
  problem,
  form,
  children,
  ...rest
}: HTMLAttributes<HTMLDivElement> & {
  action?: ReactNode;
  onSave?: () => unknown;
  onCancel?: () => void;
  submit?: boolean;
  saving?: boolean;
  saveProps?: ButtonExtras;
  cancelProps?: ButtonExtras;
  panel?: boolean;
  problem?: ReactNode;
  form?: string;
}) {
  const { t } = useTranslation();
  const [pending, setPending] = useState(false);
  const head = useContext(PanelActionsContext);
  const notice = useContext(PanelNoticeContext);
  // A form read is not saved: it offers no Save or Cancel.
  if (useFormRead()) return null;
  const inHead = panel && head !== undefined;
  const busy = saving || pending;
  const save = () => {
    const result = onSave?.();
    if (result instanceof Promise) {
      setPending(true);
      // Settled either way: a refused save returns the button to rest, and the form shows why.
      void result.then(
        () => setPending(false),
        () => setPending(false),
      );
    }
  };
  // In a panel's head the two take the head's one style (PanelHeadStyle): borderless and
  // compact; the words and icons are the same. Cancel leaves its bordered look at the foot.
  const inHeadLook = inHead ? ({ variant: undefined } as const) : {};
  const said = problem ? (
    <div className="form-problem" role="alert" data-testid="form-problem">
      {problem}
    </div>
  ) : null;
  const buttons = (
    <>
      {(onSave !== undefined || submit) && (
        <Button
          {...inHeadLook}
          {...saveProps}
          // In a panel's head both are borderless: the tint is what says which is the primary.
          data-primary={inHead || undefined}
          form={form}
          type={submit ? "submit" : "button"}
          onClick={submit ? undefined : save}
          leftSection={busy ? <Loader size={14} color="currentColor" /> : <Check size={14} />}
          disabled={busy || saveProps?.disabled === true}
          data-saving={busy || undefined}
          // REQ-2115: what Polly presses here changes something; she asks the user first.
          data-changes="true"
        >
          {t("common.save")}
        </Button>
      )}
      {action}
      {onCancel !== undefined && (
        <Button
          variant="default"
          {...inHeadLook}
          {...cancelProps}
          type="button"
          onClick={onCancel}
          // REQ-2115: leaving a form changes nothing; Polly presses it without asking.
          data-changes="false"
          leftSection={<X size={14} />}
          disabled={busy || cancelProps?.disabled === true}
        >
          {t("common.cancel")}
        </Button>
      )}
      {children}
    </>
  );
  if (inHead) {
    // The head is not mounted on the first pass; the actions appear with it on the next.
    return (
      <>
        {head !== null &&
          createPortal(
            <div
              {...rest}
              style={undefined}
              className={classes("form-footer", "form-footer--head", rest.className)}
            >
              <PanelHeadStyle>{buttons}</PanelHeadStyle>
            </div>,
            head,
          )}
        {said !== null && notice && createPortal(said, notice)}
      </>
    );
  }
  return (
    <div {...rest} className={classes("form-footer", rest.className)}>
      {said}
      {buttons}
    </div>
  );
}
