// Copyright (c) 2026 Kenneth Stott
// Canary: 9b3e6a14-2d57-4c80-a1f9-6e0c7d5b8a32
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the shared step strip names every step, marks the current one, lets a completed step
// go back, ignores a step not yet reached, and keeps the step in the URL.

import { describe, expect, it, vi } from "vitest";
import { useLocation } from "react-router-dom";
import { fireEvent, render, screen } from "../../test-utils/render";
import { StepStrip } from "../StepStrip";
import { useStepParam } from "../../hooks/useStepParam";

const STEPS = [
  { key: "a", label: "Alpha" },
  { key: "b", label: "Beta", description: "two things" },
  { key: "c", label: "Gamma" },
];

describe("StepStrip", () => {
  it("names every step, with the current one active", () => {
    render(<StepStrip steps={STEPS} active={1} onStepClick={vi.fn()} label="Steps" testId="s" />);
    for (const label of ["Alpha", "Beta", "Gamma", "two things"])
      expect(screen.getByText(label)).toBeInTheDocument();
    expect(screen.getByTestId("s")).toHaveAttribute("data-active", "1");
  });

  it("goes back through a completed step and not to one not yet reached", () => {
    const go = vi.fn();
    render(<StepStrip steps={STEPS} active={1} onStepClick={go} label="Steps" testId="s" />);
    fireEvent.click(screen.getByTestId("s-c"));
    expect(go).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("s-a"));
    expect(go).toHaveBeenCalledWith(0);
  });

  it("does not open the current step unless asked to, and then opens it", () => {
    const go = vi.fn();
    const { rerender } = render(
      <StepStrip steps={STEPS} active={1} onStepClick={go} label="Steps" testId="s" />,
    );
    fireEvent.click(screen.getByTestId("s-b"));
    expect(go).not.toHaveBeenCalled();
    rerender(
      <StepStrip
        steps={STEPS}
        active={1}
        onStepClick={go}
        currentSelectable
        label="Steps"
        testId="s"
      />,
    );
    fireEvent.click(screen.getByTestId("s-c"));
    expect(go).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("s-b"));
    expect(go).toHaveBeenCalledWith(1);
  });
});

function Probe({ reachable }: { reachable: number }) {
  const [step, setStep] = useStepParam(3, reachable);
  const loc = useLocation();
  return (
    <>
      <pre data-testid="step">{step}</pre>
      <pre data-testid="search">{loc.search}</pre>
      <button data-testid="go" onClick={() => setStep(2)} />
    </>
  );
}

describe("useStepParam", () => {
  it("reads ?step= 1-based and writes it", () => {
    render(<Probe reachable={2} />, { initialEntries: ["/x?step=2&keep=1"] });
    expect(screen.getByTestId("step")).toHaveTextContent("1");
    fireEvent.click(screen.getByTestId("go"));
    expect(screen.getByTestId("step")).toHaveTextContent("2");
    expect(screen.getByTestId("search")).toHaveTextContent("step=3");
    expect(screen.getByTestId("search")).toHaveTextContent("keep=1");
  });

  it("opens at the first step without a parameter and at the last reachable past it", () => {
    render(<Probe reachable={0} />, { initialEntries: ["/x?step=3"] });
    expect(screen.getByTestId("step")).toHaveTextContent("0");
  });
});
