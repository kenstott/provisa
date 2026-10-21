// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3a1995-f546-43f8-8256-5381a7481419
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: a 3D profile view drawn in place and, on its expand button, again in a 95% modal with a
// camera of its own. The two copies share whatever state the caller keeps.

import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Modal } from "@mantine/core";
import { Maximize2 } from "lucide-react";
import { Tip } from "../../../components/Tip";

export function ExpandButton({ onClick, testId }: { onClick: () => void; testId: string }) {
  const { t } = useTranslation();
  return (
    <Tip text={t("profile3d.expand")} named>
      <button
        type="button"
        className="p3d-expand"
        onClick={onClick}
        aria-label={t("profile3d.expand")}
        data-testid={testId}
      >
        <Maximize2 size={14} />
      </button>
    </Tip>
  );
}

export function Expandable3D({
  title,
  testId,
  render,
}: {
  title: string;
  testId: string;
  /** One copy of the view; ``onExpand`` is given to the in-place copy only. */
  render: (expanded: boolean, onExpand?: () => void) => ReactNode;
}) {
  const [expanded, setExpanded] = useState(false);
  return (
    <>
      {render(false, () => setExpanded(true))}
      <Modal
        opened={expanded}
        onClose={() => setExpanded(false)}
        size="95%"
        title={title}
        styles={{
          content: { height: "95vh" },
          body: { height: "calc(95vh - 3.5rem)", overflow: "hidden" },
        }}
        data-testid={`${testId}-modal`}
      >
        {expanded && render(true)}
      </Modal>
    </>
  );
}
