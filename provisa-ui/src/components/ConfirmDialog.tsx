// Copyright (c) 2026 Kenneth Stott
// Canary: d59e130f-265e-482d-b41e-f689fc8b6c56
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, ReactNode } from "react";

interface Props {
  title: string;
  consequence: string;
  onConfirm: () => void | Promise<void>;
  children: (open: () => void) => ReactNode;
}

/** Confirmation dialog for destructive actions with consequence summary (REQ-061). */
export function ConfirmDialog({ title, consequence, onConfirm, children }: Props) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleConfirm = async () => {
    setLoading(true);
    try {
      await onConfirm();
    } finally {
      setLoading(false);
      setOpen(false);
    }
  };

  return (
    <>
      {children(() => setOpen(true))}
      {open && (
        <div className="modal-overlay" onClick={() => setOpen(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{title}</h3>
            <p className="consequence">{consequence}</p>
            <div className="modal-actions">
              <button onClick={() => setOpen(false)} disabled={loading}>
                Cancel
              </button>
              <button
                className="destructive"
                onClick={handleConfirm}
                disabled={loading}
              >
                {loading ? "Processing..." : "Confirm"}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
