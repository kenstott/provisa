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
