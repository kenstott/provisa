import type { ReactNode } from "react";
import { useCapability } from "../hooks/useCapability";
import type { Capability } from "../types/auth";

interface Props {
  capability: Capability;
  children: ReactNode;
  fallback?: ReactNode;
}

/** Only render children if current role has the required capability. */
export function CapabilityGate({ capability, children, fallback }: Props) {
  const allowed = useCapability(capability);
  if (!allowed) return fallback ?? null;
  return <>{children}</>;
}
