import { useAuth } from "../context/AuthContext";
import type { Capability } from "../types/auth";

/** Check if unioned capabilities include a capability (admin has all). */
export function useCapability(cap: Capability): boolean {
  const { capabilities } = useAuth();
  if (capabilities.length === 0) return false;
  return capabilities.includes(cap) || capabilities.includes("admin");
}

/** Check multiple capabilities — returns true if unioned capabilities have ALL. */
export function useCapabilities(caps: Capability[]): boolean {
  const { capabilities } = useAuth();
  if (capabilities.length === 0) return false;
  if (capabilities.includes("admin")) return true;
  return caps.every((c) => capabilities.includes(c));
}
