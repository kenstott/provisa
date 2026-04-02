import { useAuth } from "../context/AuthContext";
import type { Capability } from "../types/auth";

/** Check if current role has a capability (admin has all). */
export function useCapability(cap: Capability): boolean {
  const { role } = useAuth();
  if (!role) return false;
  return role.capabilities.includes(cap) || role.capabilities.includes("admin");
}

/** Check multiple capabilities — returns true if role has ALL. */
export function useCapabilities(caps: Capability[]): boolean {
  const { role } = useAuth();
  if (!role) return false;
  if (role.capabilities.includes("admin")) return true;
  return caps.every((c) => role.capabilities.includes(c));
}
