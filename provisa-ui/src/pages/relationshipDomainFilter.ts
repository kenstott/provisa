// The navbar's domain multi-select narrows the Relationships list: a relationship stays when its
// source, target or owner domain is checked. No selection, or every domain selected, is no
// narrowing (as on the Tables page). `narrowTo` holds normalized domain ids.
export function relationshipInCheckedDomains(
  narrowTo: ReadonlySet<string> | null,
  domains: ReadonlyArray<string | null | undefined>,
): boolean {
  if (narrowTo === null) return true;
  return domains.some((d) => d != null && narrowTo.has(d));
}
