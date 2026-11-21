export interface Row {
  id: number;
  name: string;
}

/** @table */
export function rows(): Record<string, unknown>[] {
  return [];
}
