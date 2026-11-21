export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** @table
 * @loader
 */
export function rows(inputs: Inputs): Row[] {
  return [];
}
