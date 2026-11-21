export interface Row {
  id: number;
  name: string;
}

/** Rows from the API.
 * @table
 * @requires API_KEY:secret
 * @requires REGION
 */
export function rows(): Row[] {
  return [];
}
