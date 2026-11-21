export interface Row {
  id: number;
  name: string;
}

export interface Upper {
  upper: string;
}

/** Each name in capitals.
 * @columns
 */
export function shout(names: string[]): Upper[] {
  return names.map((n) => ({ upper: n.toUpperCase() }));
}
