import type { Tagged } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export declare const rows: (() => Row[]) & Tagged<"table", {}, 2>;
