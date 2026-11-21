import { health, onStart, onStop, table } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Open the client. */
export const start = onStart(async function start(values: Record<string, string>): Promise<void> {});

/** Close the client. */
export const stop = onStop(function stop(): void {});

export const status = health(async function status(): Promise<string> {
  return "ok";
});

export const rows = table(function rows(): Row[] {
  return [];
});
