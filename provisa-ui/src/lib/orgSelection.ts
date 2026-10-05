// Copyright (c) 2026 Kenneth Stott
// Canary: 68e93bd9-a965-453d-a400-9f4cdcc14428
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1935: whether the server has refused a request because it names no org, as one piece of
 * app-wide state.
 *
 * Every request is served in the org it names; one that names none is refused with 401 and the
 * stable code below -- a platform operator's included, since no org is implied. Each call site
 * would otherwise render that refusal in its own vocabulary (an empty page, a GraphiQL error), so
 * the fetch interceptor records it here and one prompt answers it: select an org.
 */

/** The stable code of the server's refusal (provisa/auth/middleware.py ORG_SELECTION_REQUIRED). */
export const ORG_SELECTION_REQUIRED = "auth.org_selection_required";

type Listener = (required: boolean) => void;

let required = false;
const listeners = new Set<Listener>();

function publish(next: boolean): void {
  if (required === next) return;
  required = next;
  for (const listener of listeners) listener(required);
}

/** Whether a request has been refused for naming no org since an org was last selected. */
export function orgSelectionRequired(): boolean {
  return required;
}

export function subscribeOrgSelection(listener: Listener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Record the refusal when ``res`` is it; any other response is left exactly as it is. */
export async function noteOrgSelectionRefusal(res: Response): Promise<void> {
  if (res.status !== 401) return;
  let body: unknown;
  try {
    body = await res.clone().json();
  } catch {
    return; // not a JSON body, so not this refusal
  }
  if ((body as { code?: unknown })?.code === ORG_SELECTION_REQUIRED) publish(true);
}

/** An org was selected: the next requests name it. */
export function clearOrgSelectionRequired(): void {
  publish(false);
}
