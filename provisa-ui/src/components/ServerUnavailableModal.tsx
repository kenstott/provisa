// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Group, Loader, Modal, Stack, Text } from "@mantine/core";
import { CloudOff } from "lucide-react";
import { serverUnreachable, subscribeServerReachability } from "../lib/serverReachability";

/**
 * REQ-1514: the app-wide "the server is not answering" notice.
 *
 * A deploy restart, a node coming back from an idle stop, or a dropped network makes every
 * same-origin request reject at the transport, and each page then renders that in its own
 * vocabulary — GraphiQL prints "TypeError: Failed to fetch" over the result pane, a list simply
 * comes up empty. This says the one thing that is actually true, in one place, for every route.
 *
 * Modal rather than a banner, and not dismissible: nothing the user does while the server is down
 * can succeed, so the interruption is the message. It clears itself the moment the deployment
 * answers again (serverReachability polls /health), because the recovery the user is waiting for
 * should not also cost them a reload.
 */
export function ServerUnavailableModal() {
  const [down, setDown] = useState(serverUnreachable);

  useEffect(() => subscribeServerReachability(setDown), []);

  return (
    <Modal
      opened={down}
      onClose={() => {
        /* not dismissible — see the note above */
      }}
      withCloseButton={false}
      closeOnClickOutside={false}
      closeOnEscape={false}
      centered
      title={
        <Group gap="xs">
          <CloudOff size={18} />
          <Text fw={600}>Server unavailable</Text>
        </Group>
      }
      data-testid="server-unavailable-modal"
    >
      <Stack gap="sm">
        <Text size="sm">
          Provisa is not answering right now — it is usually restarting after a deploy, or waking
          from an idle stop.
        </Text>
        <Group gap="xs">
          <Loader size="xs" />
          <Text size="sm" c="dimmed">
            Waiting for it to come back. This message clears itself.
          </Text>
        </Group>
      </Stack>
    </Modal>
  );
}
