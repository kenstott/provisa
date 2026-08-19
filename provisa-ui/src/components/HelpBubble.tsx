import { ActionIcon, HoverCard, Stack, Text } from "@mantine/core";
import { CircleHelp } from "lucide-react";

/**
 * The "?" beside a page's add control. It answers what the page is FOR — what the object
 * does once it exists and what a curator is deciding by creating one — because the button
 * already says what it does and the purpose is the part nobody can read off the screen.
 */
export function HelpBubble({
  title,
  paragraphs,
  ariaLabel,
  testId,
}: {
  title: string;
  paragraphs: string[];
  ariaLabel: string;
  testId: string;
}) {
  return (
    <HoverCard width={340} shadow="md" withArrow position="bottom-end">
      <HoverCard.Target>
        <ActionIcon variant="subtle" color="gray" aria-label={ariaLabel} data-testid={testId}>
          <CircleHelp size={16} />
        </ActionIcon>
      </HoverCard.Target>
      <HoverCard.Dropdown>
        <Stack gap="xs">
          <Text size="sm" fw={600}>
            {title}
          </Text>
          {paragraphs.map((p) => (
            <Text key={p} size="xs" c="dimmed">
              {p}
            </Text>
          ))}
        </Stack>
      </HoverCard.Dropdown>
    </HoverCard>
  );
}
