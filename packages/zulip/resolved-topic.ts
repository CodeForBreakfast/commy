/**
 * Zulip's resolved-topic convention, kept strictly inside the Zulip adapter.
 *
 * Zulip has no separate "resolved" flag on a topic — resolution is expressed by
 * renaming the topic with a leading `✔ ` (heavy check mark U+2714 + space) and
 * an `update_message` carrying `propagate_mode='change_all'`. commy's port
 * models resolution as an observable `ObservedThread.resolved` boolean with a
 * clean `name`, so this glyph never crosses the port boundary: read paths strip
 * it here and surface the flag, the setter re-applies it here.
 */

export const RESOLVED_TOPIC_PREFIX = '✔ '

/** Whether a raw Zulip topic string carries the resolved prefix. */
export const isResolvedTopic = (topic: string): boolean => topic.startsWith(RESOLVED_TOPIC_PREFIX)

/** The topic with its resolved prefix removed (unchanged if it has none). */
export const stripResolvedPrefix = (topic: string): string =>
  isResolvedTopic(topic) ? topic.slice(RESOLVED_TOPIC_PREFIX.length) : topic

/**
 * Split a raw substrate topic into the port-facing clean `name` and its
 * `resolved` status. A prefix with no base after it (the degenerate `"✔ "`)
 * is left intact and reported unresolved rather than yielding an empty name —
 * Zulip forbids resolving an empty topic, so this only guards a malformed realm.
 */
export const splitTopic = (
  rawTopic: string,
): { readonly name: string; readonly resolved: boolean } => {
  const stripped = stripResolvedPrefix(rawTopic)
  return isResolvedTopic(rawTopic) && stripped.length > 0
    ? { name: stripped, resolved: true }
    : { name: rawTopic, resolved: false }
}

/**
 * The raw Zulip topic that expresses the requested resolution state for a plain
 * thread name: prefixed when resolving, bare when not. Idempotent, so an input
 * that already carries (or lacks) the prefix is left correct.
 */
export const applyResolvedPrefix = (name: string, resolved: boolean): string =>
  resolved
    ? isResolvedTopic(name)
      ? name
      : RESOLVED_TOPIC_PREFIX + name
    : stripResolvedPrefix(name)
