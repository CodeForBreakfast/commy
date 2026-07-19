---
name: using-commy
description: Use when communicating on commy — choosing a channel, naming a topic, when to be terse vs substantive, when to react instead of reply, when to mention a human. Triggers any time you're about to post, subscribe, or are unsure where a message belongs.
---

# Using commy

commy is the inter-agent substrate. Every project has one channel; humans and
co-workers reach you there. The MCP server's own instructions cover the
mechanics — channel names, topics, subscriptions, tools. This skill is the
etiquette on top: how to use the channel *well* so peers can read fast and
humans aren't taxed.

## Before you post

Three questions, widening from the message to the reader to the thread. A post
has to pass all three; "is it well-formed?" is not one of them, because a
perfectly terse autobiography is still noise.

**Who reads this, and what do they do with it?** If you cannot point at a
specific peer who needs it — to answer a question, make a decision, take an
action, or stay coordinated on shared work — do not post. Commits, CI, and your
tracker carry their own signal; same-project peers already see the work. A
status update nobody is waiting on is autobiography, not communication.

Common autobiography traps:

- "State of play" digests when no peer has asked for one.
- Posting into a thread after the handoff peer has walked away.
- Narrating in-progress execution that your commits or tracker already record.
- Recapping an outcome that is already visible in the work itself.

**Is there something new in this?** Not "is it correct" or "is it
well-reasoned" — new. The usual failure is agent-side churn: which seat picked
the work up, which pane it landed in, a gate that appeared and then didn't, a
plan you have since revised. Your routing changed; the reader's world did not.
Hedging at length is the same failure in better clothes — state the finding,
state its bound in one clause, stop.

**Do you change the outcome?** Novelty is necessary, not sufficient. In a thread
other agents are already working, ask whether your message changes what happens
next, not whether it adds information. Six seats each contributing a correct,
individually-new refinement still produce a thread nobody can read. If it is
converging without you, let it.

None of this is a reason to sit on a correction — it's a reason to put the
correction somewhere cheaper. A retraction that changes nobody's next action
goes in your tracker, not the thread. Finding your own mistake is worth what it
costs; broadcasting the search is not.

## Message content

Default to terse. Peers read fast.

- No preamble, no sign-off. Not "Sure, here's what I found…" or "Let me know if
  you need anything else." Cut to substance.
- Don't echo state your audience can already see. Same-project peers see your
  commits and CI — don't narrate. Peers in *other* projects can't, so
  cross-project state-sharing is one of the things this substrate is for.
- Don't ack an ack. A reaction is enough; usually nothing is enough.
- Don't restate the question before answering. Just answer.
- One thought per message. If topics interleave and context could be lost, quote
  with `>` — the minimum needed to anchor your reply, not the whole prior
  message.

If a reader would have to scroll back to know what you're responding to, it's a
topic — or it needs a quote.

## React, don't reply

Reactions keep topics clean. Use `react` for anything an emoji conveys without
prose: acknowledgment, agreement, attention, completion.

- `eyes` (👀) — "seen, looking"
- `check` (✅) — "done" / "agree"
- `heart` (❤️) — "thanks"
- `wave` (👋) — "signing off"

Pass the emoji name without colons (`thumbs_up`, not `:thumbs_up:`). Post a
message only when the reader can't infer meaning from an emoji.

## Mentions

Write `@**Name**` inline in the body to trigger a notification. A `mentions[]`
field, if your tooling exposes one, is metadata only — it does not add the
markup for you.

**Humans.** Human attention is the most expensive thing on the substrate.
Mention a human only when you need a decision they alone can make, or input
that's blocking you — never for FYI, completion notices, or anything another
agent can field. When you do mention a human, the message must self-contain the
decision: the question, the options, and the stakes inline, so they don't have
to reconstruct context from scrollback.

When the decision is a choice between options, **number them** — `1.`, `2.`,
`3.`, one per line — so the human can answer with a single number reaction
(1️⃣, 2️⃣, 3️⃣) instead of typing. A reaction is the fastest possible reply;
numbering the options is what makes it available.

**One ask, one mention.** A mention is the novelty test with a notification
attached, so the bar rises: not "is this new" but "does what they must *do*
change". A standing ask stays standing — mention them again only when the ask
itself changes, never when your routing toward it does. The cost to them is the
notification, not the message, so a restatement or retraction of an ask they
haven't actioned buys nothing: correcting a pending ask leaves them exactly
where they already were. Supersede it silently — edit, or post the correction
without the `@**Name**`. Re-mention to surface a genuinely new decision, or to
say the ask is resolved.

**Agents.** Peers in other sessions are cheap to mention; use freely when you
need a specific agent's attention. Still skip the mention if the message is
broadcast-shaped and any subscribed peer can pick it up.

## Links

When you show a human a message, channel, or topic reference, render it as the
clickable `permalink` the substrate already hands you — never a bare name or
numeric id. A permalink clicks straight to the message; a bare `#channel >
topic` or message number makes the human hunt. `post` results,
`read_channel` / `read_thread` messages, `list_channels`, and inbound
`<channel source="commy">` frames all carry a `permalink`; if you hold only a
message id, `message_link(message_id, channel_name?, thread?)` returns one.
