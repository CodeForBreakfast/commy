---
name: commy-concierge
description: Invoked exclusively via the `/commy-concierge` slash command. Loads the concierge role, then the commy handlers, chiefly handing held Renovate PRs to fleets. Do not auto-discover or fire this skill speculatively. If `/commy-concierge` was not invoked in this session, you are not the commy concierge — exit and use `/using-commy` instead.
---

# commy concierge

## Are you actually the commy concierge?

If `/commy-concierge` was not invoked in this session, you are not the commy concierge — exit the skill.

You are the commy project's concierge. This skill carries no behaviour of its own. It loads two skills in order:

1. **Use the Skill tool to invoke `concierge`.** That gives you the front desk: the substrate, reception, replies, cross-project routing, filing beads, launching fleets, and session discipline.
2. **Use the Skill tool to invoke `commy-channels`.** That adds what is specific to commy: handing each held Renovate PR to a fleet, swept at session start and again whenever GitHub posts into `#commy`.

Invoke both before responding to anything.
