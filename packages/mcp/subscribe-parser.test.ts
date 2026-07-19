import { expect, test } from 'bun:test'
import { decodeChannelNameSync, decodeThreadNameSync } from '@commy/core/ports'
import { Effect } from 'effect'
import type { SubscribeIntent } from './subscribe-parser.ts'
import { intentToTarget, parseSubscribeTarget, SubscribeTokenError } from './subscribe-parser.ts'

const rejection = (token: string): SubscribeTokenError =>
  Effect.runSync(Effect.flip(parseSubscribeTarget(token)))

const intentOf = (token: string): SubscribeIntent => Effect.runSync(parseSubscribeTarget(token))

test('parseSubscribeTarget reads a bare word as a whole channel', () => {
  expect(intentOf('home')).toEqual({ kind: 'channel', channelName: decodeChannelNameSync('home') })
})

test('parseSubscribeTarget reads a bare word with hyphens as a whole channel', () => {
  expect(intentOf('llm-feed')).toEqual({
    kind: 'channel',
    channelName: decodeChannelNameSync('llm-feed'),
  })
})

test('parseSubscribeTarget reads <channel>/<thread> as one topic', () => {
  expect(intentOf('home/payments')).toEqual({
    kind: 'thread',
    channelName: decodeChannelNameSync('home'),
    threadName: decodeThreadNameSync('payments'),
  })
})

// Channel names cannot contain a slash but Zulip topics can, so only the first
// slash divides scope — the rest belongs to the topic.
test('parseSubscribeTarget gives every slash after the first to the thread name', () => {
  expect(intentOf('home/2026/payments')).toEqual({
    kind: 'thread',
    channelName: decodeChannelNameSync('home'),
    threadName: decodeThreadNameSync('2026/payments'),
  })
})

test('parseSubscribeTarget accepts new-topics:<channel>', () => {
  expect(intentOf('new-topics:home')).toEqual({
    kind: 'new-topics-in-channel',
    channelName: decodeChannelNameSync('home'),
  })
})

test('parseSubscribeTarget accepts new-topics:<channel> with hyphens', () => {
  expect(intentOf('new-topics:llm-feed')).toEqual({
    kind: 'new-topics-in-channel',
    channelName: decodeChannelNameSync('llm-feed'),
  })
})

// ─── The retired forms ────────────────────────────────────────────
// Each of these once parsed. Rejecting them is what stops them parsing
// *successfully but wrongly*: channel names are validated for non-emptiness
// only, so an un-rejected `channel:home` would name a channel called
// "channel:home" — the seat boots, registers a narrow nothing posts to, and
// goes quiet with no error anywhere.

test('parseSubscribeTarget rejects the retired channel: prefix', () => {
  const err = rejection('channel:home')
  expect(err).toBeInstanceOf(SubscribeTokenError)
  expect(err.message).toContain('retired')
})

test('parseSubscribeTarget rejects the retired thread: prefix', () => {
  const err = rejection('thread:home/payments')
  expect(err).toBeInstanceOf(SubscribeTokenError)
  expect(err.message).toContain('retired')
})

test('parseSubscribeTarget rejects the retired mentions token', () => {
  const err = rejection('mentions')
  expect(err).toBeInstanceOf(SubscribeTokenError)
  expect(err.message).toContain('retired')
})

// The whole point of rejecting rather than translating is that the operator
// learns what to write instead, so the replacement form is part of the contract.
test('a retired-form rejection names the form that replaced it', () => {
  expect(rejection('channel:home').message).toContain('<channel>')
  expect(rejection('thread:home/payments').message).toContain('<channel>/<thread>')
  expect(rejection('mentions').message).toContain('unconditionally')
})

// ─── Malformed scope ──────────────────────────────────────────────

test('parseSubscribeTarget rejects empty token', () => {
  expect(rejection('')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects <channel>/<thread> with empty thread name', () => {
  expect(rejection('home/')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects <channel>/<thread> with empty channel name', () => {
  expect(rejection('/payments')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects leading whitespace', () => {
  expect(rejection(' home')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects trailing whitespace', () => {
  expect(rejection('home ')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects new-topics: with empty channel name', () => {
  expect(rejection('new-topics:')).toBeInstanceOf(SubscribeTokenError)
})

test('SubscribeTokenError includes the offending token in its message', () => {
  const err = rejection('home/')
  expect(err).toBeInstanceOf(SubscribeTokenError)
  expect(err.message).toContain('home/')
})

test('intentToTarget maps new-topics intent to port-shaped NewTopicsInChannelSubscription', () => {
  expect(intentToTarget(intentOf('new-topics:home'))).toEqual({
    kind: 'new-topics-in-channel',
    channel: decodeChannelNameSync('home'),
  })
})

test('intentToTarget collapses a channel intent to the bare channel name', () => {
  expect(intentToTarget(intentOf('home'))).toEqual(decodeChannelNameSync('home'))
})

test('intentToTarget maps a thread intent to the port-shaped thread record', () => {
  expect(intentToTarget(intentOf('home/payments'))).toEqual({
    channel: decodeChannelNameSync('home'),
    thread: decodeThreadNameSync('payments'),
  })
})
