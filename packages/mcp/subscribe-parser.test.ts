import { expect, test } from 'bun:test'
import { decodeChannelNameSync, decodeThreadNameSync } from '@commy/core/ports'
import { Effect, Option } from 'effect'
import type { SubscribeIntent } from './subscribe-parser.ts'
import { intentToTarget, parseSubscribeTarget, SubscribeTokenError } from './subscribe-parser.ts'

const rejection = (token: string): SubscribeTokenError =>
  Effect.runSync(Effect.flip(parseSubscribeTarget(token)))

// Every token below names a narrow, so `None` here is a test failure rather
// than an expected outcome — the one token that yields `None` has its own test.
const intentOf = (token: string): SubscribeIntent =>
  Option.getOrThrowWith(
    Effect.runSync(parseSubscribeTarget(token)),
    () => new Error(`expected ${token} to name a narrow`),
  )

test('parseSubscribeTarget accepts channel:<name>', () => {
  const intent = intentOf('channel:home')
  expect(intent).toEqual({ kind: 'channel', channelName: decodeChannelNameSync('home') })
})

test('parseSubscribeTarget accepts channel:<name> with hyphens', () => {
  const intent = intentOf('channel:llm-feed')
  expect(intent).toEqual({ kind: 'channel', channelName: decodeChannelNameSync('llm-feed') })
})

test('parseSubscribeTarget accepts thread:<channel>/<thread>', () => {
  const intent = intentOf('thread:home/payments')
  expect(intent).toEqual({
    kind: 'thread',
    channelName: decodeChannelNameSync('home'),
    threadName: decodeThreadNameSync('payments'),
  })
})

// Accepted-and-ignored, not rejected: the keyword is retired, but a config
// written before it was retired must still boot. `None` means "valid, and it
// names no narrow" — mentions arrive unconditionally, so there is nothing to
// register on either sink.
test('parseSubscribeTarget accepts the retired mentions token and yields no intent', () => {
  expect(Effect.runSync(parseSubscribeTarget('mentions'))).toEqual(Option.none())
})

test('parseSubscribeTarget rejects empty token', () => {
  expect(rejection('')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects token with no prefix delimiter', () => {
  expect(rejection('channelhome')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects channel: with empty name', () => {
  expect(rejection('channel:')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects thread: missing slash', () => {
  expect(rejection('thread:home')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects thread: with empty thread name', () => {
  expect(rejection('thread:home/')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects thread: with empty channel name', () => {
  expect(rejection('thread:/payments')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects leading whitespace', () => {
  expect(rejection(' channel:home')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects trailing whitespace', () => {
  expect(rejection('channel:home ')).toBeInstanceOf(SubscribeTokenError)
})

test('parseSubscribeTarget rejects unknown prefix', () => {
  expect(rejection('bogus:thing')).toBeInstanceOf(SubscribeTokenError)
})

test('SubscribeTokenError includes the offending token in its message', () => {
  const err = rejection('thread:home')
  expect(err).toBeInstanceOf(SubscribeTokenError)
  expect(err.message).toContain('thread:home')
})

test('parseSubscribeTarget accepts new-topics:<channel>', () => {
  const intent = intentOf('new-topics:home')
  expect(intent).toEqual({
    kind: 'new-topics-in-channel',
    channelName: decodeChannelNameSync('home'),
  })
})

test('parseSubscribeTarget accepts new-topics:<channel> with hyphens', () => {
  const intent = intentOf('new-topics:llm-feed')
  expect(intent).toEqual({
    kind: 'new-topics-in-channel',
    channelName: decodeChannelNameSync('llm-feed'),
  })
})

test('parseSubscribeTarget rejects new-topics: with empty channel name', () => {
  expect(rejection('new-topics:')).toBeInstanceOf(SubscribeTokenError)
})

test('intentToTarget maps new-topics intent to port-shaped NewTopicsInChannelSubscription', () => {
  const intent = intentOf('new-topics:home')
  const target = intentToTarget(intent)
  expect(target).toEqual({
    kind: 'new-topics-in-channel',
    channel: decodeChannelNameSync('home'),
  })
})
