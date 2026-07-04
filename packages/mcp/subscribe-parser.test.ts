import { expect, test } from 'bun:test'
import { decodeChannelNameSync, decodeThreadNameSync } from '@commy/core/ports'
import { Effect } from 'effect'
import { intentToTarget, parseSubscribeTarget, SubscribeTokenError } from './subscribe-parser.ts'

const rejection = (token: string): SubscribeTokenError =>
  Effect.runSync(Effect.flip(parseSubscribeTarget(token)))

test('parseSubscribeTarget accepts channel:<name>', () => {
  const intent = Effect.runSync(parseSubscribeTarget('channel:home'))
  expect(intent).toEqual({ kind: 'channel', channelName: decodeChannelNameSync('home') })
})

test('parseSubscribeTarget accepts channel:<name> with hyphens', () => {
  const intent = Effect.runSync(parseSubscribeTarget('channel:llm-feed'))
  expect(intent).toEqual({ kind: 'channel', channelName: decodeChannelNameSync('llm-feed') })
})

test('parseSubscribeTarget accepts thread:<channel>/<thread>', () => {
  const intent = Effect.runSync(parseSubscribeTarget('thread:home/payments'))
  expect(intent).toEqual({
    kind: 'thread',
    channelName: decodeChannelNameSync('home'),
    threadName: decodeThreadNameSync('payments'),
  })
})

test('parseSubscribeTarget accepts mentions', () => {
  const intent = Effect.runSync(parseSubscribeTarget('mentions'))
  expect(intent).toEqual({ kind: 'mentions' })
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
  const intent = Effect.runSync(parseSubscribeTarget('new-topics:home'))
  expect(intent).toEqual({
    kind: 'new-topics-in-channel',
    channelName: decodeChannelNameSync('home'),
  })
})

test('parseSubscribeTarget accepts new-topics:<channel> with hyphens', () => {
  const intent = Effect.runSync(parseSubscribeTarget('new-topics:llm-feed'))
  expect(intent).toEqual({
    kind: 'new-topics-in-channel',
    channelName: decodeChannelNameSync('llm-feed'),
  })
})

test('parseSubscribeTarget rejects new-topics: with empty channel name', () => {
  expect(rejection('new-topics:')).toBeInstanceOf(SubscribeTokenError)
})

test('intentToTarget maps new-topics intent to port-shaped NewTopicsInChannelSubscription', () => {
  const intent = Effect.runSync(parseSubscribeTarget('new-topics:home'))
  const target = intentToTarget(intent)
  expect(target).toEqual({
    kind: 'new-topics-in-channel',
    channel: decodeChannelNameSync('home'),
  })
})
