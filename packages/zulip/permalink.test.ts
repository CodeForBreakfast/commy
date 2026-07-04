import { expect, test } from 'bun:test'
import {
  ChannelPermalinkSchema,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  MessagePermalinkSchema,
  ThreadPermalinkSchema,
} from '@commy/core/ports'
import { Option } from 'effect'
import {
  buildMessageRef,
  channelPermalink,
  encodeHashComponent,
  messagePermalink,
  permalinkBase,
  topicPermalink,
  withChannelPermalink,
} from './permalink.ts'

// The narrow-URL fragment is parsed client-side by Zulip's web app via
// decodeHashComponent (`.`→`%` then decodeURIComponent), so encodeHashComponent
// is the exact clickability contract. Expected outputs mirror
// web/src/internal_url.ts and zerver/lib/url_encoding.py in the Zulip source.
test('encodeHashComponent leaves unreserved characters untouched', () => {
  expect(encodeHashComponent('plain-topic_1~')).toBe('plain-topic_1~')
})

test('encodeHashComponent encodes a space as .20', () => {
  expect(encodeHashComponent('with space')).toBe('with.20space')
})

test('encodeHashComponent encodes a literal dot as .2E', () => {
  expect(encodeHashComponent('dot.dot')).toBe('dot.2Edot')
})

test('encodeHashComponent encodes parentheses as .28 / .29', () => {
  expect(encodeHashComponent('paren(s)')).toBe('paren.28s.29')
})

test("encodeHashComponent encodes ' and * as .27 / .2A", () => {
  expect(encodeHashComponent("it's*")).toBe('it.27s.2A')
})

test('encodeHashComponent encodes a percent sign as .25', () => {
  expect(encodeHashComponent('100%')).toBe('100.25')
})

test('encodeHashComponent encodes multibyte characters', () => {
  expect(encodeHashComponent('café')).toBe('caf.C3.A9')
})

test('permalinkBase returns the realm URL when no host header override is set', () => {
  expect(permalinkBase({ realmUrl: 'https://zulip.example.com' })).toBe('https://zulip.example.com')
})

test('permalinkBase strips a trailing slash from the realm URL', () => {
  expect(permalinkBase({ realmUrl: 'https://zulip.example.com/' })).toBe(
    'https://zulip.example.com',
  )
})

test('permalinkBase prefers the public host header over an internal realm URL', () => {
  expect(
    permalinkBase({ realmUrl: 'http://zulip-internal:9991', hostHeader: 'zulip.example.com' }),
  ).toBe('https://zulip.example.com')
})

const base = 'https://zulip.example.com'
const channel = { id: decodeChannelIdSync('9'), name: decodeChannelNameSync('general') }

test('channelPermalink builds a channel narrow with the numeric stream id slug', () => {
  expect(channelPermalink(base, channel)).toBe(
    ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/9-general'),
  )
})

test('channelPermalink replaces spaces in the channel name with hyphens', () => {
  const spaced = { id: decodeChannelIdSync('12'), name: decodeChannelNameSync('my stream') }
  expect(channelPermalink(base, spaced)).toBe(
    ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/12-my-stream'),
  )
})

test('topicPermalink appends an encoded topic segment', () => {
  expect(topicPermalink(base, channel, decodeThreadNameSync('my topic'))).toBe(
    ThreadPermalinkSchema.make(
      'https://zulip.example.com/#narrow/channel/9-general/topic/my.20topic',
    ),
  )
})

test('messagePermalink appends a near/<id> segment under the topic', () => {
  expect(
    messagePermalink(base, channel, decodeMessageIdSync('42'), decodeThreadNameSync('my topic')),
  ).toBe(
    MessagePermalinkSchema.make(
      'https://zulip.example.com/#narrow/channel/9-general/topic/my.20topic/near/42',
    ),
  )
})

test('messagePermalink omits the topic segment for a thread-less message', () => {
  expect(messagePermalink(base, channel, decodeMessageIdSync('42'))).toBe(
    MessagePermalinkSchema.make('https://zulip.example.com/#narrow/channel/9-general/near/42'),
  )
})

test('withChannelPermalink decorates a channel ref with its permalink', () => {
  expect(withChannelPermalink(base, channel)).toEqual({
    id: decodeChannelIdSync('9'),
    name: decodeChannelNameSync('general'),
    permalink: 'https://zulip.example.com/#narrow/channel/9-general',
  })
})

test('buildMessageRef decorates message, channel and topic for a threaded message', () => {
  expect(
    buildMessageRef(base, decodeMessageIdSync('42'), channel, decodeThreadNameSync('lobby')),
  ).toEqual({
    id: decodeMessageIdSync('42'),
    channel: {
      id: decodeChannelIdSync('9'),
      name: decodeChannelNameSync('general'),
      permalink: 'https://zulip.example.com/#narrow/channel/9-general',
    },
    thread: Option.some({
      name: decodeThreadNameSync('lobby'),
      permalink: ThreadPermalinkSchema.make(
        'https://zulip.example.com/#narrow/channel/9-general/topic/lobby',
      ),
    }),
    permalink: 'https://zulip.example.com/#narrow/channel/9-general/topic/lobby/near/42',
  })
})

test('buildMessageRef omits the thread for a thread-less message', () => {
  expect(buildMessageRef(base, decodeMessageIdSync('42'), channel)).toEqual({
    id: decodeMessageIdSync('42'),
    channel: {
      id: decodeChannelIdSync('9'),
      name: decodeChannelNameSync('general'),
      permalink: 'https://zulip.example.com/#narrow/channel/9-general',
    },
    thread: Option.none(),
    permalink: 'https://zulip.example.com/#narrow/channel/9-general/near/42',
  })
})
