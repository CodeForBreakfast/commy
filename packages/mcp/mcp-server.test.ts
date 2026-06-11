import { expect, test } from 'bun:test'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'

import { buildMcpServer } from './mcp-server.ts'

const pairAndConnect = async (): Promise<{
  readonly client: Client
  readonly close: () => Promise<void>
}> => {
  const server = buildMcpServer()
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
  const client = new Client({ name: 'commy-test-client', version: '0.0.0' }, { capabilities: {} })
  await Promise.all([server.connect(serverTransport), client.connect(clientTransport)])
  return {
    client,
    close: async () => {
      await client.close()
      await server.close()
    },
  }
}

test('initialize response declares experimental claude/channel capability', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const capabilities = client.getServerCapabilities()
    expect(capabilities?.experimental).toBeDefined()
    expect(capabilities?.experimental?.['claude/channel']).toEqual({})
  } finally {
    await close()
  }
})

test('initialize response does not declare claude/channel/permission capability', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const capabilities = client.getServerCapabilities()
    expect(capabilities?.experimental?.['claude/channel/permission']).toBeUndefined()
  } finally {
    await close()
  }
})

test('initialize response declares the logging capability required to emit notifications/message (comms-bb7.1)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const capabilities = client.getServerCapabilities()
    expect(capabilities?.logging).toEqual({})
  } finally {
    await close()
  }
})

test('initialize response declares an empty tools capability for later registration', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const capabilities = client.getServerCapabilities()
    expect(capabilities?.tools).toEqual({})
  } finally {
    await close()
  }
})

test('initialize response carries instructions explaining the session_id contract (ass-2dhb)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).toBeDefined()
    expect(instructions).toMatch(/session_id/)
    expect(instructions).toMatch(/post|react|unreact|current_identity/)
  } finally {
    await close()
  }
})

test('initialize instructions name commy as the canonical substrate over its overlapping peers (comms-msx)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).toMatch(/canonical/i)
    expect(instructions).toMatch(/claude-peers/)
    expect(instructions).toMatch(/agent-mail/)
    expect(instructions).toMatch(/Discord/)
  } finally {
    await close()
  }
})

test('initialize instructions describe subscription discipline — project channel plus #general, leak-tolerance (comms-msx, comms-tg6)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).toMatch(/#general/)
    expect(instructions).toMatch(/COMMY_SUBSCRIBE/)
    expect(instructions).toMatch(/don't reply|do not reply/i)
  } finally {
    await close()
  }
})

test('initialize instructions nudge agents to refer to peers by name, not number (comms-dtcm)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).toMatch(/by name/i)
    expect(instructions).toMatch(/sender_name/)
    expect(instructions).toMatch(/bead ids|message and bead/i)
  } finally {
    await close()
  }
})

test('initialize instructions name the project-slug rule and point at list_channels for discovery (comms-tg6)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).toMatch(/#<project-slug>|#<project>/)
    expect(instructions).toMatch(/COMMY_PROJECT/)
    expect(instructions).toMatch(/remote|origin/i)
    expect(instructions).toMatch(/git root|basename/i)
    expect(instructions).toMatch(/`list_channels`/)
    expect(instructions).toMatch(/UnknownChannel/)
  } finally {
    await close()
  }
})

test('initialize instructions cover topic discipline — when to open new vs reply, naming hygiene (comms-tg6)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).toMatch(/topic/i)
    expect(instructions).toMatch(/new topic|open.*topic/i)
    expect(instructions).toMatch(/reply|continue/i)
  } finally {
    await close()
  }
})

test('initialize instructions do not use the retired "home channel" metaphor (comms-tg6)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).not.toMatch(/home channel/i)
  } finally {
    await close()
  }
})

test('initialize instructions cover the tool cheat sheet (comms-msx, comms-tg6)', async () => {
  const { client, close } = await pairAndConnect()
  try {
    const instructions = client.getInstructions()
    expect(instructions).toMatch(/`post`/)
    expect(instructions).toMatch(/`subscribe`/)
    expect(instructions).toMatch(/`unsubscribe`/)
    expect(instructions).toMatch(/`react`/)
    expect(instructions).toMatch(/`read_channel`/)
    expect(instructions).toMatch(/`read_thread`/)
    expect(instructions).toMatch(/`resolve`/)
    expect(instructions).toMatch(/`list_channels`/)
  } finally {
    await close()
  }
})
