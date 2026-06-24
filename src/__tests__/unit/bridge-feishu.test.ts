import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { initBridgeContext } from '../../lib/bridge/context';
import { FeishuAdapter } from '../../lib/bridge/adapters/feishu-adapter';
import type { BridgeStore } from '../../lib/bridge/host';
import type { InboundMessage } from '../../lib/bridge/types';

function createMockStore(settings: Record<string, string> = {}) {
  const auditLogs: any[] = [];
  return {
    auditLogs,
    getSetting: (key: string) => settings[key] ?? null,
    getChannelBinding: () => null,
    upsertChannelBinding: () => ({} as any),
    updateChannelBinding: () => {},
    listChannelBindings: () => [],
    getSession: () => null,
    createSession: () => ({ id: '1', working_directory: '', model: '' }),
    updateSessionProviderId: () => {},
    addMessage: () => {},
    getMessages: () => ({ messages: [] }),
    acquireSessionLock: () => true,
    renewSessionLock: () => {},
    releaseSessionLock: () => {},
    setSessionRuntimeStatus: () => {},
    updateSdkSessionId: () => {},
    updateSessionModel: () => {},
    syncSdkTasks: () => {},
    getProvider: () => undefined,
    getDefaultProviderId: () => null,
    insertAuditLog: (entry: any) => { auditLogs.push(entry); },
    checkDedup: () => false,
    insertDedup: () => {},
    cleanupExpiredDedup: () => {},
    insertOutboundRef: () => {},
    insertPermissionLink: () => {},
    getPermissionLink: () => null,
    markPermissionLinkResolved: () => false,
    listPendingPermissionLinksByChat: () => [],
    getChannelOffset: () => '0',
    setChannelOffset: () => {},
  };
}

type MockStore = ReturnType<typeof createMockStore>;

function setupContext(settings: Record<string, string | null> = {}): MockStore {
  delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  const baseSettings: Record<string, string> = {
    bridge_feishu_group_trigger_mode: 'mention',
    bridge_feishu_group_policy: 'open',
  };
  for (const [key, value] of Object.entries(settings)) {
    if (value === null) {
      delete baseSettings[key];
    } else {
      baseSettings[key] = value;
    }
  }
  const store = createMockStore(baseSettings);
  initBridgeContext({
    store: store as unknown as BridgeStore,
    llm: { streamChat: () => new ReadableStream() },
    permissions: { resolvePendingPermission: () => false },
    lifecycle: {},
  });
  return store;
}

function event(overrides: any = {}) {
  const text = overrides.text ?? 'hello';
  const messageType = overrides.message_type ?? 'text';
  const content = overrides.content ?? (messageType === 'text'
    ? JSON.stringify({ text })
    : JSON.stringify({ file_key: 'file-key-1', image_key: 'image-key-1' }));

  return {
    sender: {
      sender_id: { open_id: 'user-open-1', user_id: 'user-id-1', union_id: 'user-union-1' },
      sender_type: 'user',
      ...overrides.sender,
    },
    message: {
      message_id: overrides.message_id ?? `om_${Math.random().toString(16).slice(2)}`,
      chat_id: overrides.chat_id ?? 'oc_group_1',
      chat_type: overrides.chat_type ?? 'private',
      chat_mode: overrides.chat_mode,
      group_message_type: overrides.group_message_type,
      message_type: messageType,
      content,
      create_time: String(overrides.create_time ?? Date.now()),
      thread_id: overrides.thread_id,
      root_id: overrides.root_id,
      mentions: overrides.mentions,
    },
  };
}

async function process(adapter: FeishuAdapter, data: any): Promise<InboundMessage | null> {
  await (adapter as any).processIncomingEvent(data);
  return adapter.consumeOne();
}

describe('FeishuAdapter group and mention routing', () => {
  beforeEach(() => {
    delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  });

  it('treats ambiguous private chats as groups when chat lookup fails', async () => {
    setupContext();
    const adapter = new FeishuAdapter();
    (adapter as any).restClient = {
      im: { chat: { get: async () => { throw new Error('temporary lookup failure'); } } },
    };

    const msg = await process(adapter, event({ text: '未 @ 背景' }));

    assert.ok(msg, 'message should be queued as context');
    assert.equal(msg.isGroup, true);
    assert.equal(msg.contextOnly, true);
    assert.equal(msg.triggerReason, 'context_only');
  });

  it('detects company private groups from chat_mode and group_message_type', async () => {
    setupContext();
    const adapter = new FeishuAdapter();

    const msg = await process(adapter, event({
      text: '公司私有群背景',
      chat_type: 'private',
      chat_mode: 'group',
      group_message_type: 'chat',
    }));

    assert.ok(msg);
    assert.equal(msg.isGroup, true);
    assert.equal(msg.contextOnly, true);
  });

  it('defaults groups to mention mode when no new trigger setting is configured', async () => {
    setupContext({ bridge_feishu_group_trigger_mode: null });
    const adapter = new FeishuAdapter();

    const msg = await process(adapter, event({
      text: '未显式配置时不能全群回复',
      chat_mode: 'group',
      group_message_type: 'chat',
    }));

    assert.ok(msg);
    assert.equal(msg.contextOnly, true);
    assert.equal(msg.triggerReason, 'context_only');
  });

  it('allows legacy bridge_feishu_require_mention=false to opt into all-message mode', async () => {
    setupContext({
      bridge_feishu_group_trigger_mode: null,
      bridge_feishu_require_mention: 'false',
    });
    const adapter = new FeishuAdapter();

    const msg = await process(adapter, event({
      text: '显式旧配置允许全群回复',
      chat_mode: 'group',
      group_message_type: 'chat',
    }));

    assert.ok(msg);
    assert.equal(msg.contextOnly, false);
    assert.equal(msg.triggerReason, 'group_all');
  });

  it('triggers on structured mentions matching open_id, user_id, or union_id', async () => {
    for (const [field, value] of [
      ['open_id', 'bot-open'],
      ['user_id', 'bot-user'],
      ['union_id', 'bot-union'],
    ] as const) {
      setupContext();
      const adapter = new FeishuAdapter();
      (adapter as any).botIds = new Set([value]);

      const msg = await process(adapter, event({
        message_id: `om_${field}`,
        text: '@_user_1 请总结',
        mentions: [{ key: '@_user_1', id: { [field]: value }, name: 'VIJIM 战略助理' }],
      }));

      assert.ok(msg, `${field} mention should enqueue`);
      assert.equal(msg.isBotMentioned, true);
      assert.equal(msg.contextOnly, false);
      assert.equal(msg.triggerReason, 'mention');
      assert.equal(msg.text, '请总结');
    }
  });

  it('uses only exact @VIJIM 战略助理 fallback when bot identity is unavailable', async () => {
    setupContext();
    const adapter = new FeishuAdapter();

    const exact = await process(adapter, event({ message_id: 'om_exact', text: '@VIJIM 战略助理 请回复 ok' }));
    assert.ok(exact);
    assert.equal(exact.isBotMentioned, true);
    assert.equal(exact.contextOnly, false);
    assert.equal(exact.triggerReason, 'mention');
    assert.equal(exact.text, '请回复 ok');

    for (const text of ['@', '@VIJIM', '@其他人 请回复', 'VIJIM 战略助理 请回复']) {
      const next = await process(adapter, event({ message_id: `om_${Buffer.from(text).toString('hex')}`, text }));
      assert.ok(next, `${text} should still be recorded as context`);
      assert.equal(next.isBotMentioned, false);
      assert.equal(next.contextOnly, true);
      assert.equal(next.triggerReason, 'context_only');
    }
  });

  it('ignores self sender IDs even when sender_type is not bot', async () => {
    setupContext();
    const adapter = new FeishuAdapter();
    (adapter as any).botIds = new Set(['bot-open']);

    const msg = await process(adapter, event({
      text: '机器人自己的消息',
      sender: { sender_id: { open_id: 'bot-open' }, sender_type: 'user' },
    }));

    assert.equal(msg, null);
  });

  it('does not download media for unmentioned mention-mode group messages', async () => {
    setupContext();
    const adapter = new FeishuAdapter();
    let downloads = 0;
    (adapter as any).downloadResource = async () => {
      downloads += 1;
      return null;
    };

    const msg = await process(adapter, event({ message_type: 'image' }));

    assert.ok(msg);
    assert.equal(msg.contextOnly, true);
    assert.equal(msg.text, '[image]');
    assert.equal(downloads, 0);
  });

  it('allows slash commands in mention mode without a bot mention', async () => {
    setupContext();
    const adapter = new FeishuAdapter();

    const msg = await process(adapter, event({ text: '/status' }));

    assert.ok(msg);
    assert.equal(msg.contextOnly, false);
    assert.equal(msg.isSlashCommand, true);
    assert.equal(msg.triggerReason, 'slash_command');
  });
});
