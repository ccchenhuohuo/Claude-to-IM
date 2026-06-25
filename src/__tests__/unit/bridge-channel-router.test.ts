/**
 * Unit tests for bridge channel-router.
 *
 * Tests the routing logic with a mock BridgeStore, verifying:
 * - resolve() creates new binding when none exists
 * - resolve() returns existing binding when session exists
 * - resolve() recreates binding when session was deleted
 * - createBinding() uses default settings
 * - bindToSession() validates session existence
 * - listBindings() delegates to store
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { initBridgeContext } from '../../lib/bridge/context';
import * as router from '../../lib/bridge/channel-router';
import type { BridgeStore, LLMProvider, PermissionGateway, LifecycleHooks, BridgeSession } from '../../lib/bridge/host';
import type { BridgeOwner, ChannelBinding } from '../../lib/bridge/types';

// ── Mock Store ──────────────────────────────────────────────

function createMockStore(): BridgeStore & {
  bindings: Map<string, ChannelBinding>;
  sessions: Map<string, BridgeSession>;
  owners: Map<string, BridgeOwner>;
} {
  const bindings = new Map<string, ChannelBinding>();
  const sessions = new Map<string, BridgeSession>();
  const owners = new Map<string, BridgeOwner>();
  let nextId = 1;

  return {
    bindings,
    sessions,
    owners,
    getSetting(key: string) {
      if (key === 'bridge_default_work_dir') return '/tmp/test';
      if (key === 'bridge_default_model') return 'claude-3';
      if (key === 'bridge_default_provider_id') return '';
      return null;
    },
    getChannelBinding(channelType: string, chatId: string) {
      return bindings.get(`${channelType}:${chatId}`) ?? null;
    },
    upsertChannelBinding(data) {
      const key = `${data.channelType}:${data.chatId}`;
      const existing = bindings.get(key);
      const binding: ChannelBinding = {
        id: existing?.id ?? `binding-${nextId++}`,
        channelType: data.channelType,
        chatId: data.chatId,
        ownerKey: data.ownerKey ?? existing?.ownerKey,
        codepilotSessionId: data.codepilotSessionId,
        sdkSessionId: data.sdkSessionId ?? existing?.sdkSessionId ?? '',
        workingDirectory: data.workingDirectory,
        model: data.model,
        mode: (data.mode as ChannelBinding['mode']) ?? existing?.mode ?? 'code',
        generation: data.generation ?? existing?.generation,
        active: true,
        createdAt: existing?.createdAt ?? new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      bindings.set(key, binding);
      return binding;
    },
    updateChannelBinding(id: string, updates: Partial<ChannelBinding>) {
      for (const [key, b] of bindings) {
        if (b.id === id) {
          bindings.set(key, { ...b, ...updates });
          break;
        }
      }
    },
    listChannelBindings(channelType?: string) {
      const all = Array.from(bindings.values());
      return channelType ? all.filter(b => b.channelType === channelType) : all;
    },
    getSession(id: string) {
      return sessions.get(id) ?? null;
    },
    createSession(name: string, model: string, _systemPrompt?: string, cwd?: string) {
      const session: BridgeSession = { id: `session-${nextId++}`, title: name, working_directory: cwd || '', model };
      sessions.set(session.id, session);
      return session;
    },
    updateSessionProviderId() {},
    addMessage() {},
    getMessages() { return { messages: [] }; },
    acquireSessionLock() { return true; },
    renewSessionLock() {},
    releaseSessionLock() {},
    setSessionRuntimeStatus() {},
    updateSdkSessionId() {},
    updateSessionModel() {},
    syncSdkTasks() {},
    getProvider() { return undefined; },
    getDefaultProviderId() { return null; },
    insertAuditLog() {},
    checkDedup() { return false; },
    insertDedup() {},
    cleanupExpiredDedup() {},
    insertOutboundRef() {},
    insertPermissionLink() {},
    getPermissionLink() { return null; },
    markPermissionLinkResolved() { return false; },
    listPendingPermissionLinksByChat() { return []; },
    getChannelOffset() { return '0'; },
    setChannelOffset() {},
    getOrCreateOwner(address, chatType = 'unknown') {
      const ownerKey = `feishu:feishu:${chatType}:${address.chatId}`;
      const existing = owners.get(ownerKey);
      if (existing) return existing;
      const owner: BridgeOwner = {
        ownerKey,
        channelType: 'feishu',
        chatId: address.chatId,
        chatType,
        displayName: address.displayName,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      owners.set(ownerKey, owner);
      return owner;
    },
    getOwnerWorkspace(ownerKey: string) {
      return `/tmp/lark/${ownerKey.replace(/[^a-zA-Z0-9_-]+/g, '-')}`;
    },
    listSessionsByOwner(ownerKey: string) {
      return Array.from(sessions.values()).filter((session) => session.ownerKey === ownerKey);
    },
    createSessionForOwner(ownerKey, input) {
      const session: BridgeSession = {
        id: `session-${nextId++}`,
        ownerKey,
        title: input.title,
        titleStatus: input.titleStatus,
        generation: 1,
        working_directory: `/tmp/lark/${ownerKey.replace(/[^a-zA-Z0-9_-]+/g, '-')}`,
        model: input.model,
        mode: input.mode,
      };
      sessions.set(session.id, session);
      return session;
    },
  };
}

const noopLLM: LLMProvider = { streamChat: () => new ReadableStream() };
const noopPerms: PermissionGateway = { resolvePendingPermission: () => false };
const noopLifecycle: LifecycleHooks = {};

function setupContext(store: BridgeStore) {
  // Force re-initialization by clearing the global
  delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  initBridgeContext({
    store,
    llm: noopLLM,
    permissions: noopPerms,
    lifecycle: noopLifecycle,
  });
}

// ── Tests ───────────────────────────────────────────────────

describe('channel-router', () => {
  let store: ReturnType<typeof createMockStore>;

  beforeEach(() => {
    store = createMockStore();
    setupContext(store);
  });

  it('resolve() creates new binding when none exists', () => {
    const binding = router.resolve({
      channelType: 'telegram',
      chatId: '123',
      displayName: 'Test User',
    });

    assert.ok(binding.id);
    assert.equal(binding.channelType, 'telegram');
    assert.equal(binding.chatId, '123');
    assert.equal(binding.workingDirectory, '/tmp/test');
    assert.equal(binding.model, 'claude-3');
    assert.equal(store.bindings.size, 1);
    assert.equal(store.sessions.size, 1);
  });

  it('resolve() returns existing binding when session exists', () => {
    // Create initial binding
    const first = router.resolve({ channelType: 'telegram', chatId: '123' });
    const second = router.resolve({ channelType: 'telegram', chatId: '123' });

    assert.equal(first.id, second.id);
    assert.equal(store.bindings.size, 1);
  });

  it('resolve() recreates binding when session was deleted', () => {
    const first = router.resolve({ channelType: 'telegram', chatId: '123' });
    // Delete the session
    store.sessions.delete(first.codepilotSessionId);

    const second = router.resolve({ channelType: 'telegram', chatId: '123' });
    assert.notEqual(first.codepilotSessionId, second.codepilotSessionId);
  });

  it('createBinding() uses custom working directory', () => {
    const binding = router.createBinding(
      { channelType: 'telegram', chatId: '456' },
      '/custom/path',
    );
    assert.equal(binding.workingDirectory, '/custom/path');
  });

  it('bindToSession() returns null for non-existent session', () => {
    const result = router.bindToSession(
      { channelType: 'telegram', chatId: '789' },
      'non-existent',
    );
    assert.equal(result, null);
  });

  it('bindToSession() binds to existing session', () => {
    const session = store.createSession('Test', 'claude-3', undefined, '/test');
    const binding = router.bindToSession(
      { channelType: 'telegram', chatId: '789' },
      session.id,
    );
    assert.ok(binding);
    assert.equal(binding!.codepilotSessionId, session.id);
  });

  it('bindToSession() replaces the binding sdkSessionId with the target session sdk_session_id', () => {
    const sessionA = store.createSession('A', 'claude-3', undefined, '/a');
    sessionA.sdk_session_id = 'sdk-A';
    const sessionB = store.createSession('B', 'claude-3', undefined, '/b');
    sessionB.sdk_session_id = 'sdk-B';
    store.bindings.set('telegram:789', {
      id: 'binding-existing',
      channelType: 'telegram',
      chatId: '789',
      codepilotSessionId: sessionA.id,
      sdkSessionId: 'sdk-A',
      workingDirectory: '/a',
      model: 'claude-3',
      mode: 'code',
      active: true,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });

    const binding = router.bindToSession(
      { channelType: 'telegram', chatId: '789' },
      sessionB.id,
    );

    assert.ok(binding);
    assert.equal(binding!.codepilotSessionId, sessionB.id);
    assert.equal(binding!.sdkSessionId, 'sdk-B');
  });

  it('listBindings() filters by channel type', () => {
    router.createBinding({ channelType: 'telegram', chatId: '1' });
    router.createBinding({ channelType: 'discord', chatId: '2' });
    router.createBinding({ channelType: 'telegram', chatId: '3' });

    const telegramBindings = router.listBindings('telegram');
    assert.equal(telegramBindings.length, 2);

    const allBindings = router.listBindings();
    assert.equal(allBindings.length, 3);
  });

  it('updateBinding() updates binding properties', () => {
    const binding = router.createBinding({ channelType: 'telegram', chatId: '1' });
    router.updateBinding(binding.id, { mode: 'plan', generation: 2 });

    const updated = store.bindings.get('telegram:1');
    assert.equal(updated?.mode, 'plan');
    assert.equal(updated?.generation, 2);
  });

  it('createBinding() uses owner-scoped workspace for Feishu chats', () => {
    const binding = router.createBinding(
      { channelType: 'feishu', chatId: 'oc_123', displayName: 'Group' },
      undefined,
      { chatType: 'group', title: '新话题' },
    );

    assert.equal(binding.ownerKey, 'feishu:feishu:group:oc_123');
    assert.equal(binding.workingDirectory, '/tmp/lark/feishu-feishu-group-oc_123');
    const session = store.sessions.get(binding.codepilotSessionId);
    assert.equal(session?.ownerKey, binding.ownerKey);
    assert.equal(session?.title, '新话题');
  });

  it('bindToSession() rejects sessions owned by a different Feishu chat', () => {
    const owner = store.getOrCreateOwner!(
      { channelType: 'feishu', chatId: 'oc_1' },
      'group',
    );
    const session = store.createSessionForOwner!(owner.ownerKey, {
      title: 'chat 1',
      model: 'claude-3',
    });

    const result = router.bindToSession(
      { channelType: 'feishu', chatId: 'oc_2' },
      session.id,
      { chatType: 'group' },
    );

    assert.equal(result, null);
  });

  it('bindToSession() rejects legacy unowned sessions for Feishu chats', () => {
    const legacy = store.createSession('legacy', 'claude-3', undefined, '/tmp/legacy');

    const result = router.bindToSession(
      { channelType: 'feishu', chatId: 'oc_1' },
      legacy.id,
      { chatType: 'group' },
    );

    assert.equal(result, null);
  });

  it('resolve() replaces legacy Feishu binding with owner-scoped session and workspace', () => {
    const legacy = store.createSession('legacy', 'claude-3', undefined, '/tmp/legacy');
    store.bindings.set('feishu:oc_legacy', {
      id: 'binding-legacy',
      channelType: 'feishu',
      chatId: 'oc_legacy',
      codepilotSessionId: legacy.id,
      sdkSessionId: '',
      workingDirectory: '/tmp/legacy',
      model: 'claude-3',
      mode: 'code',
      active: true,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });

    const binding = router.resolve(
      { channelType: 'feishu', chatId: 'oc_legacy' },
      { chatType: 'group' },
    );

    assert.notEqual(binding.codepilotSessionId, legacy.id);
    assert.equal(binding.ownerKey, 'feishu:feishu:group:oc_legacy');
    assert.equal(binding.workingDirectory, '/tmp/lark/feishu-feishu-group-oc_legacy');
    const session = store.sessions.get(binding.codepilotSessionId);
    assert.equal(session?.ownerKey, binding.ownerKey);
    assert.equal(session?.working_directory, binding.workingDirectory);
  });
});
