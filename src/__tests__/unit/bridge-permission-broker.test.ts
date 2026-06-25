/**
 * Unit tests for bridge permission-broker.
 *
 * Tests cover:
 * - handlePermissionCallback: action parsing, chat validation, dedup
 * - Permission resolution via PermissionGateway
 * - Callback data parsing with colons in permId
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { initBridgeContext } from '../../lib/bridge/context';
import { forwardPermissionRequest, handlePermissionCallback } from '../../lib/bridge/permission-broker';
import type { BridgeStore, PermissionGateway, PermissionResolution } from '../../lib/bridge/host';
import { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';
import type { InboundMessage, OutboundMessage, SendResult } from '../../lib/bridge/types';

// ── Mock Store ──────────────────────────────────────────────

function createMockStore() {
  const links = new Map<string, {
    permissionRequestId?: string;
    channelType?: string;
    chatId: string;
    messageId: string;
    sessionId?: string;
    toolName?: string;
    toolInput?: string;
    resolved: boolean;
    suggestions: string;
  }>();

  return {
    links,
    getSetting: () => null,
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
    insertAuditLog: () => {},
    checkDedup: () => false,
    insertDedup: () => {},
    cleanupExpiredDedup: () => {},
    insertOutboundRef: () => {},
    insertPermissionLink: (link: any) => {
      links.set(link.permissionRequestId, { ...link, resolved: false });
    },
    getPermissionLink: (id: string) => {
      return links.get(id) ?? null;
    },
    markPermissionLinkResolved: (id: string) => {
      const link = links.get(id);
      if (!link || link.resolved) return false;
      link.resolved = true;
      return true;
    },
    listPendingPermissionLinksByChat: (chatId: string) => {
      return [...links.values()].filter(l => l.chatId === chatId && !l.resolved);
    },
    getChannelOffset: () => '0',
    setChannelOffset: () => {},
  };
}

// ── Mock Permission Gateway ─────────────────────────────────

function createMockGateway() {
  const resolved: Array<{ id: string; resolution: PermissionResolution }> = [];
  return {
    resolved,
    resolvePendingPermission(id: string, resolution: PermissionResolution) {
      resolved.push({ id, resolution });
      return true;
    },
  };
}

type MockStore = ReturnType<typeof createMockStore>;
type MockGateway = ReturnType<typeof createMockGateway>;

class MockAdapter extends BaseChannelAdapter {
  readonly channelType = 'feishu' as const;

  constructor(private sendResult: SendResult, readonly sent: OutboundMessage[] = []) {
    super();
  }

  async start(): Promise<void> {}
  async stop(): Promise<void> {}
  isRunning(): boolean { return true; }
  async consumeOne(): Promise<InboundMessage | null> { return null; }
  async send(message: OutboundMessage): Promise<SendResult> {
    this.sent.push(message);
    return this.sendResult;
  }
  validateConfig(): string | null { return null; }
  isAuthorized(): boolean { return true; }
}

function setupContext(store: MockStore, gateway: MockGateway) {
  delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  initBridgeContext({
    store: store as unknown as BridgeStore,
    llm: { streamChat: () => new ReadableStream() },
    permissions: gateway,
    lifecycle: {},
  });
}

// ── Tests ───────────────────────────────────────────────────

describe('permission-broker', () => {
  let store: MockStore;
  let gateway: MockGateway;

  beforeEach(() => {
    store = createMockStore();
    gateway = createMockGateway();
    setupContext(store, gateway);
  });

  it('returns false for non-perm callback data', () => {
    assert.equal(handlePermissionCallback('other:data', '123'), false);
  });

  it('returns false when permission link not found', () => {
    assert.equal(handlePermissionCallback('perm:allow:unknown-id', '123'), false);
  });

  it('returns false when chatId does not match', () => {
    store.links.set('perm-1', {
      chatId: '999',
      messageId: 'msg-1',
      resolved: false,
      suggestions: '',
    });

    assert.equal(handlePermissionCallback('perm:allow:perm-1', '123'), false);
  });

  it('returns false when messageId does not match', () => {
    store.links.set('perm-1', {
      chatId: '123',
      messageId: 'msg-1',
      resolved: false,
      suggestions: '',
    });

    assert.equal(handlePermissionCallback('perm:allow:perm-1', '123', 'wrong-msg'), false);
  });

  it('resolves allow action correctly', () => {
    store.links.set('perm-1', {
      chatId: '123',
      messageId: 'msg-1',
      resolved: false,
      suggestions: '',
    });

    const result = handlePermissionCallback('perm:allow:perm-1', '123');
    assert.ok(result);
    assert.equal(gateway.resolved.length, 1);
    assert.equal(gateway.resolved[0].resolution.behavior, 'allow');
  });

  it('resolves deny action correctly', () => {
    store.links.set('perm-2', {
      chatId: '456',
      messageId: 'msg-2',
      resolved: false,
      suggestions: '',
    });

    const result = handlePermissionCallback('perm:deny:perm-2', '456');
    assert.ok(result);
    assert.equal(gateway.resolved[0].resolution.behavior, 'deny');
    assert.equal(gateway.resolved[0].resolution.message, 'Denied via IM bridge');
  });

  it('prevents duplicate resolution', () => {
    store.links.set('perm-3', {
      chatId: '123',
      messageId: 'msg-3',
      resolved: false,
      suggestions: '',
    });

    const first = handlePermissionCallback('perm:allow:perm-3', '123');
    assert.ok(first);

    const second = handlePermissionCallback('perm:allow:perm-3', '123');
    assert.equal(second, false);
    assert.equal(gateway.resolved.length, 1);
  });

  it('handles permId with colons', () => {
    store.links.set('perm:with:colons', {
      chatId: '123',
      messageId: 'msg-4',
      resolved: false,
      suggestions: '',
    });

    const result = handlePermissionCallback('perm:allow:perm:with:colons', '123');
    assert.ok(result);
    assert.equal(gateway.resolved[0].id, 'perm:with:colons');
  });

  it('allow_session passes suggestions as updatedPermissions', () => {
    const suggestions = JSON.stringify([{ type: 'allow', toolName: 'Bash' }]);
    store.links.set('perm-4', {
      chatId: '123',
      messageId: 'msg-5',
      resolved: false,
      suggestions,
    });

    const result = handlePermissionCallback('perm:allow_session:perm-4', '123');
    assert.ok(result);
    assert.equal(gateway.resolved[0].resolution.behavior, 'allow');
    assert.ok((gateway.resolved[0].resolution as any).updatedPermissions);
  });

  it('does not claim a permission link for an invalid action', () => {
    store.links.set('perm-invalid', {
      permissionRequestId: 'perm-invalid',
      channelType: 'feishu',
      chatId: '123',
      messageId: 'msg-invalid',
      resolved: false,
      suggestions: '',
    });

    assert.equal(handlePermissionCallback('perm:bogus:perm-invalid', '123', undefined, 'feishu'), false);
    assert.equal(store.links.get('perm-invalid')?.resolved, false);
    assert.equal(gateway.resolved.length, 0);
  });

  it('rejects callbacks from a different channel with the same chat id', () => {
    store.links.set('perm-channel', {
      permissionRequestId: 'perm-channel',
      channelType: 'telegram',
      chatId: 'same-chat',
      messageId: 'msg-channel',
      resolved: false,
      suggestions: '',
    });

    assert.equal(handlePermissionCallback('perm:allow:perm-channel', 'same-chat', undefined, 'qq'), false);
    assert.equal(store.links.get('perm-channel')?.resolved, false);
    assert.equal(gateway.resolved.length, 0);
  });

  it('records permission metadata even when delivery returns no message id', async () => {
    const adapter = new MockAdapter({ ok: true });

    await forwardPermissionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'chat-meta' },
      'perm-meta-1',
      'Bash',
      { command: 'pwd' },
      'session-meta',
      [{ type: 'addRules', rules: [{ toolName: 'Bash' }], behavior: 'allow', destination: 'session' }],
    );

    const link = store.getPermissionLink('perm-meta-1');
    assert.ok(link);
    assert.equal(link.messageId, '');
    assert.equal(link.channelType, 'feishu');
    assert.equal(link.sessionId, 'session-meta');
    assert.equal(link.toolName, 'Bash');
    assert.match(link.toolInput!, /pwd/);
  });

  it('denies the pending request when prompt delivery fails', async () => {
    const adapter = new MockAdapter({ ok: false, error: 'bad request', httpStatus: 400 } as SendResult);

    await forwardPermissionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'chat-fail' },
      'perm-fail-1',
      'Bash',
      { command: 'rm -rf /tmp/nope' },
      'session-fail',
      [],
    );

    assert.equal(store.getPermissionLink('perm-fail-1'), null);
    assert.equal(gateway.resolved.length, 1);
    assert.equal(gateway.resolved[0].id, 'perm-fail-1');
    assert.equal(gateway.resolved[0].resolution.behavior, 'deny');
    assert.match(gateway.resolved[0].resolution.message!, /delivery failed/i);
  });
});
