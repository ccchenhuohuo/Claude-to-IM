/**
 * Bridge Manager — singleton orchestrator for the multi-IM bridge system.
 *
 * Manages adapter lifecycles, routes inbound messages through the
 * conversation engine, and coordinates permission handling.
 *
 * Uses globalThis to survive Next.js HMR in development.
 */

import type { BridgeOwner, BridgeStatus, ChannelAddress, ChannelBinding, InboundMessage, OutboundMessage, SendResult, StreamingPreviewState, ToolCallInfo } from './types.js';
import { createAdapter, getRegisteredTypes } from './channel-adapter.js';
import type { BaseChannelAdapter } from './channel-adapter.js';
// Side-effect import: triggers self-registration of all adapter factories
import './adapters/index.js';
import * as router from './channel-router.js';
import * as engine from './conversation-engine.js';
import * as broker from './permission-broker.js';
import { deliver, deliverRendered } from './delivery-layer.js';
import { markdownToTelegramChunks } from './markdown/telegram.js';
import { markdownToDiscordChunks } from './markdown/discord.js';
import { getBridgeContext } from './context.js';
import { escapeHtml } from './adapters/telegram-utils.js';
import {
  validateWorkingDirectory,
  validateSessionId,
  isDangerousInput,
  sanitizeInput,
  validateMode,
} from './security/validators.js';

const GLOBAL_KEY = '__bridge_manager__';

// ── Streaming preview helpers ──────────────────────────────────

/** Generate a non-zero random 31-bit integer for use as draft_id. */
function generateDraftId(): number {
  return (Math.floor(Math.random() * 0x7FFFFFFE) + 1); // 1 .. 2^31-1
}

interface StreamConfig {
  intervalMs: number;
  minDeltaChars: number;
  maxChars: number;
}

/** Default stream config per channel type. */
const STREAM_DEFAULTS: Record<string, StreamConfig> = {
  feishu: { intervalMs: 1000, minDeltaChars: 30, maxChars: 6000 },
  telegram: { intervalMs: 700, minDeltaChars: 20, maxChars: 3900 },
  discord: { intervalMs: 1500, minDeltaChars: 40, maxChars: 1900 },
};

function getStreamConfig(channelType = 'feishu'): StreamConfig {
  const { store } = getBridgeContext();
  const defaults = STREAM_DEFAULTS[channelType] || STREAM_DEFAULTS.feishu;
  const prefix = `bridge_${channelType}_stream_`;
  const intervalMs = parseInt(store.getSetting(`${prefix}interval_ms`) || '', 10) || defaults.intervalMs;
  const minDeltaChars = parseInt(store.getSetting(`${prefix}min_delta_chars`) || '', 10) || defaults.minDeltaChars;
  const maxChars = parseInt(store.getSetting(`${prefix}max_chars`) || '', 10) || defaults.maxChars;
  return { intervalMs, minDeltaChars, maxChars };
}


interface GroupContextEntry {
  id: string;
  chatId: string;
  threadId: string | null;
  senderId?: string;
  senderName?: string | null;
  text: string;
  createdAt: number;
}

const groupContextBuffer = new Map<string, GroupContextEntry[]>();

function groupContextKey(msg: InboundMessage): string {
  return `${msg.address.channelType}:${msg.address.chatId}:${msg.threadId || 'main'}`;
}

function routingOptionsFor(msg: InboundMessage): router.RoutingOptions {
  return { chatType: messageChatType(msg) };
}

function messageChatType(msg: InboundMessage): BridgeOwner['chatType'] {
  if (msg.isGroup === true) return 'group';
  if (msg.isGroup === false) return 'private';
  return msg.address.chatType || 'unknown';
}

function getGroupContextConfig() {
  const { store } = getBridgeContext();
  const num = (key: string, fallback: number) => {
    const value = parseInt(store.getSetting(key) || '', 10);
    return Number.isFinite(value) && value > 0 ? value : fallback;
  };
  return {
    maxMessages: num('bridge_feishu_group_context_max_messages', 20),
    maxAgeMs: num('bridge_feishu_group_context_max_age_minutes', 60) * 60_000,
    maxChars: num('bridge_feishu_group_context_max_chars', 8000),
    perMessageMaxChars: num('bridge_feishu_group_context_per_message_max_chars', 800),
    maxKeys: num('bridge_feishu_group_context_max_keys', 100),
  };
}

function appendGroupContext(msg: InboundMessage): { rawLength: number; storedLength: number; truncated: boolean } | null {
  const rawText = msg.text.trim();
  if (!rawText || msg.isSlashCommand) return null;
  const cfg = getGroupContextConfig();
  const sanitized = sanitizeInput(rawText, cfg.perMessageMaxChars);
  const text = sanitized.text.trim();
  if (!text) return { rawLength: rawText.length, storedLength: 0, truncated: sanitized.truncated };
  const key = groupContextKey(msg);
  const now = Date.now();
  const createdAt = msg.timestamp || now;
  const clipped = text.length > cfg.perMessageMaxChars
    ? `${text.slice(0, cfg.perMessageMaxChars)}…[truncated]`
    : text;
  const existing = groupContextBuffer.get(key) || [];
  if (existing.some((entry) => entry.id === msg.messageId)) {
    return { rawLength: rawText.length, storedLength: clipped.length, truncated: sanitized.truncated };
  }
  const fresh = existing
    .filter((entry) => now - entry.createdAt <= cfg.maxAgeMs)
    .concat({
      id: msg.messageId,
      chatId: msg.address.chatId,
      threadId: msg.threadId || null,
      senderId: msg.address.userId,
      senderName: msg.senderName,
      text: clipped,
      createdAt,
    })
    .slice(-cfg.maxMessages);
  groupContextBuffer.set(key, fresh);
  pruneGroupContextBuffer(now, cfg.maxAgeMs, cfg.maxKeys);
  return { rawLength: rawText.length, storedLength: clipped.length, truncated: sanitized.truncated };
}

function pruneGroupContextBuffer(now = Date.now(), maxAgeMs = getGroupContextConfig().maxAgeMs, maxKeys = getGroupContextConfig().maxKeys): void {
  for (const [key, entries] of groupContextBuffer) {
    const fresh = entries.filter((entry) => now - entry.createdAt <= maxAgeMs);
    if (fresh.length === 0) {
      groupContextBuffer.delete(key);
    } else if (fresh.length !== entries.length) {
      groupContextBuffer.set(key, fresh);
    }
  }

  while (groupContextBuffer.size > maxKeys) {
    let oldestKey: string | null = null;
    let oldestCreatedAt = Infinity;
    for (const [key, entries] of groupContextBuffer) {
      const first = entries[0];
      if (first && first.createdAt < oldestCreatedAt) {
        oldestCreatedAt = first.createdAt;
        oldestKey = key;
      }
    }
    if (!oldestKey) break;
    groupContextBuffer.delete(oldestKey);
  }
}

function getRecentGroupContext(msg: InboundMessage): GroupContextEntry[] {
  const cfg = getGroupContextConfig();
  const key = groupContextKey(msg);
  const now = Date.now();
  let totalChars = 0;
  const recent = (groupContextBuffer.get(key) || [])
    .filter((entry) => entry.id !== msg.messageId)
    .filter((entry) => entry.createdAt < (msg.timestamp || now))
    .filter((entry) => now - entry.createdAt <= cfg.maxAgeMs)
    .slice(-cfg.maxMessages)
    .reverse()
    .filter((entry) => {
      totalChars += entry.text.length;
      return totalChars <= cfg.maxChars;
    })
    .reverse();
  return recent;
}

function clearGroupContext(msg: InboundMessage): void {
  groupContextBuffer.delete(groupContextKey(msg));
}

/**
 * Check if a message looks like a numeric permission shortcut (1/2/3) for
 * feishu/qq channels WITH at least one pending permission in that chat.
 *
 * This is used by the adapter loop to route these messages to the inline
 * (non-session-locked) path, avoiding deadlock: the session is blocked
 * waiting for the permission to be resolved, so putting "1" behind the
 * session lock would deadlock.
 */
function isNumericPermissionShortcut(channelType: string, rawText: string, chatId: string): boolean {
  if (channelType !== 'feishu' && channelType !== 'qq' && channelType !== 'weixin') return false;
  const normalized = rawText.normalize('NFKC').replace(/[\u200B-\u200D\uFEFF]/g, '').trim();
  if (!/^[123]$/.test(normalized)) return false;
  const { store } = getBridgeContext();
  const pending = store.listPendingPermissionLinksByChat(chatId, channelType);
  return pending.length > 0; // any pending → route to inline path
}

/** Fire-and-forget: send a preview draft. Only degrades on permanent failure. */
function flushPreview(
  adapter: BaseChannelAdapter,
  state: StreamingPreviewState,
  config: StreamConfig,
): void {
  if (state.degraded || !adapter.sendPreview) return;

  const text = state.pendingText.length > config.maxChars
    ? state.pendingText.slice(0, config.maxChars) + '...'
    : state.pendingText;

  state.lastSentText = text;
  state.lastSentAt = Date.now();

  adapter.sendPreview(state.chatId, text, state.draftId).then(result => {
    if (result === 'degrade') state.degraded = true;
    // 'skip' — transient failure, next flush will retry naturally
  }).catch(() => {
    // Network error — transient, don't degrade
  });
}

// ── Channel-aware rendering dispatch ──────────────────────────

/**
 * Render response text and deliver via the appropriate channel format.
 * Telegram: Markdown → HTML chunks via deliverRendered.
 * Other channels: plain text via deliver (no HTML).
 */
async function deliverResponse(
  adapter: BaseChannelAdapter,
  address: ChannelAddress,
  responseText: string,
  sessionId: string,
  replyToMessageId?: string,
  shouldContinue?: () => boolean,
): Promise<SendResult> {
  if (adapter.channelType === 'telegram') {
    const chunks = markdownToTelegramChunks(responseText, 4096);
    if (chunks.length > 0) {
      return deliverRendered(adapter, address, chunks, { sessionId, replyToMessageId, shouldContinue });
    }
    return { ok: true };
  }
  if (adapter.channelType === 'discord') {
    // Discord: native markdown, chunk at 2000 chars with fence repair
    const chunks = markdownToDiscordChunks(responseText, 2000);
    for (let i = 0; i < chunks.length; i++) {
      const result = await deliver(adapter, {
        address,
        text: chunks[i].text,
        parseMode: 'Markdown',
        replyToMessageId,
      }, { sessionId, shouldContinue });
      if (!result.ok) return result;
    }
    return { ok: true };
  }
  if (adapter.channelType === 'feishu') {
    // Feishu: pass markdown through for adapter to format as post/card
    return deliver(adapter, {
      address,
      text: responseText,
      parseMode: 'Markdown',
      replyToMessageId,
    }, { sessionId, shouldContinue });
  }
  // Generic fallback: deliver as plain text (deliver() handles chunking internally)
  return deliver(adapter, {
    address,
    text: responseText,
    parseMode: 'plain',
    replyToMessageId,
  }, { sessionId, shouldContinue });
}

interface AdapterMeta {
  lastMessageAt: string | null;
  lastError: string | null;
}

interface BridgeManagerState {
  adapters: Map<string, BaseChannelAdapter>;
  adapterMeta: Map<string, AdapterMeta>;
  running: boolean;
  startedAt: string | null;
  loopAborts: Map<string, AbortController>;
  activeTasks: Map<string, AbortController>;
  titleTasks: Set<string>;
  /** Per-session processing chains for concurrency control */
  sessionLocks: Map<string, Promise<void>>;
  autoStartChecked: boolean;
}

function getState(): BridgeManagerState {
  const g = globalThis as unknown as Record<string, BridgeManagerState>;
  if (!g[GLOBAL_KEY]) {
    g[GLOBAL_KEY] = {
      adapters: new Map(),
      adapterMeta: new Map(),
      running: false,
      startedAt: null,
      loopAborts: new Map(),
      activeTasks: new Map(),
      titleTasks: new Set(),
      sessionLocks: new Map(),
      autoStartChecked: false,
    };
  }
  // Backfill sessionLocks for states created before this field existed
  if (!g[GLOBAL_KEY].sessionLocks) {
    g[GLOBAL_KEY].sessionLocks = new Map();
  }
  if (!g[GLOBAL_KEY].titleTasks) {
    g[GLOBAL_KEY].titleTasks = new Set();
  }
  return g[GLOBAL_KEY];
}

/**
 * Process a function with per-session serialization.
 * Different sessions run concurrently; same-session requests are serialized.
 */
function processWithSessionLock(sessionId: string, fn: () => Promise<void>): Promise<void> {
  const state = getState();
  const prev = state.sessionLocks.get(sessionId) || Promise.resolve();
  const current = prev.then(fn, fn);
  state.sessionLocks.set(sessionId, current);
  // Cleanup when the chain completes.
  // Suppress rejection on the cleanup chain — callers handle errors on `current` directly.
  current.finally(() => {
    if (state.sessionLocks.get(sessionId) === current) {
      state.sessionLocks.delete(sessionId);
    }
  }).catch(() => {});
  return current;
}

function isCurrentBindingSnapshot(msg: InboundMessage, snapshot: {
  id: string;
  codepilotSessionId: string;
  generation?: number;
}): boolean {
  const { store } = getBridgeContext();
  const current = store.getChannelBinding(msg.address.channelType, msg.address.chatId);
  if (!current) return false;
  return current.id === snapshot.id
    && current.codepilotSessionId === snapshot.codepilotSessionId
    && (current.generation || 0) === (snapshot.generation || 0);
}

function acknowledgeMessage(adapter: BaseChannelAdapter, msg: InboundMessage): void {
  if (msg.updateId != null && adapter.acknowledgeUpdate) {
    adapter.acknowledgeUpdate(msg.updateId);
  }
}

function abortActiveTaskForBinding(binding: ChannelBinding | null | undefined): void {
  if (!binding) return;
  const state = getState();
  const taskAbort = state.activeTasks.get(binding.codepilotSessionId);
  if (!taskAbort) return;
  taskAbort.abort();
  state.activeTasks.delete(binding.codepilotSessionId);
}

function abortCurrentTaskForChat(address: ChannelAddress): void {
  const { store } = getBridgeContext();
  abortActiveTaskForBinding(store.getChannelBinding(address.channelType, address.chatId));
}

/**
 * Start the bridge system.
 * Checks feature flags, registers enabled adapters, starts polling loops.
 */
export async function start(): Promise<void> {
  const state = getState();
  if (state.running) return;

  const { store, lifecycle } = getBridgeContext();

  const bridgeEnabled = store.getSetting('remote_bridge_enabled') === 'true';
  if (!bridgeEnabled) {
    console.log('[bridge-manager] Bridge not enabled (remote_bridge_enabled != true)');
    return;
  }

  // Iterate all registered adapter types and create those that are enabled
  for (const channelType of getRegisteredTypes()) {
    const settingKey = `bridge_${channelType}_enabled`;
    if (store.getSetting(settingKey) !== 'true') continue;

    const adapter = createAdapter(channelType);
    if (!adapter) continue;

    const configError = adapter.validateConfig();
    if (!configError) {
      registerAdapter(adapter);
    } else {
      console.warn(`[bridge-manager] ${channelType} adapter not valid:`, configError);
    }
  }

  // Start all registered adapters, track how many succeeded
  let startedCount = 0;
  for (const [type, adapter] of state.adapters) {
    try {
      await adapter.start();
      console.log(`[bridge-manager] Started adapter: ${type}`);
      startedCount++;
    } catch (err) {
      console.error(`[bridge-manager] Failed to start adapter ${type}:`, err);
    }
  }

  // Only mark as running if at least one adapter started successfully
  if (startedCount === 0) {
    console.warn('[bridge-manager] No adapters started successfully, bridge not activated');
    state.adapters.clear();
    state.adapterMeta.clear();
    return;
  }

  // Mark running BEFORE starting consumer loops — runAdapterLoop checks
  // state.running in its while-condition, so it must be true first.
  state.running = true;
  state.startedAt = new Date().toISOString();

  // Notify host that bridge is starting (e.g., suppress competing polling)
  lifecycle.onBridgeStart?.();

  // Now start the consumer loops (state.running is already true)
  for (const [, adapter] of state.adapters) {
    if (adapter.isRunning()) {
      runAdapterLoop(adapter);
    }
  }

  console.log(`[bridge-manager] Bridge started with ${startedCount} adapter(s)`);
}

/**
 * Stop the bridge system gracefully.
 */
export async function stop(): Promise<void> {
  const state = getState();
  if (!state.running) return;

  const { lifecycle } = getBridgeContext();

  state.running = false;

  // Abort all event loops
  for (const [, abort] of state.loopAborts) {
    abort.abort();
  }
  state.loopAborts.clear();

  // Stop all adapters
  for (const [type, adapter] of state.adapters) {
    try {
      await adapter.stop();
      console.log(`[bridge-manager] Stopped adapter: ${type}`);
    } catch (err) {
      console.error(`[bridge-manager] Error stopping adapter ${type}:`, err);
    }
  }

  state.adapters.clear();
  state.adapterMeta.clear();
  state.startedAt = null;
  groupContextBuffer.clear();

  // Notify host that bridge stopped
  lifecycle.onBridgeStop?.();

  console.log('[bridge-manager] Bridge stopped');
}

/**
 * Lazy auto-start: checks bridge_auto_start setting once and starts if enabled.
 * Called from POST /api/bridge with action 'auto-start' (triggered by Electron on startup).
 */
export function tryAutoStart(): void {
  const state = getState();
  if (state.autoStartChecked) return;
  state.autoStartChecked = true;

  if (state.running) return;

  const { store } = getBridgeContext();
  const autoStart = store.getSetting('bridge_auto_start');
  if (autoStart !== 'true') return;

  start().catch(err => {
    console.error('[bridge-manager] Auto-start failed:', err);
  });
}

/**
 * Get the current bridge status.
 */
export function getStatus(): BridgeStatus {
  const state = getState();
  return {
    running: state.running,
    startedAt: state.startedAt,
    adapters: Array.from(state.adapters.entries()).map(([type, adapter]) => {
      const meta = state.adapterMeta.get(type);
      return {
        channelType: adapter.channelType,
        running: adapter.isRunning(),
        connectedAt: state.startedAt,
        lastMessageAt: meta?.lastMessageAt ?? null,
        error: meta?.lastError ?? null,
      };
    }),
  };
}

/**
 * Register a channel adapter.
 */
export function registerAdapter(adapter: BaseChannelAdapter): void {
  const state = getState();
  state.adapters.set(adapter.channelType, adapter);
}

/**
 * Run the event loop for a single adapter.
 * Messages for different sessions are dispatched concurrently;
 * messages for the same session are serialized via session locks.
 */
function runAdapterLoop(adapter: BaseChannelAdapter): void {
  const state = getState();
  const abort = new AbortController();
  state.loopAborts.set(adapter.channelType, abort);

  (async () => {
    while (state.running && adapter.isRunning()) {
      try {
        const msg = await adapter.consumeOne();
        if (!msg) continue; // Adapter stopped

        await dispatchInboundMessage(adapter, msg);
      } catch (err) {
        if (abort.signal.aborted) break;
        const errMsg = err instanceof Error ? err.message : String(err);
        console.error(`[bridge-manager] Error in ${adapter.channelType} loop:`, err);
        // Track last error per adapter
        const meta = state.adapterMeta.get(adapter.channelType) || { lastMessageAt: null, lastError: null };
        meta.lastError = errMsg;
        state.adapterMeta.set(adapter.channelType, meta);
        // Brief delay to prevent tight error loops
        await new Promise(r => setTimeout(r, 1000));
      }
    }
  })().catch(err => {
    if (!abort.signal.aborted) {
      const errMsg = err instanceof Error ? err.message : String(err);
      console.error(`[bridge-manager] ${adapter.channelType} loop crashed:`, err);
      const meta = state.adapterMeta.get(adapter.channelType) || { lastMessageAt: null, lastError: null };
      meta.lastError = errMsg;
      state.adapterMeta.set(adapter.channelType, meta);
    }
  });
}

async function dispatchInboundMessage(adapter: BaseChannelAdapter, msg: InboundMessage): Promise<void> {
  // Callback queries, commands, context-only updates, and numeric permission shortcuts are
  // lightweight — process inline (outside session lock). Context-only updates must stay
  // here so they do not create a channel binding before being recorded as ambient context.
  // Regular messages use per-session locking for concurrency.
  //
  // IMPORTANT: numeric shortcuts (1/2/3) for feishu/qq MUST run outside
  // the session lock. The current session is blocked waiting for the
  // permission to be resolved; if "1" enters the session lock queue it
  // deadlocks (permission waits for "1", "1" waits for lock release).
  if (
    msg.contextOnly ||
    msg.callbackData ||
    msg.text.trim().startsWith('/') ||
    isNumericPermissionShortcut(adapter.channelType, msg.text.trim(), msg.address.chatId)
  ) {
    await handleMessage(adapter, msg);
    return;
  }

  const binding = router.resolve(msg.address, routingOptionsFor(msg));
  const snapshot = {
    id: binding.id,
    codepilotSessionId: binding.codepilotSessionId,
    generation: binding.generation,
  };
  // Fire-and-forget into session lock — loop continues to accept
  // messages for other sessions immediately.
  processWithSessionLock(binding.codepilotSessionId, async () => {
    if (!isCurrentBindingSnapshot(msg, snapshot)) {
      const { store } = getBridgeContext();
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'inbound',
        messageId: msg.messageId,
        summary: '[STALE_BINDING] Dropped queued message after session changed',
      });
      acknowledgeMessage(adapter, msg);
      return;
    }
    await handleMessage(adapter, msg);
  }).catch(err => {
    console.error(`[bridge-manager] Session ${binding.codepilotSessionId.slice(0, 8)} error:`, err);
  });
}

/**
 * Handle a single inbound message.
 */
async function handleMessage(
  adapter: BaseChannelAdapter,
  msg: InboundMessage,
): Promise<void> {
  const { store } = getBridgeContext();

  // Update lastMessageAt for this adapter
  const adapterState = getState();
  const meta = adapterState.adapterMeta.get(adapter.channelType) || { lastMessageAt: null, lastError: null };
  meta.lastMessageAt = new Date().toISOString();
  adapterState.adapterMeta.set(adapter.channelType, meta);

  // Acknowledge the update offset after processing completes (or fails).
  // This ensures the adapter only advances its committed offset once the
  // message has been fully handled, preventing message loss on crash.
  const ack = () => {
    if (msg.updateId != null && adapter.acknowledgeUpdate) {
      adapter.acknowledgeUpdate(msg.updateId);
    }
  };

  // Handle callback queries (permission buttons)
  if (msg.callbackData) {
    if (msg.callbackData.startsWith('perm:') && await rejectUnauthorizedFeishuCommand(adapter, msg)) {
      ack();
      return;
    }
    const handled = broker.handlePermissionCallback(
      msg.callbackData,
      msg.address.chatId,
      msg.callbackMessageId,
      adapter.channelType,
    );
    if (handled) {
      // Send confirmation
      const confirmMsg: OutboundMessage = {
        address: msg.address,
        text: 'Permission response recorded.',
        parseMode: 'plain',
      };
      await deliver(adapter, confirmMsg);
    }
    ack();
    return;
  }

  const rawText = msg.text.trim();
  const hasAttachments = msg.attachments && msg.attachments.length > 0;

  if (msg.contextOnly) {
    const recorded = appendGroupContext(msg);
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: 'inbound',
      messageId: msg.messageId,
      summary: recorded?.truncated
        ? `[CONTEXT_ONLY][TRUNCATED] Recorded group context (${recorded.rawLength} -> ${recorded.storedLength} chars)`
        : `[CONTEXT_ONLY] Recorded group context (${recorded?.storedLength ?? 0} chars)`,
    });
    ack();
    return;
  }

  // Handle attachment-only download failures — surface error to user instead of silently dropping
  if (!rawText && !hasAttachments) {
    const rawData = msg.raw as {
      imageDownloadFailed?: boolean;
      attachmentDownloadFailed?: boolean;
      failedCount?: number;
      failedLabel?: string;
      userVisibleError?: string;
    } | undefined;
    if (rawData?.userVisibleError) {
      await deliver(adapter, {
        address: msg.address,
        text: rawData.userVisibleError,
        parseMode: 'plain',
        replyToMessageId: msg.messageId,
      });
    } else if (rawData?.imageDownloadFailed || rawData?.attachmentDownloadFailed) {
      const failureLabel = rawData.failedLabel || (rawData.imageDownloadFailed ? 'image(s)' : 'attachment(s)');
      await deliver(adapter, {
        address: msg.address,
        text: `Failed to download ${rawData.failedCount ?? 1} ${failureLabel}. Please try sending again.`,
        parseMode: 'plain',
        replyToMessageId: msg.messageId,
      });
    }
    ack();
    return;
  }

  // ── Numeric shortcut for permission replies (feishu/qq/weixin only) ──
  // On mobile, typing `/perm allow <uuid>` is painful.
  // If the user sends "1", "2", or "3" and there is exactly one pending
  // permission for this chat, map it: 1→allow, 2→allow_session, 3→deny.
  //
  // Input normalization: mobile keyboards / IM clients may send fullwidth
  // digits (１２３), digits with zero-width joiners, or other Unicode
  // variants. NFKC normalization folds them all to ASCII 1/2/3.
  if (
    adapter.channelType === 'feishu'
    || adapter.channelType === 'qq'
    || adapter.channelType === 'weixin'
  ) {
    // eslint-disable-next-line no-control-regex
    const normalized = rawText.normalize('NFKC').replace(/[\u200B-\u200D\uFEFF]/g, '').trim();
    if (/^[123]$/.test(normalized)) {
      if (await rejectUnauthorizedFeishuCommand(adapter, msg)) {
        ack();
        return;
      }
      const pendingLinks = store.listPendingPermissionLinksByChat(msg.address.chatId, adapter.channelType);
      if (pendingLinks.length === 1) {
        const actionMap: Record<string, string> = { '1': 'allow', '2': 'allow_session', '3': 'deny' };
        const action = actionMap[normalized];
        const permId = pendingLinks[0].permissionRequestId;
        const callbackData = `perm:${action}:${permId}`;
        const handled = broker.handlePermissionCallback(callbackData, msg.address.chatId, undefined, adapter.channelType);
        const label = normalized === '1' ? 'Allow' : normalized === '2' ? 'Allow Session' : 'Deny';
        if (handled) {
          await deliver(adapter, {
            address: msg.address,
            text: `${label}: recorded.`,
            parseMode: 'plain',
            replyToMessageId: msg.messageId,
          });
        } else {
          await deliver(adapter, {
            address: msg.address,
            text: `Permission not found or already resolved.`,
            parseMode: 'plain',
            replyToMessageId: msg.messageId,
          });
        }
        ack();
        return;
      }
      if (pendingLinks.length > 1) {
        // Multiple pending permissions — numeric shortcut is ambiguous.
        await deliver(adapter, {
          address: msg.address,
          text: `Multiple pending permissions (${pendingLinks.length}). Please use the full command:\n/perm allow|allow_session|deny <id>`,
          parseMode: 'plain',
          replyToMessageId: msg.messageId,
        });
        ack();
        return;
      }
      // pendingLinks.length === 0: no pending permissions, fall through as normal message
    } else if (rawText !== normalized && /^[123]$/.test(rawText) === false) {
      // Log when normalization changed the text — helps diagnose encoding issues
      const codePoints = [...rawText].map(c => 'U+' + c.codePointAt(0)!.toString(16).toUpperCase().padStart(4, '0'));
      console.log(`[bridge-manager] Shortcut candidate raw codepoints: ${codePoints.join(' ')} → normalized: "${normalized}"`);
    }
  }

  // Check for IM commands (before sanitization — commands are validated individually)
  if (rawText.startsWith('/')) {
    await handleCommand(adapter, msg, rawText);
    ack();
    return;
  }

  // Sanitize general message text before routing to conversation engine
  const { text, truncated } = sanitizeInput(rawText);
  if (truncated) {
    console.warn(`[bridge-manager] Input truncated from ${rawText.length} to ${text.length} chars for chat ${msg.address.chatId}`);
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: 'inbound',
      messageId: msg.messageId,
      summary: `[TRUNCATED] Input truncated from ${rawText.length} chars`,
    });
  }

  if (!text && !hasAttachments) { ack(); return; }

  // Regular message — route to conversation engine
  const binding = router.resolve(msg.address, routingOptionsFor(msg));
  const processingSnapshot = {
    id: binding.id,
    codepilotSessionId: binding.codepilotSessionId,
    generation: binding.generation,
  };
  const taskAbort = new AbortController();
  const isProcessingBindingCurrent = () => !taskAbort.signal.aborted && isCurrentBindingSnapshot(msg, processingSnapshot);
  scheduleSessionTitleGeneration(binding, text);
  appendOwnerChatLog(binding, msg, 'user', 'inbound', text);

  // Notify adapter that message processing is starting (e.g., typing indicator)
  adapter.onMessageStart?.(msg.address.chatId);

  // Create an AbortController so /stop can cancel this task externally
  const state = getState();
  state.activeTasks.set(binding.codepilotSessionId, taskAbort);

  // ── Streaming preview setup ──────────────────────────────────
  let previewState: StreamingPreviewState | null = null;
  const caps = adapter.getPreviewCapabilities?.(msg.address.chatId) ?? null;
  if (caps?.supported) {
    previewState = {
      draftId: generateDraftId(),
      chatId: msg.address.chatId,
      lastSentText: '',
      lastSentAt: 0,
      degraded: false,
      throttleTimer: null,
      pendingText: '',
    };
  }

  const streamCfg = previewState ? getStreamConfig(adapter.channelType) : null;

  // Build the preview onPartialText callback (or undefined if preview not supported)
  const previewOnPartialText = (previewState && streamCfg) ? (fullText: string) => {
    if (!isProcessingBindingCurrent()) return;
    const ps = previewState!;
    const cfg = streamCfg!;
    if (ps.degraded) return;

    // Truncate to maxChars + ellipsis
    ps.pendingText = fullText.length > cfg.maxChars
      ? fullText.slice(0, cfg.maxChars) + '...'
      : fullText;

    const delta = ps.pendingText.length - ps.lastSentText.length;
    const elapsed = Date.now() - ps.lastSentAt;

    if (delta < cfg.minDeltaChars && ps.lastSentAt > 0) {
      // Not enough new content — schedule trailing-edge timer if not already set
      if (!ps.throttleTimer) {
        ps.throttleTimer = setTimeout(() => {
          ps.throttleTimer = null;
          if (!isProcessingBindingCurrent()) return;
          if (!ps.degraded) flushPreview(adapter, ps, cfg);
        }, cfg.intervalMs);
      }
      return;
    }

    if (elapsed < cfg.intervalMs && ps.lastSentAt > 0) {
      // Too soon — schedule trailing-edge timer to ensure latest text is sent
      if (!ps.throttleTimer) {
        ps.throttleTimer = setTimeout(() => {
          ps.throttleTimer = null;
          if (!isProcessingBindingCurrent()) return;
          if (!ps.degraded) flushPreview(adapter, ps, cfg);
        }, cfg.intervalMs - elapsed);
      }
      return;
    }

    // Clear any pending trailing-edge timer and flush immediately
    if (ps.throttleTimer) {
      clearTimeout(ps.throttleTimer);
      ps.throttleTimer = null;
    }
    flushPreview(adapter, ps, cfg);
  } : undefined;

  // ── Streaming card setup (Feishu CardKit v2) ──────────────────
  // If the adapter supports streaming cards (e.g. Feishu), wire up
  // onStreamText, onToolEvent, and onStreamEnd callbacks.
  // These run in parallel with the existing preview system — Feishu
  // uses cards instead of message edit for streaming.
  const hasStreamingCards = typeof adapter.onStreamText === 'function';
  const toolCallTracker = new Map<string, ToolCallInfo>();
  let staleResponseDropped = false;
  const dropIfProcessingBindingChanged = async (interruptStreamingCard: boolean): Promise<boolean> => {
    if (isProcessingBindingCurrent()) return false;
    if (!staleResponseDropped) {
      staleResponseDropped = true;
      if (interruptStreamingCard && hasStreamingCards && adapter.onStreamEnd) {
        try { await adapter.onStreamEnd(msg.address.chatId, 'interrupted', ''); } catch { /* best effort */ }
      }
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'outbound',
        messageId: msg.messageId,
        summary: '[STALE_BINDING] Dropped response after session changed',
      });
    }
    return true;
  };

  const onStreamCardText = hasStreamingCards ? (fullText: string) => {
    if (!isProcessingBindingCurrent()) return;
    try { adapter.onStreamText!(msg.address.chatId, fullText); } catch { /* non-critical */ }
  } : undefined;

  const onToolEvent = hasStreamingCards ? (toolId: string, toolName: string, status: 'running' | 'complete' | 'error') => {
    if (!isProcessingBindingCurrent()) return;
    if (toolName) {
      toolCallTracker.set(toolId, { id: toolId, name: toolName, status });
    } else {
      // tool_result doesn't carry name — update existing entry's status
      const existing = toolCallTracker.get(toolId);
      if (existing) existing.status = status;
    }
    try {
      adapter.onToolEvent!(msg.address.chatId, Array.from(toolCallTracker.values()));
    } catch { /* non-critical */ }
  } : undefined;

  // Combined partial text callback: streaming preview + streaming cards
  const onPartialText = (previewOnPartialText || onStreamCardText) ? (fullText: string) => {
    if (previewOnPartialText) previewOnPartialText(fullText);
    if (onStreamCardText) onStreamCardText(fullText);
  } : undefined;

  try {
    // Pass permission callback so requests are forwarded to IM immediately
    // during streaming (the stream blocks until permission is resolved).
    // Use text or empty string for image-only messages (prompt is still required by streamClaude)
    const promptText = text || (hasAttachments ? 'Describe this image.' : '');

    const groupContext = msg.triggerReason === 'mention' ? getRecentGroupContext(msg) : undefined;
    const result = await engine.processMessage(binding, promptText, async (perm) => {
      await broker.forwardPermissionRequest(
        adapter,
        msg.address,
        perm.permissionRequestId,
        perm.toolName,
        perm.toolInput,
        binding.codepilotSessionId,
        perm.suggestions,
        msg.messageId,
      );
    }, taskAbort.signal, hasAttachments ? msg.attachments : undefined, onPartialText, onToolEvent, groupContext);

    if (await dropIfProcessingBindingChanged(true)) return;

    // Finalize streaming card if adapter supports it.
    // onStreamEnd awaits any in-flight card creation and returns true if a card
    // was actually finalized (meaning content is already visible to the user).
    let cardFinalized = false;
    if (hasStreamingCards && adapter.onStreamEnd) {
      try {
        if (await dropIfProcessingBindingChanged(true)) return;
        const status = result.hasError ? 'error' : 'completed';
        cardFinalized = await adapter.onStreamEnd(msg.address.chatId, status, result.responseText, {
          shouldContinue: isProcessingBindingCurrent,
        });
        if (await dropIfProcessingBindingChanged(false)) return;
      } catch (err) {
        console.warn('[bridge-manager] Card finalize failed:', err instanceof Error ? err.message : err);
      }
    }

    // Send response text — render via channel-appropriate format.
    // Skip if streaming card was finalized (content already in card).
    if (result.responseText) {
      if (await dropIfProcessingBindingChanged(false)) return;
      appendOwnerChatLog(binding, msg, 'assistant', 'outbound', result.responseText);
      if (!cardFinalized) {
        if (await dropIfProcessingBindingChanged(false)) return;
        await deliverResponse(
          adapter,
          msg.address,
          result.responseText,
          binding.codepilotSessionId,
          msg.messageId,
          isProcessingBindingCurrent,
        );
        if (await dropIfProcessingBindingChanged(false)) return;
      }
    } else if (result.hasError) {
      if (await dropIfProcessingBindingChanged(false)) return;
      const errorResponse: OutboundMessage = {
        address: msg.address,
        text: `<b>Error:</b> ${escapeHtml(result.errorMessage)}`,
        parseMode: 'HTML',
        replyToMessageId: msg.messageId,
      };
      if (await dropIfProcessingBindingChanged(false)) return;
      await deliver(adapter, errorResponse, { shouldContinue: isProcessingBindingCurrent });
      if (await dropIfProcessingBindingChanged(false)) return;
    }

    // Persist the actual SDK session ID for future resume.
    // If the result has an error and no session ID was captured, clear the
    // stale ID so the next message starts fresh instead of retrying a broken resume.
    if (await dropIfProcessingBindingChanged(false)) return;
    try {
      const update = computeSdkSessionUpdate(result.sdkSessionId, result.hasError);
      if (update !== null) {
        if (await dropIfProcessingBindingChanged(false)) return;
        store.updateSdkSessionId(binding.codepilotSessionId, update);
        if (binding.id) {
          if (await dropIfProcessingBindingChanged(false)) return;
          store.updateChannelBinding(binding.id, { sdkSessionId: update });
        }
      }
    } catch { /* best effort */ }
  } finally {
    // Clean up preview state
    if (previewState) {
      if (previewState.throttleTimer) {
        clearTimeout(previewState.throttleTimer);
        previewState.throttleTimer = null;
      }
      adapter.endPreview?.(msg.address.chatId, previewState.draftId);
    }

    // If task was aborted and streaming card is still active, finalize as interrupted
    if (hasStreamingCards && adapter.onStreamEnd && taskAbort.signal.aborted) {
      try {
        await adapter.onStreamEnd(msg.address.chatId, 'interrupted', '');
      } catch { /* best effort */ }
    }

    state.activeTasks.delete(binding.codepilotSessionId);
    // Notify adapter that message processing ended
    adapter.onMessageEnd?.(msg.address.chatId);
    // Commit the offset only after full processing (success or failure)
    ack();
  }
}

/**
 * Handle IM slash commands.
 */
function splitCsvSetting(value: string | null): string[] {
  return (value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function isFeishuCommandAuthorized(msg: InboundMessage): boolean {
  if (msg.address.channelType !== 'feishu') return true;
  const { store } = getBridgeContext();
  const admins = splitCsvSetting(store.getSetting('bridge_feishu_command_admins'));
  return admins.length === 0 || admins.includes(msg.address.userId || '');
}

async function rejectUnauthorizedFeishuCommand(
  adapter: BaseChannelAdapter,
  msg: InboundMessage,
): Promise<boolean> {
  if (isFeishuCommandAuthorized(msg)) return false;
  await deliver(adapter, {
    address: msg.address,
    text: 'Command rejected: this Feishu user is not allowed to run slash commands.',
    parseMode: 'plain',
    replyToMessageId: msg.messageId,
  });
  return true;
}

function newTopicTitle(): string {
  const formatter = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  return `新话题 ${formatter.format(new Date()).replace('T', ' ')}`;
}

function fallbackTitleFromText(text: string): string {
  const cleaned = text
    .replace(/\s+/g, ' ')
    .replace(/[<>{}`]/g, '')
    .trim();
  if (!cleaned) return '新话题';
  return cleaned.length > 32 ? `${cleaned.slice(0, 32)}...` : cleaned;
}

function normalizeGeneratedTitle(raw: string): string {
  return raw
    .replace(/\s+/g, ' ')
    .replace(/^["'`“”‘’]+|["'`“”‘’]+$/g, '')
    .replace(/^标题[:：]\s*/i, '')
    .trim()
    .slice(0, 60);
}

async function collectTitleText(stream: ReadableStream<string>): Promise<string> {
  const reader = stream.getReader();
  let output = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    for (const line of value.split('\n')) {
      if (!line.startsWith('data: ')) continue;
      try {
        const event = JSON.parse(line.slice(6)) as { type?: string; data?: string };
        if (event.type === 'text' && typeof event.data === 'string') {
          output += event.data;
        }
      } catch { /* ignore malformed stream chunks */ }
    }
  }
  return normalizeGeneratedTitle(output);
}

function scheduleSessionTitleGeneration(binding: ChannelBinding, firstText: string): void {
  const { store, llm } = getBridgeContext();
  if (!store.updateSessionTitle) return;
  const session = store.getSession(binding.codepilotSessionId);
  if (!session || (session.titleStatus && !['pending', 'fallback'].includes(session.titleStatus))) return;

  const state = getState();
  if (state.titleTasks.has(binding.codepilotSessionId)) return;
  state.titleTasks.add(binding.codepilotSessionId);

  const fallback = fallbackTitleFromText(firstText);
  if (!session.title || session.titleStatus === 'pending') {
    try { store.updateSessionTitle(binding.codepilotSessionId, fallback, 'fallback'); } catch { /* best effort */ }
  }

  const abortController = new AbortController();
  const timeout = setTimeout(() => abortController.abort(), 30_000);
  const prompt = [
    '为下面这条飞书会话首条消息生成一个简短中文标题。',
    '只输出标题本身，不要解释，不要引号，最多 18 个汉字或 8 个英文词。',
    '',
    firstText.slice(0, 1200),
  ].join('\n');

  void (async () => {
    try {
      const stream = llm.streamChat({
        prompt,
        sessionId: `${binding.codepilotSessionId}:title`,
        model: binding.model || undefined,
        workingDirectory: binding.workingDirectory || undefined,
        abortController,
        permissionMode: 'plan',
        conversationHistory: [],
      });
      const title = await collectTitleText(stream);
      if (title) {
        const latest = store.getSession(binding.codepilotSessionId);
        if (latest && (!latest.titleStatus || ['pending', 'fallback'].includes(latest.titleStatus))) {
          store.updateSessionTitle?.(binding.codepilotSessionId, title, 'generated');
        }
      }
    } catch (err) {
      console.warn('[bridge-manager] Session title generation failed:', err instanceof Error ? err.message : err);
    } finally {
      clearTimeout(timeout);
      state.titleTasks.delete(binding.codepilotSessionId);
    }
  })();
}

function appendOwnerChatLog(
  binding: ChannelBinding,
  msg: InboundMessage,
  role: 'user' | 'assistant' | 'system',
  direction: 'inbound' | 'outbound',
  text: string,
): void {
  if (!binding.ownerKey || !text.trim()) return;
  const { store } = getBridgeContext();
  if (!store.appendOwnerChatLog) return;
  try {
    store.appendOwnerChatLog({
      ownerKey: binding.ownerKey,
      sessionId: binding.codepilotSessionId,
      channelType: binding.channelType,
      chatId: binding.chatId,
      direction,
      role,
      messageId: msg.messageId,
      senderId: direction === 'inbound' ? msg.address.userId : undefined,
      senderName: direction === 'inbound' ? msg.senderName : undefined,
      text,
      triggerReason: msg.triggerReason,
      createdAt: new Date(msg.timestamp || Date.now()).toISOString(),
    });
  } catch (err) {
    console.warn('[bridge-manager] Failed to append owner chat log:', err instanceof Error ? err.message : err);
  }
}

function sessionTimestamp(session: { lastActiveAt?: string; updatedAt?: string; createdAt?: string }): number {
  const raw = session.lastActiveAt || session.updatedAt || session.createdAt || '';
  const time = Date.parse(raw);
  return Number.isFinite(time) ? time : 0;
}

async function handleCommand(
  adapter: BaseChannelAdapter,
  msg: InboundMessage,
  text: string,
): Promise<void> {
  const { store } = getBridgeContext();

  // Extract command and args (handle /command@botname format)
  const parts = text.split(/\s+/);
  const command = parts[0].split('@')[0].toLowerCase();
  const args = parts.slice(1).join(' ').trim();

  // Run dangerous-input detection on the full command text
  const dangerCheck = isDangerousInput(text);
  if (dangerCheck.dangerous) {
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: 'inbound',
      messageId: msg.messageId,
      summary: `[BLOCKED] Dangerous input detected: ${dangerCheck.reason}`,
    });
    console.warn(`[bridge-manager] Blocked dangerous command input from chat ${msg.address.chatId}: ${dangerCheck.reason}`);
    await deliver(adapter, {
      address: msg.address,
      text: `Command rejected: invalid input detected.`,
      parseMode: 'plain',
      replyToMessageId: msg.messageId,
    });
    return;
  }

  let response = '';
  const routeOptions = routingOptionsFor(msg);

  if (await rejectUnauthorizedFeishuCommand(adapter, msg)) {
    return;
  }

  switch (command) {
    case '/start':
      response = [
        '<b>Feishu Claude Bridge</b>',
        '',
        'Send any message to interact with Claude.',
        '',
        '<b>Commands:</b>',
        '/new - Start a new topic in this Feishu chat',
        '/resume [session_id|number] - Resume a topic from this Feishu chat',
        '/bind &lt;session_id&gt; - Bind to a topic owned by this Feishu chat',
        '/cwd - Show the fixed lark workspace',
        '/mode plan|code|ask - Change mode',
        '/status - Show current status',
        '/sessions - List topics for this Feishu chat',
        '/stop - Stop current session',
        '/perm allow|allow_session|deny &lt;id&gt; - Respond to permission',
        '/help - Show this help',
      ].join('\n');
      break;

    case '/new': {
      if (args) {
        response = 'Path arguments are deprecated. Workspaces are fixed per Feishu chat; run /new without a path.';
        break;
      }
      clearGroupContext(msg);
      // Abort any running task on the current session before creating a new one
      const oldBinding = router.resolve(msg.address, routeOptions);
      const st = getState();
      const oldTask = st.activeTasks.get(oldBinding.codepilotSessionId);
      if (oldTask) {
        oldTask.abort();
        st.activeTasks.delete(oldBinding.codepilotSessionId);
      }

      const binding = router.createBinding(msg.address, undefined, {
        ...routeOptions,
        title: newTopicTitle(),
      });
      const session = store.getSession(binding.codepilotSessionId);
      response = [
        'New topic created.',
        `Session: <code>${binding.codepilotSessionId.slice(0, 8)}...</code>`,
        `Title: <b>${escapeHtml(session?.title || 'New topic')}</b>`,
        `CWD: <code>${escapeHtml(binding.workingDirectory || '~')}</code>`,
      ].join('\n');
      break;
    }

    case '/bind': {
      if (!args) {
        response = 'Usage: /bind &lt;session_id&gt;';
        break;
      }
      if (!validateSessionId(args)) {
        response = 'Invalid session ID format. Expected a 32-64 character hex/UUID string.';
        break;
      }
      const previousBinding = store.getChannelBinding(msg.address.channelType, msg.address.chatId);
      const binding = router.bindToSession(msg.address, args, routeOptions);
      if (binding) {
        abortActiveTaskForBinding(previousBinding);
        response = `Bound to session <code>${args.slice(0, 8)}...</code>`;
      } else {
        response = 'Session not found for this Feishu chat.';
      }
      break;
    }

    case '/resume': {
      const sessions = router
        .listSessions(msg.address, routeOptions)
        .sort((a, b) => sessionTimestamp(b) - sessionTimestamp(a));
      if (!args) {
        if (sessions.length === 0) {
          response = 'No sessions found for this Feishu chat.';
        } else {
          const lines = ['<b>Sessions for this Feishu chat:</b>', ''];
          sessions.slice(0, 10).forEach((session, index) => {
            const title = session.title || session.id;
            lines.push(`${index + 1}. <code>${session.id.slice(0, 8)}...</code> ${escapeHtml(title)}`);
          });
          lines.push('', 'Use /resume &lt;number&gt; or /resume &lt;session_id&gt;.');
          response = lines.join('\n');
        }
        break;
      }

      let targetId = args;
      if (/^\d+$/.test(args)) {
        const index = Number(args) - 1;
        if (index < 0 || index >= sessions.length) {
          response = 'Session number not found for this Feishu chat.';
          break;
        }
        targetId = sessions[index].id;
      } else {
        const matches = sessions.filter((session) => session.id === args || session.id.startsWith(args));
        if (matches.length === 0) {
          response = 'Session not found for this Feishu chat.';
          break;
        }
        if (matches.length > 1) {
          response = 'Session ID prefix is ambiguous. Use a longer session ID.';
          break;
        }
        targetId = matches[0].id;
      }

      abortCurrentTaskForChat(msg.address);
      const binding = router.bindToSession(msg.address, targetId, routeOptions);
      if (!binding) {
        response = 'Session not found for this Feishu chat.';
        break;
      }
      const session = store.getSession(binding.codepilotSessionId);
      response = [
        `Resumed session <code>${binding.codepilotSessionId.slice(0, 8)}...</code>`,
        `Title: <b>${escapeHtml(session?.title || binding.codepilotSessionId)}</b>`,
        `CWD: <code>${escapeHtml(binding.workingDirectory || '~')}</code>`,
      ].join('\n');
      break;
    }

    case '/cwd': {
      if (args) {
        response = 'Path arguments are deprecated. Workspaces are fixed per Feishu chat; /cwd only displays the current workspace.';
        break;
      }
      const binding = router.resolve(msg.address, routeOptions);
      response = `Working directory is fixed for this Feishu chat:\n<code>${escapeHtml(binding.workingDirectory || '~')}</code>`;
      break;
    }

    case '/mode': {
      if (!validateMode(args)) {
        response = 'Usage: /mode plan|code|ask';
        break;
      }
      const binding = router.resolve(msg.address, routeOptions);
      abortActiveTaskForBinding(binding);
      store.updateSessionMode?.(binding.codepilotSessionId, args as 'code' | 'plan' | 'ask');
      const generation = store.bumpSessionGeneration?.(binding.codepilotSessionId);
      router.updateBinding(binding.id, {
        mode: args,
        ...(generation ? { generation } : {}),
      });
      response = `Mode set to <b>${args}</b>`;
      break;
    }

    case '/status': {
      const binding = router.resolve(msg.address, routeOptions);
      const session = store.getSession(binding.codepilotSessionId);
      response = [
        '<b>Bridge Status</b>',
        '',
        `Session: <code>${binding.codepilotSessionId.slice(0, 8)}...</code>`,
        `Title: <b>${escapeHtml(session?.title || binding.codepilotSessionId)}</b>`,
        `CWD: <code>${escapeHtml(binding.workingDirectory || '~')}</code>`,
        `Mode: <b>${binding.mode}</b>`,
        `Model: <code>${binding.model || 'default'}</code>`,
      ].join('\n');
      break;
    }

    case '/sessions': {
      const sessions = router
        .listSessions(msg.address, routeOptions)
        .sort((a, b) => sessionTimestamp(b) - sessionTimestamp(a));
      if (sessions.length === 0) {
        response = 'No sessions found for this Feishu chat.';
      } else {
        const lines = ['<b>Sessions for this Feishu chat:</b>', ''];
        sessions.slice(0, 10).forEach((session, index) => {
          const title = session.title || session.id;
          lines.push(`${index + 1}. <code>${session.id.slice(0, 8)}...</code> ${escapeHtml(title)}`);
        });
        response = lines.join('\n');
      }
      break;
    }

    case '/stop': {
      const binding = router.resolve(msg.address, routeOptions);
      const st = getState();
      const taskAbort = st.activeTasks.get(binding.codepilotSessionId);
      if (taskAbort) {
        taskAbort.abort();
        st.activeTasks.delete(binding.codepilotSessionId);
        response = 'Stopping current task...';
      } else {
        response = 'No task is currently running.';
      }
      break;
    }

    case '/perm': {
      // Text-based permission approval fallback (for channels without inline buttons)
      // Usage: /perm allow <id> | /perm allow_session <id> | /perm deny <id>
      const permParts = args.split(/\s+/);
      const permAction = permParts[0];
      const permId = permParts.slice(1).join(' ');
      if (!permAction || !permId || !['allow', 'allow_session', 'deny'].includes(permAction)) {
        response = 'Usage: /perm allow|allow_session|deny &lt;permission_id&gt;';
        break;
      }
      const callbackData = `perm:${permAction}:${permId}`;
      const handled = broker.handlePermissionCallback(callbackData, msg.address.chatId, undefined, adapter.channelType);
      if (handled) {
        response = `Permission ${permAction}: recorded.`;
      } else {
        response = `Permission not found or already resolved.`;
      }
      break;
    }

    case '/help':
      response = [
        '<b>Feishu Claude Bridge Commands</b>',
        '',
        '/new - Start a new topic in this Feishu chat',
        '/resume [session_id|number] - Resume a topic from this Feishu chat',
        '/bind &lt;session_id&gt; - Bind to a topic owned by this Feishu chat',
        '/cwd - Show the fixed lark workspace',
        '/mode plan|code|ask - Change mode',
        '/status - Show current status',
        '/sessions - List topics for this Feishu chat',
        '/stop - Stop current session',
        '/perm allow|allow_session|deny &lt;id&gt; - Respond to permission request',
        '1/2/3 - Quick permission reply (Feishu, single pending)',
        '/help - Show this help',
      ].join('\n');
      break;

    default:
      response = `Unknown command: ${escapeHtml(command)}\nType /help for available commands.`;
  }

  if (response) {
    await deliver(adapter, {
      address: msg.address,
      text: response,
      parseMode: 'HTML',
      replyToMessageId: msg.messageId,
    });
  }
}

// ── SDK Session Update Logic ─────────────────────────────────

/**
 * Compute the sdkSessionId value to persist after a conversation result.
 * Returns the new value to write, or null if no update is needed.
 *
 * Rules:
 * - If result has sdkSessionId AND no error → save the new ID
 * - If result has error (regardless of sdkSessionId) → clear to empty string
 * - Otherwise → no update needed
 */
export function computeSdkSessionUpdate(
  sdkSessionId: string | null | undefined,
  hasError: boolean,
): string | null {
  if (sdkSessionId && !hasError) {
    return sdkSessionId;
  }
  if (hasError) {
    return '';
  }
  return null;
}

// ── Test-only export ─────────────────────────────────────────
// Exposed so integration tests can exercise handleMessage directly
// without wiring up the full adapter loop.
/** @internal */
export const _testOnly = {
  handleMessage,
  dispatchInboundMessage,
  isNumericPermissionShortcut,
  getGroupContextBufferSize: () => groupContextBuffer.size,
  clearGroupContextBuffer: () => groupContextBuffer.clear(),
};
