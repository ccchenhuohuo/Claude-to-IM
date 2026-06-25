/**
 * Channel Router — resolves IM addresses to CodePilot sessions.
 *
 * When a message arrives from an IM channel, the router finds or creates
 * the corresponding ChannelBinding (and underlying chat_session).
 */

import type { BridgeOwner, ChannelAddress, ChannelBinding, ChannelType } from './types.js';
import type { BridgeSession } from './host.js';
import { getBridgeContext } from './context.js';

export interface RoutingOptions {
  chatType?: BridgeOwner['chatType'];
  title?: string;
}

/**
 * Resolve an inbound address to a ChannelBinding.
 * If no binding exists, auto-creates a new session and binding.
 */
export function resolve(address: ChannelAddress, options: RoutingOptions = {}): ChannelBinding {
  const { store } = getBridgeContext();
  const owner = getOwner(address, options);
  const existing = store.getChannelBinding(address.channelType, address.chatId);
  if (existing) {
    // Verify the linked session still exists; if not, create a new one
    const session = store.getSession(existing.codepilotSessionId);
    if (session) {
      if (owner?.ownerKey) {
        const ownerWorkspace = store.getOwnerWorkspace?.(owner.ownerKey);
        const ownerMismatch = existing.ownerKey !== owner.ownerKey || session.ownerKey !== owner.ownerKey;
        const workspaceMismatch = !!ownerWorkspace
          && (existing.workingDirectory !== ownerWorkspace || session.working_directory !== ownerWorkspace);
        if (ownerMismatch || workspaceMismatch) {
          return createBinding(address, undefined, options);
        }
      }
      return existing;
    }
    // Session was deleted — recreate
    return createBinding(address, undefined, options);
  }
  return createBinding(address, undefined, options);
}

/**
 * Create a new binding with a fresh CodePilot session.
 */
export function createBinding(
  address: ChannelAddress,
  workingDirectory?: string,
  options: RoutingOptions = {},
): ChannelBinding {
  const { store } = getBridgeContext();
  const owner = getOwner(address, options);
  const ownerWorkspace = owner?.ownerKey && store.getOwnerWorkspace
    ? store.getOwnerWorkspace(owner.ownerKey)
    : undefined;
  const defaultCwd = workingDirectory
    || ownerWorkspace
    || store.getSetting('bridge_default_work_dir')
    || process.env.HOME
    || '';
  const defaultModel = store.getSetting('bridge_default_model') || '';
  const defaultProviderId = store.getSetting('bridge_default_provider_id') || '';

  const displayName = address.displayName || address.chatId;
  const title = options.title || `Bridge: ${displayName}`;
  const session = owner?.ownerKey && store.createSessionForOwner
    ? store.createSessionForOwner(owner.ownerKey, {
        title,
        titleStatus: options.title ? 'fallback' : 'pending',
        model: defaultModel,
        mode: 'code',
      })
    : store.createSession(
        title,
        defaultModel,
        undefined,
        defaultCwd,
        'code',
      );

  if (defaultProviderId) {
    store.updateSessionProviderId(session.id, defaultProviderId);
  }

  return store.upsertChannelBinding({
    channelType: address.channelType,
    chatId: address.chatId,
    ownerKey: owner?.ownerKey,
    codepilotSessionId: session.id,
    sdkSessionId: '',
    workingDirectory: session.working_directory || defaultCwd,
    model: defaultModel,
    mode: 'code',
    generation: session.generation,
  });
}

/**
 * Bind an IM chat to an existing CodePilot session.
 */
export function bindToSession(
  address: ChannelAddress,
  codepilotSessionId: string,
  options: RoutingOptions = {},
): ChannelBinding | null {
  const { store } = getBridgeContext();
  const session = store.getSession(codepilotSessionId);
  if (!session) return null;
  const owner = getOwner(address, options);
  if (owner?.ownerKey) {
    if (!session.ownerKey || session.ownerKey !== owner.ownerKey) {
      return null;
    }
  }

  return store.upsertChannelBinding({
    channelType: address.channelType,
    chatId: address.chatId,
    ownerKey: owner?.ownerKey ?? session.ownerKey,
    codepilotSessionId,
    sdkSessionId: session.sdk_session_id || '',
    workingDirectory: session.working_directory,
    model: session.model,
    mode: session.mode,
    generation: session.generation,
  });
}

/**
 * Update properties of an existing binding.
 */
export function updateBinding(
  id: string,
  updates: Partial<Pick<ChannelBinding, 'sdkSessionId' | 'workingDirectory' | 'model' | 'mode' | 'active' | 'generation'>>,
): void {
  getBridgeContext().store.updateChannelBinding(id, updates);
}

/**
 * List all bindings, optionally filtered by channel type.
 */
export function listBindings(channelType?: ChannelType): ChannelBinding[] {
  return getBridgeContext().store.listChannelBindings(channelType);
}

export function listSessions(
  address: ChannelAddress,
  options: RoutingOptions = {},
): BridgeSession[] {
  const { store } = getBridgeContext();
  const owner = getOwner(address, options);
  if (owner?.ownerKey && store.listSessionsByOwner) {
    return store.listSessionsByOwner(owner.ownerKey);
  }
  const bindings = store
    .listChannelBindings(address.channelType)
    .filter((binding) => binding.chatId === address.chatId);
  return bindings
    .map((binding) => store.getSession(binding.codepilotSessionId))
    .filter((session): session is BridgeSession => session !== null);
}

export function getOwner(
  address: ChannelAddress,
  options: RoutingOptions = {},
): BridgeOwner | null {
  const { store } = getBridgeContext();
  if (address.channelType !== 'feishu') return null;
  if (!store.getOrCreateOwner) return null;
  const chatType = options.chatType || address.chatType || 'unknown';
  return store.getOrCreateOwner(address, chatType);
}
