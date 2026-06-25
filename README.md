# Claude-to-IM

English | [中文](README.zh-CN.md)

Claude-to-IM is a host-agnostic bridge library that connects [Claude Code SDK](https://docs.anthropic.com/en/docs/claude-code/sdk) to Feishu/Lark, allowing users to interact with Claude from a Feishu chat.

This library handles all IM-side complexity — message routing, streaming previews, permission approval flows, Markdown rendering, chunking, retry, rate limiting — while delegating persistence, LLM calls, and permission resolution to the host application through a set of dependency injection interfaces.

## Out-of-the-Box Solution

If you want a ready-to-use desktop application without writing any integration code, check out [CodePilot](https://github.com/op7418/CodePilot) — a desktop GUI client for Claude Code with built-in IM bridge support. CodePilot implements all the host interfaces for you and provides a complete UI for managing sessions, settings, and bridge connections.

Claude-to-IM was extracted from CodePilot as a standalone library for developers who want to embed the IM bridge capability in their own applications.

## Features

- **Feishu/Lark adapter**: Long-connection WSClient integration with Feishu rich text/cards
- **Streaming previews**: Real-time response drafts via Feishu cards
- **Permission management**: Interactive inline buttons for Claude Code tool approvals (allow / deny / allow for session)
- **Session binding**: Each IM chat maps to a persistent conversation session with working directory and model settings
- **Markdown rendering**: Feishu rich text/card rendering for Claude responses
- **Reliable delivery**: Auto-chunking at Feishu limits, retry with exponential backoff, message deduplication
- **Security**: Input validation, token bucket rate limiting (20 msg/min per chat), user authorization whitelists, full audit logging
- **Host-agnostic**: All host dependencies abstracted via 4 DI interfaces — no database driver, no LLM client, no framework lock-in

## Architecture

```
Feishu / Lark
        |
        | InboundMessage
        v
   +-----------+     +------------------+
   |  Adapter   |---->| Bridge Manager   |  (orchestrator)
   +-----------+     |  |- Channel Router     -> session binding
                     |  |- Conversation Engine -> LLM streaming
                     |  |- Permission Broker   -> tool approval flow
                     |  |- Delivery Layer      -> chunking, retry, dedup
                     +------------------+
                            |
                            | Host Interfaces (DI)
                            v
                     +------------------+
                     | Host Application |  (implements BridgeStore,
                     |                  |   LLMProvider, etc.)
                     +------------------+
```

All bridge modules access host services through a DI context (`getBridgeContext()`), never through direct imports. This means you can plug the bridge into any Node.js application by implementing four interfaces.

## Quick Start

### 1. Install

```bash
npm install claude-to-im
```

Or clone this repo and install dependencies:

```bash
git clone https://github.com/op7418/Claude-to-IM.git
cd Claude-to-IM
npm install
```

### 2. Implement Host Interfaces

The bridge requires four interfaces. See [`docs/development.md`](docs/development.md) for the full specification of each interface.

```typescript
import { initBridgeContext } from 'claude-to-im/context';
import type { BridgeStore, LLMProvider, PermissionGateway, LifecycleHooks } from 'claude-to-im/host';

const store: BridgeStore = { /* your persistence layer (~30 methods) */ };
const llm: LLMProvider = { /* wraps Claude Code SDK streamChat */ };
const permissions: PermissionGateway = { /* resolves pending tool permissions */ };
const lifecycle: LifecycleHooks = { /* optional start/stop callbacks */ };

initBridgeContext({ store, llm, permissions, lifecycle });
```

### 3. Start the Bridge

```typescript
import * as bridgeManager from 'claude-to-im/bridge-manager';

await bridgeManager.start();

const status = bridgeManager.getStatus();
// { running: true, adapters: [{ channelType: 'feishu', running: true, ... }] }
```

### 4. Run the Example

A self-contained example with in-memory store and echo LLM is included:

```bash
npx tsx src/lib/bridge/examples/mock-host.ts
```

## Configuration

All settings are read through `BridgeStore.getSetting(key)`. Your host application decides how to store and surface these values (database, env vars, config file, UI settings panel, etc.).

### Required Settings

| Key | Description |
|-----|-------------|
| `remote_bridge_enabled` | Master switch — `"true"` to enable the bridge |
| `bridge_feishu_app_id` | Feishu/Lark app ID |
| `bridge_feishu_app_secret` | Feishu/Lark app secret |
| `bridge_feishu_allowed_users` | Comma-separated user IDs authorized to use the bridge |

### Optional Settings

| Key | Description | Default |
|-----|-------------|---------|
| `bridge_auto_start` | Auto-start bridge on app launch | `"false"` |
| `bridge_feishu_enabled` | Feishu adapter toggle | `"false"` |
| `bridge_feishu_stream_enabled` | Enable streaming previews | `"true"` |
| `bridge_default_cwd` | Default working directory for new sessions | `$HOME` |
| `bridge_model` | Default Claude model | Host decides |

This build registers only the Feishu/Lark adapter at runtime.

## Limitations

Before adopting this library, be aware of the following constraints:

### You Must Implement the Host Interfaces

This is a library, not a standalone application. You need to provide:

- **`BridgeStore`** — A persistence layer with ~30 methods covering settings, sessions, messages, channel bindings, audit logs, dedup tracking, permission links, and channel offsets. This is the largest integration surface. See the full interface definition in [`src/lib/bridge/host.ts`](src/lib/bridge/host.ts).
- **`LLMProvider`** — A wrapper around your LLM client that returns a `ReadableStream<string>` of SSE-formatted events. The stream format must match the Claude Code SDK's event protocol (text, tool_use, tool_result, permission_request, status, result events). See [`docs/development.md`](docs/development.md) for the full event format spec.
- **`PermissionGateway`** — A way to resolve pending tool permissions from the Claude Code SDK.
- **`LifecycleHooks`** — Optional callbacks for bridge start/stop events.

### LLM Stream Format

The `LLMProvider.streamChat()` must return SSE-formatted strings matching the Claude Code SDK's event protocol. If you are not using the Claude Code SDK, you need to adapt your LLM client's output to match this format. This is not a generic "chat completion" interface.

### No Built-in Persistence

The bridge does not bundle any database driver. You provide all persistence through `BridgeStore`. This gives you full control but means you need to implement storage for sessions, messages, bindings, audit logs, dedup keys, permission links, and channel offsets.

### Session Locking

The bridge uses a session lock mechanism (`acquireSessionLock` / `renewSessionLock` / `releaseSessionLock`) to serialize messages within the same session. Your `BridgeStore` implementation must provide atomic lock operations. For single-process deployments, in-memory locks work fine. For multi-process deployments, you need distributed locking (e.g., database-backed).

### Feishu/Lark App Setup

You still need to create a Feishu/Lark app in the [Developer Console](https://open.feishu.cn/app), enable bot capability, subscribe to `im.message.receive_v1` over long connection, grant IM permissions, and publish the app.

## Documentation

| Document | Description |
|----------|-------------|
| [Development Guide](docs/development.md) | Host interface specs, SSE format, adapter development, step-by-step integration tutorial |
| [Architecture](src/lib/bridge/ARCHITECTURE.md) | Module dependency graph, message flows, design decisions |
| [Security](src/lib/bridge/SECURITY.md) | Threat model, mitigations, deployment recommendations |
| [Contributing](src/lib/bridge/CONTRIBUTING.md) | Dev setup, code style, testing guide |
| [Migration](src/lib/bridge/MIGRATION.md) | Before/after import patterns for migrating from direct imports |

## Project Structure

```
src/
  lib/bridge/
    context.ts              # DI container (initBridgeContext / getBridgeContext)
    host.ts                 # Host interface definitions (BridgeStore, LLMProvider, etc.)
    types.ts                # Shared type definitions (messages, bindings, status)
    bridge-manager.ts       # Orchestrator — start/stop, message dispatch, session locks
    channel-adapter.ts      # Abstract base class + adapter registry
    channel-router.ts       # ChannelAddress -> ChannelBinding resolution
    conversation-engine.ts  # LLM stream processing, SSE consumption
    delivery-layer.ts       # Reliable outbound delivery (chunk, retry, dedup, audit)
    permission-broker.ts    # Tool permission forwarding and callback handling
    adapters/
      feishu-adapter.ts     # Feishu/Lark WSClient
      index.ts              # Feishu-only runtime adapter catalog
    markdown/
      ir.ts                 # Intermediate representation for Markdown AST
      feishu.ts             # Markdown -> Feishu rich text / cards
    security/
      validators.ts         # Input validation (path traversal, injection, sanitization)
      rate-limiter.ts       # Token bucket rate limiter (per chat)
    examples/
      mock-host.ts          # Runnable example with InMemoryStore + EchoLLM
  __tests__/unit/
    bridge-channel-router.test.ts
    bridge-delivery-layer.test.ts
    bridge-manager.test.ts
    bridge-permission-broker.test.ts
```

## Testing

```bash
# Type checking
npm run typecheck

# Unit tests (28 tests)
npm run test:unit

# Both
npm run test
```

Tests use Node.js built-in test runner (`node:test`) with mock implementations of all host interfaces — no real database or LLM required.

## License

MIT
