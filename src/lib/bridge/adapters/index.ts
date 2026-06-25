/**
 * Feishu-only adapter catalog — side-effect import that triggers
 * self-registration of the single supported channel adapter.
 *
 * The bridge used to load every bundled IM adapter from this catalog. The
 * product surface is now intentionally Feishu/Lark-only, so bridge-manager.ts
 * should discover only the Feishu adapter at runtime.
 */

import './feishu-adapter.js';
