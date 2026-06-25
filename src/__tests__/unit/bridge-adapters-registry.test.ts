import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import '../../lib/bridge/adapters/index';
import { getRegisteredTypes } from '../../lib/bridge/channel-adapter';

describe('adapter registry', () => {
  it('auto-registers only the Feishu adapter', () => {
    assert.deepEqual(getRegisteredTypes(), ['feishu']);
  });
});
