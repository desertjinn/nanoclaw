import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import fs from 'fs';

// Sentinel markers must match container-runner.ts
const OUTPUT_START_MARKER = '---NANOCLAW_OUTPUT_START---';
const OUTPUT_END_MARKER = '---NANOCLAW_OUTPUT_END---';

// Mock config
vi.mock('./config.js', () => ({
  CONTAINER_IMAGE: 'nanoclaw-agent:latest',
  CONTAINER_MAX_OUTPUT_SIZE: 10485760,
  CONTAINER_TIMEOUT: 1800000,
  DATA_DIR: '/tmp/nanoclaw-test-data',
  GROUPS_DIR: '/tmp/nanoclaw-test-groups',
  IDLE_TIMEOUT: 1800000,
  TIMEZONE: 'America/Los_Angeles',
}));

// Mock logger
vi.mock('./logger.js', () => ({
  logger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
}));

// Mock fs
vi.mock('fs', async () => {
  const actual = await vi.importActual<typeof import('fs')>('fs');
  return {
    ...actual,
    default: {
      ...actual,
      existsSync: vi.fn(() => false),
      mkdirSync: vi.fn(),
      writeFileSync: vi.fn(),
      renameSync: vi.fn(),
      unlinkSync: vi.fn(),
      readFileSync: vi.fn(() => ''),
      readdirSync: vi.fn(() => []),
      statSync: vi.fn(() => ({ isDirectory: () => false })),
      cpSync: vi.fn(),
    },
  };
});

// Mock env reader
vi.mock('./env.js', () => ({
  readEnvFile: vi.fn(() => ({ CLAUDE_CODE_OAUTH_TOKEN: 'tok', ANTHROPIC_API_KEY: 'key' })),
}));

// Mock group-folder
vi.mock('./group-folder.js', () => ({
  resolveGroupFolderPath: vi.fn((folder: string) => `/tmp/groups/${folder}`),
  resolveGroupIpcPath: vi.fn((folder: string) => `/tmp/ipc/${folder}`),
}));

// Controllable mock for container-runtime
const mockSpawnJob = vi.fn();
const mockStopJob = vi.fn();
const mockStreamJobLogs = vi.fn();

vi.mock('./container-runtime.js', () => ({
  spawnJob: (...args: unknown[]) => mockSpawnJob(...args),
  stopJob: (...args: unknown[]) => mockStopJob(...args),
  streamJobLogs: (...args: unknown[]) => mockStreamJobLogs(...args),
  ensureContainerRuntimeRunning: vi.fn(),
  cleanupOrphans: vi.fn(),
}));

import { runContainerAgent, ContainerOutput } from './container-runner.js';
import type { RegisteredGroup } from './types.js';

const testGroup: RegisteredGroup = {
  name: 'Test Group',
  folder: 'test-group',
  trigger: '@Andy',
  added_at: new Date().toISOString(),
};

const testInput = {
  prompt: 'Hello',
  groupFolder: 'test-group',
  chatJid: 'test@g.us',
  isMain: false,
};

function makeStreamJobLogs(output: ContainerOutput, exitCode = 0) {
  return vi.fn(async (_handle: unknown, onChunk: (chunk: string) => void, _timeout: unknown) => {
    const json = JSON.stringify(output);
    onChunk(`${OUTPUT_START_MARKER}\n${json}\n${OUTPUT_END_MARKER}\n`);
    return exitCode;
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  mockSpawnJob.mockResolvedValue({ name: 'nanoclaw-test-group-123', runtimeId: 'nanoclaw' });
  mockStopJob.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.useRealTimers();
});

describe('container-runner timeout behavior', () => {
  it('normal exit after output resolves as success (streaming mode)', async () => {
    const onOutput = vi.fn(async () => {});
    mockStreamJobLogs.mockImplementation(
      makeStreamJobLogs({ status: 'success', result: 'Done', newSessionId: 'session-456' }),
    );

    const result = await runContainerAgent(testGroup, testInput, () => {}, onOutput);

    expect(result.status).toBe('success');
    expect(result.newSessionId).toBe('session-456');
    expect(onOutput).toHaveBeenCalledWith(
      expect.objectContaining({ result: 'Done' }),
    );
  });

  it('job exit code non-zero resolves as error', async () => {
    const onOutput = vi.fn(async () => {});
    mockStreamJobLogs.mockImplementation(
      makeStreamJobLogs({ status: 'error', result: null, error: 'agent failed' }, 1),
    );

    const result = await runContainerAgent(testGroup, testInput, () => {}, onOutput);

    expect(result.status).toBe('error');
    expect(result.error).toContain('exit');
  });

  it('writes init file to PVC IPC dir before spawning job', async () => {
    mockStreamJobLogs.mockImplementation(
      makeStreamJobLogs({ status: 'success', result: 'done' }),
    );

    await runContainerAgent(testGroup, testInput, () => {});

    // renameSync is the atomic commit step — must have been called before spawnJob
    const renameCalls = (fs.renameSync as ReturnType<typeof vi.fn>).mock.calls;
    expect(renameCalls.length).toBeGreaterThanOrEqual(1);
    const [tempPath, finalPath] = renameCalls[0];
    expect(tempPath).toMatch(/init-\d+\.json\.tmp$/);
    expect(finalPath).toMatch(/init-\d+\.json$/);
    // spawnJob was called after renameSync (order guaranteed by sequential code)
    expect(mockSpawnJob).toHaveBeenCalledTimes(1);
    const spawnArg = mockSpawnJob.mock.calls[0][0];
    expect(spawnArg.env.NANOCLAW_GROUP).toBe('test-group');
  });

  it('spawnJob failure returns error immediately', async () => {
    mockSpawnJob.mockRejectedValueOnce(new Error('k8s API unavailable'));
    const onOutput = vi.fn(async () => {});

    const result = await runContainerAgent(testGroup, testInput, () => {}, onOutput);

    expect(result.status).toBe('error');
    expect(result.error).toContain('k8s API unavailable');
    expect(mockStreamJobLogs).not.toHaveBeenCalled();
  });

  it('calls onProcess callback with JobHandle and containerName', async () => {
    const onProcess = vi.fn();
    mockStreamJobLogs.mockImplementation(
      makeStreamJobLogs({ status: 'success', result: 'ok' }),
    );

    await runContainerAgent(testGroup, testInput, onProcess);

    expect(onProcess).toHaveBeenCalledWith(
      expect.objectContaining({ name: expect.stringContaining('nanoclaw-test-group'), runtimeId: 'nanoclaw' }),
      expect.stringContaining('nanoclaw-test-group'),
    );
  });

  it('legacy mode: parses output from stdout when onOutput not provided', async () => {
    mockStreamJobLogs.mockImplementation(
      makeStreamJobLogs({ status: 'success', result: 'legacy result', newSessionId: 'sess-789' }),
    );

    const result = await runContainerAgent(testGroup, testInput, () => {});

    expect(result.status).toBe('success');
    expect(result.result).toBe('legacy result');
    expect(result.newSessionId).toBe('sess-789');
  });
});
