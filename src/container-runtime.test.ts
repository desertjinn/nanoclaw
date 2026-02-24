import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock logger
vi.mock('./logger.js', () => ({
  logger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
}));

// ─── K8s mocks ────────────────────────────────────────────────────────────────

const {
  mockCreateNamespacedJob,
  mockDeleteNamespacedJob,
  mockListNamespacedJob,
  mockReadNamespacedJob,
  mockListNamespacedPod,
  mockReadNamespacedPodLog,
} = vi.hoisted(() => ({
  mockCreateNamespacedJob: vi.fn(),
  mockDeleteNamespacedJob: vi.fn(),
  mockListNamespacedJob: vi.fn(),
  mockReadNamespacedJob: vi.fn(),
  mockListNamespacedPod: vi.fn(),
  mockReadNamespacedPodLog: vi.fn(),
}));

vi.mock('@kubernetes/client-node', () => {
  class BatchV1Api {
    createNamespacedJob = mockCreateNamespacedJob;
    deleteNamespacedJob = mockDeleteNamespacedJob;
    listNamespacedJob = mockListNamespacedJob;
    readNamespacedJob = mockReadNamespacedJob;
  }
  class CoreV1Api {
    listNamespacedPod = mockListNamespacedPod;
    readNamespacedPodLog = mockReadNamespacedPodLog;
  }
  class KubeConfig {
    loadFromCluster = vi.fn();
    makeApiClient(ApiClass: unknown) {
      if (ApiClass === BatchV1Api) return new BatchV1Api();
      return new CoreV1Api();
    }
  }
  return { KubeConfig, BatchV1Api, CoreV1Api };
});

// ─── Docker mocks ─────────────────────────────────────────────────────────────

const { mockExecSync } = vi.hoisted(() => ({ mockExecSync: vi.fn() }));

vi.mock('child_process', async () => {
  const actual = await vi.importActual<typeof import('child_process')>('child_process');
  return {
    ...actual,
    execSync: (...args: unknown[]) => mockExecSync(...args),
  };
});

// ─── Imports ──────────────────────────────────────────────────────────────────

import { DockerRuntime } from './docker-runtime.js';
import { K8sRuntime } from './k8s-runtime.js';
import { logger } from './logger.js';

beforeEach(() => {
  vi.clearAllMocks();
});

// ─── DockerRuntime ────────────────────────────────────────────────────────────

describe('DockerRuntime', () => {
  describe('ensureRunning', () => {
    it('does nothing when docker info succeeds', () => {
      mockExecSync.mockReturnValueOnce('');
      new DockerRuntime().ensureRunning();
      expect(mockExecSync).toHaveBeenCalledWith('docker info', { stdio: 'pipe', timeout: 10000 });
      expect(logger.debug).toHaveBeenCalledWith('Container runtime already running');
    });

    it('throws when docker info fails', () => {
      mockExecSync.mockImplementationOnce(() => { throw new Error('no daemon'); });
      expect(() => new DockerRuntime().ensureRunning()).toThrow(
        'Container runtime is required but failed to start',
      );
      expect(logger.error).toHaveBeenCalled();
    });
  });

  describe('cleanupOrphans', () => {
    it('stops orphaned nanoclaw containers', async () => {
      mockExecSync.mockReturnValueOnce('nanoclaw-group1-111\nnanoclaw-group2-222\n');
      mockExecSync.mockReturnValue('');

      await new DockerRuntime().cleanupOrphans();

      expect(mockExecSync).toHaveBeenCalledTimes(3);
      expect(logger.info).toHaveBeenCalledWith(
        { count: 2, names: ['nanoclaw-group1-111', 'nanoclaw-group2-222'] },
        'Stopped orphaned containers',
      );
    });

    it('does nothing when no orphans exist', async () => {
      mockExecSync.mockReturnValueOnce('');
      await new DockerRuntime().cleanupOrphans();
      expect(mockExecSync).toHaveBeenCalledTimes(1);
      expect(logger.info).not.toHaveBeenCalled();
    });

    it('warns and continues when ps fails', async () => {
      mockExecSync.mockImplementationOnce(() => { throw new Error('docker not available'); });
      await new DockerRuntime().cleanupOrphans();
      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({ err: expect.any(Error) }),
        'Failed to clean up orphaned containers',
      );
    });
  });
});

// ─── K8sRuntime ───────────────────────────────────────────────────────────────

describe('K8sRuntime', () => {
  describe('ensureRunning', () => {
    it('is a no-op and logs debug', () => {
      new K8sRuntime().ensureRunning();
      expect(logger.debug).toHaveBeenCalledWith(
        'Using Kubernetes Jobs API — no runtime check needed',
      );
    });
  });

  describe('spawnContainer', () => {
    it('creates a namespaced Job with PVC mounts and returns a handle', async () => {
      mockCreateNamespacedJob.mockResolvedValueOnce({});

      const handle = await new K8sRuntime().spawnContainer({
        name: 'nanoclaw-main-123',
        image: 'ghcr.io/desertjinn/nanoclaw-agent:latest',
        stdin: '{"prompt":"hello"}',
        env: { TZ: 'UTC', NANOCLAW_GROUP: 'test-group' },
      });

      expect(mockCreateNamespacedJob).toHaveBeenCalledTimes(1);
      const call = mockCreateNamespacedJob.mock.calls[0][0];
      expect(call.namespace).toBe('nanoclaw');
      expect(call.body.metadata.name).toBe('nanoclaw-main-123');
      expect(call.body.spec.backoffLimit).toBe(0);
      expect(call.body.spec.ttlSecondsAfterFinished).toBe(60);

      // Verify PVC volume mounts with subPath isolation
      const volumeMounts: Array<{ name: string; mountPath: string; subPath?: string }> =
        call.body.spec.template.spec.containers[0].volumeMounts;
      const ipcMount = volumeMounts.find((m) => m.mountPath === '/workspace/ipc');
      expect(ipcMount).toBeDefined();
      expect(ipcMount!.subPath).toBe('ipc/test-group');
      const groupMount = volumeMounts.find((m) => m.mountPath === '/workspace/groups/test-group');
      expect(groupMount).toBeDefined();
      expect(groupMount!.subPath).toBe('groups/test-group');

      // NANOCLAW_INPUT must NOT be present — init goes via PVC file
      const envVars: Array<{ name: string; value: string }> =
        call.body.spec.template.spec.containers[0].env;
      expect(envVars.find((e) => e.name === 'NANOCLAW_INPUT')).toBeUndefined();
      // NANOCLAW_GROUP is filtered out before forwarding to the container
      expect(envVars.find((e) => e.name === 'NANOCLAW_GROUP')).toBeUndefined();

      expect(handle).toEqual({ name: 'nanoclaw-main-123', runtimeId: 'nanoclaw' });
      expect(logger.info).toHaveBeenCalledWith(
        expect.objectContaining({ jobName: 'nanoclaw-main-123' }),
        'Kubernetes Job created',
      );
    });

    it('passes fsGroup: 1000 in pod securityContext', async () => {
      mockCreateNamespacedJob.mockResolvedValueOnce({});

      await new K8sRuntime().spawnContainer({
        name: 'nanoclaw-test-456',
        image: 'nanoclaw-agent:latest',
        stdin: '{}',
        env: { NANOCLAW_GROUP: 'my-group' },
      });

      const podSpec = mockCreateNamespacedJob.mock.calls[0][0].body.spec.template.spec;
      expect(podSpec.securityContext.fsGroup).toBe(1000);
      expect(podSpec.securityContext.runAsUser).toBe(1000);
    });
  });

  describe('stopContainer', () => {
    it('deletes the Job by name', async () => {
      mockDeleteNamespacedJob.mockResolvedValueOnce({});

      await new K8sRuntime().stopContainer({ name: 'nanoclaw-main-999', runtimeId: 'nanoclaw' });

      expect(mockDeleteNamespacedJob).toHaveBeenCalledWith({
        name: 'nanoclaw-main-999',
        namespace: 'nanoclaw',
        body: { propagationPolicy: 'Background' },
      });
    });

    it('warns but does not throw when delete fails', async () => {
      mockDeleteNamespacedJob.mockRejectedValueOnce(new Error('not found'));

      await expect(
        new K8sRuntime().stopContainer({ name: 'nanoclaw-gone', runtimeId: 'nanoclaw' }),
      ).resolves.toBeUndefined();

      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({ err: expect.any(Error) }),
        'Failed to delete Kubernetes Job',
      );
    });
  });

  describe('cleanupOrphans', () => {
    it('deletes stale Jobs (no startTime, no completionTime)', async () => {
      mockListNamespacedJob.mockResolvedValueOnce({
        items: [
          { metadata: { name: 'nanoclaw-stale-1' }, status: {} },
          { metadata: { name: 'nanoclaw-active-2' }, status: { startTime: '2024-01-01T00:00:00Z' } },
        ],
      });
      mockDeleteNamespacedJob.mockResolvedValue({});

      await new K8sRuntime().cleanupOrphans();

      expect(mockDeleteNamespacedJob).toHaveBeenCalledTimes(1);
      expect(mockDeleteNamespacedJob).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'nanoclaw-stale-1' }),
      );
      expect(logger.info).toHaveBeenCalledWith(
        { count: 1 },
        'Deleted stale Kubernetes Jobs',
      );
    });

    it('warns and does not throw when list fails', async () => {
      mockListNamespacedJob.mockRejectedValueOnce(new Error('k8s unreachable'));

      await new K8sRuntime().cleanupOrphans();

      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({ err: expect.any(Error) }),
        'Failed to clean up orphaned Kubernetes Jobs',
      );
    });
  });
});

// ─── Dispatcher (container-runtime.ts) ───────────────────────────────────────
//
// The dispatcher calls selectRuntime() at module load time, so we must
// reset modules and re-import with a fresh env for each selection test.

describe('runtime dispatcher', () => {
  const savedEnv = process.env.CONTAINER_RUNTIME;

  afterEach(() => {
    if (savedEnv === undefined) {
      delete process.env.CONTAINER_RUNTIME;
    } else {
      process.env.CONTAINER_RUNTIME = savedEnv;
    }
    vi.resetModules();
  });

  it('selects DockerRuntime when CONTAINER_RUNTIME=docker', async () => {
    process.env.CONTAINER_RUNTIME = 'docker';
    vi.resetModules();
    const mod = await import('./container-runtime.js');
    // Docker ensureRunning calls execSync('docker info', ...)
    mockExecSync.mockReturnValueOnce('');
    mod.ensureContainerRuntimeRunning();
    expect(mockExecSync).toHaveBeenCalledWith('docker info', expect.any(Object));
  });

  it('selects DockerRuntime when CONTAINER_RUNTIME is unset (default)', async () => {
    delete process.env.CONTAINER_RUNTIME;
    vi.resetModules();
    const mod = await import('./container-runtime.js');
    mockExecSync.mockReturnValueOnce('');
    mod.ensureContainerRuntimeRunning();
    expect(mockExecSync).toHaveBeenCalledWith('docker info', expect.any(Object));
  });

  it('selects K8sRuntime when CONTAINER_RUNTIME=k8s', async () => {
    process.env.CONTAINER_RUNTIME = 'k8s';
    vi.resetModules();
    const mod = await import('./container-runtime.js');
    // K8s ensureRunning is a no-op that calls logger.debug
    mod.ensureContainerRuntimeRunning();
    expect(logger.debug).toHaveBeenCalledWith(
      'Using Kubernetes Jobs API — no runtime check needed',
    );
    expect(mockExecSync).not.toHaveBeenCalled();
  });

  it('selects K8sRuntime when CONTAINER_RUNTIME=kubernetes', async () => {
    process.env.CONTAINER_RUNTIME = 'kubernetes';
    vi.resetModules();
    const mod = await import('./container-runtime.js');
    mod.ensureContainerRuntimeRunning();
    expect(logger.debug).toHaveBeenCalledWith(
      'Using Kubernetes Jobs API — no runtime check needed',
    );
  });

  it('exports the public API surface', async () => {
    vi.resetModules();
    const mod = await import('./container-runtime.js');
    expect(typeof mod.ensureContainerRuntimeRunning).toBe('function');
    expect(typeof mod.spawnJob).toBe('function');
    expect(typeof mod.stopJob).toBe('function');
    expect(typeof mod.streamJobLogs).toBe('function');
    expect(typeof mod.cleanupOrphans).toBe('function');
  });
});
