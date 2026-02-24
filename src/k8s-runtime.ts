/**
 * Kubernetes Jobs runtime implementation for NanoClaw.
 * Uses the Kubernetes Jobs API to spawn per-request agent containers.
 * Suitable for k3s/k8s clusters (e.g. Talos OS) where Docker is not available.
 */
import { BatchV1Api, CoreV1Api, KubeConfig } from '@kubernetes/client-node';

import { CONTAINER_TIMEOUT } from './config.js';
import { logger } from './logger.js';
import { ContainerHandle, ContainerRuntime, ContainerSpec } from './runtime-interface.js';

const NAMESPACE = 'nanoclaw';
const JOB_POLL_INTERVAL_MS = 1000;
const PVC_NAME = 'nanoclaw-workspace';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getK8sClients(): { batch: BatchV1Api; core: CoreV1Api } {
  const kc = new KubeConfig();
  kc.loadFromCluster();
  return {
    batch: kc.makeApiClient(BatchV1Api),
    core: kc.makeApiClient(CoreV1Api),
  };
}

export class K8sRuntime implements ContainerRuntime {
  /** No-op: k8s client validates connectivity on first API call. */
  ensureRunning(): void {
    logger.debug('Using Kubernetes Jobs API — no runtime check needed');
  }

  async spawnContainer(spec: ContainerSpec): Promise<ContainerHandle> {
    const { batch } = getK8sClients();

    // groupFolder comes in via env; filter it out before forwarding to the container
    const groupFolder = spec.env.NANOCLAW_GROUP ?? '';
    const envVars = Object.entries(spec.env)
      .filter(([name]) => name !== 'NANOCLAW_GROUP')
      .map(([name, value]) => ({ name, value }));

    const jobManifest = {
      apiVersion: 'batch/v1',
      kind: 'Job',
      metadata: {
        name: spec.name,
        namespace: NAMESPACE,
        labels: {
          'app.kubernetes.io/name': 'nanoclaw-agent',
          'app.kubernetes.io/component': 'agent',
          'nanoclaw/group': groupFolder,
        },
      },
      spec: {
        ttlSecondsAfterFinished: 60,
        backoffLimit: 0,
        template: {
          metadata: {
            labels: {
              'app.kubernetes.io/name': 'nanoclaw-agent',
              'app.kubernetes.io/component': 'agent',
              'nanoclaw/group': groupFolder,
            },
          },
          spec: {
            serviceAccountName: 'nanoclaw-agent',
            automountServiceAccountToken: false,
            restartPolicy: 'Never',
            securityContext: {
              runAsNonRoot: true,
              runAsUser: 1000,
              runAsGroup: 1000,
              fsGroup: 1000,
              seccompProfile: { type: 'RuntimeDefault' },
            },
            containers: [
              {
                name: 'agent',
                image: spec.image,
                env: envVars,
                resources: {
                  requests: { cpu: '100m', memory: '256Mi' },
                  limits: { cpu: '500m', memory: '1Gi' },
                },
                securityContext: {
                  allowPrivilegeEscalation: false,
                  readOnlyRootFilesystem: true,
                  capabilities: { drop: ['ALL'] },
                },
                volumeMounts: [
                  // Agent hardcodes IPC_INPUT_DIR = '/workspace/ipc/input'
                  // so mount at /workspace/ipc, not /workspace/ipc/<group>
                  {
                    name: 'workspace',
                    mountPath: '/workspace/ipc',
                    subPath: `ipc/${groupFolder}`,
                  },
                  {
                    name: 'workspace',
                    mountPath: `/workspace/groups/${groupFolder}`,
                    subPath: `groups/${groupFolder}`,
                  },
                  { name: 'tmp', mountPath: '/tmp' },
                  { name: 'npm-cache', mountPath: '/home/node/.npm' },
                ],
              },
            ],
            volumes: [
              {
                name: 'workspace',
                persistentVolumeClaim: { claimName: PVC_NAME },
              },
              { name: 'tmp', emptyDir: { sizeLimit: '200Mi' } },
              { name: 'npm-cache', emptyDir: { sizeLimit: '100Mi' } },
            ],
          },
        },
      },
    };

    await batch.createNamespacedJob({ namespace: NAMESPACE, body: jobManifest });
    logger.info({ jobName: spec.name, namespace: NAMESPACE, groupFolder }, 'Kubernetes Job created');

    return { name: spec.name, runtimeId: NAMESPACE };
  }

  async streamLogs(
    handle: ContainerHandle,
    onChunk: (chunk: string) => void,
    timeoutMs: number = CONTAINER_TIMEOUT,
  ): Promise<number> {
    const { batch, core } = getK8sClients();
    const deadline = Date.now() + timeoutMs;

    // Wait for the pod to exist
    const podName = await this._waitForJobPod(handle.name, handle.runtimeId, deadline, core);
    if (!podName) {
      throw new Error(`Timed out waiting for pod for Job ${handle.name}`);
    }

    // Wait for pod to be in a runnable phase before streaming
    await this._waitForPodRunning(podName, handle.runtimeId, deadline, core);

    // Stream logs until the pod exits or timeout fires
    const remainingMs = Math.max(0, deadline - Date.now());
    try {
      const logResponse = await core.readNamespacedPodLog({
        name: podName,
        namespace: handle.runtimeId,
        follow: true,
        timestamps: false,
      }) as unknown as NodeJS.ReadableStream;

      await new Promise<void>((resolve) => {
        const killTimer = setTimeout(() => {
          (logResponse as { destroy?: () => void }).destroy?.();
          resolve();
        }, remainingMs);

        logResponse.on('data', (chunk: Buffer | string) => {
          onChunk(typeof chunk === 'string' ? chunk : chunk.toString());
        });
        logResponse.on('end', () => {
          clearTimeout(killTimer);
          resolve();
        });
        logResponse.on('error', (err: Error) => {
          clearTimeout(killTimer);
          logger.warn({ podName, err }, 'Pod log stream error');
          resolve();
        });
      });
    } catch (err) {
      logger.warn({ podName, err }, 'Pod log stream failed');
    }

    // Poll Job status for final exit code
    while (Date.now() < deadline) {
      try {
        const job = await batch.readNamespacedJob({ name: handle.name, namespace: handle.runtimeId });
        if ((job.status?.succeeded ?? 0) > 0) return 0;
        if ((job.status?.failed ?? 0) > 0) return 1;
      } catch { /* job may have been cleaned up by TTL */ }
      await sleep(JOB_POLL_INTERVAL_MS);
    }

    return 1; // timed out
  }

  async stopContainer(handle: ContainerHandle): Promise<void> {
    const { batch } = getK8sClients();
    try {
      await batch.deleteNamespacedJob({
        name: handle.name,
        namespace: handle.runtimeId,
        body: { propagationPolicy: 'Background' },
      });
      logger.info({ jobName: handle.name }, 'Kubernetes Job deleted');
    } catch (err) {
      logger.warn({ jobName: handle.name, err }, 'Failed to delete Kubernetes Job');
    }
  }

  async cleanupOrphans(): Promise<void> {
    const { batch } = getK8sClients();
    try {
      const res = await batch.listNamespacedJob({
        namespace: NAMESPACE,
        labelSelector: 'app.kubernetes.io/name=nanoclaw-agent',
      });
      const stale = (res.items ?? []).filter(
        (job) => !job.status?.completionTime,
      );
      for (const job of stale) {
        const name = job.metadata?.name;
        if (!name) continue;
        try {
          await batch.deleteNamespacedJob({
            name,
            namespace: NAMESPACE,
            body: { propagationPolicy: 'Background' },
          });
        } catch { /* already gone */ }
      }
      if (stale.length > 0) {
        logger.info({ count: stale.length }, 'Deleted stale Kubernetes Jobs');
      }
    } catch (err) {
      logger.warn({ err }, 'Failed to clean up orphaned Kubernetes Jobs');
    }
  }

  private async _waitForJobPod(
    jobName: string,
    namespace: string,
    deadline: number,
    core: CoreV1Api,
  ): Promise<string | undefined> {
    while (Date.now() < deadline) {
      const pods = await core.listNamespacedPod({
        namespace,
        labelSelector: `job-name=${jobName}`,
      });
      const pod = pods.items?.[0];
      if (pod?.metadata?.name) {
        return pod.metadata.name;
      }
      await sleep(JOB_POLL_INTERVAL_MS);
    }
    return undefined;
  }

  private async _waitForPodRunning(
    podName: string,
    namespace: string,
    deadline: number,
    core: CoreV1Api,
  ): Promise<void> {
    const coreWithPod = core as unknown as {
      readNamespacedPod?: (args: { name: string; namespace: string }) => Promise<{
        status?: { phase?: string };
      }>;
    };
    while (Date.now() < deadline) {
      try {
        const pod = await coreWithPod.readNamespacedPod?.({ name: podName, namespace });
        const phase = pod?.status?.phase;
        if (phase === 'Running' || phase === 'Succeeded' || phase === 'Failed') {
          return;
        }
      } catch { /* pod may not be ready yet */ }
      await sleep(JOB_POLL_INTERVAL_MS);
    }
  }
}
