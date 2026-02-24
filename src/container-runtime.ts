/**
 * Container runtime dispatcher for NanoClaw.
 *
 * Selects the backend at startup based on the CONTAINER_RUNTIME env var:
 *   CONTAINER_RUNTIME=k8s    → Kubernetes Jobs API (default for cluster deployments)
 *   CONTAINER_RUNTIME=docker → Docker CLI          (default for local dev)
 *
 * Both backends implement the ContainerRuntime interface from runtime-interface.ts.
 * To add a new runtime, implement ContainerRuntime and add a case below.
 */
import { DockerRuntime } from './docker-runtime.js';
import { K8sRuntime } from './k8s-runtime.js';
import { logger } from './logger.js';
import { ContainerHandle, ContainerRuntime, ContainerSpec } from './runtime-interface.js';

export type { ContainerHandle, ContainerRuntime, ContainerSpec };
export type { ContainerSpec as JobSpec };   // backwards-compat alias used by container-runner
export type { ContainerHandle as JobHandle }; // backwards-compat alias

function selectRuntime(): ContainerRuntime {
  const runtimeEnv = (process.env.CONTAINER_RUNTIME ?? 'docker').toLowerCase();
  switch (runtimeEnv) {
    case 'k8s':
    case 'kubernetes':
      logger.info('Container runtime: Kubernetes Jobs');
      return new K8sRuntime();
    case 'docker':
    default:
      logger.info('Container runtime: Docker');
      return new DockerRuntime();
  }
}

const runtime: ContainerRuntime = selectRuntime();

// --- Public API (delegates to the selected runtime) ---

export function ensureContainerRuntimeRunning(): void {
  runtime.ensureRunning();
}

export async function spawnJob(spec: ContainerSpec): Promise<ContainerHandle> {
  return runtime.spawnContainer(spec);
}

export async function stopJob(handle: ContainerHandle): Promise<void> {
  return runtime.stopContainer(handle);
}

export async function streamJobLogs(
  handle: ContainerHandle,
  onChunk: (chunk: string) => void,
  timeoutMs: number,
): Promise<number> {
  return runtime.streamLogs(handle, onChunk, timeoutMs);
}

export async function cleanupOrphans(): Promise<void> {
  return runtime.cleanupOrphans();
}
