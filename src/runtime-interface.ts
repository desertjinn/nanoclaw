/**
 * Common interface for NanoClaw container runtimes.
 * Both Docker and Kubernetes implementations must satisfy this contract.
 */

export interface ContainerSpec {
  name: string;
  image: string;
  /** JSON payload (secrets + prompt) passed to the agent */
  stdin: string;
  env: Record<string, string>;
}

export interface ContainerHandle {
  name: string;
  /** Runtime-specific identifier (container name for Docker, namespace for k8s) */
  runtimeId: string;
}

export interface ContainerRuntime {
  /**
   * Ensure the runtime is available. Throws or logs fatal if not.
   * May be a no-op for runtimes that self-validate on first call.
   */
  ensureRunning(): void;

  /** Spawn a new agent container/Job. Returns a handle for later operations. */
  spawnContainer(spec: ContainerSpec): Promise<ContainerHandle>;

  /**
   * Stream stdout from the running container/Job, calling onChunk for each chunk.
   * Resolves with the exit code once the container/Job finishes or the timeout fires.
   */
  streamLogs(
    handle: ContainerHandle,
    onChunk: (chunk: string) => void,
    timeoutMs: number,
  ): Promise<number>;

  /** Best-effort stop/delete of a running container/Job. */
  stopContainer(handle: ContainerHandle): Promise<void>;

  /** Delete stale nanoclaw- containers/Jobs left from previous runs. */
  cleanupOrphans(): Promise<void>;
}
