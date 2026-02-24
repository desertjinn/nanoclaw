/**
 * Docker runtime implementation for NanoClaw.
 * Uses the Docker CLI to spawn per-request agent containers.
 * Suitable for local development and non-k8s deployments.
 */
import { ChildProcess, exec, execSync, spawn } from 'child_process';

import { CONTAINER_MAX_OUTPUT_SIZE } from './config.js';
import { logger } from './logger.js';
import { ContainerHandle, ContainerRuntime, ContainerSpec } from './runtime-interface.js';

const RUNTIME_BIN = 'docker';

export class DockerRuntime implements ContainerRuntime {
  ensureRunning(): void {
    try {
      execSync(`${RUNTIME_BIN} info`, { stdio: 'pipe', timeout: 10000 });
      logger.debug('Container runtime already running');
    } catch (err) {
      logger.error({ err }, 'Failed to reach container runtime');
      console.error(
        '\n╔════════════════════════════════════════════════════════════════╗',
      );
      console.error(
        '║  FATAL: Container runtime failed to start                      ║',
      );
      console.error(
        '║                                                                ║',
      );
      console.error(
        '║  Agents cannot run without a container runtime. To fix:        ║',
      );
      console.error(
        '║  1. Ensure Docker is installed and running                     ║',
      );
      console.error(
        '║  2. Run: docker info                                           ║',
      );
      console.error(
        '║  3. Restart NanoClaw                                           ║',
      );
      console.error(
        '╚════════════════════════════════════════════════════════════════╝\n',
      );
      throw new Error('Container runtime is required but failed to start');
    }
  }

  async spawnContainer(spec: ContainerSpec): Promise<ContainerHandle> {
    // Store the spec so streamLogs can spawn the process
    this._pendingSpecs.set(spec.name, spec);
    return { name: spec.name, runtimeId: spec.name };
  }

  async streamLogs(
    handle: ContainerHandle,
    onChunk: (chunk: string) => void,
    timeoutMs: number,
  ): Promise<number> {
    const spec = this._pendingSpecs.get(handle.name);
    this._pendingSpecs.delete(handle.name);
    if (!spec) throw new Error(`No pending spec for container ${handle.name}`);

    const args = this._buildArgs(spec);

    return new Promise((resolve) => {
      const proc: ChildProcess = spawn(RUNTIME_BIN, args, {
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      this._processes.set(handle.name, proc);

      let stdout = '';
      let stdoutTruncated = false;
      let timedOut = false;

      proc.stdin!.write(spec.stdin);
      proc.stdin!.end();

      proc.stdout!.on('data', (data: Buffer) => {
        const chunk = data.toString();
        if (!stdoutTruncated) {
          const remaining = CONTAINER_MAX_OUTPUT_SIZE - stdout.length;
          if (chunk.length > remaining) {
            stdout += chunk.slice(0, remaining);
            stdoutTruncated = true;
          } else {
            stdout += chunk;
          }
        }
        onChunk(chunk);
      });

      proc.stderr!.on('data', (data: Buffer) => {
        const lines = data.toString().trim().split('\n');
        for (const line of lines) {
          if (line) logger.debug({ container: handle.name }, line);
        }
      });

      const killTimer = setTimeout(() => {
        timedOut = true;
        exec(`${RUNTIME_BIN} stop ${handle.name}`, { timeout: 15000 }, (err) => {
          if (err) proc.kill('SIGKILL');
        });
      }, timeoutMs);

      proc.on('close', (code) => {
        clearTimeout(killTimer);
        this._processes.delete(handle.name);
        resolve(timedOut ? 1 : (code ?? 1));
      });

      proc.on('error', (err) => {
        clearTimeout(killTimer);
        this._processes.delete(handle.name);
        logger.error({ container: handle.name, err }, 'Docker spawn error');
        resolve(1);
      });
    });
  }

  async stopContainer(handle: ContainerHandle): Promise<void> {
    const proc = this._processes.get(handle.name);
    return new Promise((resolve) => {
      exec(`${RUNTIME_BIN} stop ${handle.name}`, { timeout: 15000 }, (err) => {
        if (err && proc) proc.kill('SIGKILL');
        resolve();
      });
    });
  }

  async cleanupOrphans(): Promise<void> {
    try {
      const output = execSync(
        `${RUNTIME_BIN} ps --filter name=nanoclaw- --format '{{.Names}}'`,
        { stdio: ['pipe', 'pipe', 'pipe'], encoding: 'utf-8' },
      );
      const orphans = output.trim().split('\n').filter(Boolean);
      for (const name of orphans) {
        try {
          execSync(`${RUNTIME_BIN} stop ${name}`, { stdio: 'pipe' });
        } catch { /* already stopped */ }
      }
      if (orphans.length > 0) {
        logger.info({ count: orphans.length, names: orphans }, 'Stopped orphaned containers');
      }
    } catch (err) {
      logger.warn({ err }, 'Failed to clean up orphaned containers');
    }
  }

  private _buildArgs(spec: ContainerSpec): string[] {
    const args: string[] = ['run', '-i', '--rm', '--name', spec.name];
    args.push('-e', `TZ=${spec.env.TZ ?? 'UTC'}`);

    const hostUid = process.getuid?.();
    const hostGid = process.getgid?.();
    if (hostUid != null && hostUid !== 0 && hostUid !== 1000) {
      args.push('--user', `${hostUid}:${hostGid}`);
      args.push('-e', 'HOME=/home/node');
    }

    for (const [k, v] of Object.entries(spec.env)) {
      if (k !== 'TZ') args.push('-e', `${k}=${v}`);
    }

    args.push(spec.image);
    return args;
  }

  private _pendingSpecs = new Map<string, ContainerSpec>();
  private _processes = new Map<string, ChildProcess>();
}
