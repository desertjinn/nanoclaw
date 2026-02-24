/**
 * Container Runner for NanoClaw
 * Spawns agent execution as Kubernetes Jobs and handles IPC
 */
import fs from 'fs';
import path from 'path';

import {
  CONTAINER_IMAGE,
  CONTAINER_MAX_OUTPUT_SIZE,
  CONTAINER_TIMEOUT,
  DATA_DIR,
  GROUPS_DIR,
  IDLE_TIMEOUT,
  TIMEZONE,
} from './config.js';
import { readEnvFile } from './env.js';
import { resolveGroupFolderPath, resolveGroupIpcPath } from './group-folder.js';
import { logger } from './logger.js';
import { JobHandle, spawnJob, stopJob, streamJobLogs } from './container-runtime.js';
import { RegisteredGroup } from './types.js';

// Sentinel markers for robust output parsing (must match agent-runner)
const OUTPUT_START_MARKER = '---NANOCLAW_OUTPUT_START---';
const OUTPUT_END_MARKER = '---NANOCLAW_OUTPUT_END---';

export interface ContainerInput {
  prompt: string;
  sessionId?: string;
  /** lastAssistantUuid from the previous Job — passed as resumeAt to the SDK */
  resumeAt?: string;
  groupFolder: string;
  chatJid: string;
  isMain: boolean;
  isScheduledTask?: boolean;
  assistantName?: string;
  secrets?: Record<string, string>;
}

export interface ContainerOutput {
  status: 'success' | 'error';
  result: string | null;
  newSessionId?: string;
  lastAssistantUuid?: string;
  error?: string;
}

/**
 * Read allowed secrets from .env for passing to the container via stdin.
 * Secrets are never written to disk or mounted as files.
 */
function readSecrets(): Record<string, string> {
  return readEnvFile(['CLAUDE_CODE_OAUTH_TOKEN', 'ANTHROPIC_API_KEY']);
}

function setupGroupDirectories(group: RegisteredGroup): void {
  const projectRoot = process.cwd();
  const groupDir = resolveGroupFolderPath(group.folder);

  // Per-group Claude sessions directory (isolated from other groups)
  const groupSessionsDir = path.join(DATA_DIR, 'sessions', group.folder, '.claude');
  fs.mkdirSync(groupSessionsDir, { recursive: true });
  const settingsFile = path.join(groupSessionsDir, 'settings.json');
  if (!fs.existsSync(settingsFile)) {
    fs.writeFileSync(settingsFile, JSON.stringify({
      env: {
        CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS: '1',
        CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD: '1',
        CLAUDE_CODE_DISABLE_AUTO_MEMORY: '0',
      },
    }, null, 2) + '\n');
  }

  // Sync skills from container/skills/ into each group's .claude/skills/
  const skillsSrc = path.join(projectRoot, 'container', 'skills');
  const skillsDst = path.join(groupSessionsDir, 'skills');
  if (fs.existsSync(skillsSrc)) {
    for (const skillDir of fs.readdirSync(skillsSrc)) {
      const srcDir = path.join(skillsSrc, skillDir);
      if (!fs.statSync(srcDir).isDirectory()) continue;
      const dstDir = path.join(skillsDst, skillDir);
      fs.cpSync(srcDir, dstDir, { recursive: true });
    }
  }

  // Per-group IPC namespace
  const groupIpcDir = resolveGroupIpcPath(group.folder);
  fs.mkdirSync(path.join(groupIpcDir, 'messages'), { recursive: true });
  fs.mkdirSync(path.join(groupIpcDir, 'tasks'), { recursive: true });
  fs.mkdirSync(path.join(groupIpcDir, 'input'), { recursive: true });

  // Copy agent-runner source into a per-group writable location
  const agentRunnerSrc = path.join(projectRoot, 'container', 'agent-runner', 'src');
  const groupAgentRunnerDir = path.join(DATA_DIR, 'sessions', group.folder, 'agent-runner-src');
  if (!fs.existsSync(groupAgentRunnerDir) && fs.existsSync(agentRunnerSrc)) {
    fs.cpSync(agentRunnerSrc, groupAgentRunnerDir, { recursive: true });
  }

  // Global memory directory for non-main groups (ensure it exists)
  if (!group.folder.startsWith('main')) {
    const globalDir = path.join(GROUPS_DIR, 'global');
    if (fs.existsSync(globalDir)) {
      // Not mounted in k8s — orchestrator copies data to PVC before spawning
    }
  }

  void groupDir;
}

export async function runContainerAgent(
  group: RegisteredGroup,
  input: ContainerInput,
  onProcess: (job: JobHandle, containerName: string) => void,
  onOutput?: (output: ContainerOutput) => Promise<void>,
): Promise<ContainerOutput> {
  const startTime = Date.now();

  const groupDir = resolveGroupFolderPath(group.folder);
  fs.mkdirSync(groupDir, { recursive: true });

  setupGroupDirectories(group);

  const safeName = group.folder.replace(/[^a-zA-Z0-9-]/g, '-').toLowerCase();
  const containerName = `nanoclaw-${safeName}-${Date.now()}`;

  logger.info(
    { group: group.name, containerName, isMain: input.isMain },
    'Spawning agent Job',
  );

  const logsDir = path.join(groupDir, 'logs');
  fs.mkdirSync(logsDir, { recursive: true });

  // Write init payload atomically to PVC IPC dir BEFORE spawning the Job.
  // The agent reads this file at startup (Docker falls back to stdin).
  const ipcInputDir = path.join(resolveGroupIpcPath(group.folder), 'input');
  fs.mkdirSync(ipcInputDir, { recursive: true });
  // Clean up any stale init files from previous failed spawns
  for (const f of fs.readdirSync(ipcInputDir).filter((n) => n.startsWith('init-'))) {
    try { fs.unlinkSync(path.join(ipcInputDir, f)); } catch { /* already gone */ }
  }
  const initFile = path.join(ipcInputDir, `init-${Date.now()}.json`);
  const payload = { ...input, secrets: readSecrets() };
  const tempPath = `${initFile}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(payload));
  fs.renameSync(tempPath, initFile); // atomic
  delete (payload as Partial<typeof payload>).secrets;

  // Keep stdin as a fallback for DockerRuntime (K8sRuntime ignores it)
  const stdinJson = JSON.stringify({ ...input, secrets: readSecrets() });

  const configTimeout = group.containerConfig?.timeout || CONTAINER_TIMEOUT;
  const timeoutMs = Math.max(configTimeout, IDLE_TIMEOUT + 30_000);

  let jobHandle: JobHandle;
  try {
    jobHandle = await spawnJob({
      name: containerName,
      image: CONTAINER_IMAGE,
      stdin: stdinJson,
      env: { TZ: TIMEZONE, NANOCLAW_GROUP: group.folder },
    });
  } catch (err) {
    logger.error({ group: group.name, containerName, err }, 'Failed to spawn agent Job');
    // Clean up the init file so it doesn't confuse the next spawn
    try { fs.unlinkSync(initFile); } catch { /* already gone */ }
    return {
      status: 'error',
      result: null,
      error: `Failed to spawn agent Job: ${err instanceof Error ? err.message : String(err)}`,
    };
  }

  // Signal handle to caller (JobHandle — used by GroupQueue to track the running Job)
  onProcess(jobHandle, containerName);

  let stdout = '';
  let stdoutTruncated = false;
  let timedOut = false;
  let hadStreamingOutput = false;
  let newSessionId: string | undefined;
  let lastAssistantUuid: string | undefined;
  let outputChain = Promise.resolve();
  let parseBuffer = '';

  let hardTimeout: ReturnType<typeof setTimeout> | undefined;

  const killOnTimeout = async () => {
    timedOut = true;
    logger.error({ group: group.name, containerName }, 'Job timeout, stopping gracefully');
    await stopJob(jobHandle);
  };

  hardTimeout = setTimeout(() => { void killOnTimeout(); }, timeoutMs);

  const resetTimeout = () => {
    clearTimeout(hardTimeout);
    hardTimeout = setTimeout(() => { void killOnTimeout(); }, timeoutMs);
  };

  const onChunk = (chunk: string) => {
    if (!stdoutTruncated) {
      const remaining = CONTAINER_MAX_OUTPUT_SIZE - stdout.length;
      if (chunk.length > remaining) {
        stdout += chunk.slice(0, remaining);
        stdoutTruncated = true;
        logger.warn({ group: group.name, size: stdout.length }, 'Job stdout truncated due to size limit');
      } else {
        stdout += chunk;
      }
    }

    if (onOutput) {
      parseBuffer += chunk;
      let startIdx: number;
      while ((startIdx = parseBuffer.indexOf(OUTPUT_START_MARKER)) !== -1) {
        const endIdx = parseBuffer.indexOf(OUTPUT_END_MARKER, startIdx);
        if (endIdx === -1) break;

        const jsonStr = parseBuffer
          .slice(startIdx + OUTPUT_START_MARKER.length, endIdx)
          .trim();
        parseBuffer = parseBuffer.slice(endIdx + OUTPUT_END_MARKER.length);

        try {
          const parsed: ContainerOutput = JSON.parse(jsonStr);
          if (parsed.newSessionId) newSessionId = parsed.newSessionId;
          if (parsed.lastAssistantUuid) lastAssistantUuid = parsed.lastAssistantUuid;
          hadStreamingOutput = true;
          resetTimeout();
          outputChain = outputChain.then(() => onOutput(parsed));
        } catch (err) {
          logger.warn({ group: group.name, error: err }, 'Failed to parse streamed output chunk');
        }
      }
    }
  };

  let exitCode: number;
  try {
    exitCode = await streamJobLogs(jobHandle, onChunk, timeoutMs);
  } catch (err) {
    clearTimeout(hardTimeout);
    logger.error({ group: group.name, containerName, err }, 'Job log stream failed');
    return {
      status: 'error',
      result: null,
      error: `Job log stream failed: ${err instanceof Error ? err.message : String(err)}`,
    };
  }

  clearTimeout(hardTimeout);
  const duration = Date.now() - startTime;

  if (timedOut) {
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const timeoutLog = path.join(logsDir, `container-${ts}.log`);
    fs.writeFileSync(timeoutLog, [
      `=== Job Run Log (TIMEOUT) ===`,
      `Timestamp: ${new Date().toISOString()}`,
      `Group: ${group.name}`,
      `Job: ${containerName}`,
      `Duration: ${duration}ms`,
      `Exit Code: ${exitCode}`,
      `Had Streaming Output: ${hadStreamingOutput}`,
    ].join('\n'));

    if (hadStreamingOutput) {
      logger.info({ group: group.name, containerName, duration }, 'Job timed out after output (idle cleanup)');
      return new Promise((resolve) => {
        outputChain.then(() => resolve({ status: 'success', result: null, newSessionId, lastAssistantUuid }));
      });
    }

    logger.error({ group: group.name, containerName, duration }, 'Job timed out with no output');
    return { status: 'error', result: null, error: `Job timed out after ${configTimeout}ms` };
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const logFile = path.join(logsDir, `container-${timestamp}.log`);
  const isVerbose = process.env.LOG_LEVEL === 'debug' || process.env.LOG_LEVEL === 'trace';

  const logLines = [
    `=== Job Run Log ===`,
    `Timestamp: ${new Date().toISOString()}`,
    `Group: ${group.name}`,
    `IsMain: ${input.isMain}`,
    `Duration: ${duration}ms`,
    `Exit Code: ${exitCode}`,
    `Stdout Truncated: ${stdoutTruncated}`,
    ``,
  ];

  if (isVerbose || exitCode !== 0) {
    logLines.push(
      `=== Input ===`,
      JSON.stringify(input, null, 2),
      ``,
      `=== Job Name ===`,
      containerName,
      ``,
      `=== Stdout${stdoutTruncated ? ' (TRUNCATED)' : ''} ===`,
      stdout,
    );
  } else {
    logLines.push(
      `=== Input Summary ===`,
      `Prompt length: ${input.prompt.length} chars`,
      `Session ID: ${input.sessionId || 'new'}`,
      ``,
    );
  }

  fs.writeFileSync(logFile, logLines.join('\n'));
  logger.debug({ logFile, verbose: isVerbose }, 'Job log written');

  if (exitCode !== 0) {
    logger.error({ group: group.name, exitCode, duration, stdout, logFile }, 'Job exited with error');
    return {
      status: 'error',
      result: null,
      error: `Job exited with code ${exitCode}: ${stdout.slice(-200)}`,
    };
  }

  // Streaming mode
  if (onOutput) {
    return new Promise((resolve) => {
      outputChain.then(() => {
        logger.info({ group: group.name, duration, newSessionId }, 'Job completed (streaming mode)');
        resolve({ status: 'success', result: null, newSessionId, lastAssistantUuid });
      });
    });
  }

  // Legacy mode: parse last output marker pair from accumulated stdout
  try {
    const startIdx = stdout.indexOf(OUTPUT_START_MARKER);
    const endIdx = stdout.indexOf(OUTPUT_END_MARKER);

    let jsonLine: string;
    if (startIdx !== -1 && endIdx !== -1 && endIdx > startIdx) {
      jsonLine = stdout.slice(startIdx + OUTPUT_START_MARKER.length, endIdx).trim();
    } else {
      const lines = stdout.trim().split('\n');
      jsonLine = lines[lines.length - 1];
    }

    const output: ContainerOutput = JSON.parse(jsonLine);
    logger.info({ group: group.name, duration, status: output.status, hasResult: !!output.result }, 'Job completed');
    return output;
  } catch (err) {
    logger.error({ group: group.name, stdout, error: err }, 'Failed to parse job output');
    return {
      status: 'error',
      result: null,
      error: `Failed to parse job output: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}

export function writeTasksSnapshot(
  groupFolder: string,
  isMain: boolean,
  tasks: Array<{
    id: string;
    groupFolder: string;
    prompt: string;
    schedule_type: string;
    schedule_value: string;
    status: string;
    next_run: string | null;
  }>,
): void {
  const groupIpcDir = resolveGroupIpcPath(groupFolder);
  fs.mkdirSync(groupIpcDir, { recursive: true });

  const filteredTasks = isMain
    ? tasks
    : tasks.filter((t) => t.groupFolder === groupFolder);

  const tasksFile = path.join(groupIpcDir, 'current_tasks.json');
  fs.writeFileSync(tasksFile, JSON.stringify(filteredTasks, null, 2));
}

export interface AvailableGroup {
  jid: string;
  name: string;
  lastActivity: string;
  isRegistered: boolean;
}

/**
 * Write available groups snapshot for the container to read.
 * Only main group can see all available groups (for activation).
 * Non-main groups only see their own registration status.
 */
export function writeGroupsSnapshot(
  groupFolder: string,
  isMain: boolean,
  groups: AvailableGroup[],
  registeredJids: Set<string>,
): void {
  const groupIpcDir = resolveGroupIpcPath(groupFolder);
  fs.mkdirSync(groupIpcDir, { recursive: true });

  const visibleGroups = isMain ? groups : [];

  const groupsFile = path.join(groupIpcDir, 'available_groups.json');
  fs.writeFileSync(
    groupsFile,
    JSON.stringify(
      {
        groups: visibleGroups,
        lastSync: new Date().toISOString(),
      },
      null,
      2,
    ),
  );
}
