import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TaskResponse } from "../types";

const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;
const DEFAULT_CWD = "/workspace";
const SAFE_SANDBOXES = new Set(["read-only", "workspace-write"]);
const WEB_SEARCH_MODES = new Set(["disabled", "cached", "live"]);

export interface CodexRunOptions {
  binary?: string;
  cwd?: string;
  timeoutMs?: number;
  model?: string;
  sandbox?: string;
  webSearch?: string;
  outputRoot?: string;
  env?: Record<string, string | undefined>;
  requireAuth?: boolean;
  authPath?: string;
}

function positiveTimeout(value: number | undefined): number {
  if (!value || !Number.isFinite(value) || value <= 0) return DEFAULT_TIMEOUT_MS;
  return Math.min(Math.trunc(value), 30 * 60 * 1000);
}

function safeSandbox(value: string | undefined): string {
  const sandbox = value || "read-only";
  if (!SAFE_SANDBOXES.has(sandbox)) {
    throw new Error(`unsupported CODEX_SANDBOX: ${sandbox}`);
  }
  return sandbox;
}

function safeWebSearch(value: string | undefined): string {
  const mode = value || "live";
  if (!WEB_SEARCH_MODES.has(mode)) {
    throw new Error(`unsupported CODEX_WEB_SEARCH: ${mode}`);
  }
  return mode;
}

export function buildCodexArgs(
  outputPath: string,
  options: CodexRunOptions = {},
): string[] {
  const args = [
    "exec",
    "--ephemeral",
    "--ignore-user-config",
    "--ignore-rules",
    "--skip-git-repo-check",
    "--color",
    "never",
    "--sandbox",
    safeSandbox(options.sandbox),
    "-c",
    'approval_policy="never"',
    "-c",
    `web_search="${safeWebSearch(options.webSearch)}"`,
  ];
  if (options.model) {
    args.push("--model", options.model);
  }
  args.push("--output-last-message", outputPath, "-");
  return args;
}

function authAvailable(options: CodexRunOptions): boolean {
  const env = options.env || process.env;
  if (env.CODEX_API_KEY) return true;
  const authPath =
    options.authPath ||
    join(env.HOME || "/home/worker", ".codex", "auth.json");
  return existsSync(authPath);
}

async function stopProcess(proc: ReturnType<typeof Bun.spawn>): Promise<void> {
  try {
    proc.kill("SIGTERM");
  } catch {
    return;
  }
  await Promise.race([
    proc.exited.then(() => undefined),
    Bun.sleep(1000).then(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // The process exited between the timer and the signal.
      }
    }),
  ]);
}

export async function executeTaskWithOptions(
  prompt: string,
  options: CodexRunOptions = {},
): Promise<TaskResponse> {
  if (!prompt.trim()) throw new Error("codex prompt is empty");
  if (options.requireAuth && !authAvailable(options)) {
    throw new Error("codex authentication is not configured");
  }

  const startedAt = Date.now();
  const timeoutMs = positiveTimeout(options.timeoutMs);
  const outputDir = await mkdtemp(
    join(options.outputRoot || tmpdir(), "mecha-codex-"),
  );
  const outputPath = join(outputDir, "last-message.txt");
  const binary = options.binary || "codex";
  const args = buildCodexArgs(outputPath, options);
  let proc: ReturnType<typeof Bun.spawn> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;

  try {
    proc = Bun.spawn([binary, ...args], {
      cwd: options.cwd || DEFAULT_CWD,
      env: options.env || process.env,
      stdin: "pipe",
      stdout: "ignore",
      stderr: "ignore",
    });
    proc.stdin.write(prompt);
    proc.stdin.end();

    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("codex task timed out")), timeoutMs);
    });
    const exitCode = await Promise.race([proc.exited, timeout]);
    if (exitCode !== 0) {
      throw new Error(`codex process exited with code ${exitCode}`);
    }

    const output = (await readFile(outputPath, "utf8")).trim();
    if (!output) throw new Error("codex returned an empty response");

    return {
      output,
      metadata: {
        model: options.model,
        duration_ms: Date.now() - startedAt,
        exit_code: 0,
      },
    };
  } catch (error) {
    if (proc) await stopProcess(proc);
    if (error instanceof Error) throw error;
    throw new Error("codex execution failed");
  } finally {
    if (timer) clearTimeout(timer);
    await rm(outputDir, { recursive: true, force: true });
  }
}

/** Execute a task directly with Codex CLI. No Claude credential is required. */
export async function executeTask(prompt: string): Promise<TaskResponse> {
  const configuredTimeout = Number.parseInt(
    process.env.CODEX_TIMEOUT_MS || `${DEFAULT_TIMEOUT_MS}`,
    10,
  );
  return executeTaskWithOptions(prompt, {
    timeoutMs: configuredTimeout,
    model: process.env.CODEX_MODEL,
    sandbox: process.env.CODEX_SANDBOX,
    webSearch: process.env.CODEX_WEB_SEARCH,
    requireAuth: true,
  });
}
