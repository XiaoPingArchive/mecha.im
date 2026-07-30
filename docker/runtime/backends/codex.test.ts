import { afterEach, describe, expect, test } from "bun:test";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildCodexArgs, executeTaskWithOptions } from "./codex";

const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(
    tempDirs.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function fakeCodex(body: string): Promise<{ binary: string; root: string }> {
  const root = await mkdtemp(join(tmpdir(), "fake-codex-"));
  tempDirs.push(root);
  const binary = join(root, "codex");
  await writeFile(binary, `#!/bin/sh\nset -eu\n${body}\n`, "utf8");
  await chmod(binary, 0o755);
  return { binary, root };
}

describe("Codex argument policy", () => {
  test("uses stdin, an output file, and safe defaults", () => {
    const args = buildCodexArgs("/tmp/result.txt");
    expect(args.at(-1)).toBe("-");
    expect(args).toContain("--ephemeral");
    expect(args).toContain("--ignore-user-config");
    expect(args).toContain("--ignore-rules");
    expect(args).toContain("read-only");
    expect(args).not.toContain("--dangerously-bypass-approvals-and-sandbox");
  });

  test("rejects danger-full-access", () => {
    expect(() =>
      buildCodexArgs("/tmp/result.txt", { sandbox: "danger-full-access" }),
    ).toThrow("unsupported CODEX_SANDBOX");
  });

  test("rejects unknown web-search mode", () => {
    expect(() =>
      buildCodexArgs("/tmp/result.txt", { webSearch: "anything" }),
    ).toThrow("unsupported CODEX_WEB_SEARCH");
  });
});

describe("direct Codex execution", () => {
  test("passes the prompt only through stdin and returns the last message", async () => {
    const fake = await fakeCodex(`
out=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then
    shift
    out="$1"
  fi
  shift
done
prompt="$(cat)"
printf 'reply:%s' "$prompt" > "$out"
`);
    const result = await executeTaskWithOptions("line 1\nline 2; $(unsafe)", {
      binary: fake.binary,
      cwd: fake.root,
      outputRoot: fake.root,
      timeoutMs: 2000,
    });
    expect(result.output).toBe("reply:line 1\nline 2; $(unsafe)");
    expect(result.metadata?.exit_code).toBe(0);
  });

  test("reports non-zero exits without returning child output", async () => {
    const fake = await fakeCodex(`
cat >/dev/null
printf 'secret child error' >&2
exit 23
`);
    await expect(
      executeTaskWithOptions("hello", {
        binary: fake.binary,
        cwd: fake.root,
        outputRoot: fake.root,
        timeoutMs: 2000,
      }),
    ).rejects.toThrow("codex process exited with code 23");
  });

  test("kills a timed-out process", async () => {
    const fake = await fakeCodex(`
cat >/dev/null
sleep 10
`);
    await expect(
      executeTaskWithOptions("hello", {
        binary: fake.binary,
        cwd: fake.root,
        outputRoot: fake.root,
        timeoutMs: 20,
      }),
    ).rejects.toThrow("codex task timed out");
  });

  test("requires explicit authentication in production mode", async () => {
    const fake = await fakeCodex("exit 0");
    await expect(
      executeTaskWithOptions("hello", {
        binary: fake.binary,
        cwd: fake.root,
        outputRoot: fake.root,
        requireAuth: true,
        authPath: join(fake.root, "missing-auth.json"),
        env: { HOME: fake.root },
      }),
    ).rejects.toThrow("codex authentication is not configured");
  });
});
