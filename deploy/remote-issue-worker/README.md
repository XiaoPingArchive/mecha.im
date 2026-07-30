# Remote Issue Worker

This deployment turns Mecha into a private-Issue-driven Codex worker without
exposing the Mecha HTTP API or mounting a host project into task containers.

Security invariants:

- `mecha serve` listens only on `127.0.0.1:21212` with an API key.
- GitHub Actions reaches a dedicated SSH account whose key has a forced command.
- The forced gateway rejects arbitrary SSH commands and pauses during the local
  07:00–09:05 morning-report window.
- `mecha-submit` accepts one JSON object of at most 40 KiB, fixes the worker name
  to `xiaoping-codex`, serializes tasks with `/var/lib/mecha-submit/submit.lock`, and redacts errors.
  Create `/var/lib/mecha-submit` for that account before enabling the key.
- Each task gets a disposable container with no host project mount, dropped
  capabilities, two CPUs, 2200 MiB RAM, and 256 PIDs.
- Codex executes with `read-only` sandboxing. `danger-full-access` is rejected.
- `credentials_rw: [codex]` is restricted to disposable workers. It exists
  because subscription authentication refreshes `auth.json` in place. Use a
  dedicated service account and never reuse another host user's credential dir.

The versioned release contains `mecha`, `mecha-submit`, checksums, and the exact
GHCR image digest. Production should pin the worker image by that digest.

The companion workflow belongs in the private repository receiving Issues. It
needs a dedicated Ed25519 private key, a pinned `known_hosts` line, and the VPS
host variable. The authorized key must use:

```text
restrict,command="/usr/local/libexec/mecha-submit-gateway" ssh-ed25519 AAAA...
```

Do not add a public reverse proxy, MCP endpoint, workspace mount, PAT, or Docker
access to the SSH gateway account.
