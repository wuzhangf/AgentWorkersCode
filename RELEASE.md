# Native Workers — release, upgrade, rollback, migration (W7 / P24 / P32)

Scope note: this file records what is **implemented and verified on Windows x64**
and what remains unqualified. Linux, packaged installers, and live enterprise
cutover are **not** claimed here — see Gaps.

## Package contents and provenance

| Component | Artifact | Pin |
|---|---|---|
| Pi host | `workers/pi` TypeScript → `dist/` (`npm run build`); runtime Node.js ≥ 24.16 (`node:sqlite`) | Engine `@earendil-works/pi-coding-agent@1.4.0` via lockfile |
| Codex host | `workers/codex` Rust → `target/.../codex-native-worker.exe` (build: `scripts/build.ps1 -LlvmMingwHome <llvm-mingw>`) | `protocol/codex.lock.json` — codex-cli `0.163.0-alpha.11.2`, exe SHA-256 `3b4f5aeb66f35273f046e4e2baf0143832286a0dcba9db8e1a26dbcd92c90876`, generated schema SHA-256 `408e09849d5d4931992a4227a7f3eb80b15d24901f84aca0562e816634651a82` |

`init` refuses an unpinned codex executable (`ENGINE_VERSION_UNQUALIFIED`), a
mismatched binary (`ENGINE_BINARY_CHANGED`) or schema (`ENGINE_SCHEMA_CHANGED`).
`workers/Run-NativeWorker.ps1` is the Windows PS5.1 launcher.

## Platform qualification

| Platform | Start/stop | Crash recovery | Package | Status |
|---|---|---|---|---|
| Windows x64 | verified (all suites) | verified (SIGKILL/restart reconciliation tests) | source+build verified | **QUALIFIED (partial)** |
| Linux | — | — | — | **NOT_RUN** — no claim until a Linux run exists |
| macOS | — | — | — | not attempted |

## Upgrade, backup, rollback

- **Schema refusal is enforced**: codex `user_version > 6` → `SCHEMA_VERSION_UNSUPPORTED`
  (old binary refuses a newer DB; covered by
  `identity_is_durable_and_future_schema_refused`); pi `pi-ledger/1` mismatch →
  `SCHEMA_INCOMPATIBLE`. A downgraded binary never silently rewrites state.
- **Upgrade order**: stop intake (drain or let tasks settle), verify no active
  tasks/pending effects, back up the state directory, then start the new binary.
  In-place SQLite migrations run forward-only (v1→…→v6 committed migrations with
  tests); a backup is the only rollback path — restoring a newer schema onto the
  old binary is refused, not corrupted.
- **Rollback drill**: previous binary + restored backup state keeps old request
  receipts and original source identities; nothing re-dispatches completed ops
  (idempotent ledger). Verified via the restart/reconcile suites; a full
  packaged-binary rollback rehearsal has not been staged — **NOT_RUN**.
- **Backup**: quiesce → copy the whole state dir (SQLite WAL+FULL makes a stopped
  copy consistent). No secrets are stored in state; credentials stay in the
  central environment.

## Migration gating (design §14)

Switching "new task default backend" to a native host requires, in order:
qualified profile evidence (per-row capabilities, not a single flag) → zero
active tasks + zero pending effects → history queryable → rollback drill passed
→ then flip the default for **new** tasks only; old tasks keep their old store
references. The Java ACS provider is not retired; un-migrated stage paths stay.

Running tasks never migrate in place — a live task handoff is an explicit
transition/AMC operation, not a state copy.

## Privacy cleanup

`state/` contains task ledgers, events, workspace copies, aux/collab snapshots
and artifacts — no secrets, no prompt bodies in audit/diagnostics projections
(sha256+size refs). Deleting a task's `state/` subtree after terminal state +
backup is the cleanup; there is no telemetry channel to flush.

## Honest gaps (W7)

- Linux qualification, packaged installer/msi, macOS — NOT_RUN.
- Packaged-binary rollback rehearsal — NOT_RUN (source-level rollback verified).
- Live enterprise registration/DCR cutover — fixture-verified wire only.
- `cost` remains UNKNOWN (provider does not report cost); budgets are SOFT.
