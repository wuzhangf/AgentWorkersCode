# Shared local command and build Runner

Pi and Codex use the same Node 24.16+ service for shell commands, builds and
tests in their explicit `macos-runner-v1` profile. No npm dependencies or JVM
are required by the service. A build may use its own registered toolchain.

The model process retains its no-fork configuration. Its tools call the owning
Worker, which binds the request to task/generation/segment/native turn and
submits it once to a private authenticated Runner. Every context has an owned
workspace, private HOME/tmp/cache, and an explicit non-secret environment.
Original keys, intent, dispatch, bounded output, exit status and cleanup facts
are durable in SQLite. An uncertain response is queried under the original key.

Use [the Mac launcher](../Run-NativeWorker.sh) to initialize a **new state** for
this profile; existing file-only/native states keep their own immutable profile.
Pi finds this service in its installed sibling directory. Codex records the
explicit `--command-runner` path and the selected physical Node executable.
Both bind the service's top-level `.mjs` files by content digest. The previous
`macos-in-process-v1` and `native` profiles remain explicit alternatives.

Commands support either an argv array or a shell script, a workspace-contained
cwd, a timeout, separate stdout/stderr and bounded paginated output. `build`
and `test` label intent; exit zero alone never proves a nonzero test count.
Read the real test report (for example Node TAP, Surefire XML or XCTest output).
The protocol is frozen in [CONTRACT.md](CONTRACT.md).

STOP closes all task contexts before cancelling command resources and the
model process. Normal compiler forks and waited-for process-group changes can
settle. A surviving observed detached writer or observation failure remains
UNKNOWN and blocks a new seal/reopen. An independent guardian retains each
registered group and kills it when the Runner dies; owner stdin EOF also
closes admission and cleans groups. Restart preserves source identity and
uncertain requests without replaying the command.

This trusted development profile proves cleanup of **registered process groups**.
It is not filesystem/network isolation or exhaustive arbitrary fork containment.
`arbitraryEscapedProcesses` remains `NOT_PROVEN`, and remote effects are separate.
Daemon/service jobs, interactive PTY and full isolation need their own capability.
Do not run a global process/Simulator kill to settle one task.

Verification:

```sh
node --test workers/command-runner/test/runner.test.mjs
# Toolchain qualification is explicit; absent opt-ins are SKIP, not PASS:
NWF_TEST_CARGO=/absolute/cargo NWF_TEST_JAVAC=/absolute/javac \
  NWF_TEST_JAVA=/absolute/java NWF_TEST_MAVEN=/absolute/mvn NWF_TEST_SWIFT=1 \
  node --test workers/command-runner/test/build-tools.test.mjs
```

Cargo also needs its approved `RUSTUP_HOME`/`CARGO_HOME` if isolated; JDK/Maven
may need `JAVA_HOME`. The tests create their own projects, Maven settings and
caches; no existing service or user source is modified. Worker-specific real
SDK/app-server and OpenCode Go model acceptance lives in each Worker's tests.
