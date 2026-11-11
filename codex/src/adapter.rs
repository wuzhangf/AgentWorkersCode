//! All app-server wire handling is centralized here and pinned by protocol/codex.lock.json.
use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, mpsc, oneshot},
    time::{Duration, timeout},
};

// Inbound bound (app-server -> host): a single JSON-RPC line may embed a whole
// repo file inside one tool result — a live task already hit the former 1 MiB
// ceiling and died UNKNOWN/PROTOCOL_FRAME_LIMIT after codex.exe read a ~500KB
// file. The bound stays finite at 8 MiB so a peer that writes without limit
// still kills the transport honestly instead of growing memory forever.
const MAX_FRAME_IN: usize = 8 * 1024 * 1024;
// Outbound bound (host -> app-server): our own writes are small control
// frames; an oversized request is an honest reject, never a silent truncation.
const MAX_FRAME_OUT: usize = 1024 * 1024;
// The host projects completed items, not private reasoning or raw tool/token
// deltas. Do not let those already-ignored bursts exhaust the control queue.
const UNUSED_DELTAS: &[&str] = &[
    "item/agentMessage/delta",
    "item/commandExecution/outputDelta",
    "item/fileChange/outputDelta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta",
];

fn projected_notification(frame: &Value) -> bool {
    let method = frame["method"].as_str().unwrap_or("");
    frame.get("id").is_some()
        || !(method.starts_with("codex/event/") || UNUSED_DELTAS.contains(&method))
}

pub fn failure_code(frame: &Value) -> &'static str {
    match frame["params"]["code"].as_str() {
        Some("PROTOCOL_FRAME_LIMIT") => "PROTOCOL_FRAME_LIMIT",
        Some("PROTOCOL_INVALID_JSON") => "PROTOCOL_INVALID_JSON",
        Some("PROTOCOL_INVALID_ENVELOPE") => "PROTOCOL_INVALID_ENVELOPE",
        Some("PROTOCOL_BACKPRESSURE") => "PROTOCOL_BACKPRESSURE",
        Some("PROTOCOL_UNKNOWN_RESPONSE") => "PROTOCOL_UNKNOWN_RESPONSE",
        _ => "PROTOCOL_ADAPTER_FAILED",
    }
}
// ThreadItem variants whose execution can produce effects outside the transcript.
// item/started + item/completed of these kinds maintain the durable effect inventory.
pub const EFFECT_KINDS: &[&str] = &[
    "commandExecution",
    "fileChange",
    "mcpToolCall",
    "dynamicToolCall",
    "collabAgentToolCall",
    "webSearch",
    "imageGeneration",
];
type Reply = oneshot::Sender<Result<Value>>;
// One bounded read step of the inbound pump, extracted so the frame ceiling is
// exercised by tests without a live child.
enum InboundLine {
    // Clean stream end: the child closed stdout.
    Eof,
    // One complete newline-terminated line inside MAX_FRAME_IN.
    Line(Vec<u8>),
    // The peer wrote past the bound or left an unterminated tail: the
    // transport dies with PROTOCOL_FRAME_LIMIT rather than buffer unboundedly.
    Overrun,
}
async fn read_inbound_line<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> InboundLine {
    let mut line = Vec::new();
    match reader
        .take((MAX_FRAME_IN + 1) as u64)
        .read_until(b'\n', &mut line)
        .await
    {
        Ok(0) => InboundLine::Eof,
        Ok(_) if line.len() <= MAX_FRAME_IN && line.last() == Some(&b'\n') => {
            InboundLine::Line(line)
        }
        _ => InboundLine::Overrun,
    }
}
#[derive(Debug)]
pub struct NativeRpcError {
    pub code: i64,
}
impl std::fmt::Display for NativeRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NATIVE_RPC_REJECTED")
    }
}
impl std::error::Error for NativeRpcError {}
pub struct Peer {
    stdin: Mutex<ChildStdin>,
    child: Mutex<Child>,
    pending: Mutex<HashMap<u64, Reply>>,
    next: AtomicU64,
    _job: Job,
}
#[cfg(windows)]
struct Job(windows_sys::Win32::Foundation::HANDLE);
#[cfg(windows)]
unsafe impl Send for Job {}
#[cfg(windows)]
unsafe impl Sync for Job {}
#[cfg(windows)]
impl Drop for Job {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}
// Retained handles pin the identities selected from this private Job. Never
// terminate a PID merely because it appeared in an earlier process snapshot.
#[cfg(windows)]
struct StopProcess(windows_sys::Win32::Foundation::HANDLE);
#[cfg(windows)]
unsafe impl Send for StopProcess {}
#[cfg(windows)]
impl Drop for StopProcess {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}
#[cfg(windows)]
impl Job {
    fn stop_children(&self, root: u32) -> Result<Vec<StopProcess>> {
        use windows_sys::Win32::{
            Foundation::*,
            System::{JobObjects::*, Threading::*},
        };
        // Aligned backing storage for the variable-length PID array. A larger
        // job fails this optional stage and still takes the full-job STOP path.
        let mut buffer = vec![0usize; 4098];
        let ok = unsafe {
            QueryInformationJobObject(
                self.0,
                JobObjectBasicProcessIdList,
                buffer.as_mut_ptr() as _,
                (buffer.len() * std::mem::size_of::<usize>()) as u32,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            bail!("JOB_CHILD_SNAPSHOT_UNPROVEN");
        }
        let list = unsafe { &*(buffer.as_ptr() as *const JOBOBJECT_BASIC_PROCESS_ID_LIST) };
        let count = list.NumberOfProcessIdsInList as usize;
        if list.NumberOfAssignedProcesses != list.NumberOfProcessIdsInList || count > 4096 {
            bail!("JOB_CHILD_SNAPSHOT_UNPROVEN");
        }
        let pids = unsafe { std::slice::from_raw_parts(list.ProcessIdList.as_ptr(), count) };
        if !pids.contains(&(root as usize)) {
            bail!("JOB_ROOT_UNPROVEN");
        }
        let mut children = Vec::new();
        for &pid in pids {
            if pid == root as usize {
                continue;
            }
            let pid = u32::try_from(pid).map_err(|_| anyhow!("JOB_CHILD_ID_UNPROVEN"))?;
            let raw = unsafe {
                OpenProcess(
                    PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                    0,
                    pid,
                )
            };
            if raw.is_null() {
                if unsafe { GetLastError() } == ERROR_INVALID_PARAMETER {
                    continue;
                }
                bail!("JOB_CHILD_HANDLE_UNPROVEN");
            }
            let handle = StopProcess(raw);
            let mut member = 0;
            if unsafe { IsProcessInJob(handle.0, self.0, &mut member) } == 0 {
                bail!("JOB_CHILD_MEMBERSHIP_UNPROVEN");
            }
            if member != 0 {
                children.push(handle);
            }
        }
        for handle in &children {
            if unsafe { WaitForSingleObject(handle.0, 0) } != WAIT_OBJECT_0
                && unsafe { TerminateProcess(handle.0, 1) } == 0
            {
                bail!("JOB_CHILD_TERMINATION_UNPROVEN");
            }
        }
        Ok(children)
    }
}
// macOS has no PR_SET_PDEATHSIG. A separate process owns the group and
// watches a pipe held only by this host. Even SIGKILL closes that pipe; EOF
// kills the group, including the guardian itself. This does not contain a
// process that deliberately escapes the group with setsid/setpgid.
#[cfg(target_os = "macos")]
struct Job {
    guardian: Mutex<Child>,
    replies: Mutex<BufReader<tokio::process::ChildStdout>>,
    fork_marker: std::path::PathBuf,
    sealed: std::sync::atomic::AtomicBool,
    pgid: i32,
}
#[cfg(all(not(windows), not(target_os = "macos")))]
struct Job;

// Managed-process environment: core OS variables, model credentials and the
// per-task validated envAllowlist names are the only inherited names.
pub(crate) fn spawn_env_allowed(name: &str, allowlist: &[String]) -> bool {
    allowlist.iter().any(|n| n == name)
        || name.starts_with("LLM_HEADER_")
        || matches!(
            name.to_uppercase().as_str(),
            "PATH"
                | "PATHEXT"
                | "SYSTEMROOT"
                | "WINDIR"
                | "SYSTEMDRIVE"
                | "USERPROFILE"
                | "HOME"
                | "HOMEDRIVE"
                | "HOMEPATH"
                | "APPDATA"
                | "LOCALAPPDATA"
                | "TEMP"
                | "TMP"
                | "COMSPEC"
                | "LANG"
                | "NUMBER_OF_PROCESSORS"
                | "PROCESSOR_ARCHITECTURE"
                | "OS"
                | "MODEL_API_KEY"
                // Preserve an owner's explicit bypass for local model/MCP
                // endpoints; otherwise Windows system proxy can intercept them.
                | "NO_PROXY"
        )
}

#[cfg(target_os = "macos")]
async fn macos_guarded_child(
    executable: &Path,
    args: &[std::ffi::OsString],
    home: &Path,
    cwd: &Path,
    environment: Vec<(String, String)>,
    no_fork: bool,
) -> Result<(Child, Job)> {
    let host = std::env::current_exe()?;
    #[cfg(test)]
    let host = std::path::PathBuf::from(
        std::env::var("NWF_CODEX_HOST_EXECUTABLE")
            .unwrap_or_else(|_| host.to_string_lossy().into_owned()),
    );
    let fork_marker = home.join(format!(".process-fork-{}", uuid::Uuid::new_v4()));
    let mut guardian = Command::new(&host);
    guardian
        .args([
            "--nwf-internal-guard",
            fork_marker
                .to_str()
                .ok_or_else(|| anyhow!("INVALID_PATH"))?,
        ])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0);
    let mut guardian = guardian.spawn()?;
    let pgid = guardian
        .id()
        .ok_or_else(|| anyhow!("GROUP_GUARD_MISSING"))? as i32;
    let replies = BufReader::new(guardian.stdout.take().unwrap());
    let mut gated = Command::new(&host);
    gated
        .arg(if no_fork {
            "--nwf-internal-exec-no-fork"
        } else {
            "--nwf-internal-exec"
        })
        .arg(executable)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(pgid)
        .env_clear()
        .envs(environment)
        .env("CODEX_HOME", home);
    let mut cmd = gated;
    let job = Job {
        guardian: Mutex::new(guardian),
        replies: Mutex::new(replies),
        fork_marker,
        sealed: std::sync::atomic::AtomicBool::new(false),
        pgid,
    };
    let child = cmd.spawn()?;
    // The wrapper stops before exec, so no engine instruction can fork
    // before the guardian has installed its NOTE_FORK watch.
    let pid = child.id().ok_or_else(|| anyhow!("CHILD_HANDLE_MISSING"))? as i32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut status = 0;
        let r = unsafe { libc::waitpid(pid, &mut status, libc::WUNTRACED | libc::WNOHANG) };
        if r == pid && libc::WIFSTOPPED(status) {
            break;
        }
        if r < 0 || r == pid || tokio::time::Instant::now() >= deadline {
            bail!("GROUP_GUARD_UNPROVEN")
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    job.guardian
        .lock()
        .await
        .stdin
        .as_mut()
        .unwrap()
        .write_all(format!("{pid}\n").as_bytes())
        .await?;
    let mut ready = String::new();
    timeout(
        Duration::from_secs(10),
        job.replies.lock().await.read_line(&mut ready),
    )
    .await??;
    if ready.trim() != "READY" {
        bail!("GROUP_GUARD_UNPROVEN")
    }
    if unsafe { libc::kill(pid, libc::SIGCONT) } != 0 {
        bail!("GROUP_GUARD_UNPROVEN")
    }
    Ok((child, job))
}

// Fixed host-owned Git preparation. Credentials and user Git extensions are
// excluded. Root completion is followed by owned-group cleanup, not inferred
// to mean arbitrary escaped descendants have settled.
#[cfg(target_os = "macos")]
pub(crate) async fn prepare_git(
    args: &[&std::ffi::OsStr],
    cwd: &Path,
    marker_root: &Path,
) -> Result<std::process::Output> {
    std::fs::create_dir_all(marker_root)?;
    let mut environment: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| {
            spawn_env_allowed(k, &[]) && k != "MODEL_API_KEY" && !k.starts_with("LLM_HEADER_")
        })
        .collect();
    environment.push(("GIT_CONFIG_NOSYSTEM".into(), "1".into()));
    environment.push(("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()));
    let mut fixed = vec!["-c".into(), "core.hooksPath=/dev/null".into()];
    fixed.extend(args.iter().map(|s| s.to_os_string()));
    let (child, job) = macos_guarded_child(
        Path::new("git"),
        &fixed,
        marker_root,
        cwd,
        environment,
        false,
    )
    .await?;
    let result = timeout(Duration::from_secs(60), child.wait_with_output()).await;
    // Closing the lease terminates any writers that outlive the trusted root.
    drop(job.guardian.lock().await.stdin.take());
    timeout(Duration::from_secs(10), job.guardian.lock().await.wait())
        .await
        .map_err(|_| anyhow!("NATIVE_PREPARATION_UNPROVEN"))??;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if unsafe { libc::kill(-job.pgid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("NATIVE_PREPARATION_UNPROVEN")
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    result
        .map_err(|_| anyhow!("LOCAL_PROCESS_TIMEOUT"))?
        .map_err(Into::into)
}

impl Peer {
    // `env_names` is a per-task validated allowlist: only names already present in
    // the worker's own environment are forwarded to the managed peer. Nothing is
    // read from disk and no values cross this API.
    pub async fn spawn(
        executable: &Path,
        home: &Path,
        cwd: &Path,
        env_names: &[String],
        no_fork: bool,
    ) -> Result<(Arc<Self>, mpsc::Receiver<Value>)> {
        #[cfg(not(target_os = "macos"))]
        let _ = no_fork;
        let mut cmd = Command::new(executable);
        cmd.args(["app-server", "--stdio", "--strict-config"])
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let inherited = std::env::vars().filter(|(k, _)| spawn_env_allowed(k, env_names));
        cmd.env_clear().envs(inherited).env("CODEX_HOME", home);
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            cmd.process_group(0);
        }
        #[cfg(target_os = "linux")]
        unsafe {
            cmd.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        #[cfg(not(target_os = "macos"))]
        let mut child = cmd.spawn()?;
        #[cfg(target_os = "macos")]
        let (mut child, job) = macos_guarded_child(
            executable,
            &[
                "app-server".into(),
                "--stdio".into(),
                "--strict-config".into(),
            ],
            home,
            cwd,
            std::env::vars()
                .filter(|(k, _)| spawn_env_allowed(k, env_names))
                .collect(),
            no_fork,
        )
        .await?;

        #[cfg(windows)]
        let job = unsafe {
            use windows_sys::Win32::System::JobObjects::*;
            let handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if handle.is_null() {
                child.start_kill()?;
                bail!("JOB_CREATE_FAILED")
            }
            let job = Job(handle);
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                std::mem::size_of_val(&info) as u32,
            ) == 0
                || AssignProcessToJobObject(
                    handle,
                    child
                        .raw_handle()
                        .ok_or_else(|| anyhow!("CHILD_HANDLE_MISSING"))? as _,
                ) == 0
            {
                child.start_kill()?;
                bail!("JOB_ASSIGN_FAILED")
            };
            job
        };
        #[cfg(all(not(windows), not(target_os = "macos")))]
        let job = Job;
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let peer = Arc::new(Self {
            stdin: Mutex::new(stdin),
            child: Mutex::new(child),
            pending: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            _job: job,
        });
        let (tx, rx) = mpsc::channel(256);
        let weak = Arc::downgrade(&peer);
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                let frame = match read_inbound_line(&mut reader).await {
                    InboundLine::Eof => break,
                    InboundLine::Overrun => {
                        let _=tx.send(json!({"method":"adapter/failure","params":{"code":"PROTOCOL_FRAME_LIMIT"}})).await;
                        break;
                    }
                    InboundLine::Line(line) => serde_json::from_slice::<Value>(&line),
                };
                let Ok(v) = frame else {
                    let _=tx.send(json!({"method":"adapter/failure","params":{"code":"PROTOCOL_INVALID_JSON"}})).await;
                    break;
                };
                let Some(p) = weak.upgrade() else { break };
                if v.get("method").is_some() {
                    if !v["method"].is_string() || !v["params"].is_object() {
                        let _=tx.send(json!({"method":"adapter/failure","params":{"code":"PROTOCOL_INVALID_ENVELOPE"}})).await;
                        break;
                    }
                    if !projected_notification(&v) {
                        continue;
                    }
                    if tx.try_send(v).is_err() {
                        let _=tx.send(json!({"method":"adapter/failure","params":{"code":"PROTOCOL_BACKPRESSURE"}})).await;
                        break;
                    }
                } else if let Some(i) = v["id"].as_u64() {
                    let Some(reply) = p.pending.lock().await.remove(&i) else {
                        let _=tx.send(json!({"method":"adapter/failure","params":{"code":"PROTOCOL_UNKNOWN_RESPONSE"}})).await;
                        break;
                    };
                    let result = if v.get("error").is_some() {
                        Err(NativeRpcError {
                            code: v["error"]["code"].as_i64().unwrap_or(0),
                        }
                        .into())
                    } else if v.get("result").is_some() {
                        Ok(v["result"].clone())
                    } else {
                        Err(anyhow!("PROTOCOL_RESULT_MISSING"))
                    };
                    let _ = reply.send(result);
                } else {
                    let _=tx.send(json!({"method":"adapter/failure","params":{"code":"PROTOCOL_INVALID_ENVELOPE"}})).await;
                    break;
                }
            }
            if let Some(p) = weak.upgrade() {
                for (_, reply) in p.pending.lock().await.drain() {
                    let _ = reply.send(Err(anyhow!("NATIVE_PROCESS_EXITED")));
                }
            }
            let _ = tx
                .send(json!({"method":"adapter/exited","params":{}}))
                .await;
        });
        // Drain diagnostics, retaining no untrusted raw text or credentials in logs.
        tokio::spawn(async move {
            let mut b = [0u8; 4096];
            let mut consumed = 0usize;
            while let Ok(n) = stderr.read(&mut b).await {
                if n == 0 {
                    break;
                }
                consumed = consumed.saturating_add(n).min(256 * 1024);
            }
        });
        Ok((peer, rx))
    }
    pub async fn send(&self, v: &Value) -> Result<()> {
        let mut data = serde_json::to_vec(v)?;
        if data.len() > MAX_FRAME_OUT {
            bail!("PROTOCOL_FRAME_LIMIT")
        }
        data.push(b'\n');
        let mut s = self.stdin.lock().await;
        s.write_all(&data).await?;
        s.flush().await?;
        Ok(())
    }
    pub async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        self.rpc_within(method, params, Some(Duration::from_secs(45)))
            .await
    }
    // `command/exec` defers its success response until the spawned process
    // exits, so a terminal session needs a call with no response deadline.
    // The request stays in the bounded pending set and is still resolved with
    // NATIVE_PROCESS_EXITED if the transport dies underneath it.
    pub async fn rpc_deferred(&self, method: &str, params: Value) -> Result<Value> {
        self.rpc_within(method, params, None).await
    }
    async fn rpc_within(
        &self,
        method: &str,
        params: Value,
        wait: Option<Duration>,
    ) -> Result<Value> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            if pending.len() >= 32 {
                bail!("PROTOCOL_REQUEST_LIMIT")
            }
            pending.insert(id, tx);
        }
        if let Err(e) = self
            .send(&json!({"id":id,"method":method,"params":params}))
            .await
        {
            self.pending.lock().await.remove(&id);
            return Err(e);
        }
        let answer = match wait {
            Some(limit) => match timeout(limit, rx).await {
                Ok(Ok(v)) => Some(v),
                _ => None,
            },
            None => match rx.await {
                Ok(v) => Some(v),
                Err(_) => None,
            },
        };
        match answer {
            Some(v) => v,
            None => {
                self.pending.lock().await.remove(&id);
                bail!("NATIVE_RPC_UNCONFIRMED")
            }
        }
    }
    #[cfg(test)]
    pub async fn initialize(&self) -> Result<Value> {
        self.initialize_with_experimental(false).await
    }
    pub async fn initialize_with_experimental(&self, experimental: bool) -> Result<Value> {
        let result=self.rpc("initialize",json!({"clientInfo":{"name":"codex-native-worker","title":"Native Worker","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":experimental,"optOutNotificationMethods":UNUSED_DELTAS}})).await?;
        self.send(&json!({"method":"initialized","params":{}}))
            .await?;
        Ok(result)
    }
    // Keep the app-server alive long enough to observe the owned commands'
    // exits and emit item/completed. This stage never settles an effect itself.
    #[cfg(windows)]
    pub async fn stop_owned_children(&self) -> Result<usize> {
        use windows_sys::Win32::{
            Foundation::WAIT_OBJECT_0, System::Threading::WaitForSingleObject,
        };
        let root = self
            .child
            .lock()
            .await
            .id()
            .ok_or_else(|| anyhow!("JOB_ROOT_UNPROVEN"))?;
        let handles = self._job.stop_children(root)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            if handles
                .iter()
                .all(|h| unsafe { WaitForSingleObject(h.0, 0) } == WAIT_OBJECT_0)
            {
                return Ok(handles.len());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("JOB_CHILD_EXIT_UNPROVEN");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    pub async fn stop(&self) -> Result<()> {
        #[cfg(windows)]
        unsafe {
            if windows_sys::Win32::System::JobObjects::TerminateJobObject(self._job.0, 1) == 0 {
                bail!("JOB_TERMINATION_UNCONFIRMED")
            }
        }
        #[cfg(target_os = "macos")]
        let mut reaped_root = false;
        #[cfg(target_os = "macos")]
        {
            let child = self.child.lock().await;
            if let Some(pid) = child.id() {
                let result = unsafe { libc::kill(pid as i32, libc::SIGSTOP) };
                if result != 0
                    && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
                {
                    bail!("NATIVE_WRITER_UNPROVEN")
                }
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                loop {
                    let mut status = 0;
                    let r = unsafe {
                        libc::waitpid(pid as i32, &mut status, libc::WUNTRACED | libc::WNOHANG)
                    };
                    if r == pid as i32 {
                        reaped_root = !libc::WIFSTOPPED(status);
                        break;
                    }
                    if r < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
                    {
                        reaped_root = true;
                        break;
                    }
                    if r < 0 || tokio::time::Instant::now() >= deadline {
                        bail!("NATIVE_WRITER_UNPROVEN")
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            // Once the engine is stopped, draining NOTE_FORK cannot race a new
            // first fork. A previous fork permanently taints this process.
            self._job
                .guardian
                .lock()
                .await
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"SEAL\n")
                .await?;
            let mut reply = String::new();
            timeout(
                Duration::from_secs(10),
                self._job.replies.lock().await.read_line(&mut reply),
            )
            .await??;
            if !matches!(reply.trim(), "SEALED" | "TAINTED") {
                bail!("NATIVE_WRITER_UNPROVEN")
            }
            self._job
                .sealed
                .store(reply.trim() == "SEALED", Ordering::SeqCst);
        }
        #[cfg(unix)]
        let pgid = {
            #[cfg(target_os = "macos")]
            let pgid = self._job.pgid;
            #[cfg(not(target_os = "macos"))]
            let pgid = self.child.lock().await.id().unwrap_or(0) as i32;
            if pgid > 0
                && unsafe { libc::kill(-pgid, libc::SIGKILL) } != 0
                && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
            {
                bail!("NATIVE_WRITER_UNPROVEN")
            }
            pgid
        };
        let mut child = self.child.lock().await;
        let _ = child.start_kill();
        let waited = timeout(Duration::from_secs(10), child.wait())
            .await
            .map_err(|_| anyhow!("NATIVE_WRITER_UNPROVEN"))?;
        #[cfg(target_os = "macos")]
        let waited = if reaped_root
            && waited
                .as_ref()
                .err()
                .is_some_and(|e| e.raw_os_error() == Some(libc::ECHILD))
        {
            Ok(std::os::unix::process::ExitStatusExt::from_raw(0))
        } else {
            waited
        };
        waited?;
        #[cfg(target_os = "macos")]
        timeout(
            Duration::from_secs(10),
            self._job.guardian.lock().await.wait(),
        )
        .await
        .map_err(|_| anyhow!("NATIVE_WRITER_UNPROVEN"))??;
        #[cfg(unix)]
        if pgid > 0 {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let result = unsafe { libc::kill(-pgid, 0) };
                if result == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    bail!("NATIVE_WRITER_UNPROVEN")
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::*;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                    unsafe { std::mem::zeroed() };
                let ok = unsafe {
                    QueryInformationJobObject(
                        self._job.0,
                        JobObjectBasicAccountingInformation,
                        &mut info as *mut _ as _,
                        std::mem::size_of_val(&info) as u32,
                        std::ptr::null_mut(),
                    )
                };
                if ok == 0 {
                    bail!("NATIVE_WRITER_UNPROVEN")
                }
                if info.ActiveProcesses == 0 {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    bail!("NATIVE_WRITER_UNPROVEN")
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        Ok(())
    }
    pub fn quiescence_proven(&self) -> bool {
        #[cfg(target_os = "macos")]
        {
            self._job.sealed.load(Ordering::SeqCst) && !self._job.fork_marker.exists()
        }
        #[cfg(not(target_os = "macos"))]
        {
            true
        }
    }
    pub async fn alive(&self) -> Result<bool> {
        Ok(self.child.lock().await.try_wait()?.is_none())
    }
}

pub fn response(method: &str, params: &Value, response: &Value) -> Result<Value> {
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            let d = response["decision"]
                .as_str()
                .ok_or_else(|| anyhow!("NATIVE_OPTION_NOT_ALLOWED"))?;
            if response.as_object().map(|o| o.len()) != Some(1)
                || !matches!(d, "accept" | "decline" | "cancel")
            {
                bail!("NATIVE_OPTION_NOT_ALLOWED")
            }
            Ok(json!({"decision":d}))
        }
        "item/tool/requestUserInput" => {
            let questions = params["questions"]
                .as_array()
                .ok_or_else(|| anyhow!("PROTOCOL_REQUIRED_FIELD"))?;
            if questions.len() != 1 || questions[0]["isSecret"] == true {
                bail!("NATIVE_INPUT_TYPE_UNSUPPORTED")
            }
            let value = response["value"]
                .as_str()
                .ok_or_else(|| anyhow!("NATIVE_INPUT_TYPE_UNSUPPORTED"))?;
            let qid = questions[0]["id"]
                .as_str()
                .ok_or_else(|| anyhow!("PROTOCOL_REQUIRED_FIELD"))?;
            let mut answers = serde_json::Map::new();
            answers.insert(qid.into(), json!({"answers":[value]}));
            Ok(json!({"answers":answers}))
        }
        "mcpServer/elicitation/request" => {
            // Pinned response schema: {action: accept|decline|cancel,
            // content?, _meta?}. The pin reuses elicitation for MCP tool-call
            // approval (_meta.codex_approval_kind="mcp_tool_call") and for
            // genuine form elicitations. A bare "accept" is only honest for
            // approvals and empty-schema forms; a form that asks for real
            // structured input cannot be truthfully satisfied here.
            let d = response["decision"]
                .as_str()
                .ok_or_else(|| anyhow!("NATIVE_OPTION_NOT_ALLOWED"))?;
            if response.as_object().map(|o| o.len()) != Some(1)
                || !matches!(d, "accept" | "decline" | "cancel")
            {
                bail!("NATIVE_OPTION_NOT_ALLOWED")
            }
            let approval =
                params["_meta"]["codex_approval_kind"].as_str() == Some("mcp_tool_call");
            let empty_schema = params["requestedSchema"]["properties"]
                .as_object()
                .map(|o| o.is_empty())
                .unwrap_or(true);
            if d == "accept" && !approval && !empty_schema {
                bail!("NATIVE_INPUT_TYPE_UNSUPPORTED")
            }
            if d == "accept" && !approval {
                return Ok(json!({"action":d,"content":{}}));
            }
            Ok(json!({"action":d}))
        }
        _ => bail!("NATIVE_INPUT_TYPE_UNSUPPORTED"),
    }
}
pub fn strict_pause_gap() -> Value {
    json!({"id":"execution.pause","implementation":"GAP","qualification":"NOT_RUN","availability":"UNAVAILABLE","semantics":"BOUNDARY_CHECKPOINT","reasonCode":"NATIVE_BOUNDARY_GATE_ABSENT","evidenceRef":"protocol/strict-pause-gap.json"})
}
pub fn validate_item(item: &Value) -> Result<()> {
    if !item["id"].is_string() {
        bail!("PROTOCOL_REQUIRED_FIELD")
    }
    let kind = item["type"]
        .as_str()
        .ok_or_else(|| anyhow!("PROTOCOL_REQUIRED_FIELD"))?;
    if !matches!(
        kind,
        "userMessage"
            | "hookPrompt"
            | "agentMessage"
            | "functionCallOutput"
            | "plan"
            | "reasoning"
            | "commandExecution"
            | "fileChange"
            | "mcpToolCall"
            | "dynamicToolCall"
            | "collabAgentToolCall"
            | "subAgentActivity"
            | "webSearch"
            | "imageView"
            | "sleep"
            | "imageGeneration"
            | "enteredReviewMode"
            | "exitedReviewMode"
            | "contextCompaction"
    ) {
        bail!("PROTOCOL_UNKNOWN_ITEM_VARIANT")
    }
    if kind == "agentMessage" && !item["text"].is_string() {
        bail!("PROTOCOL_REQUIRED_FIELD")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(windows)]
    fn stop_test_job() -> Job {
        use windows_sys::Win32::System::JobObjects::*;
        unsafe {
            let handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            assert!(!handle.is_null());
            let job = Job(handle);
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            assert_ne!(
                SetInformationJobObject(
                    handle,
                    JobObjectExtendedLimitInformation,
                    &info as *const _ as _,
                    std::mem::size_of_val(&info) as u32
                ),
                0
            );
            job
        }
    }
    #[cfg(windows)]
    fn stop_test_sleeper() -> Child {
        let program = Path::new(&std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        Command::new(program)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ])
            .creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW)
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }
    #[cfg(windows)]
    fn stop_test_assign(job: &Job, child: &Child) {
        assert_ne!(
            unsafe {
                windows_sys::Win32::System::JobObjects::AssignProcessToJobObject(
                    job.0,
                    child.raw_handle().unwrap() as _,
                )
            },
            0
        );
    }
    #[cfg(windows)]
    #[tokio::test]
    async fn staged_stop_keeps_root_and_unrelated_process_alive() {
        let job = stop_test_job();
        let mut root = stop_test_sleeper();
        let mut child = stop_test_sleeper();
        let mut outside = stop_test_sleeper();
        stop_test_assign(&job, &root);
        stop_test_assign(&job, &child);
        let held = job.stop_children(root.id().unwrap()).unwrap();
        assert_eq!(held.len(), 1);
        timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(root.try_wait().unwrap().is_none());
        assert!(outside.try_wait().unwrap().is_none());
        assert_ne!(
            unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(job.0, 1) },
            0
        );
        root.wait().await.unwrap();
        outside.start_kill().unwrap();
        outside.wait().await.unwrap();
    }
    #[cfg(windows)]
    #[tokio::test]
    async fn staged_stop_refuses_missing_job_root_before_killing_children() {
        let job = stop_test_job();
        let mut root = stop_test_sleeper();
        let mut child = stop_test_sleeper();
        stop_test_assign(&job, &root);
        stop_test_assign(&job, &child);
        assert!(job.stop_children(u32::MAX).is_err());
        assert!(root.try_wait().unwrap().is_none());
        assert!(child.try_wait().unwrap().is_none());
        assert_ne!(
            unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(job.0, 1) },
            0
        );
        root.wait().await.unwrap();
        child.wait().await.unwrap();
    }
    #[tokio::test]
    async fn unused_delta_burst_leaves_room_for_lifecycle_and_requests() {
        let (tx, mut rx) = mpsc::channel(2);
        for method in UNUSED_DELTAS.iter().copied().chain(["codex/event/exec_command_output_delta"]) {
            for _ in 0..1024 {
                let frame = json!({"method":method,"params":{"delta":"private tool output"}});
                if projected_notification(&frame) { tx.try_send(frame).unwrap(); }
            }
        }
        let completed = json!({"method":"item/completed","params":{"item":{"type":"commandExecution"}}});
        let request = json!({"id":9,"method":"item/commandExecution/requestApproval","params":{}});
        for frame in [&completed, &request] {
            assert!(projected_notification(frame));
            tx.try_send(frame.clone()).unwrap();
        }
        assert_eq!(rx.recv().await.unwrap(), completed);
        assert_eq!(rx.recv().await.unwrap(), request);
        assert!(projected_notification(&json!({"method":"command/exec/outputDelta","params":{}})));
        assert!(projected_notification(&json!({"id":7,"method":"codex/event/request","params":{}})));
    }

    #[tokio::test]
    async fn inbound_line_accepts_repo_sized_frames_up_to_the_documented_ceiling() {
        // Live evidence: a real task died UNKNOWN/PROTOCOL_FRAME_LIMIT when a
        // ~500KB workspace file rode one tool-result line past the former
        // 1 MiB bound. Lines up to the 8 MiB inbound ceiling are real frames.
        assert_eq!(MAX_FRAME_IN, 8 * 1024 * 1024);
        assert_eq!(MAX_FRAME_OUT, 1024 * 1024);
        let big = format!(
            "{{\"method\":\"item/completed\",\"params\":{{\"pad\":\"{}\"}}}}\n",
            "x".repeat(2 * 1024 * 1024)
        )
        .into_bytes();
        let small = b"{\"method\":\"turn/started\",\"params\":{}}\n".to_vec();
        let wire: Vec<u8> = [big.clone(), small].concat();
        let mut reader = BufReader::new(&wire[..]);
        let InboundLine::Line(line) = read_inbound_line(&mut reader).await else {
            panic!("INBOUND_FRAME_REFUSED")
        };
        assert_eq!(line, big);
        assert_eq!(
            serde_json::from_slice::<Value>(&line).unwrap()["method"],
            "item/completed"
        );
        // A following line on the same stream still reads: the bounded take is
        // per-line state, not a consumed stream.
        let InboundLine::Line(line) = read_inbound_line(&mut reader).await else {
            panic!("SECOND_INBOUND_FRAME_REFUSED")
        };
        assert_eq!(
            serde_json::from_slice::<Value>(&line).unwrap()["method"],
            "turn/started"
        );
        assert!(matches!(
            read_inbound_line(&mut reader).await,
            InboundLine::Eof
        ));
    }
    #[tokio::test]
    async fn inbound_line_bound_is_exact_and_unterminated_tails_refuse() {
        // A line of exactly MAX_FRAME_IN bytes (newline included) is accepted.
        let edge: Vec<u8> = [
            vec![b'x'; MAX_FRAME_IN - 1],
            vec![b'\n'],
        ]
        .concat();
        let mut reader = BufReader::new(&edge[..]);
        assert!(matches!(
            read_inbound_line(&mut reader).await,
            InboundLine::Line(_)
        ));
        // One byte past the ceiling — terminated or not — is an honest kill.
        for oversized in [
            {
                let mut v = vec![b'x'; MAX_FRAME_IN + 1];
                v.push(b'\n');
                v
            },
            {
                let mut v = vec![b'x'; MAX_FRAME_IN + 1];
                v.extend_from_slice(b"tail");
                v
            },
            vec![b'x'; MAX_FRAME_IN], // unterminated tail at the bound
        ] {
            let mut reader = BufReader::new(&oversized[..]);
            assert!(matches!(
                read_inbound_line(&mut reader).await,
                InboundLine::Overrun
            ));
        }
    }
    #[tokio::test]
    async fn inbound_line_at_exact_ceiling_still_parses_as_a_frame() {
        // L2-CDX-03 boundary leg: a newline-terminated line of exactly
        // MAX_FRAME_IN bytes is a real frame — accepted AND JSON-parseable,
        // not merely tolerated as opaque bytes.
        let prefix = "{\"method\":\"item/completed\",\"params\":{\"pad\":\"";
        let suffix = "\"}}\n";
        let mut line = String::from(prefix);
        line.push_str(&"x".repeat(MAX_FRAME_IN - prefix.len() - suffix.len()));
        line.push_str(suffix);
        assert_eq!(line.len(), MAX_FRAME_IN);
        let mut reader = BufReader::new(line.as_bytes());
        let InboundLine::Line(read) = read_inbound_line(&mut reader).await else {
            panic!("BOUNDARY_FRAME_REFUSED")
        };
        assert_eq!(
            serde_json::from_slice::<Value>(&read).unwrap()["method"],
            "item/completed"
        );
        // The overrun half of the contract is the sibling test; the failure
        // frame the pump emits for it carries the transport code and nothing
        // else — no line bytes, no params beyond `code`.
        assert_eq!(
            failure_code(&json!({"method":"adapter/failure","params":{"code":"PROTOCOL_FRAME_LIMIT"}})),
            "PROTOCOL_FRAME_LIMIT"
        );
    }
    #[cfg(windows)]
    #[tokio::test]
    async fn outbound_frame_bound_still_rejects_oversized_writes() {
        let program = Path::new(&std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let mut child = Command::new(program)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ])
            .creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let peer = Peer {
            stdin: Mutex::new(child.stdin.take().unwrap()),
            child: Mutex::new(child),
            pending: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            _job: stop_test_job(),
        };
        peer.send(&json!({"method":"small","params":{}}))
            .await
            .unwrap();
        let oversized = json!({"method":"too/big","params":{"pad":"x".repeat(2 * 1024 * 1024)}});
        let e = peer.send(&oversized).await.unwrap_err();
        assert_eq!(e.to_string(), "PROTOCOL_FRAME_LIMIT");
        // The honest reject happens before any write: the transport is still usable.
        peer.send(&json!({"method":"still/small","params":{}}))
            .await
            .unwrap();
        peer.stop().await.unwrap();
    }
    #[test]
    fn adapter_failure_keeps_only_known_reason_codes() {
        assert_eq!(failure_code(&json!({"params":{"code":"PROTOCOL_BACKPRESSURE"}})), "PROTOCOL_BACKPRESSURE");
        assert_eq!(failure_code(&json!({"params":{"code":"untrusted diagnostic text"}})), "PROTOCOL_ADAPTER_FAILED");
    }
    #[test]
    fn pinned_schema_has_interrupt_but_no_boundary_pause() {
        let methods: Vec<String> =
            serde_json::from_str(include_str!("../protocol/client-methods.json")).unwrap();
        assert!(methods.iter().any(|m| m == "turn/interrupt"));
        assert!(methods.iter().any(|m| m == "thread/resume"));
        assert!(
            !methods
                .iter()
                .any(|m| m.to_lowercase().contains("pause")
                    || m.to_lowercase().contains("checkpoint"))
        );
    }
    #[cfg(windows)]
    #[tokio::test]
    #[ignore = "requires NWF_CODEX_EXECUTABLE: real pinned engine with owned local Responses fixture, no external model"]
    async fn pinned_native_child_stop_preserves_command_completion() {
        use axum::{Json, Router, extract::State, routing::post};
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        async fn model(
            State(calls): State<Arc<std::sync::atomic::AtomicUsize>>,
            Json(_): Json<Value>,
        ) -> ([(axum::http::HeaderName, &'static str); 1], String) {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            // Keep the subsequent turn alive until the test interrupts it.
            if call > 0 {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            let item = json!({"id":"fc_stop","type":"function_call","status":"completed","call_id":"owned-stop-command","name":"exec_command","arguments":json!({"cmd":"Set-Content -LiteralPath owned-stop-ready.txt -Value ready -NoNewline; Start-Sleep -Seconds 30","yield_time_ms":1000,"max_output_tokens":1000}).to_string()});
            let response = json!({"id":"resp_stop","object":"response","created_at":1,"status":"in_progress","model":"fixture-model","output":[]});
            let mut body = String::new();
            for (kind, fields) in [
                ("response.created", json!({"response":response})),
                (
                    "response.output_item.done",
                    json!({"output_index":0,"item":item}),
                ),
                (
                    "response.completed",
                    json!({"response":{"id":"resp_stop","object":"response","created_at":1,"status":"completed","model":"fixture-model","output":[item],"usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}),
                ),
            ] {
                let mut event = fields;
                event["type"] = json!(kind);
                body.push_str(&format!("event: {kind}\ndata: {event}\n\n"));
            }
            (
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                body,
            )
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/v1/responses", post(model))
            .with_state(requests.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(home.join("config.toml"),format!("model = \"fixture-model\"\nmodel_provider = \"fixture\"\n[analytics]\nenabled = false\n[model_providers.fixture]\nname = \"Owned stop fixture\"\nbase_url = {base:?}\nwire_api = \"responses\"\nrequires_openai_auth = false\nsupports_websockets = false\n")).unwrap();
        let executable = std::env::var("NWF_CODEX_EXECUTABLE").expect("NWF_CODEX_EXECUTABLE");
        let (peer, mut notifications) =
            Peer::spawn(Path::new(&executable), &home, dir.path(), &[], false)
                .await
                .unwrap();
        let facts = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed = facts.clone();
        tokio::spawn(async move {
            while let Some(event) = notifications.recv().await {
                let method = event["method"].as_str().unwrap_or("");
                let p = &event["params"];
                if (matches!(method, "item/started" | "item/completed")
                    && p["item"]["type"] == "commandExecution")
                    || method == "turn/completed"
                {
                    observed.lock().await.push(json!({"method":method,"threadId":p["threadId"],"turnId":p["turnId"],"turn":p["turn"]["id"],"turnStatus":p["turn"]["status"],"itemId":p["item"]["id"],"itemStatus":p["item"]["status"],"exitCode":p["item"]["exitCode"]}));
                }
            }
        });
        peer.initialize().await.unwrap();
        let thread = peer.rpc("thread/start",json!({"model":"fixture-model","modelProvider":"fixture","cwd":dir.path(),"approvalPolicy":"never","sandbox":"danger-full-access","config":{"features.multi_agent":false}})).await.unwrap();
        let thread = thread["thread"]["id"].as_str().unwrap().to_owned();
        let turn = peer.rpc("turn/start",json!({"threadId":thread,"input":[{"type":"text","text":"Run the single owned stop-fixture command."}]})).await.unwrap();
        let turn = turn["turn"]["id"].as_str().unwrap().to_owned();
        let ready = dir.path().join("owned-stop-ready.txt");
        for _ in 0..400 {
            if ready.is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !ready.is_file() {
            peer.stop().await.unwrap();
            server.abort();
            panic!("LOCAL_NATIVE_COMMAND_NOT_READY");
        }
        let children = peer.stop_owned_children().await.unwrap();
        let mut completed = false;
        for _ in 0..20 {
            completed = facts.lock().await.iter().any(|e| {
                e["method"] == "item/completed"
                    && e["threadId"] == thread
                    && e["turnId"] == turn
                    && e["itemId"] == "owned-stop-command"
                    && matches!(e["itemStatus"].as_str(), Some("completed" | "failed"))
            });
            if completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(peer.alive().await.unwrap());
        let interrupt = peer
            .rpc("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
            .await;
        for _ in 0..40 {
            if facts
                .lock()
                .await
                .iter()
                .any(|e| e["method"] == "turn/completed")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        peer.stop().await.unwrap();
        server.abort();
        println!(
            "OWNED_LOCAL_NATIVE_STOP_FACTS {}",
            json!({"childrenExited":children,"commandCompletionBeforeInterrupt":completed,"interruptAcknowledged":interrupt.is_ok(),"facts":*facts.lock().await})
        );
        assert!(
            children > 0 && completed,
            "NATIVE_COMMAND_COMPLETION_MISSING"
        );
        assert!(interrupt.is_ok());
        assert!(
            facts
                .lock()
                .await
                .iter()
                .any(|e| e["method"] == "turn/completed"
                    && e["turn"] == turn
                    && e["turnStatus"] == "interrupted")
        );
    }
    #[tokio::test]
    #[ignore = "requires NWF_CODEX_EXECUTABLE: real pinned app-server, no model turn or network request"]
    async fn pinned_native_initialization_and_pause_failure() {
        let executable = std::env::var("NWF_CODEX_EXECUTABLE").expect("NWF_CODEX_EXECUTABLE");
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(home.join("config.toml"), "[analytics]\nenabled = false\n[shell_environment_policy]\ninherit = 'core'\nexclude = ['MODEL_API_KEY','LLM_HEADER_*','WORKER_*']\n").unwrap();
        let (peer, mut notifications) =
            Peer::spawn(Path::new(&executable), &home, dir.path(), &[], false)
                .await
                .unwrap();
        tokio::spawn(async move { while notifications.recv().await.is_some() {} });
        let initialized = peer.initialize().await.unwrap();
        assert!(initialized.is_object());
        let thread = peer
            .rpc(
                "thread/start",
                json!({"cwd":dir.path(),"approvalPolicy":"never","sandbox":"read-only"}),
            )
            .await
            .unwrap();
        let thread_id = thread["thread"]["id"].as_str().unwrap();
        let error = peer
            .rpc(
                "turn/pause",
                json!({"threadId":thread_id,"turnId":"no-turn-was-dispatched"}),
            )
            .await
            .unwrap_err();
        let native = error
            .downcast_ref::<NativeRpcError>()
            .expect("native method rejection, not timeout/process failure");
        assert!(
            [-32600, -32601].contains(&native.code),
            "unexpected native code {}",
            native.code
        );
        println!("NATIVE_BOUNDARY_PAUSE_METHOD_REJECTED code={}", native.code);
        #[cfg(windows)]
        if std::env::var("NWF_TEST_MODEL_ENV_SENTINELS").as_deref() == Ok("1") {
            assert!(std::env::var("MODEL_API_KEY").is_ok());
            assert!(std::env::var("LLM_HEADER_x_opencode_session").is_ok());
            let clean=peer.rpc("command/exec",json!({"command":["cmd.exe","/d","/c","if defined MODEL_API_KEY (exit /b 71) else if defined LLM_HEADER_x_opencode_session (exit /b 72) else (echo ENV_CLEAN)"],"cwd":dir.path(),"sandboxPolicy":{"type":"dangerFullAccess"},"timeoutMs":5000})).await.unwrap();
            assert_eq!(clean["exitCode"], 0);
            assert!(clean["stdout"].as_str().unwrap_or("").contains("ENV_CLEAN"));
            println!("NATIVE_SHELL_MODEL_ENV_EXCLUDED");
        }
        peer.stop().await.unwrap();
        assert!(!peer.alive().await.unwrap());
    }
    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "requires built NWF_CODEX_HOST_EXECUTABLE: actual Darwin guardian/fork/setsid lifecycle"]
    async fn macos_guard_proves_only_no_fork_and_retains_deleted_marker_taint() {
        let dir = tempfile::tempdir().unwrap();
        let engine = dir.path().join("fixture.py");
        std::fs::write(
            &engine,
            r#"#!/usr/bin/python3
import os, sys, json, time
for line in sys.stdin:
    m=json.loads(line)
    if 'id' not in m: continue
    result={}
    if m.get('method') == 'fixture/fork':
        try: pid=os.fork()
        except PermissionError:
            print(json.dumps({'id':m['id'],'result':{'denied':True}}),flush=True)
            continue
        if pid == 0:
            if m.get('params',{}).get('escape'): os.setsid()
            time.sleep(120)
            os._exit(0)
        result={'pid':pid}
    elif m.get('method') == 'fixture/spawn':
        try: result={'pid':os.posix_spawn('/usr/bin/true',['true'],{})}
        except PermissionError: result={'denied':True}
    print(json.dumps({'id':m['id'],'result':result}),flush=True)
"#,
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o700)).unwrap();
        let (clean, _rx) = Peer::spawn(&engine, dir.path(), dir.path(), &[], false)
            .await
            .unwrap();
        clean.initialize().await.unwrap();
        clean.stop().await.unwrap();
        assert!(clean.quiescence_proven(), "NO_FORK_SEAL_MISSING");
        let (restricted, _rx) = Peer::spawn(&engine, dir.path(), dir.path(), &[], true)
            .await
            .unwrap();
        restricted.initialize().await.unwrap();
        assert_eq!(
            restricted.rpc("fixture/fork", json!({})).await.unwrap()["denied"],
            true
        );
        assert_eq!(
            restricted.rpc("fixture/spawn", json!({})).await.unwrap()["denied"],
            true
        );
        restricted.stop().await.unwrap();
        assert!(
            restricted.quiescence_proven(),
            "FORK_DENIAL_WAS_NOT_ENFORCED"
        );
        let (forked, _rx) = Peer::spawn(&engine, dir.path(), dir.path(), &[], false)
            .await
            .unwrap();
        forked.initialize().await.unwrap();
        let child = forked
            .rpc("fixture/fork", json!({"escape":true}))
            .await
            .unwrap()["pid"]
            .as_i64()
            .unwrap() as i32;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !forked._job.fork_marker.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "FORK_MONITOR_MISSED_EVENT"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        std::fs::remove_file(&forked._job.fork_marker).unwrap();
        forked.stop().await.unwrap();
        assert!(
            !forked.quiescence_proven(),
            "DELETED_MARKER_MUST_NOT_CLEAR_STICKY_TAINT"
        );
        // An escaped process is outside the STOP proof. Clean up only this
        // fixture's exact PID, whose lifetime is still owned by this test.
        assert_eq!(unsafe { libc::kill(child, 0) }, 0);
        assert_eq!(unsafe { libc::kill(child, libc::SIGKILL) }, 0);
        let (failed, _rx) = Peer::spawn(&engine, dir.path(), dir.path(), &[], false)
            .await
            .unwrap();
        failed.initialize().await.unwrap();
        std::fs::create_dir(&failed._job.fork_marker).unwrap();
        let _ = failed.rpc("fixture/fork", json!({"escape":false})).await;
        assert!(
            failed.stop().await.is_err(),
            "MONITOR_FAILURE_MUST_NOT_SEAL"
        );
        assert!(!failed.quiescence_proven());
    }
    #[test]
    fn decisions_are_method_scoped() {
        assert!(
            response(
                "item/fileChange/requestApproval",
                &json!({}),
                &json!({"decision":"accept"})
            )
            .is_ok()
        );
        assert!(
            response(
                "item/tool/requestUserInput",
                &json!({"questions":[{"id":"q"}]}),
                &json!({"value":"yes"})
            )
            .is_ok()
        );
        assert!(response("unknown", &json!({}), &json!({"decision":"accept"})).is_err());
        assert!(
            response(
                "item/fileChange/requestApproval",
                &json!({}),
                &json!({"value":"accept"})
            )
            .is_err()
        )
    }
    #[test]
    fn elicitation_decisions_map_to_pinned_action_shape() {
        let approval = json!({"_meta":{"codex_approval_kind":"mcp_tool_call"},"requestedSchema":{"type":"object","properties":{}}});
        let r = response(
            "mcpServer/elicitation/request",
            &approval,
            &json!({"decision":"accept"}),
        )
        .unwrap();
        assert_eq!(r, json!({"action":"accept"}));
        for d in ["decline", "cancel"] {
            assert_eq!(
                response("mcpServer/elicitation/request", &approval, &json!({"decision":d}))
                    .unwrap(),
                json!({"action":d})
            );
        }
        // Empty-schema form accept gets an explicit empty content object.
        let form = json!({"mode":"form","requestedSchema":{"type":"object","properties":{}}});
        assert_eq!(
            response("mcpServer/elicitation/request", &form, &json!({"decision":"accept"}))
                .unwrap(),
            json!({"action":"accept","content":{}})
        );
        // A form that actually asks for structured input cannot be truthfully
        // auto-accepted; declining it is still honest.
        let real_form = json!({"mode":"form","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}});
        assert!(
            response("mcpServer/elicitation/request", &real_form, &json!({"decision":"accept"}))
                .is_err()
        );
        assert!(
            response("mcpServer/elicitation/request", &real_form, &json!({"decision":"decline"}))
                .is_ok()
        );
        // Decision scope is preserved: stray fields or wrong carriers refuse.
        assert!(
            response("mcpServer/elicitation/request", &approval, &json!({"decision":"accept","content":{"x":1}}))
                .is_err()
        );
        assert!(
            response("mcpServer/elicitation/request", &approval, &json!({"value":"accept"}))
                .is_err()
        );
    }
}
