use crate::{
    adapter::{self, Peer},
    store::{self, Store},
};
use anyhow::{Result, anyhow, bail};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::Mutex as AsyncMutex,
    task::JoinHandle,
    time::{Duration, timeout},
};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Action,
}
#[derive(Subcommand)]
enum Action {
    Init {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        executable: PathBuf,
        #[arg(long)]
        project: PathBuf,
        #[arg(long)]
        project_id: String,
        #[arg(long, env = "MODEL_DEFAULT_MODEL")]
        model: String,
        #[arg(long, env = "MODEL_BASE_URL")]
        base_url: String,
        /// Explicit macOS in-process apply_patch profile, or the existing native tool profile.
        #[arg(long, default_value = "native", value_parser = ["native", "macos-in-process-v1", "macos-runner-v1"])]
        tool_profile: String,
        /// Required for macos-runner-v1; frozen local command-runner entry point.
        #[arg(long, env = "NWF_COMMAND_RUNNER_MAIN")]
        command_runner: Option<PathBuf>,
        #[arg(long, env = "NWF_COMMAND_RUNNER_NODE")]
        runner_node: Option<PathBuf>,
        /// Optional JSON array of managed execution-policy presets (C7).
        #[arg(long, env = "NWF_POLICY_PRESETS")]
        policy_presets: Option<PathBuf>,
    },
    Serve {
        #[arg(long)]
        state: PathBuf,
        #[arg(long, default_value_t = 0)]
        port: u16,
        #[arg(long, default_value = "WORKER_LOCAL_TOKEN")]
        token_env: String,
    },
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Registry {
    executable: PathBuf,
    version: String,
    executable_digest: String,
    schema_digest: String,
    launch_config_digest: String,
    project: PathBuf,
    pub(crate) project_id: String,
    model: String,
    base_url: String,
    #[serde(default = "native_tool_profile")]
    tool_profile: String,
    #[serde(default)]
    model_catalog: Option<PathBuf>,
    #[serde(default)]
    model_catalog_digest: Option<String>,
    #[serde(default)]
    experimental_schema_digest: Option<String>,
    #[serde(default)]
    command_runner: Option<PathBuf>,
    #[serde(default)]
    command_runner_digest: Option<String>,
    #[serde(default)]
    runner_node: Option<PathBuf>,
    #[serde(default)]
    runner_node_digest: Option<String>,
    // C7: frozen managed execution-policy presets published as policyOptions.
    // Each preset is a validated JSON object; its digest is computed over the
    // canonical stored form and never trusted from the file.
    #[serde(default)]
    policy_presets: Vec<Value>,
}
fn bounded_profile(profile: &str) -> bool {
    matches!(profile, "macos-in-process-v1" | "macos-runner-v1")
}
fn native_tool_profile() -> String {
    "native".into()
}
type ReadVerificationStamp = Vec<(u64, std::time::SystemTime)>;
type ReadVerificationCache = AsyncMutex<Option<(std::time::Instant, Option<ReadVerificationStamp>, bool)>>;

pub(crate) struct Host {
    pub(crate) root: PathBuf,
    pub(crate) registry: Registry,
    pub(crate) store: Mutex<Store>,
    peers: AsyncMutex<HashMap<String, Arc<Peer>>>,
    // Live auxiliary-session peers: task -> auxSessionId -> peer. Each owns a
    // separate app-server bound to an isolated workspace copy; facts are
    // durable in aux_sessions, the peer itself is never claimed across restart.
    aux_peers: AsyncMutex<HashMap<String, HashMap<String, Arc<Peer>>>>,
    // Live collaboration child peers: task -> "collaborationId/childId" -> peer.
    // Each is a separate managed app-server bound to its own writable checkout;
    // durable facts live in collab_children/collaborations, never in the peer.
    collab_peers: AsyncMutex<HashMap<String, HashMap<String, Arc<Peer>>>>,
    // Live terminal output rings: task -> native processId -> buffered bytes.
    // Bytes are transient by design (bounded ring, cursor-addressed); durable
    // facts live in the terminals table and the event stream.
    terminals: Mutex<HashMap<String, HashMap<String, TermLive>>>,
    preparation: AsyncMutex<()>,
    read_verification: ReadVerificationCache,
    runners: AsyncMutex<HashMap<String, TaskRunner>>,
    runner_admission: AsyncMutex<()>,
    // Serializes queue-drain passes so two drains cannot oversubscribe the
    // bounded slot set admitted between them.
    queue: Mutex<()>,
    max_active: usize,
    // Host-wide bound on simultaneously-open aux sessions
    // (NWF_CODEX_MAX_ACTIVE_AUX, default 2).
    max_active_aux: usize,
    // Host-wide bound on simultaneously-RUNNING collaboration children
    // (NWF_CODEX_MAX_ACTIVE_COLLAB_CHILDREN, default 4).
    max_collab_children: usize,
    boot: String,
    token: String,
    _lock: File,
}
struct TaskRunner {
    client: Arc<crate::runner::Runner>,
    contexts: HashSet<String>,
}

// One in-flight `command/exec` session. `base` is the absolute cursor of
// buf[0]; appended bytes beyond TERMINAL_RING drop the oldest prefix and the
// reader is told via `truncated`.
struct TermLive {
    buf: std::collections::VecDeque<u8>,
    base: u64,
    capped: bool,
    // Owns the deferred-exec settle task so the session's exit fact is written
    // by exactly the generation that spawned it.
    _waiter: Option<JoinHandle<()>>,
}
const TERMINAL_RING: usize = 128 * 1024;
const TERMINAL_MAX_PER_TASK: i64 = 8;
const TERMINAL_OUTPUT_MAX: i64 = 64 * 1024;
pub(crate) type Shared = Arc<Host>;
pub(crate) fn hash(bytes: impl AsRef<[u8]>) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn file_hash(path: impl AsRef<Path>) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 { break; }
        digest.update(&buffer[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}
fn canonical_path(path: impl AsRef<Path>) -> Result<PathBuf> {
    let p = fs::canonicalize(path)?;
    #[cfg(windows)]
    {
        if let Some(s) = p.to_str().and_then(|s| s.strip_prefix(r"\\?\")) {
            return Ok(PathBuf::from(s));
        }
    }
    Ok(p)
}
pub fn safe_code(e: &anyhow::Error) -> String {
    let s = e.to_string();
    if s.len() <= 100
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b == b'_' || b.is_ascii_digit())
    {
        s
    } else {
        "LOCAL_SOURCE_UNAVAILABLE".into()
    }
}
pub(crate) fn string<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("INVALID_REQUEST"))
}
pub(crate) fn valid_id(s: &str) -> Result<&str> {
    if s.is_empty()
        || s.len() > 128
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        bail!("INVALID_ID")
    }
    Ok(s)
}
pub(crate) fn exact(v: &Value, keys: &[&str]) -> Result<()> {
    let o = v.as_object().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
    if o.keys().any(|k| !keys.contains(&k.as_str())) {
        bail!("UNEXPECTED_FIELD")
    }
    Ok(())
}
fn instruction(v: &Value, key: &str) -> Result<String> {
    let s = string(v, key)?;
    if s.trim().is_empty() || s.len() > 16384 {
        bail!("INVALID_INSTRUCTION")
    }
    Ok(s.into())
}
fn run(args: &[&str], cwd: Option<&Path>) -> Result<String> {
    let mut cmd = Command::new(args[0]);
    cmd.args(&args[1..]);
    if let Some(p) = cwd {
        cmd.current_dir(p);
    }
    let o = cmd.output()?;
    if !o.status.success() || o.stdout.len() > 1024 * 1024 {
        bail!("LOCAL_PROCESS_FAILED")
    }
    Ok(String::from_utf8(o.stdout)?.trim_end().into())
}
fn git(path: &Path, args: &[&str]) -> Result<String> {
    let mut a = vec![
        "git",
        "-c",
        "core.longpaths=true",
        "-C",
        path.to_str().ok_or_else(|| anyhow!("INVALID_PATH"))?,
    ];
    a.extend(args);
    run(&a, None)
}
fn project(r: &Registry) -> Result<Value> {
    let root = canonical_path(&r.project)?;
    let actual = canonical_path(git(&root, &["rev-parse", "--show-toplevel"])?)?;
    if root != actual {
        bail!("SELECT_GIT_ROOT")
    }
    let head = git(&root, &["rev-parse", "HEAD"])?;
    if head.len() != 40 && head.len() != 64 {
        bail!("COMMITTED_BASE_REQUIRED")
    }
    Ok(
        json!({"projectId":r.project_id,"displayName":r.project_id,"path":root,"headRevision":head,"hasUncommittedChanges":!git(&root,&["status","--porcelain"] )?.is_empty()}),
    )
}
fn lock(root: &Path) -> Result<File> {
    fs::create_dir_all(root)?;
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("host.lock"))?;
    f.try_lock_exclusive()
        .map_err(|_| anyhow!("LOCAL_HOST_ALREADY_OWNS_STATE"))?;
    Ok(f)
}
fn toml_string(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}
// Explicit local tool metadata; this keeps the configured model ID unchanged.
// The pinned server's ModelInfo supports only freeform apply_patch. Qualification
// requires the real provider accepting and completing that exact custom tool.
fn model_catalog(model: &str, runner: bool) -> Value {
    let instructions = if runner {
        "You are an implementation worker. Use apply_patch for workspace edits. Use run_command, run_build and run_tests for shell, build and tests through the host runner. Native shell execution is disabled. Inspect actual command output and exitCode before reporting success."
    } else {
        "You are an implementation worker. Use apply_patch for workspace edits. Shell execution is unavailable in this explicitly selected profile. Report actual changes."
    };
    json!({"models":[{
        "slug":model,"display_name":model,"description":if runner {"macos-runner-v1 local tool binding"} else {"macos-in-process-v1 local tool binding"},
        "default_reasoning_level":"low","supported_reasoning_levels":[{"effort":"low","description":"Configured low reasoning"}],
        "shell_type":"disabled","visibility":"list","supported_in_api":true,"priority":0,
        "availability_nux":null,"upgrade":null,"base_instructions":instructions,
        "include_apps_usage_instructions":false,"support_verbosity":false,"default_verbosity":null,
        "apply_patch_tool_type":"freeform","truncation_policy":{"mode":"tokens","limit":10000},
        "experimental_supported_tools":[],"input_modalities":["text"],"supports_search_tool":false
    }]})
}
fn config(r: &Registry) -> String {
    let catalog = r
        .model_catalog
        .as_ref()
        .map(|p| {
            format!(
                "model_catalog_json = {}\n",
                toml_string(&p.to_string_lossy())
            )
        })
        .unwrap_or_default();
    let features = if bounded_profile(&r.tool_profile) {
        "shell_tool = false\nshell_snapshot = false\napps = false\nplugins = false\nremote_plugin = false\nplugin_sharing = false\n"
    } else {
        ""
    };
    let mut headers = String::new();
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("LLM_HEADER_")) {
        let header = k.trim_start_matches("LLM_HEADER_").replace('_', "-");
        if header
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            headers.push_str(&format!("{} = {}\n", toml_string(&header), toml_string(&k)));
        }
    }
    format!(
        "{catalog}model = {}\nmodel_provider = \"nwf\"\nmodel_reasoning_effort = \"low\"\napproval_policy = \"on-request\"\nsandbox_mode = \"workspace-write\"\n[model_providers.nwf]\nname = \"Configured model endpoint\"\nbase_url = {}\nwire_api = \"responses\"\nenv_key = \"MODEL_API_KEY\"\nrequest_max_retries = 0\nstream_max_retries = 0\n[model_providers.nwf.env_http_headers]\n{}\n[shell_environment_policy]\ninherit = \"core\"\nexclude = [\"MODEL_API_KEY\",\"LLM_HEADER_*\",\"WORKER_*\",\"ACS_*\",\"PAC_*\",\"PERSONAL_*\"]\n[features]\nmulti_agent = false\n{features}[analytics]\nenabled = false\n",
        toml_string(&r.model),
        toml_string(&r.base_url),
        headers
    )
}
pub async fn main() -> Result<()> {
    match Cli::parse().command {
        Action::Init {
            state,
            executable,
            project: source,
            project_id,
            model,
            base_url,
            tool_profile,
            command_runner,
            runner_node,
            policy_presets,
        } => {
            let _lock = lock(&state)?;
            if state.join("registry.json").exists()
                || state.join("profile").exists()
                || state.join("worker.sqlite").exists()
            {
                bail!("LOCAL_STATE_ALREADY_INITIALIZED")
            }
            if model.trim().is_empty() || base_url.trim().is_empty() {
                bail!("INVALID_MODEL_BINDING")
            }
            valid_id(&project_id)?;
            let executable = canonical_path(executable)?;
            let version = run(
                &[
                    executable.to_str().ok_or_else(|| anyhow!("INVALID_PATH"))?,
                    "--version",
                ],
                None,
            )?;
            let pin: Value = serde_json::from_str(include_str!("../protocol/codex.lock.json"))?;
            if version != pin["version"] {
                bail!("ENGINE_VERSION_UNQUALIFIED")
            }
            let executable_digest = file_hash(&executable)?;
            verify_pin(&pin, &version, &executable_digest)?;
            let schema = state.join("schema");
            fs::create_dir_all(&schema)?;
            run(
                &[
                    executable.to_str().unwrap(),
                    "app-server",
                    "generate-json-schema",
                    "--out",
                    schema.to_str().unwrap(),
                ],
                None,
            )?;
            let schema_digest = hash(fs::read(
                schema.join("codex_app_server_protocol.schemas.json"),
            )?);
            if schema_digest != pin["schemaSha256"] {
                bail!("ENGINE_SCHEMA_CHANGED")
            }
            if bounded_profile(&tool_profile) && !cfg!(target_os = "macos") {
                bail!("ENGINE_PLATFORM_UNQUALIFIED")
            }
            if bounded_profile(&tool_profile) && !Path::new("/usr/bin/sandbox-exec").is_file() {
                bail!("MACOS_FORK_SANDBOX_REQUIRED")
            }
            let catalog_path = if bounded_profile(&tool_profile) {
                Some(canonical_path(&state)?.join("profile/model-catalog.json"))
            } else {
                None
            };
            let mut r = Registry {
                executable,
                version,
                executable_digest,
                schema_digest,
                launch_config_digest: String::new(),
                project: canonical_path(source)?,
                project_id,
                model,
                base_url,
                tool_profile,
                model_catalog: catalog_path,
                model_catalog_digest: None,
                experimental_schema_digest: None,
                command_runner: None,
                command_runner_digest: None,
                runner_node: None,
                runner_node_digest: None,
                policy_presets: vec![],
            };
            if let Some(path) = policy_presets {
                r.policy_presets = load_policy_presets(&path, &r.project_id)?;
            }
            if r.tool_profile == "macos-runner-v1" {
                let experimental = state.join("schema-experimental");
                fs::create_dir_all(&experimental)?;
                run(
                    &[
                        r.executable.to_str().unwrap(),
                        "app-server",
                        "generate-json-schema",
                        "--experimental",
                        "--out",
                        experimental.to_str().unwrap(),
                    ],
                    None,
                )?;
                let digest = hash(fs::read(
                    experimental.join("codex_app_server_protocol.schemas.json"),
                )?);
                if digest != crate::runner::EXPERIMENTAL_SCHEMA_SHA256 {
                    bail!("ENGINE_EXPERIMENTAL_SCHEMA_CHANGED")
                }
                r.experimental_schema_digest = Some(digest);
                let main = canonical_path(
                    command_runner.ok_or_else(|| anyhow!("RUNNER_CONFIGURATION_REQUIRED"))?,
                )?;
                if main.file_name().and_then(|n| n.to_str()) != Some("main.mjs") {
                    bail!("RUNNER_CONFIGURATION_REQUIRED")
                }
                r.command_runner_digest = Some(crate::runner::bundle_digest(&main)?);
                r.command_runner = Some(main);
                let node = canonical_path(match runner_node {
                    Some(p) => p,
                    None => PathBuf::from(run(&["which", "node"], None)?),
                })?;
                let node_version = run(&[node.to_str().unwrap(), "--version"], None)?;
                if !crate::runner::node_qualified(&node_version) {
                    bail!("RUNNER_NODE_UNQUALIFIED")
                }
                r.runner_node_digest = Some(hash(fs::read(&node)?));
                r.runner_node = Some(node);
            }
            project(&r)?;
            fs::create_dir_all(state.join("profile"))?;
            if let Some(path) = &r.model_catalog {
                let contents = serde_json::to_vec_pretty(&model_catalog(
                    &r.model,
                    r.tool_profile == "macos-runner-v1",
                ))?;
                r.model_catalog_digest = Some(hash(&contents));
                fs::write(path, contents)?;
            }
            let contents = config(&r);
            r.launch_config_digest = hash(&contents);
            fs::create_dir_all(state.join("profile"))?;
            fs::write(state.join("profile/config.toml"), contents)?;
            fs::write(state.join("registry.json"), serde_json::to_vec_pretty(&r)?)?;
            println!("NATIVE_WORKER_REGISTERED codex {}", r.version);
            Ok(())
        }
        Action::Serve {
            state,
            port,
            token_env,
        } => {
            let held = lock(&state)?;
            let root = canonical_path(state)?;
            let registry: Registry =
                serde_json::from_slice(&fs::read(root.join("registry.json"))?)?;
            let token = std::env::var(token_env).map_err(|_| anyhow!("LOCAL_TOKEN_REQUIRED"))?;
            if token.len() < 24 {
                bail!("LOCAL_TOKEN_REQUIRED")
            }
            verify(&root, &registry)?;
            let read_verification_stamp = runtime_read_stamp(&root, &registry);
            // Bounded active slots: NWF_CODEX_MAX_ACTIVE_TASKS, default 4. A value
            // that cannot be parsed is a configuration fault, not a silent default.
            let max_active = match std::env::var("NWF_CODEX_MAX_ACTIVE_TASKS") {
                Err(_) => 4,
                Ok(v) => v
                    .parse::<usize>()
                    .map_err(|_| anyhow!("LOCAL_CONFIG_INVALID"))?,
            };
            // Host-wide live-aux bound: NWF_CODEX_MAX_ACTIVE_AUX, default 2 —
            // the same admission shape as NWF_PI_MAX_ACTIVE_AUX on the Pi host.
            let max_active_aux = match std::env::var("NWF_CODEX_MAX_ACTIVE_AUX") {
                Err(_) => 2,
                Ok(v) => v
                    .parse::<usize>()
                    .map_err(|_| anyhow!("LOCAL_CONFIG_INVALID"))?,
            };
            // Host-wide running-collab-child bound:
            // NWF_CODEX_MAX_ACTIVE_COLLAB_CHILDREN, default 4 — the same
            // admission shape as NWF_PI_MAX_ACTIVE_COLLAB_CHILDREN on the Pi
            // host.
            let max_collab_children = match std::env::var("NWF_CODEX_MAX_ACTIVE_COLLAB_CHILDREN")
            {
                Err(_) => 4,
                Ok(v) => v
                    .parse::<usize>()
                    .map_err(|_| anyhow!("LOCAL_CONFIG_INVALID"))?,
            };
            let mut s = Store::open(&root.join("worker.sqlite"))?;
            s.recover()?;
            let h = Arc::new(Host {
                root,
                registry,
                store: Mutex::new(s),
                peers: AsyncMutex::new(HashMap::new()),
                aux_peers: AsyncMutex::new(HashMap::new()),
                collab_peers: AsyncMutex::new(HashMap::new()),
                terminals: Mutex::new(HashMap::new()),
                preparation: AsyncMutex::new(()),
                read_verification: AsyncMutex::new(Some((std::time::Instant::now(), read_verification_stamp, true))),
                runners: AsyncMutex::new(HashMap::new()),
                runner_admission: AsyncMutex::new(()),
                queue: Mutex::new(()),
                max_active,
                max_active_aux,
                max_collab_children,
                boot: store::id(),
                token,
                _lock: held,
            });
            // Queued intents surviving the restart get their single admission
            // evaluation now; every later dispatch window stays crash-safe.
            drain_queue(&h);
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
            println!(
                "LOCAL_TASK_READY http://127.0.0.1:{} store={}",
                listener.local_addr()?.port(),
                h.store.lock().unwrap().identity["storeId"]
                    .as_str()
                    .unwrap()
            );
            // Aux + collaboration deadline sweep (1s cadence like the Pi host):
            // an open row past its durable deadline settles CANCELLED and its
            // managed peer is released.
            let sweep = h.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    sweep_aux_deadlines(&sweep).await;
                    sweep_collab_deadlines(&sweep).await;
                }
            });
            let shutdown = h.clone();
            axum::serve(listener, Router::new().fallback(route).with_state(h))
                .with_graceful_shutdown(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    // Close through the same durable stop/proof path as owner
                    // STOP. Killing the transport alone can race the exit watcher
                    // and leave a completed session unsealed on the next boot.
                    let owned: Vec<_> = shutdown.peers.lock().await.iter()
                        .map(|(task, peer)| (task.clone(), peer.clone())).collect();
                    for (task, peer) in owned {
                        if stop_owner(&shutdown, &task, Some(("HOST_CLOSE", "HOST_CLOSE"))).await.is_err() {
                            // Exit remains bounded, but an unproven outcome stays
                            // unreconciled rather than acquiring a completed seal.
                            let _ = peer.stop().await;
                        }
                    }
                    for task_peers in shutdown.aux_peers.lock().await.values() {
                        for peer in task_peers.values() {
                            let _ = peer.stop().await;
                        }
                    }
                    for task_peers in shutdown.collab_peers.lock().await.values() {
                        for peer in task_peers.values() {
                            let _ = peer.stop().await;
                        }
                    }
                    // Durable close proof: every still-open aux session settles
                    // CANCELLED/HOST_CLOSE — the row is the durable record even
                    // when the process exits before a restart observes it.
                    {
                        let mut s = shutdown.store.lock().unwrap();
                        if let Ok(tasks) = s.tasks() {
                            for task_row in tasks {
                                let tid =
                                    task_row["taskId"].as_str().unwrap_or("").to_owned();
                                if let Ok(list) = s.aux_sessions(&tid) {
                                    for aux in list {
                                        if aux["state"].as_str() == Some("RUNNING") {
                                            let id = aux["auxSessionId"]
                                                .as_str()
                                                .unwrap_or("")
                                                .to_owned();
                                            if s.aux_transition(
                                                &tid,
                                                &id,
                                                AUX_OPEN,
                                                "CANCELLED",
                                                Some("HOST_CLOSE"),
                                                |_| {},
                                            )
                                            .unwrap_or(false)
                                            {
                                                let _ = s.event(
                                                    &tid,
                                                    "aux-session.completed",
                                                    json!({"auxSessionId":id,"state":"CANCELLED","reasonCode":"HOST_CLOSE"}),
                                                );
                                            }
                                        }
                                    }
                                }
                                // W5b close proof: pending children settle BLOCKED
                                // and the collaboration seals CANCELLED; running
                                // children settle CANCELLED individually so their
                                // completion events stay honest.
                                if let Ok(list) = s.collaborations(&tid) {
                                    for collab in list {
                                        if collab["state"].as_str() != Some("RUNNING") {
                                            continue;
                                        }
                                        let cid = collab["collaborationId"]
                                            .as_str()
                                            .unwrap_or("")
                                            .to_owned();
                                        if let Ok(running) =
                                            s.collab_stop_mark(&tid, &cid, "HOST_CLOSE")
                                        {
                                            for ch in running {
                                                if s.collab_child_transition(
                                                    &tid,
                                                    &cid,
                                                    &ch,
                                                    COLLAB_OPEN,
                                                    "CANCELLED",
                                                    Some("HOST_CLOSE"),
                                                    |_| {},
                                                )
                                                .unwrap_or(false)
                                                {
                                                    let _ = s.event(
                                                        &tid,
                                                        "collaboration.child.completed",
                                                        json!({"collaborationId":cid,"childId":ch,"state":"CANCELLED","reasonCode":"HOST_CLOSE"}),
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .await?;
            Ok(())
        }
    }
}
fn verify_pin(pin: &Value, version: &str, digest: &str) -> Result<()> {
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "windows-x86_64",
        ("macos", "aarch64") => "macos-arm64",
        ("linux", "x86_64") => "linux-x86_64",
        _ => bail!("ENGINE_PLATFORM_UNQUALIFIED"),
    };
    if version != pin["version"] {
        bail!("ENGINE_VERSION_UNQUALIFIED")
    }
    let expected = pin["platforms"][platform]["executableSha256"]
        .as_str()
        .ok_or_else(|| anyhow!("ENGINE_PLATFORM_UNQUALIFIED"))?;
    if digest != expected {
        bail!("ENGINE_BINARY_CHANGED")
    }
    Ok(())
}
fn apply_stop_proof(t: &mut Value, proven: bool, reason: Option<&str>) {
    t["quiescent"] = json!(proven);
    let mut proof = json!({"managedProcessTree":"EXITED_OR_NEVER_STARTED","preparation":"DRAINED","remoteEffects":"NOT_PROVEN"});
    #[cfg(target_os = "macos")]
    {
        proof["managedProcessGroup"] = json!("EXITED_OR_NEVER_STARTED");
        proof["managedProcessTree"] = json!(if proven {
            "NO_FORK_OBSERVED_ROOT_AND_GROUP_EXITED"
        } else {
            "UNPROVEN_AFTER_FORK"
        });
        proof["forkMonitor"] = json!("KQUEUE_NOTE_FORK_BEFORE_ENGINE_EXEC");
        proof["escapedProcesses"] = json!(if proven {
            "NO_ENGINE_FORK_OBSERVED"
        } else {
            "NOT_PROVEN"
        });
    }
    if !proven {
        t["state"] = json!("UNKNOWN");
        t["reasonCode"] = json!("NATIVE_WRITER_UNPROVEN");
    }
    if let Some(reason) = reason {
        proof["reason"] = json!(reason);
    }
    t["stopProof"] = proof;
}
fn verify(root: &Path, r: &Registry) -> Result<()> {
    let pin: Value = serde_json::from_str(include_str!("../protocol/codex.lock.json"))?;
    verify_pin(&pin, &r.version, &r.executable_digest)?;
    if r.schema_digest != pin["schemaSha256"] {
        bail!("ENGINE_SCHEMA_CHANGED")
    }
    if file_hash(&r.executable)? != r.executable_digest {
        bail!("ENGINE_BINARY_CHANGED")
    }
    if bounded_profile(&r.tool_profile) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if !fs::metadata("/usr/bin/sandbox-exec")
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            {
                bail!("MACOS_FORK_SANDBOX_REQUIRED")
            }
        }
        #[cfg(not(target_os = "macos"))]
        bail!("ENGINE_PLATFORM_UNQUALIFIED");
    }
    if r.tool_profile == "macos-runner-v1"
        && (r.experimental_schema_digest.as_deref()
            != Some(crate::runner::EXPERIMENTAL_SCHEMA_SHA256)
            || hash(fs::read(root.join(
                "schema-experimental/codex_app_server_protocol.schemas.json",
            ))?) != crate::runner::EXPERIMENTAL_SCHEMA_SHA256)
    {
        bail!("ENGINE_EXPERIMENTAL_SCHEMA_CHANGED")
    }
    if r.tool_profile == "macos-runner-v1" {
        let main = r
            .command_runner
            .as_ref()
            .ok_or_else(|| anyhow!("RUNNER_CONFIGURATION_REQUIRED"))?;
        let node = r
            .runner_node
            .as_ref()
            .ok_or_else(|| anyhow!("RUNNER_CONFIGURATION_REQUIRED"))?;
        if Some(crate::runner::bundle_digest(main)?) != r.command_runner_digest
            || Some(hash(fs::read(node)?)) != r.runner_node_digest
        {
            bail!("RUNNER_CONFIG_CHANGED")
        }
    }
    if let Some(path) = &r.model_catalog {
        if Some(hash(fs::read(path)?)) != r.model_catalog_digest {
            bail!("ENGINE_CONFIG_CHANGED")
        }
    } else if r.tool_profile != "native" {
        bail!("ENGINE_CONFIG_CHANGED")
    }
    if hash(fs::read(root.join("profile/config.toml"))?) != r.launch_config_digest {
        bail!("ENGINE_CONFIG_CHANGED")
    }
    Ok(())
}
fn identity(h: &Host) -> Value {
    let mut v = h.store.lock().unwrap().identity.clone();
    v["schemaVersion"] = json!("worker-local/1");
    v["bootId"] = json!(h.boot);
    v["storeGuard"] = json!("HEADER_V1");
    v["nativeToolProfile"] = json!(h.registry.tool_profile);
    if h.registry.tool_profile == "macos-runner-v1" {
        v["commandRunnerScope"] = json!({"scope":"REGISTERED_PROCESS_GROUP","modelProcessFork":"DENIED","arbitraryEscapedProcesses":"NOT_PROVEN","remoteEffects":"NOT_PROVEN","runnerFilesystemIsolation":"NOT_PROVEN"});
    }
    v
}
// CA-ANDROID-V2 C7 — managed execution-policy presets. The Init flag loads a
// JSON array of preset objects; every field is validated and the digest is
// computed over the canonical stored form (never trusted from the file).
fn load_policy_presets(path: &Path, project_id: &str) -> Result<Vec<Value>> {
    let v: Value = serde_json::from_slice(&fs::read(path)?)?;
    let a = v.as_array().ok_or_else(|| anyhow!("POLICY_PRESET_INVALID"))?;
    if a.is_empty() || a.len() > 16 {
        bail!("POLICY_PRESET_INVALID")
    }
    let mut seen = HashSet::new();
    for p in a {
        exact(
            p,
            &[
                "presetId",
                "revision",
                "engine",
                "projectScope",
                "effectiveRange",
                "escalationNote",
                "revocable",
                "sandbox",
                "approval",
                "writableRoots",
                "denyPaths",
                "networkAccess",
                "maxDurationMs",
            ],
        )?;
        let id = string(p, "presetId")?;
        valid_id(id)?;
        if !seen.insert(id.to_owned()) {
            bail!("POLICY_PRESET_INVALID")
        }
        if !p["revision"]
            .as_str()
            .is_some_and(|r| !r.is_empty() && r.len() <= 64)
        {
            bail!("POLICY_PRESET_INVALID")
        }
        if p["engine"].as_str().unwrap_or("codex") != "codex" {
            bail!("POLICY_PRESET_INVALID")
        }
        if p["projectScope"].as_str() != Some("*")
            && p["projectScope"].as_str() != Some(project_id)
        {
            bail!("POLICY_PRESET_INVALID")
        }
        if p["effectiveRange"].as_str() != Some("NEXT_TASK") {
            bail!("POLICY_PRESET_INVALID")
        }
        match p["sandbox"].as_str() {
            Some("workspace-write") | Some("read-only") | Some("danger-full-access") => {}
            _ => bail!("POLICY_PRESET_INVALID"),
        }
        match p["approval"].as_str() {
            Some("on-request") | Some("never") | Some("untrusted") | Some("on-failure") => {}
            _ => bail!("POLICY_PRESET_INVALID"),
        }
        if let Some(n) = p["networkAccess"].as_str() {
            if !matches!(n, "ALLOWLIST" | "DENIED" | "UNRESTRICTED") {
                bail!("POLICY_PRESET_INVALID")
            }
        }
        if let Some(w) = p["writableRoots"].as_array() {
            if w.len() > 16
                || w.iter().any(|r| {
                    r.as_str()
                        .map(|s| {
                            s.is_empty()
                                || s.len() > 1024
                                || !(s.starts_with('/') || s.chars().nth(1) == Some(':'))
                        })
                        .unwrap_or(true)
                })
            {
                bail!("POLICY_PRESET_INVALID")
            }
        } else if !p["writableRoots"].is_null() {
            bail!("POLICY_PRESET_INVALID")
        }
        if let Some(d) = p["denyPaths"].as_array() {
            if d.len() > 64
                || d.iter().any(|r| {
                    r.as_str()
                        .map(|s| s.is_empty() || s.len() > 1024 || s.contains(".."))
                        .unwrap_or(true)
                })
            {
                bail!("POLICY_PRESET_INVALID")
            }
        } else if !p["denyPaths"].is_null() {
            bail!("POLICY_PRESET_INVALID")
        }
        if !p["maxDurationMs"].is_null()
            && p["maxDurationMs"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .is_none()
        {
            bail!("POLICY_PRESET_INVALID")
        }
        if p["escalationNote"].as_str().map(|s| s.len() > 512).unwrap_or(false) {
            bail!("POLICY_PRESET_INVALID")
        }
        if !p["revocable"].is_boolean() && !p["revocable"].is_null() {
            bail!("POLICY_PRESET_INVALID")
        }
    }
    Ok(a.iter().cloned().collect())
}
// Published policyOptions for preflight/engines surfaces: stable preset ids,
// revisions and digests plus the engine-fixed policy baseline.
fn policy_options(h: &Host) -> Value {
    let presets: Vec<Value> = h
        .registry
        .policy_presets
        .iter()
        .map(|p| {
            json!({
                "presetId": p["presetId"],
                "revision": p["revision"],
                "engine": "codex",
                "projectScope": p["projectScope"],
                "effectiveRange": p["effectiveRange"],
                "escalationNote": p["escalationNote"],
                "revocable": p["revocable"],
                "policyPresetDigest": hash(store::canonical(p).unwrap_or_default()),
                "policy": {
                    "sandbox": p["sandbox"],
                    "approval": p["approval"],
                    "writableRoots": p["writableRoots"],
                    "denyPaths": p["denyPaths"],
                    "networkAccess": p["networkAccess"],
                    "maxDurationMs": p["maxDurationMs"],
                },
            })
        })
        .collect();
    json!({
        "presets": presets,
        "engineFixed": {"sandbox":"workspace-write","approval":"on-request","networkAccess":"ALLOWLIST"},
        "default": {"policyPresetId":null,"sandbox":"workspace-write","approval":"on-request","networkAccess":"ALLOWLIST"},
    })
}
// C7: resolve a request's optional policyPresetId against the registry —
// an unknown id refuses POLICY_PRESET_UNKNOWN at intake, never silently
// degrading to engine defaults.
fn policy_preset(h: &Host, b: &Value) -> Result<Option<Value>> {
    if b["policyPresetId"].is_null() {
        return Ok(None);
    }
    let id = b["policyPresetId"]
        .as_str()
        .and_then(|s| valid_id(s).ok())
        .ok_or_else(|| anyhow!("POLICY_PRESET_UNKNOWN"))?;
    Ok(Some(
        h.registry
            .policy_presets
            .iter()
            .find(|p| p["presetId"].as_str() == Some(id))
            .cloned()
            .ok_or_else(|| anyhow!("POLICY_PRESET_UNKNOWN"))?,
    ))
}
// C7 — the task's frozen execution policy. A task that named a preset carries
// its presetId+digest forever; the effective sandbox/approval are bound into
// the thread params at launch. A preset whose digest no longer matches the
// registry is reported honestly instead of silently re-applied.
fn execution_policy(h: &Host, t: &Value) -> Value {
    let preset = &t["policyPreset"];
    let mut v = json!({
        "state": "FROZEN_AT_TASK_CREATE",
        "engineFixed": {"sandbox":"workspace-write","approval":"on-request","networkAccess":"ALLOWLIST"},
        "turnParamsChanged": false,
    });
    if preset.is_object() {
        v["policyPresetId"] = preset["presetId"].clone();
        v["policyPresetDigest"] = preset["digest"].clone();
        v["policy"] = json!({
            "sandbox": preset["sandbox"],
            "approval": preset["approval"],
            "writableRoots": preset["writableRoots"],
            "denyPaths": preset["denyPaths"],
            "networkAccess": preset["networkAccess"],
            "maxDurationMs": preset["maxDurationMs"],
        });
        let still = h.registry.policy_presets.iter().any(|p| {
            p["presetId"] == preset["presetId"]
                && hash(store::canonical(p).unwrap_or_default())
                    == preset["digest"].as_str().unwrap_or("")
        });
        if !still {
            v["turnParamsChanged"] = json!("PRESET_REMOVED");
        }
    } else {
        v["policyPresetId"] = Value::Null;
        v["policy"] = json!({
            "sandbox": "workspace-write",
            "approval": "on-request",
            "writableRoots": t["writeScope"]["extraWriteDirs"],
            "denyPaths": t["writeScope"]["denyPaths"],
            "networkAccess": "ALLOWLIST",
        });
    }
    v
}
// C0 — source-capabilities/1 source capability projection. States use the
// contract vocabulary (AVAILABLE/NOT_INTEGRATED/UNSUPPORTED/DENIED/
// BLOCKED_BY_STATE/TEMPORARILY_UNAVAILABLE/UNKNOWN); absent fields mean
// unknown, and state-based availability is computed per call, never cached.
// Action keys use the dotted grammar shared with PAC
// (PersonalMobileControls) and the Pi host; names without a PAC/Pi
// analogue stay honest source-local facts instead of borrowing a
// contract name the boundary does not read.
fn source_capabilities(h: &Host, t: &Value) -> Value {
    let sealed = t["sealedFor"].is_object();
    let inactive = sealed
        || t["stopProof"].is_object()
        || matches!(
            t["state"].as_str(),
            Some(
                "STOP_REQUESTED"
                    | "CANCELLED"
                    | "FAILED"
                    | "BUDGET_EXCEEDED"
                    | "BINDING_UNAVAILABLE"
                    | "REJECTED"
                    | "UNKNOWN"
            )
        );
    let write_state = |sealed_code: &str| {
        if sealed {
            ("BLOCKED_BY_STATE", json!(sealed_code))
        } else if inactive {
            ("BLOCKED_BY_STATE", json!("COLLABORATION_TASK_INACTIVE"))
        } else {
            ("AVAILABLE", Value::Null)
        }
    };
    let mut a = serde_json::Map::new();
    let act = |state: &str, reason: Value, limits: Value, semantics: &str| {
        json!({"state":state,"reasonCode":reason,"limits":limits,"semantics":semantics})
    };
    a.insert(
        "childMessages.read".into(),
        act(
            "AVAILABLE",
            Value::Null,
            json!({"pageDefault":"50","pageMax":"100","kinds":["ASSIGNMENT","USER_INPUT","ASSISTANT_MESSAGE","TOOL_CALL","TOOL_RESULT","RETURN","STATUS"],"coverage":["FULL_SINCE_CAPTURE","SUMMARY_ONLY","PARTIAL","UNAVAILABLE"]}),
            "DURABLE_PER_CHILD_PUBLIC_STREAM",
        ),
    );
    let (s, r) = write_state("SOURCE_SEALED");
    a.insert(
        "collaboration.preflight".into(),
        act(
            s,
            r,
            json!({"maxChildren":COLLAB_MAX_CHILDREN.to_string(),"maxGoalBytes":COLLAB_MAX_GOAL.to_string(),"ttlMs":COLLAB_PLAN_TTL_MS.to_string()}),
            "FROZEN_PLAN_DIGEST_EXPIRY_GUARDED_CREATE",
        ),
    );
    let (s, r) = write_state("SOURCE_SEALED");
    a.insert(
        "collaboration.create".into(),
        act(
            s,
            r,
            json!({"maxChildren":COLLAB_MAX_CHILDREN.to_string(),"maxActivePerTask":COLLAB_MAX_PER_TASK.to_string(),"expectedPreflight":"OPTIONAL_GUARD"}),
            "ISOLATED_WRITABLE_CHECKOUT_DAG",
        ),
    );
    let (s, r) = write_state("SOURCE_SEALED");
    a.insert(
        "adoption.preview".into(),
        act(
            s,
            r,
            json!({"maxPaths":COLLAB_MAX_PATHS.to_string(),"modes":["ALL","FILES"],"ttlMs":COLLAB_PLAN_TTL_MS.to_string()}),
            "READ_ONLY_FILE_EFFECT_PLAN_CANDIDATE_LOCKED",
        ),
    );
    let (s, r) = write_state("SOURCE_SEALED");
    a.insert(
        "adoption.apply".into(),
        act(
            s,
            r,
            json!({"expectedPreview":"OPTIONAL_GUARD","zeroWriteOnConflict":true}),
            "EXPLICIT_FILE_LEVEL_ADOPTION",
        ),
    );
    a.insert(
        "messageSend".into(),
        if sealed {
            act("BLOCKED_BY_STATE", json!("SOURCE_SEALED"), json!({"maxMaterials":crate::materials::MAX_PER_MESSAGE.to_string()}), "BOUND_MATERIALS_INJECTION")
        } else {
            act("AVAILABLE", Value::Null, json!({"maxMaterials":crate::materials::MAX_PER_MESSAGE.to_string(),"maxTotalBytes":crate::materials::TOTAL_MAX.to_string()}), "TEXT_PLUS_BOUND_MATERIALS")
        },
    );
    a.insert(
        "sideQuestion.create".into(),
        if sealed {
            act("BLOCKED_BY_STATE", json!("SOURCE_SEALED"), json!({}), "ISOLATED_SNAPSHOT_READ_ONLY")
        } else {
            act("AVAILABLE", Value::Null, json!({"maxPerTask":AUX_MAX_PER_TASK.to_string()}), "ISOLATED_SNAPSHOT_READ_ONLY")
        },
    );
    a.insert(
        "handoff.seal".into(),
        if sealed {
            act("BLOCKED_BY_STATE", json!("SOURCE_ALREADY_SEALED"), json!({}), "ONE_WAY_IMMUTABLE_SEAL")
        } else if !seal_quiescent(t) {
            // Same gate the POST enforces: a live owner or a still-dispatchable
            // queue intent reports its real refusal instead of advertising a
            // seal that would 409 anyway.
            act("BLOCKED_BY_STATE", json!("TASK_NOT_QUIESCENT"), json!({}), "ONE_WAY_IMMUTABLE_SEAL_REFUSES_MUTATIONS")
        } else {
            act("AVAILABLE", Value::Null, json!({}), "ONE_WAY_IMMUTABLE_SEAL_REFUSES_MUTATIONS")
        },
    );
    a.insert(
        "handoff.export".into(),
        act(
            "AVAILABLE",
            Value::Null,
            json!({"parts":["manifest","messages","events","auxiliary","collaborations","child-messages","artifacts","requests","handoff.md"],"chunkBytes":crate::materials::CHUNK_MAX.to_string()}),
            "BOUNDED_PART_READS_DIGESTED_ITEMS",
        ),
    );
    a.insert(
        "materials.upload".into(),
        act(
            "AVAILABLE",
            Value::Null,
            json!({"roles":["IMAGE","REFERENCE_TEXT"],"mediaTypes":crate::materials::MEDIA_TYPES,"imageBytes":crate::materials::IMAGE_MAX.to_string(),"textBytes":crate::materials::TEXT_MAX.to_string(),"maxPerMessage":crate::materials::MAX_PER_MESSAGE.to_string(),"maxTotalBytes":crate::materials::TOTAL_MAX.to_string(),"chunkBytes":crate::materials::CHUNK_MAX.to_string()}),
            "STAGED_CHUNK_SEAL_BIND_CONSUME",
        ),
    );
    a.insert(
        "policy.presets".into(),
        if h.registry.policy_presets.is_empty() {
            act("NOT_INTEGRATED", json!("POLICY_PRESETS_UNCONFIGURED"), json!({}), "MANAGED_PRESET_BINDING")
        } else {
            act("AVAILABLE", Value::Null, json!({"effectiveRange":"NEXT_TASK"}), "MANAGED_PRESET_BINDING_DIGEST_FROZEN")
        },
    );
    a.insert(
        "stop.affectedChildren".into(),
        act("AVAILABLE", Value::Null, json!({}), "CODEX_STOP_DRAINS_AUX_AND_COLLAB_CHILDREN"),
    );
    a.insert(
        "stop".into(),
        act("AVAILABLE", Value::Null, json!({}), "INTERRUPT_MANAGED_PROCESS_TREE"),
    );
    a.insert(
        "pause".into(),
        act("UNSUPPORTED", json!("NATIVE_BOUNDARY_GATE_ABSENT"), json!({}), "STRICT_PAUSE_GAP"),
    );
    a.insert(
        "resume".into(),
        act("UNSUPPORTED", json!("NATIVE_BOUNDARY_GATE_ABSENT"), json!({}), "STRICT_PAUSE_GAP"),
    );
    a.insert(
        "transitions".into(),
        if sealed {
            act("BLOCKED_BY_STATE", json!("SOURCE_SEALED"), json!({"crossEngine":false}), "SAME_ENGINE_COMPLETED_SESSION_RESUME")
        } else {
            act("AVAILABLE", Value::Null, json!({"crossEngine":false,"sameEngine":true}), "SAME_ENGINE_COMPLETED_SESSION_RESUME")
        },
    );
    json!({"schemaVersion":"source-capabilities/1","taskId":t["taskId"],"actions":Value::Object(a)})
}
// Sealed-source refusal: any mutation path that would write new work into a
// sealed task refuses with SOURCE_SEALED. Reads and STOP stay admitted.
fn ensure_not_sealed(t: &Value) -> Result<()> {
    if t["sealedFor"].is_object() {
        bail!("SOURCE_SEALED")
    }
    Ok(())
}
// Source-seal quiescence gate (design §9.4/F33, Pi sealQuiescent parity): the
// write fence may only land once the MAIN task can no longer write — a durable
// terminal verdict, or recorded quiescence (owned STOP proof, managed peer
// exit, restart reconciliation) with no dispatch left behind it.
//
// Codex admission model: `queued()`/`admit_queued` select on launchState alone,
// so a row whose launchState is still QUEUED is still schedulable — drain_queue
// can launch it at any freed slot even when its state or quiescent flag look
// settled — and is never sealed source material. A live RUNNING/PREPARING
// owner still has turn/tool writes in flight and refuses honestly.
pub(crate) fn seal_quiescent(t: &Value) -> bool {
    if t["launchState"] == "QUEUED" {
        return false;
    }
    if TASK_TERMINAL_STATES.contains(&t["state"].as_str().unwrap_or("")) {
        return true;
    }
    t["quiescent"] == true
}
const COLLAB_PLAN_TTL_MS: u64 = 10 * 60 * 1000;
// ======================= C5 — source seal & handoff export =======================
// Deterministic export part ids. `handoff.md` is a synthesized document; the
// rest map onto durable store projections in export_part.
const HANDOFF_PARTS: &[&str] = &[
    "manifest",
    "messages",
    "events",
    "auxiliary",
    "collaborations",
    "child-messages",
    "artifacts",
    "requests",
    "handoff.md",
];
// One-way seal: the seal fact, the task's sealedFor marker and the request
// record commit in one transaction; open children are drained first so the
// seal's affectedChildren names exactly what the seal cancelled/blocked.
// F33: the drain and the fence only run once the MAIN task proves quiescent —
// a live owner or a still-dispatchable queue intent refuses first, and the
// same gate re-checks inside the committing transaction so a fence can never
// land over writes that came back to life mid-seal.
async fn seal_route(
    h: Shared,
    task: &str,
    t: &Value,
    rest: &[&str],
    method: &str,
    b: &Value,
) -> Result<(u16, Value)> {
    if method != "POST" || !rest.is_empty() {
        bail!("FIXED_LOCAL_ROUTE_REQUIRED")
    }
    exact(b, &["requestKey", "targetRef"])?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
    }
    if t["sealedFor"].is_object() {
        bail!("SOURCE_ALREADY_SEALED")
    }
    {
        let s = h.store.lock().unwrap();
        // The route-entry snapshot is stale by drain time; the quiescence gate
        // reads the fresh row under the store lock. A refusal fires no
        // child-drain, no terminal kill and no fence state at all.
        let cur = s.task(task)?;
        if cur["sealedFor"].is_object() {
            bail!("SOURCE_ALREADY_SEALED")
        }
        if !seal_quiescent(&cur) {
            bail!("TASK_NOT_QUIESCENT")
        }
    }
    let target_ref = match &b["targetRef"] {
        Value::Null => Value::Null,
        v if v.is_object() => v.clone(),
        _ => bail!("INVALID_REQUEST"),
    };
    // Drain first: the seal's affectedChildren is the honest record of what
    // sealing stopped, computed before the seal fact commits.
    let mut affected = aux_stop_all(&h, task, "PARENT_SEALED").await;
    affected.extend(collab_stop_all(&h, task, "PARENT_SEALED").await);
    // Open terminals keep writing into the sealed workspace — terminate each
    // like a DELETE would and record every one in affectedChildren.
    {
        let terms = h
            .store
            .lock()
            .unwrap()
            .terminals(task)
            .unwrap_or_default();
        for term in terms {
            if term["state"] != "OPEN" {
                continue;
            }
            let tid = term["terminalId"].as_str().unwrap_or("").to_owned();
            let pid = term["processId"].as_str().unwrap_or("").to_owned();
            if let Some(peer) = h.peers.lock().await.get(task).cloned() {
                let _ = peer
                    .rpc("command/exec/terminate", json!({"processId":pid}))
                    .await;
            }
            {
                let s = h.store.lock().unwrap();
                let _ = s.terminal_finish(task, &tid, "TERMINATED", None, Some("PARENT_SEALED"), None);
                let _ = s.event(
                    task,
                    "terminal.closed",
                    json!({"terminalId":tid,"reason":"PARENT_SEALED"}),
                );
            }
            h.terminals
                .lock()
                .unwrap()
                .get_mut(task)
                .and_then(|m| m.remove(&pid));
            affected.push(json!({"kind":"TERMINAL","terminalId":tid,"state":"TERMINATED","reason":"PARENT_SEALED"}));
        }
    }
    let seal = json!({
        "sealId": format!("seal-{}", store::id()),
        "handoffId": format!("wh-{}", store::id()),
        "targetRef": target_ref,
        "proof": {
            "ownerBootId": h.boot,
            "generation": t["generation"],
            "segmentId": t["segmentId"],
            "baseCommit": t["baseCommit"],
            "taskState": t["state"],
            "sealedBy": "codex",
            "observedAt": store::now(),
        },
        "affectedChildren": affected,
        "sealedAt": store::now(),
    });
    let result = json!({
        "kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED",
        "operation":"SOURCE_SEAL","sealId":seal["sealId"],"handoffId":seal["handoffId"],
        "affectedChildren":seal["affectedChildren"],"sealedAt":seal["sealedAt"],
    });
    {
        let mut s = h.store.lock().unwrap();
        s.seal_source(task, &seal, &key, b, &result)?;
    }
    Ok((202, result))
}
// Deterministic handoff.md content: fixed field order, only durable facts.
fn handoff_md(t: &Value, seal: &Value) -> String {
    let mut md = String::new();
    md.push_str("# Work Handoff\n\n");
    for (k, v) in [
        ("handoffId", seal["handoffId"].clone()),
        ("sealId", seal["sealId"].clone()),
        ("taskId", t["taskId"].clone()),
        ("projectId", t["projectId"].clone()),
        ("engineId", t["engineId"].clone()),
        ("engineVersion", t["engineVersion"].clone()),
        ("state", t["state"].clone()),
        ("reasonCode", t["reasonCode"].clone()),
        ("generation", t["generation"].clone()),
        ("segmentId", t["segmentId"].clone()),
        ("baseCommit", t["baseCommit"].clone()),
        ("createdAt", t["createdAt"].clone()),
        ("sealedAt", seal["sealedAt"].clone()),
    ] {
        let s = if v.is_null() {
            "-".into()
        } else {
            v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string())
        };
        md.push_str(&format!("- **{k}**: {s}\n"));
    }
    md.push_str("\n## Affected Children\n\n");
    let affected = seal["affectedChildren"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[]);
    if affected.is_empty() {
        md.push_str("- none\n");
    }
    for c in affected {
        md.push_str(&format!(
            "- {} `{}` -> {} ({})\n",
            c["kind"].as_str().unwrap_or("CHILD"),
            c["childId"].as_str().or(c["childRef"].as_str()).unwrap_or(""),
            c["newState"].as_str().unwrap_or("?"),
            c["reasonCode"].as_str().unwrap_or(""),
        ));
    }
    md.push_str("\n## Output\n\n```\n");
    md.push_str(t["output"].as_str().unwrap_or(""));
    md.push_str("\n```\n");
    md
}
// ------- GAP-04 git baseline (design §9.3): fact entries over the owned task
// workspace plus binary payloads served through the chunk route. Anything we
// cannot prove degrades to an honest state row, never a silent claim.
const GIT_PART_MAX: usize = 8 * 1024 * 1024;
const GIT_UNTRACKED_MAX: usize = 256;
const GIT_SUBMODULE_MAX: usize = 32;
const GIT_PARTS: &[&str] = &["git-staged", "git-unstaged", "git-bundle"];
enum GitRead {
    Bytes(Vec<u8>),
    Oversize,
}
/// Bounded git read: `Command::output()` would buffer stdout without limit —
/// a pathological diff or bundle could exhaust host memory. We cap the stream
/// at `cap` bytes, kill the child past it and report Oversize separately from
/// a failed command so the caller can emit TOO_LARGE instead of UNAVAILABLE.
fn git_run(dir: &Path, args: &[&str], cap: usize) -> Option<GitRead> {
    let mut child = Command::new("git")
        .arg("-c")
        .arg("core.longpaths=true")
        .arg("-C")
        .arg(dir)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 65536];
    let mut oversize = false;
    loop {
        match stdout.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                if buf.len() + n > cap {
                    oversize = true;
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => break,
        }
    }
    drop(stdout);
    if oversize {
        let _ = child.kill();
    }
    let status = child.wait().ok()?;
    if oversize {
        return Some(GitRead::Oversize);
    }
    if !status.success() {
        return None;
    }
    Some(GitRead::Bytes(buf))
}
fn git_out(dir: &Path, args: &[&str]) -> Option<Vec<u8>> {
    match git_run(dir, args, GIT_PART_MAX) {
        Some(GitRead::Bytes(b)) => Some(b),
        _ => None,
    }
}
fn git_text(dir: &Path, args: &[&str]) -> Option<String> {
    match git_run(dir, args, 4 * 1024 * 1024) {
        Some(GitRead::Bytes(b)) => String::from_utf8(b).ok().map(|s| s.trim_end().to_string()),
        _ => None,
    }
}
/// name-status -z pairs so a target learns touched paths without parsing a
/// binary patch body.
fn git_name_status(dir: &Path, args: &[&str]) -> Option<(Vec<Value>, bool)> {
    let raw = git_text(dir, args)?;
    let mut files = vec![];
    let parts: Vec<&str> = raw.split('\0').collect();
    let mut i = 0;
    while i + 1 < parts.len() && files.len() < 512 {
        if !parts[i + 1].is_empty() {
            files.push(json!({"status":parts[i],"path":parts[i + 1]}));
        }
        i += 2;
    }
    Some((files, parts.len() > 1025))
}
/// Raw bytes for one materialized git payload. Empty output means the layer is
/// EMPTY and the part is not listed, so an empty result is EXPORT_PART_UNKNOWN.
fn git_export_part(workspace: &Path, part: &str) -> Result<Vec<u8>> {
    let bytes = match part {
        "git-staged" => git_out(
            workspace,
            &["diff", "--cached", "--binary", "--full-index", "--no-color", "--no-ext-diff", "--no-textconv"],
        ),
        "git-unstaged" => git_out(
            workspace,
            &["diff", "--binary", "--full-index", "--no-color", "--no-ext-diff", "--no-textconv"],
        ),
        "git-bundle" => git_out(workspace, &["bundle", "create", "-", "HEAD", "--not", "--remotes"]),
        _ => None,
    }
    .filter(|b| !b.is_empty())
    .ok_or_else(|| anyhow!("EXPORT_PART_UNKNOWN"))?;
    Ok(bytes)
}
/// Fact entries + payload parts for `export.gitBaseline`. The untracked
/// manifest hashes only small files; bigger files record size and an honest
/// UNHASHED_SIZE status — file bodies already travel as artifact parts.
fn git_baseline(t: &Value) -> (Vec<Value>, Vec<(String, Vec<u8>)>) {
    let mut entries = vec![];
    let mut payloads: Vec<(String, Vec<u8>)> = vec![];
    let ws = match t["workspace"].as_str() {
        Some(w) if !w.is_empty() => PathBuf::from(w),
        _ => {
            entries.push(json!({"kind":"NON_GIT","reason":"WORKSPACE_MISSING"}));
            return (entries, payloads);
        }
    };
    if !ws.is_dir() || git_text(&ws, &["rev-parse", "--git-dir"]).is_none() {
        entries.push(json!({"kind":"NON_GIT","reason":"NOT_A_REPOSITORY"}));
        return (entries, payloads);
    }
    let git_dir = git_text(&ws, &["rev-parse", "--absolute-git-dir"]).map(PathBuf::from);
    let head = git_text(&ws, &["rev-parse", "--verify", "HEAD"]);
    let branch = git_text(&ws, &["symbolic-ref", "-q", "--short", "HEAD"]).filter(|b| !b.is_empty());
    // In-flight markers: a rebase records rebase-merge/ or rebase-apply/
    // DIRECTORIES (REBASE_HEAD only exists mid-pick); AUTO_MERGE is the ort
    // strategy's in-progress tree.
    let in_flight: Vec<String> = git_dir
        .map(|d| {
            let mut v: Vec<String> = [
                ("MERGE_HEAD", "MERGE"),
                ("CHERRY_PICK_HEAD", "CHERRY_PICK"),
                ("REVERT_HEAD", "REVERT"),
                ("BISECT_HEAD", "BISECT"),
                ("AUTO_MERGE", "AUTO_MERGE"),
            ]
            .iter()
            .filter(|(f, _)| d.join(f).exists())
            .map(|(_, k)| k.to_string())
            .collect();
            if d.join("rebase-merge").exists() || d.join("rebase-apply").exists() {
                v.push("REBASE".to_string());
            }
            v
        })
        .unwrap_or_default();
    let linked = git_text(&ws, &["worktree", "list", "--porcelain"])
        .map(|s| s.lines().filter(|l| l.starts_with("worktree ")).count() as i64 - 1);
    entries.push(json!({
        "kind":"REPOSITORY","present":true,"head":head,"branch":branch,
        "detached":branch.is_none(),"baseCommit":t["baseCommit"],"inFlight":in_flight,
        "linkedWorktrees":linked.map(|n| n.max(0).to_string()).unwrap_or_else(|| "UNAVAILABLE".to_string()),
    }));
    for (kind, part, args, list_args) in [
        (
            "PATCH_STAGED",
            "git-staged",
            vec!["diff", "--cached", "--binary", "--full-index", "--no-color", "--no-ext-diff", "--no-textconv"],
            vec!["diff", "--cached", "--name-status", "-z", "--no-renames"],
        ),
        (
            "PATCH_UNSTAGED",
            "git-unstaged",
            vec!["diff", "--binary", "--full-index", "--no-color", "--no-ext-diff", "--no-textconv"],
            vec!["diff", "--name-status", "-z", "--no-renames"],
        ),
    ] {
        let listed = git_name_status(&ws, &list_args);
        let files = listed.as_ref().map(|(f, _)| f.clone()).unwrap_or_default();
        let files_truncated = listed.map(|(_, t)| t).unwrap_or(false);
        match git_run(&ws, &args, GIT_PART_MAX) {
            Some(GitRead::Bytes(b)) if b.is_empty() => entries.push(json!({"kind":kind,"state":"EMPTY","files":files})),
            Some(GitRead::Bytes(b)) => {
                entries.push(json!({"kind":kind,"state":"AVAILABLE","part":part,"sha256":hash(&b),"sizeBytes":b.len().to_string(),"files":files,"filesTruncated":files_truncated}));
                payloads.push((part.to_string(), b));
            }
            Some(GitRead::Oversize) => entries.push(json!({"kind":kind,"state":"TOO_LARGE","files":files})),
            None => entries.push(json!({"kind":kind,"state":"UNAVAILABLE","reason":"GIT_COMMAND_FAILED","files":files})),
        }
    }
    let local = git_text(&ws, &["rev-list", "--count", "HEAD", "--not", "--remotes"])
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if head.is_none() {
        entries.push(json!({"kind":"BUNDLE","state":"EMPTY","reason":"UNBORN_HEAD"}));
    } else if local == 0 {
        entries.push(json!({"kind":"BUNDLE","state":"EMPTY","reason":"NO_LOCAL_COMMITS","localCommits":"0"}));
    } else {
        match git_run(&ws, &["bundle", "create", "-", "HEAD", "--not", "--remotes"], GIT_PART_MAX) {
            Some(GitRead::Bytes(b)) if !b.is_empty() => {
                entries.push(json!({"kind":"BUNDLE","state":"AVAILABLE","part":"git-bundle","sha256":hash(&b),"sizeBytes":b.len().to_string(),"localCommits":local.to_string()}));
                payloads.push(("git-bundle".to_string(), b));
            }
            Some(GitRead::Oversize) => entries.push(json!({"kind":"BUNDLE","state":"TOO_LARGE","localCommits":local.to_string()})),
            _ => entries.push(json!({"kind":"BUNDLE","state":"UNAVAILABLE","reason":"BUNDLE_CREATE_FAILED","localCommits":local.to_string()})),
        }
    }
    // Stash objects are unreachable from HEAD — the bundle cannot carry them,
    // so a non-empty stash list must surface as its own fact row.
    if let Some(stash_list) = git_text(&ws, &["stash", "list"]) {
        let items: Vec<Value> = stash_list
            .lines()
            .filter(|l| !l.is_empty())
            .take(32)
            .map(|line| {
                let reference = line.split(':').next().unwrap_or("");
                let commit = git_text(&ws, &["rev-parse", reference]);
                json!({"ref":reference,"commit":commit,"summary":line.splitn(2,':').nth(1).unwrap_or("").trim()})
            })
            .collect();
        if !items.is_empty() {
            entries.push(json!({"kind":"STASHES","items":items,"note":"Stash objects are not reachable from HEAD — not included in BUNDLE"}));
        }
    }
    if let Ok(attrs) = std::fs::read(ws.join(".gitattributes")) {
        if attrs[..attrs.len().min(65536)]
            .iter()
            .map(|b| *b as char)
            .collect::<String>()
            .contains("filter=lfs")
        {
            entries.push(json!({"kind":"LFS","attributes":true,"note":"LFS objects live in the filter store — verify separately per §9.3"}));
        }
    }
    let (paths, ls_oversize) = match git_run(&ws, &["ls-files", "--others", "--exclude-standard", "-z"], 4 * 1024 * 1024) {
        Some(GitRead::Bytes(b)) => (
            b.split(|x| *x == 0)
                .filter(|s| !s.is_empty())
                .map(|s| String::from_utf8_lossy(s).to_string())
                .collect::<Vec<String>>(),
            false,
        ),
        Some(GitRead::Oversize) => (vec![], true),
        None => (vec![], true),
    };
    if ls_oversize {
        entries.push(json!({"kind":"UNTRACKED_MANIFEST","state":"OVERSIZE","note":"ls-files output exceeded the bounded read"}));
    } else {
        let mut paths = paths;
        paths.sort();
        let files: Vec<Value> = paths
            .iter()
            .take(GIT_UNTRACKED_MAX)
            .map(|p| {
                let f = ws.join(p);
                match std::fs::metadata(&f) {
                    Ok(m) if m.is_file() && m.len() <= 524288 => std::fs::read(&f)
                        .map(|b| json!({"path":p,"sizeBytes":b.len().to_string(),"sha256":hash(&b)}))
                        .unwrap_or_else(|_| json!({"path":p,"status":"UNREADABLE"})),
                    Ok(m) => json!({"path":p,"sizeBytes":m.len().to_string(),"status":"UNHASHED_SIZE"}),
                    Err(_) => json!({"path":p,"status":"UNREADABLE"}),
                }
            })
            .collect();
        entries.push(json!({"kind":"UNTRACKED_MANIFEST","files":files,"total":paths.len().to_string(),"truncated":paths.len()>GIT_UNTRACKED_MAX}));
    }
    if let Some(subs) = git_text(&ws, &["submodule", "status", "--recursive"]) {
        let items: Vec<String> = subs
            .lines()
            .filter(|l| !l.is_empty())
            .take(GIT_SUBMODULE_MAX)
            .map(|l| l.trim().to_string())
            .collect();
        if !items.is_empty() {
            entries.push(json!({"kind":"SUBMODULES","items":items,"note":"Pointer facts only — nested objects are not bundled"}));
        }
    }
    (entries, payloads)
}
fn handoff_part_bytes(h: &Shared, task: &str, part: &str) -> Result<Vec<u8>> {
    if part == "handoff.md" {
        let s = h.store.lock().unwrap();
        let t = s.task(task)?;
        let seal = s
            .task_seal(task)?
            .ok_or_else(|| anyhow!("HANDOFF_NOT_FOUND"))?;
        Ok(handoff_md(&t, &seal).into_bytes())
    } else if HANDOFF_PARTS.contains(&part) {
        h.store.lock().unwrap().export_part(task, part)
    } else if GIT_PARTS.contains(&part) {
        let ws = {
            let s = h.store.lock().unwrap();
            PathBuf::from(s.task(task)?["workspace"].as_str().unwrap_or_default())
        };
        git_export_part(&ws, part)
    } else {
        bail!("EXPORT_PART_UNKNOWN")
    }
}
// work-handoff/1 record: the seal + the part index with stable digests.
fn work_handoff_record(h: &Shared, task: &str) -> Result<Value> {
    let (t, seal) = {
        let s = h.store.lock().unwrap();
        (
            s.task(task)?,
            s.task_seal(task)?
                .ok_or_else(|| anyhow!("HANDOFF_NOT_FOUND"))?,
        )
    };
    let mut parts = vec![];
    for p in HANDOFF_PARTS {
        let bytes = handoff_part_bytes(h, task, p)?;
        parts.push(json!({
            "partId": p,
            "bytes": bytes.len().to_string(),
            "sha256": hash(&bytes),
        }));
    }
    let (git_entries, git_payloads) = git_baseline(&t);
    for (name, bytes) in &git_payloads {
        parts.push(json!({
            "partId": name,
            "bytes": bytes.len().to_string(),
            "sha256": hash(bytes),
            "role": if name == "git-bundle" { "BUNDLE" } else { "PATCH" },
        }));
    }
    Ok(json!({
        "schemaVersion": "work-handoff/1",
        "handoffId": seal["handoffId"],
        "sealId": seal["sealId"],
        "taskId": task,
        "projectId": t["projectId"],
        "engineId": t["engineId"],
        "sealedAt": seal["sealedAt"],
        "targetRef": seal["targetRef"],
        "affectedChildren": seal["affectedChildren"],
        "parts": parts,
        "gitBaseline": git_entries,
        "chunkBytes": crate::materials::CHUNK_MAX.to_string(),
    }))
}
// Chunked part read: `offset`/`limit` query ints (limit <= CHUNK_MAX), data is
// base64; the response carries the part sha256 so clients verify assembly.
fn handoff_part_read(
    h: &Shared,
    task: &str,
    part: &str,
    query: &HashMap<String, String>,
) -> Result<(u16, Value)> {
    if query.keys().any(|k| k != "offset" && k != "limit") {
        bail!("INVALID_QUERY")
    }
    let offset = query
        .get("offset")
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|_| anyhow!("INVALID_QUERY"))?
        .unwrap_or(0);
    let limit = query
        .get("limit")
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|_| anyhow!("INVALID_QUERY"))?
        .unwrap_or(crate::materials::CHUNK_MAX);
    if limit == 0 || limit > crate::materials::CHUNK_MAX {
        bail!("INVALID_QUERY")
    }
    // The part read is only defined against a sealed handoff.
    {
        let s = h.store.lock().unwrap();
        if s.task_seal(task)?.is_none() {
            bail!("HANDOFF_NOT_FOUND")
        }
    }
    let bytes = handoff_part_bytes(h, task, part)?;
    let total = bytes.len();
    if offset > total {
        bail!("INVALID_QUERY")
    }
    let end = (offset + limit).min(total);
    let chunk = &bytes[offset..end];
    Ok((
        200,
        json!({
            "partId": part,
            "offset": offset.to_string(),
            "nextOffset": end.to_string(),
            "hasMore": end < total,
            "bytesReturned": chunk.len().to_string(),
            "totalBytes": total.to_string(),
            "sha256": hash(&bytes),
            "data": STANDARD.encode(chunk),
        }),
    ))
}
// Pi-shaped chunk read alias so PAC's single proxy route works on both engines:
// `.../handoff-export/{part}/chunks/{digest}/{offset}` with the whole-part
// sha256 verified before slicing.
fn handoff_chunk_read(
    h: &Shared,
    task: &str,
    part: &str,
    digest: &str,
    offset_raw: &str,
) -> Result<(u16, Value)> {
    {
        let s = h.store.lock().unwrap();
        if s.task_seal(task)?.is_none() {
            bail!("HANDOFF_NOT_FOUND")
        }
    }
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("INVALID_QUERY")
    }
    if offset_raw.is_empty()
        || !offset_raw.bytes().all(|b| b.is_ascii_digit())
        || (offset_raw.len() > 1 && offset_raw.starts_with('0'))
    {
        bail!("INVALID_QUERY")
    }
    let offset: usize = offset_raw.parse().map_err(|_| anyhow!("INVALID_QUERY"))?;
    let bytes = handoff_part_bytes(h, task, part)?;
    let sha = hash(&bytes);
    if sha != digest {
        bail!("HANDOFF_CHUNK_DIGEST_MISMATCH")
    }
    let total = bytes.len();
    if offset > total {
        bail!("HANDOFF_OFFSET_INVALID")
    }
    let end = (offset + crate::materials::CHUNK_MAX).min(total);
    let chunk = &bytes[offset..end];
    Ok((
        200,
        json!({
            "part": part,
            "sha256": sha,
            "offset": offset.to_string(),
            "size": total.to_string(),
            "bytes": STANDARD.encode(chunk),
            "nextOffset": end.to_string(),
        }),
    ))
}
// Public /work-handoffs/{id} reads: resolve the seal row by handoff id.
async fn handoff_route(
    h: Shared,
    rest: &[&str],
    query: &HashMap<String, String>,
) -> Result<(u16, Value)> {
    match rest.len() {
        1 => {
            let id = valid_id(rest[0])?;
            let task = {
                let s = h.store.lock().unwrap();
                s.seal_by_handoff(id)?
                    .map(|(task, _)| task)
                    .ok_or_else(|| anyhow!("HANDOFF_NOT_FOUND"))?
            };
            Ok((200, work_handoff_record(&h, &task)?))
        }
        3 if rest[1] == "parts" => {
            let id = valid_id(rest[0])?;
            let task = {
                let s = h.store.lock().unwrap();
                s.seal_by_handoff(id)?
                    .map(|(task, _)| task)
                    .ok_or_else(|| anyhow!("HANDOFF_NOT_FOUND"))?
            };
            handoff_part_read(&h, &task, rest[2], query)
        }
        _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
    }
}
fn capabilities(h: &Host) -> Vec<Value> {
    let model_configured = missing_model_env(h).is_ok_and(|names| names.is_empty());
    let mut v = vec![];
    for (id, semantics) in [
        ("execution.messages", "PUBLIC_FINAL_MESSAGES"),
        ("execution.followUp", "SAME_THREAD_NEW_TURN"),
        ("execution.steer", "EXPECTED_TURN"),
        ("execution.stop", "INTERRUPT_MANAGED_PROCESS_TREE"),
        ("execution.approvals", "TURN_BOUND_DECISIONS"),
        ("operations.lookup", "READ_ONLY_ORIGINAL_KEY"),
        ("events.poll", "DURABLE_SEQUENCE"),
    ] {
        v.push(json!({"id":id,"implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":semantics,"evidenceRef":"workers/codex/README.md","reasonCode":null}))
    }
    v.push(json!({"id":"execution.stoppedSessionFollowUp","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"SEALED_STOPPED_SESSION_NEW_GENERATION","evidenceRef":"protocol/stopped-session-reopen-gap.json","reasonCode":null}));
    // W4a: honestly layered enforcement. Token caps are SOFT/post-hoc because
    // usage only arrives through native notifications; a cost cap can never
    // fire (no cost source exists) and is recorded as UNOBSERVED.
    v.push(json!({"id":"execution.budget","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"SOFT_TOKEN_CAP_POST_HOC_INTERRUPT","reasonCode":null,"limits":{"enforcement":"SOFT","costCoverage":"UNOBSERVED_IF_CAPPED"},"evidenceRef":"workers/codex/README.md"}));
    v.push(json!({"id":"execution.writeScope","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"APP_SERVER_WRITABLE_ROOTS_PLUS_HOST_OBSERVED_DENYLIST","reasonCode":null,"evidenceRef":"protocol/write-scope-gap.json"}));
    v.push(json!({"id":"execution.environmentBinding","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"ADMISSION_VERIFIED_COMMANDS_DIRS_PROCESS_ENV_ALLOWLIST","reasonCode":null,"evidenceRef":"protocol/environment-binding-gap.json"}));
    v.push(json!({"id":"scheduling.concurrencySlots","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"FIFO_DURABLE_ORDINAL","reasonCode":null,"limits":{"maxActiveTasks":h.max_active.to_string()},"evidenceRef":"workers/codex/README.md"}));
    v.push(json!({"id":"operations.audit","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"ORDERED_DURABLE_FACTS_NO_PROMPTS","reasonCode":null,"evidenceRef":"workers/codex/README.md"}));
    v.push(json!({"id":"operations.diagnostics","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"CONTENT_REDACTED_SHA256_SIZE","reasonCode":null,"evidenceRef":"workers/codex/README.md"}));
    // W4b: terminal sessions are native command/exec streams — true ConPTY only
    // when the caller asked for it; ownership is the managed peer job, so task
    // STOP kills the whole session tree and a dead transport marks sessions
    // LOST rather than exited.
    v.push(json!({"id":"execution.terminal","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"NATIVE_COMMAND_EXEC_STREAMED_SESSION_JOB_OWNED","reasonCode":null,"limits":{"maxPerTask":TERMINAL_MAX_PER_TASK.to_string(),"pty":"NATIVE_TTY_ONLY_WHEN_REQUESTED","recovery":"LOST_ON_HOST_RESTART"},"evidenceRef":"protocol/w4b-evidence.json"}));
    // MCP servers are bound through the task-scoped managed config; binding is
    // verified against mcpServerStatus/list and env values carrying live
    // credential material are refused at intake.
    v.push(json!({"id":"execution.mcpServers","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"TASK_SCOPED_STDIO_MCP_CONFIG_VERIFIED","reasonCode":null,"limits":{"transport":"STDIO_ONLY","env":"CREDENTIAL_SHAPED_NAMES_AND_VALUES_REJECTED"},"evidenceRef":"protocol/w4b-evidence.json"}));
    // Skills stage into the managed CODEX_HOME (user scope) with sha256 file
    // binding; command hooks are generated wrappers pre-trusted offline through
    // the engine's own currentHash scheme. Project-local hooks are not loaded
    // (untrusted) and non-command hook transports are unclaimed.
    v.push(json!({"id":"execution.extensions","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"STAGED_SKILLS_PLUS_PRETRUSTED_COMMAND_HOOKS","reasonCode":null,"limits":{"skills":"USER_SCOPE_MANAGED_HOME","hooks":"COMMAND_ONLY","projectHooks":"NOT_LOADED"},"evidenceRef":"protocol/w4b-evidence.json"}));
    // W5a: context package bytes are frozen into SQLite at creation — later
    // workspace changes can never mutate a package. File sources must resolve
    // inside the task workspace (lexical + realpath, symlink escape refused).
    v.push(json!({"id":"execution.contextPackages","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"FROZEN_DURABLE_PACKAGES_DIGESTED_SOURCES_DENYPATHS_HONORED","reasonCode":null,"limits":{"maxSources":PACKAGE_MAX_SOURCES.to_string(),"maxFiles":PACKAGE_MAX_FILES.to_string(),"maxFileBytes":PACKAGE_MAX_FILE_BYTES.to_string(),"maxTextBytes":PACKAGE_MAX_TEXT_BYTES.to_string(),"maxPackageBytes":PACKAGE_MAX_TOTAL_BYTES.to_string(),"maxPackagesPerTask":PACKAGE_MAX_PER_TASK.to_string(),"fileSources":"TASK_WORKSPACE_ONLY"},"evidenceRef":"protocol/w5a-evidence.json"}));
    // Auxiliary sessions are separate managed peers on isolated workspace
    // snapshots under a read-only-attempted sandbox; their output only enters
    // the main thread through an explicit deliver that uses the same
    // follow-up/reopen admission as a client command. In-flight sessions do
    // not survive a host restart — they reconcile CANCELLED honestly.
    v.push(json!({"id":"execution.auxiliarySessions","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"ISOLATED_PEER_WORKSPACE_SNAPSHOT_COPY_EXPLICIT_DELIVERY","reasonCode":"COPY_ISOLATION_NOT_OS_READONLY","limits":{"maxPerTask":AUX_MAX_PER_TASK.to_string(),"maxPackages":AUX_MAX_PACKAGES.to_string(),"maxActiveAux":"NWF_CODEX_MAX_ACTIVE_AUX default 2","budget":"OPTIONAL_SOFT_TOTAL_TOKENS_POST_HOC","deadlineMs":AUX_DEADLINE_MS.to_string(),"sandbox":"read-only attempted","recovery":"CANCELLED_ON_HOST_RESTART_OR_HOST_CLOSE"},"evidenceRef":"protocol/w5a-evidence.json"}));
    // Transitions have no boundary pause on codex, so only the Pi Path C is
    // admitted: a PROVEN completed session, already quiesced (STOP'd with its
    // completedSessionSeal, or a peer whose exit was observed after the bound
    // turn completed). A new ownerEpoch/generation/segment is admitted on the
    // SAME native thread via thread/resume + turn/start. Same engine + same
    // model only — a tool allowlist change is refused because the pinned
    // kernel exposes no per-turn tool allowlist, and cross-engine migration is
    // an explicit GAP.
    v.push(json!({"id":"execution.transitions","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"SAME_ENGINE_SAME_THREAD_COMPLETED_SESSION_RESUME_ADMIT","reasonCode":"CROSS_ENGINE_GAP","limits":{"sameEngine":true,"crossEngine":false,"method":"COMPLETED_SESSION","model":"FROZEN_BINDING_ONLY","toolAllowlist":"UNCHANGED_ONLY","liveSession":"TRANSITION_STATE_INCOMPATIBLE"}}));
    v.push(json!({"id":"execution.crossEngineTransition","implementation":"GAP","qualification":"NOT_RUN","availability":"UNAVAILABLE","semantics":"CROSS_ENGINE_THREAD_MIGRATION","reasonCode":"INCOMPATIBLE_TRANSITION","evidenceRef":"protocol/w5a-evidence.json"}));
    // W5b P14: writable collaboration DAGs. Each child is a separate managed
    // app-server peer on an isolated writable checkout COPY of the parent
    // workspace (not an OS-level sandbox — the copy IS the isolation proof).
    // Dependencies gate durable launch order; child output never touches the
    // parent workspace, and failed dependents block their children honestly.
    v.push(json!({"id":"execution.collaborations","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"ISOLATED_WRITABLE_CHECKOUT_DAG_DEPENDENCY_GATED_SOFT_BUDGET","reasonCode":"COPY_ISOLATION_NOT_OS_READONLY","limits":{"maxChildren":COLLAB_MAX_CHILDREN.to_string(),"maxActivePerTask":COLLAB_MAX_PER_TASK.to_string(),"maxActiveChildren":"NWF_CODEX_MAX_ACTIVE_COLLAB_CHILDREN default 4","maxGoalBytes":COLLAB_MAX_GOAL.to_string(),"deadlineMs":COLLAB_DEADLINE_MS.to_string(),"sandbox":"workspace-write attempted; on-request approvals host-scope-checked","recovery":"CANCELLED_OR_BLOCKED_ON_HOST_RESTART_OR_PARENT_STOP"},"evidenceRef":"protocol/w5b-evidence.json"}));
    // W5b P15: explicit file-level adoption — never an implicit merge. Parent
    // drift against the child's sealed baseline is a 409 CONFLICT fact with
    // zero writes; identical replay is NO_CHANGES; same requestKey replays the
    // stored outcome.
    v.push(json!({"id":"execution.adoptions","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"EXPLICIT_FILE_LEVEL_ADOPTION_BASELINE_DIFF_PARENT_DRIFT_CONFLICT","reasonCode":"FILE_LEVEL_NOT_IMPLICIT_MERGE","limits":{"modes":["ALL","FILES"],"maxPaths":COLLAB_MAX_PATHS.to_string(),"maxAdoptionsPerChild":COLLAB_MAX_ADOPTIONS.to_string(),"childState":"SUCCEEDED_REQUIRED","deletions":"PARENT_MATCHES_BASELINE_ONLY"},"evidenceRef":"protocol/w5b-evidence.json"}));
    // CA-ANDROID-V2 mobile-control surfaces (C2/C3/C4/C5/C6/C7 +
    // affected-children reporting). Each entry carries its contract-level
    // availability honestly; per-task BLOCKED_BY_STATE variants live on the
    // detail projection's sourceCapabilities instead.
    v.push(json!({"id":"mobile.childMessages","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"DURABLE_PER_CHILD_PUBLIC_STREAM_TX_BOUND","reasonCode":null,"limits":{"pageDefault":"50","pageMax":"100","kinds":["ASSIGNMENT","USER_INPUT","ASSISTANT_MESSAGE","TOOL_CALL","TOOL_RESULT","RETURN","STATUS"]},"evidenceRef":"workers/codex/src/store.rs#child_messages"}));
    v.push(json!({"id":"mobile.collaborationPreflight","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"FROZEN_PLAN_DIGEST_EXPIRY_GUARDED_CREATE","reasonCode":null,"limits":{"maxChildren":COLLAB_MAX_CHILDREN.to_string(),"maxGoalBytes":COLLAB_MAX_GOAL.to_string(),"ttlMs":COLLAB_PLAN_TTL_MS.to_string()},"evidenceRef":"workers/codex/src/store.rs#collab_preflights"}));
    v.push(json!({"id":"mobile.adoptionPreview","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"READ_ONLY_FILE_EFFECT_PLAN_CANDIDATE_LOCKED","reasonCode":null,"limits":{"maxPaths":COLLAB_MAX_PATHS.to_string(),"modes":["ALL","FILES"]},"evidenceRef":"workers/codex/src/store.rs#collab_previews"}));
    v.push(json!({"id":"mobile.sourceSeals","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"MAIN_TASK_QUIESCENCE_GATE_THEN_ONE_WAY_IMMUTABLE_SEAL_REFUSES_MUTATIONS","reasonCode":null,"limits":{},"evidenceRef":"workers/codex/src/store.rs#source_seals"}));
    v.push(json!({"id":"mobile.workHandoffExport","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"BOUNDED_PART_READS_DIGESTED_ITEMS","reasonCode":null,"limits":{"partMaxBytes":(8*1024*1024).to_string(),"chunkBytes":crate::materials::CHUNK_MAX.to_string()},"evidenceRef":"workers/codex/src/store.rs#export_part"}));
    v.push(json!({"id":"mobile.materialUploads","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"STAGED_CHUNK_SEAL_BIND_CONSUME","reasonCode":null,"limits":{"roles":["IMAGE","REFERENCE_TEXT"],"mediaTypes":crate::materials::MEDIA_TYPES,"imageBytes":crate::materials::IMAGE_MAX.to_string(),"textBytes":crate::materials::TEXT_MAX.to_string(),"maxPerMessage":crate::materials::MAX_PER_MESSAGE.to_string(),"maxTotalBytes":crate::materials::TOTAL_MAX.to_string(),"chunkBytes":crate::materials::CHUNK_MAX.to_string()},"evidenceRef":"workers/codex/src/materials.rs"}));
    v.push(json!({"id":"mobile.executionPolicyPresets","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":if h.registry.policy_presets.is_empty() {"NOT_INTEGRATED"} else {"AVAILABLE"},"semantics":"MANAGED_PRESET_BINDING_DIGEST_FROZEN_NEXT_TASK","reasonCode":if h.registry.policy_presets.is_empty() {json!("POLICY_PRESETS_UNCONFIGURED")} else {Value::Null},"limits":{"effectiveRange":"NEXT_TASK","presets":h.registry.policy_presets.len().to_string()},"evidenceRef":"workers/codex/src/host.rs#policy_options"}));
    v.push(json!({"id":"mobile.affectedChildren","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"CODEX_STOP_DRAINS_AUX_AND_COLLAB_CHILDREN","reasonCode":null,"limits":{},"evidenceRef":"workers/codex/src/host.rs#affected_children"}));
    v.push(adapter::strict_pause_gap());
    v.push(json!({"id":"execution.resume","implementation":"GAP","qualification":"NOT_RUN","availability":"UNAVAILABLE","semantics":"BOUNDARY_CHECKPOINT","reasonCode":"NATIVE_BOUNDARY_GATE_ABSENT","evidenceRef":"protocol/strict-pause-gap.json"}));
    for item in &mut v {
        if h.registry.tool_profile=="macos-runner-v1" {
            if item["id"]=="execution.stop" {
                item["semantics"]=json!("MODEL_NO_FORK_AND_REGISTERED_COMMAND_GROUPS");
                item["limits"]=json!({"runnerScope":"REGISTERED_PROCESS_GROUP","arbitraryEscapedProcesses":"NOT_PROVEN","remoteEffects":"NOT_PROVEN"});
            }
            if item["id"]=="execution.writeScope" {
                item["limits"]=json!({"nativeApplyPatch":"APP_SERVER_WRITABLE_ROOTS","runnerFilesystemIsolation":"NOT_PROVEN","denyPaths":"HOST_OBSERVED"});
            }
        }
        if item["schemaVersion"].is_null() {
            item["schemaVersion"] = json!("worker-capability/1");
        }
        if item["implementation"] == "IMPLEMENTED"
            && item["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("execution.") && id != "execution.stop")
            && !model_configured
        {
            item["availability"] = json!("UNCONFIGURED");
            item["reasonCode"] = json!("MODEL_CREDENTIAL_NOT_CONFIGURED");
        }
    }
    v
}
fn preflight(h: &Host, b: &Value) -> Result<Value> {
    exact(b, &["projectId", "engineId", "policyPresetId"])?;
    if string(b, "projectId")? != h.registry.project_id {
        bail!("PROJECT_NOT_REGISTERED")
    }
    if string(b, "engineId")? != "codex" {
        bail!("ENGINE_NOT_REGISTERED")
    }
    // C7: the requested preset is part of the reviewed surface — it lands in
    // the payload before optionsRevision so the digest binds the selection.
    let preset = policy_preset(h, b)?;
    verify(&h.root, &h.registry)?;
    let p = project(&h.registry)?;
    let mut result = json!({"projectId":h.registry.project_id,"engineId":"codex","engineVersion":h.registry.version,"executableDigest":h.registry.executable_digest,"profileId":"MANAGED","profileHome":h.root.join("profile"),"loadsUserExtensions":false,"headRevision":p["headRevision"],"hasUncommittedChanges":p["hasUncommittedChanges"],"workingCopyPolicy":"COMMITTED_BASE_ONLY"});
    result["nativeToolProfile"] = json!(h.registry.tool_profile);
    // C7/C0: the published managed presets and the host-level mobile-controls
    // actions ride the deterministic preflight payload (they participate in
    // optionsRevision only through fields added before the digest below).
    result["policyOptions"] = policy_options(h);
    if let Some(preset) = &preset {
        let mut frozen = preset.clone();
        frozen["digest"] = json!(hash(store::canonical(preset)?));
        result["policyPresetId"] = frozen["presetId"].clone();
        result["policyPreset"] = frozen;
    } else {
        result["policyPresetId"] = Value::Null;
        result["policyPreset"] = Value::Null;
    }
    result["sourceCapabilities"] = source_capabilities(h, &json!({"taskId":null,"state":"PREPARING"}));
    result["optionsRevision"] = json!(hash(store::canonical(&result)?));
    Ok(result)
}
fn missing_model_env(h: &Host) -> Result<Vec<String>> {
    let config = fs::read_to_string(h.root.join("profile/config.toml"))?;
    let mut required = HashSet::from(["MODEL_API_KEY".to_owned()]);
    for line in config.lines() {
        if let Some((_, value)) = line.split_once('=') {
            if let Ok(name) = serde_json::from_str::<String>(value.trim()) {
                if name.starts_with("LLM_HEADER_") {
                    required.insert(name);
                }
            }
        }
    }
    Ok(required
        .into_iter()
        .filter(|name| !std::env::var(name).is_ok_and(|value| !value.is_empty()))
        .collect())
}
fn require_model_configuration(h: &Host) -> Result<()> {
    if !missing_model_env(h)?.is_empty() {
        bail!("MODEL_CONFIGURATION_REQUIRED")
    }
    Ok(())
}
async fn route(State(h): State<Shared>, request: Request) -> Response {
    match handle(h, request).await {
        Ok((status, v)) => (
            StatusCode::from_u16(status).unwrap(),
            [("Cache-Control", "no-store")],
            Json(v),
        )
            .into_response(),
        Err(e) => {
            let code = safe_code(&e);
            let status = match code.as_str() {
                "LOCAL_AUTH_REQUIRED" => 401,
                "TASK_NOT_FOUND"
                | "REQUEST_NOT_FOUND"
                | "FIXED_LOCAL_ROUTE_REQUIRED"
                | "ARTIFACT_NOT_AVAILABLE"
                | "PACKAGE_NOT_FOUND"
                | "AUX_SESSION_NOT_FOUND"
                | "TRANSITION_NOT_FOUND"
                | "COLLABORATION_NOT_FOUND"
                | "COLLABORATION_CHILD_NOT_FOUND" => 404,
                _ => 409,
            };
            (
                StatusCode::from_u16(status).unwrap(),
                Json(json!({"code":code,"message":code,"sourceCode":e.downcast_ref::<adapter::NativeRpcError>().map(|native|native.code)})),
            )
                .into_response()
        }
    }
}
async fn handle(h: Shared, request: Request) -> Result<(u16, Value)> {
    let expected = format!("Bearer {}", h.token);
    let actual = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let equal = expected.len() == actual.len()
        && expected
            .bytes()
            .zip(actual.bytes())
            .fold(0u8, |a, (x, y)| a | (x ^ y))
            == 0;
    if !equal {
        bail!("LOCAL_AUTH_REQUIRED")
    }
    let host = request
        .headers()
        .get("host")
        .and_then(|x| x.to_str().ok())
        .unwrap_or("");
    if !host.starts_with("127.0.0.1:") && !host.starts_with("localhost:") {
        bail!("LOOPBACK_REQUIRED")
    }
    if let Some(s) = request.headers().get("X-Worker-Local-Store") {
        if s.to_str().ok() != identity(&h)["storeId"].as_str() {
            bail!("STORE_IDENTITY_CHANGED")
        }
    }
    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    let raw_query = request.uri().query().unwrap_or("").to_owned();
    let key_header = request
        .headers()
        .get("Idempotency-Key")
        .and_then(|s| s.to_str().ok())
        .map(str::to_owned);
    let mut query = HashMap::new();
    if !raw_query.is_empty() {
        for pair in raw_query.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            if query.insert(k.to_owned(), v.to_owned()).is_some() {
                bail!("DUPLICATE_QUERY")
            }
        }
    }
    let mut raw_body: Option<Vec<u8>> = None;
    let b = if method == "POST" || (method == "PUT" && path.contains("/material-uploads/")) {
        // Context-package creation carries frozen source bytes inline, so its
        // intake bound is the package ceiling (1 MiB sources + envelope);
        // a collaboration request can carry 8 child goals (8 KiB each), so it
        // takes the Pi host's 256 KiB bound; material chunks take the profile
        // bound; every other local POST stays at 64 KiB.
        let body_limit = if path.contains("/context-packages") {
            2 * 1024 * 1024
        } else if path.contains("/collaborations") || path.contains("/material-uploads/") {
            262144 + 4096
        } else {
            65536
        };
        let bytes = to_bytes(request.into_body(), body_limit)
            .await
            .map_err(|_| anyhow!("BODY_TOO_LARGE"))?;
        if method == "PUT" {
            raw_body = Some(bytes.to_vec());
            json!({})
        } else {
            let b: Value = serde_json::from_slice(&bytes).map_err(|_| anyhow!("INVALID_REQUEST"))?;
            if !b.is_object() {
                bail!("INVALID_REQUEST")
            }
            store::canonical(&b)?;
            if b.get("requestKey").is_some()
                && !path.ends_with("/operations/lookup")
                && b["requestKey"].as_str() != key_header.as_deref()
            {
                bail!("KEY_MISMATCH")
            }
            b
        }
    } else {
        json!({})
    };
    let full = path
        .strip_prefix("/worker-host/v1/")
        .or_else(|| path.strip_prefix("/worker/v1/"));
    if let Some(tail) = full {
        match (method.as_str(), tail) {
            ("GET", "manifest") => {
                let mut v = identity(&h);
                v["schemaVersion"] = json!("worker-host/1");
                v["engine"] = json!({"id":"codex","version":h.registry.version});
                v["contracts"] = json!(["worker-host/1", "worker-local/1"]);
                v["capabilities"] = json!(capabilities(&h));
                v["supportedPaths"] = json!([
                    "GET /manifest",
                    "GET /capabilities",
                    "POST /operations/lookup"
                ]);
                return Ok((200, v));
            }
            ("GET", "capabilities") => {
                return Ok((
                    200,
                    json!({"schemaVersion":"worker-capabilities/1","items":capabilities(&h)}),
                ));
            }
            ("POST", "operations/lookup") => {
                exact(&b, &["requestKey", "taskId"])?;
                let scope = b["taskId"].as_str().unwrap_or("create");
                return Ok((
                    200,
                    h.store
                        .lock()
                        .unwrap()
                        .lookup(scope, valid_id(string(&b, "requestKey")?)?)?,
                ));
            }
            _ => {
                let parts: Vec<&str> = tail.split('/').collect();
                if method == "GET" && parts.len() == 3 && parts[0] == "tasks" {
                    let task = valid_id(parts[1])?.to_owned();
                    match parts[2] {
                        "audit" => {
                            let s = h.store.lock().unwrap();
                            s.task(&task)?;
                            return Ok((
                                200,
                                json!({"schemaVersion":"worker-audit/1","taskId":task,"items":s.audit(&task)?}),
                            ));
                        }
                        "diagnostics" => {
                            if query.keys().any(|k| k != "redact")
                                || query.get("redact").map(String::as_str) != Some("content")
                            {
                                bail!("INVALID_QUERY")
                            }
                            let s = h.store.lock().unwrap();
                            let t = s.task(&task)?;
                            let ws = t["workspace"].as_str().unwrap_or("").to_owned();
                            let items: Vec<Value> = s.events(&task, 0, 1000)?["items"]
                                .as_array()
                                .map(|a| a.iter().map(|e| diagnostics_value(e, &ws)).collect())
                                .unwrap_or_default();
                            return Ok((
                                200,
                                json!({"schemaVersion":"worker-diagnostics/1","taskId":task,"redact":"content","items":items}),
                            ));
                        }
                        _ => {}
                    }
                }
                bail!("FIXED_LOCAL_ROUTE_REQUIRED")
            }
        }
    }
    let tail = path
        .strip_prefix("/local/v1/")
        .ok_or_else(|| anyhow!("FIXED_LOCAL_ROUTE_REQUIRED"))?;
    let p: Vec<&str> = tail.split('/').collect();
    // C5/C6: public work-handoff reads and the material-upload profile sit
    // outside the task subtree.
    if p[0] == "work-handoffs" && method == "GET" {
        return handoff_route(h.clone(), &p[1..], &query).await;
    }
    if p[0] == "projects" && p.len() >= 3 && p[2] == "material-uploads" {
        return crate::materials::material_route(
            h.clone(),
            valid_id(p[1])?,
            &p[3..],
            &method,
            &b,
            raw_body,
            &query,
        )
        .await;
    }
    if p[0] == "agent" && p.len() >= 4 && p[1] == "projects" && p[3] == "material-uploads" {
        // ACS-prefixed variant of the same material profile.
        return crate::materials::material_route(
            h.clone(),
            valid_id(p[2])?,
            &p[4..],
            &method,
            &b,
            raw_body,
            &query,
        )
        .await;
    }
    if p.len() == 1 {
        match (method.as_str(), p[0]) {
            ("GET", "identity") => return Ok((200, identity(&h))),
            ("GET", "engines") => {
                return Ok((
                    200,
                    json!({"schemaVersion":"worker-local/1","storeId":identity(&h)["storeId"],"items":[{"engineId":"codex","engineVersion":h.registry.version,"version":h.registry.version,"profileId":"MANAGED","pause":false,"resume":false,"stop":true,"steer":true,"followUp":true,"capabilities":{"pause":false,"resume":false,"stop":true,"steer":true,"followUp":true,"stoppedSessionFollowUp":true,"environmentBinding":true,"budget":true,"writeScope":true,"audit":true,"diagnostics":true,"terminals":{"pty":true,"resize":false,"maxPerTask":TERMINAL_MAX_PER_TASK,"outputBuffer":TERMINAL_OUTPUT_MAX,"signals":["terminate"]},"mcpServers":{"transport":"stdio","exposure":"direct","maxServers":8},"skills":{"managedProfileOnly":true,"contentBinding":"sha256"},"hooks":{"managedProfileOnly":true,"commands":true,"pretrusted":true},"contextPackages":true,"auxiliarySessions":true,"transitions":{"sameEngine":true,"crossEngine":false},"collaborations":{"dag":true,"isolatedCheckout":true,"maxChildren":COLLAB_MAX_CHILDREN,"maxActivePerTask":COLLAB_MAX_PER_TASK,"preflight":true,"childMessages":true},"adoptions":{"conflictCheck":true,"partial":true,"modes":["ALL","FILES"],"preview":true},"materialUploads":{"roles":["IMAGE","REFERENCE_TEXT"],"mediaTypes":crate::materials::MEDIA_TYPES,"chunkBytes":crate::materials::CHUNK_MAX},"sourceSeals":true,"handoffExport":true,"policyPresets":!h.registry.policy_presets.is_empty()},"executionPolicy":policy_options(&h)}]}),
                ));
            }
            ("GET", "projects") => return Ok((200, json!({"items":[project(&h.registry)?]}))),
            ("GET", "tasks") => {
                if query.keys().any(|k| k != "limit") { bail!("INVALID_QUERY"); }
                let limit = query.get("limit").map(|v| v.parse::<usize>()).transpose()?.unwrap_or(50);
                if limit == 0 || limit > 50 { bail!("INVALID_QUERY"); }
                let s = h.store.lock().unwrap();
                let items: Vec<Value> = s.tasks()?.into_iter().rev().take(limit).map(|t| json!({"taskId":t["taskId"],"projectId":t["projectId"],"engineId":t["engineId"],"engineVersion":t["engineVersion"],"state":t["state"]})).collect();
                return Ok((200, json!({"items":items})));
            }
            ("POST", "task-preflights") => return Ok((200, preflight(&h, &b)?)),
            ("POST", "tasks") => return create(h, &b).await,
            _ => {}
        }
    }
    if p.len() == 2 && p[0] == "task-requests" && method == "GET" {
        return Ok((
            200,
            h.store.lock().unwrap().lookup("create", valid_id(p[1])?)?,
        ));
    }
    if p.len() < 2 || p[0] != "tasks" {
        bail!("FIXED_LOCAL_ROUTE_REQUIRED")
    }
    let task = valid_id(p[1])?.to_owned();
    let t = h.store.lock().unwrap().task(&task)?;
    if p.len() == 2 && method == "GET" {
        let mut v = t;
        v["bootId"] = json!(h.boot);
        if v["launchState"] == "QUEUED" {
            // 1-based FIFO position; durable ordinal, recomputed per read.
            let position = h
                .store
                .lock()
                .unwrap()
                .queued()?
                .iter()
                .position(|(id, _)| id == &task)
                .map(|i| (i + 1).to_string());
            v["queuePosition"] = json!(position);
        }
        if let Some(budget) = h.store.lock().unwrap().budget(&task)? {
            v["budgetStatus"] = budget;
        }
        let live = h.peers.lock().await.get(&task).cloned();
        let alive = if let Some(peer) = live {
            peer.alive().await.unwrap_or(false)
        } else {
            false
        };
        let current_boot = v["ownerBootId"] == h.boot;
        let stopped_follow_up = reopen_ready(&h, &v, alive).await;
        v["capabilities"] = json!({"stop":v["quiescent"]!=true&&current_boot,"steer":alive&&v["state"]=="RUNNING","followUp":(alive&&v["state"]=="SUCCEEDED")||stopped_follow_up,"stoppedSessionFollowUp":stopped_follow_up,"pause":false,"resume":false,"observeAfterRestart":true,"continueAfterRestart":false,"contextPackages":true,"auxiliarySessions":true,"transitions":{"sameEngine":v["nativeSessionId"].is_string(),"crossEngine":false},"collaborations":true,"adoptions":true});
        v["terminals"] = json!(h.store.lock().unwrap().terminals(&task)?);
        {
            let s = h.store.lock().unwrap();
            v["contextPackages"] = json!(s.context_packages(&task)?);
            v["auxiliarySessions"] = json!(s.aux_sessions(&task)?);
            v["transitions"] = json!(s.transitions(&task)?);
            v["collaborations"] = json!(s.collaborations(&task)?);
            task_timing(&s, &task, &mut v)?;
        }
        // C0/C7/C5: mobile-controls action projection, the frozen execution
        // policy and the seal state all surface on the task detail read.
        v["sourceCapabilities"] = source_capabilities(&h, &v);
        v["executionPolicy"] = execution_policy(&h, &v);
        v["sealed"] = json!(v["sealedFor"].is_object());
        return Ok((200, v));
    }
    if p.len() == 4 && ["interaction-requests", "commands"].contains(&p[2]) && method == "GET" {
        reconcile_mac_stop_receipt(&h, &task, valid_id(p[3])?).await?;
        return Ok((200, h.store.lock().unwrap().lookup(&task, valid_id(p[3])?)?));
    }
    if p.len() == 4 && p[2] == "artifacts" && method == "GET" {
        return Ok((200, artifact_read(&h, &task, p[3])?));
    }
    if p.len() == 5 && p[2] == "input-requests" && p[4] == "responses" && method == "POST" {
        let mut input = b;
        exact(
            &input,
            &[
                "requestKey",
                "response",
                "generation",
                "nativeSessionId",
                "nativeTurnId",
                "targetCommandId",
            ],
        )?;
        input["kind"] = json!("INPUT_RESPONSE");
        input["inputRequestId"] = json!(valid_id(p[3])?);
        return interact(h, &task, input).await;
    }
    if p.len() >= 3 && p[2] == "terminals" {
        return terminal_route(h, &task, &t, &p[3..], &method, b, &query).await;
    }
    if p.len() >= 3 && p[2] == "context-packages" {
        return package_route(&h, &task, &t, &p[3..], &method, b).await;
    }
    if p.len() >= 3 && p[2] == "auxiliary-sessions" {
        return aux_route(h, &task, &t, &p[3..], &method, b, &query).await;
    }
    if p.len() >= 3 && p[2] == "transitions" {
        return transition_route(h, &task, &p[3..], &method, b).await;
    }
    if p.len() >= 3 && p[2] == "collaborations" {
        return collab_route(h, &task, &t, &p[3..], &method, b, &query).await;
    }
    if p.len() == 3 && p[2] == "source-seals" && method == "POST" {
        return seal_route(h, &task, &t, &p[3..], &method, &b).await;
    }
    if p.len() == 4 && p[2] == "source-seals" && method == "GET" {
        return Ok((200, h.store.lock().unwrap().source_seal(&task, valid_id(p[3])?)?));
    }
    if p.len() == 3 && p[2] == "handoff-export" && method == "GET" {
        return Ok((200, work_handoff_record(&h, &task)?));
    }
    if p.len() == 5 && p[2] == "handoff-export" && p[3] == "parts" && method == "GET" {
        return handoff_part_read(&h, &task, p[4], &query);
    }
    if p.len() == 7 && p[2] == "handoff-export" && p[4] == "chunks" && method == "GET" {
        return handoff_chunk_read(&h, &task, p[3], p[5], p[6]);
    }
    if p.len() != 3 {
        bail!("FIXED_LOCAL_ROUTE_REQUIRED")
    }
    if method == "POST" {
        match p[2] {
            "messages" => {
                exact(
                    &b,
                    &[
                        "requestKey",
                        "kind",
                        "content",
                        "generation",
                        "nativeSessionId",
                        "nativeTurnId",
                        "targetCommandId",
                        "materials",
                    ],
                )?;
                return interact(h, &task, b).await;
            }
            "commands" => {
                exact(
                    &b,
                    &[
                        "requestKey",
                        "intent",
                        "generation",
                        "nativeSessionId",
                        "nativeTurnId",
                        "targetCommandId",
                        "budget",
                        "content",
                    ],
                )?;
                if !matches!(b["intent"].as_str(), Some("STOP" | "EXTEND_BUDGET")) {
                    bail!("NATIVE_ACTION_UNSUPPORTED")
                }
                return interact(h, &task, b).await;
            }
            _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
        }
    }
    if method != "GET" {
        bail!("FIXED_LOCAL_ROUTE_REQUIRED")
    }
    let v = match p[2] {
        "events" => {
            if query.keys().any(|k| k != "cursor" && k != "limit") {
                bail!("INVALID_QUERY")
            }
            let cursor = query
                .get("cursor")
                .map(|x| x.parse::<i64>())
                .transpose()
                .map_err(|_| anyhow!("INVALID_QUERY"))?
                .unwrap_or(0);
            let limit = query
                .get("limit")
                .map(|x| x.parse::<i64>())
                .transpose()
                .map_err(|_| anyhow!("INVALID_QUERY"))?
                .unwrap_or(30);
            if cursor < 0 || !(1..=100).contains(&limit) {
                bail!("INVALID_QUERY")
            }
            h.store.lock().unwrap().events(&task, cursor, limit)?
        }
        "output" => json!({"text":t["output"],"coverage":"BOUNDED_PUBLIC_MESSAGES"}),
        "messages" => {
            // native-messages/1: the per-item public index answers with the
            // legacy text/turns fields untouched. cursor/before are mutually
            // exclusive decimal sequence bounds; every unknown or malformed
            // parameter is refused by the original error framework.
            let (cursor, before, limit) = message_page_query(&query)?;
            let mut s = h.store.lock().unwrap();
            s.messages(&task, cursor, before, limit)?
        }
        "metrics" => {
            let mut v = json!({"cost":null,"costCoverage":"UNKNOWN","configuredModel":t["configuredModel"]});
            if let Some(o) = t["usage"].as_object() {
                for (k, value) in o {
                    v[format!("usage.{}", k)] = value.clone()
                }
            }
            v
        }
        "input-requests" => {
            json!({"items":t["inputRequests"].as_object().map(|o|o.values().cloned().collect::<Vec<_>>()).unwrap_or_default()})
        }
        "artifacts" => artifacts(&h, &task)?,
        _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
    };
    Ok((200, v))
}

// Message-page query validation (design §5.3): cursor and before are mutually
// exclusive, strictly-decimal i64 bounds (cursor 0..=i64::MAX, before
// 1..=i64::MAX), limit 1..=100 default 50. Sequence values are compared as
// integers end to end — never through a floating point.
fn page_decimal(v: Option<&String>, min: i64) -> Result<Option<i64>> {
    let Some(s) = v else { return Ok(None) };
    // Canonical decimal only — the gateways reject leading zeros too.
    if s.is_empty()
        || s.len() > 19
        || !s.bytes().all(|b| b.is_ascii_digit())
        || (s.len() > 1 && s.starts_with('0'))
    {
        bail!("INVALID_QUERY")
    }
    let n: i64 = s.parse().map_err(|_| anyhow!("INVALID_QUERY"))?;
    if n < min {
        bail!("INVALID_QUERY")
    }
    Ok(Some(n))
}
fn message_page_query(query: &HashMap<String, String>) -> Result<(Option<i64>, Option<i64>, i64)> {
    if query
        .keys()
        .any(|k| k != "cursor" && k != "before" && k != "limit")
    {
        bail!("INVALID_QUERY")
    }
    let cursor = page_decimal(query.get("cursor"), 0)?;
    let before = page_decimal(query.get("before"), 1)?;
    if cursor.is_some() && before.is_some() {
        bail!("INVALID_QUERY")
    }
    let limit = page_decimal(query.get("limit"), 1)?.unwrap_or(50);
    if limit > 100 {
        bail!("INVALID_QUERY")
    }
    Ok((cursor, before, limit))
}

// W4a intake validation: a malformed budget, write scope or environment binding
// is refused before the task ever binds, and never silently dropped.
fn decimal(v: &Value) -> Result<Option<i64>> {
    match v {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(
            s.parse::<i64>().map_err(|_| anyhow!("INVALID_REQUEST"))?,
        )),
        Value::Number(n) => Ok(Some(n.as_i64().ok_or_else(|| anyhow!("INVALID_REQUEST"))?)),
        _ => bail!("INVALID_REQUEST"),
    }
}
fn validate_budget(v: &Value) -> Result<Value> {
    exact(
        v,
        &[
            "maxTotalTokens",
            "maxInputTokens",
            "maxOutputTokens",
            "maxCostMicroUsd",
            "enforcement",
        ],
    )?;
    // Only SOFT enforcement exists: native usage arrives post-hoc through
    // notifications, so a cap can only trip at a real observed boundary.
    if v["enforcement"].as_str() != Some("SOFT") {
        bail!("BUDGET_ENFORCEMENT_UNSUPPORTED")
    }
    let mut caps = serde_json::Map::new();
    for k in [
        "maxTotalTokens",
        "maxInputTokens",
        "maxOutputTokens",
        "maxCostMicroUsd",
    ] {
        if let Some(n) = decimal(&v[k])? {
            if n < 0 {
                bail!("INVALID_REQUEST")
            }
            caps.insert(k.into(), json!(n.to_string()));
        }
    }
    if caps.is_empty() {
        bail!("INVALID_REQUEST")
    }
    Ok(json!({"caps":caps,"enforcement":"SOFT"}))
}
fn lexical(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}
fn validate_write_scope(v: &Value) -> Result<Value> {
    exact(v, &["extraWriteDirs", "denyPaths"])?;
    let mut extra = vec![];
    if let Some(a) = v["extraWriteDirs"].as_array() {
        if a.len() > 16 {
            bail!("INVALID_REQUEST")
        }
        for p in a {
            let s = p.as_str().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
            if s.is_empty() || s.len() > 512 || !Path::new(s).is_absolute() {
                bail!("INVALID_REQUEST")
            }
            extra.push(json!(lexical(Path::new(s)).to_string_lossy().to_string()));
        }
    }
    let mut deny = vec![];
    if let Some(a) = v["denyPaths"].as_array() {
        if a.len() > 64 {
            bail!("INVALID_REQUEST")
        }
        for p in a {
            let s = p.as_str().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
            // Deny paths are workspace-relative prefixes; absolute paths and
            // parent escapes cannot name anything the check can bind.
            if s.is_empty()
                || s.len() > 512
                || Path::new(s).is_absolute()
                || s.split(['/', '\\']).any(|p| p == "..")
            {
                bail!("INVALID_REQUEST")
            }
            deny.push(json!(s.replace('\\', "/")));
        }
    }
    // Writable roots are handed to the pinned app-server sandbox; deny paths have
    // no native deny-list and are enforced as host-observed post-hoc facts only.
    Ok(
        json!({"extraWriteDirs":extra,"denyPaths":deny,"enforcement":"APP_SERVER_WRITABLE_ROOTS+HOST_OBSERVED_DENYLIST"}),
    )
}
fn validate_environment(v: &Value) -> Result<Value> {
    exact(v, &["requiresCommands", "requiresDirs", "envAllowlist"])?;
    let mut commands = vec![];
    if let Some(a) = v["requiresCommands"].as_array() {
        if a.len() > 32 {
            bail!("INVALID_REQUEST")
        }
        for c in a {
            let s = c.as_str().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
            if s.is_empty()
                || s.len() > 128
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                bail!("INVALID_REQUEST")
            }
            commands.push(json!(s));
        }
    }
    let mut dirs = vec![];
    if let Some(a) = v["requiresDirs"].as_array() {
        if a.len() > 32 {
            bail!("INVALID_REQUEST")
        }
        for d in a {
            let s = d.as_str().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
            if s.is_empty() || s.len() > 512 || !Path::new(s).is_absolute() {
                bail!("INVALID_REQUEST")
            }
            dirs.push(json!(lexical(Path::new(s)).to_string_lossy().to_string()));
        }
    }
    let mut allowlist = vec![];
    if let Some(a) = v["envAllowlist"].as_array() {
        if a.len() > 64 {
            bail!("INVALID_REQUEST")
        }
        for n in a {
            let s = n.as_str().ok_or_else(|| anyhow!("INVALID_ENV_NAME"))?;
            env_name_ok(s)?;
            allowlist.push(json!(s));
        }
    }
    Ok(json!({"requiresCommands":commands,"requiresDirs":dirs,"envAllowlist":allowlist}))
}
// Shared env-name rule for envAllowlist and MCP server env: boring UPPER_SNAKE
// names only; credential-shaped names are refused rather than forwarded.
fn env_name_ok(s: &str) -> Result<()> {
    let boring = !s.is_empty()
        && s.len() <= 64
        && s.bytes().next().is_some_and(|b| b.is_ascii_uppercase())
        && s.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    if !boring {
        bail!("INVALID_ENV_NAME")
    }
    if ["KEY", "TOKEN", "SECRET", "PASSWORD"].iter().any(|p| s.contains(p))
        || ["LLM_", "MODEL_", "WORKER_", "ACS_", "PAC_", "PERSONAL_"]
            .iter()
            .any(|p| s.starts_with(p))
    {
        bail!("ENV_ALLOWLIST_DENIED")
    }
    Ok(())
}
// Live credential values in this process: MODEL_API_KEY, model headers and any
// *TOKEN* variable of plausible secret length. Used to refuse secret-bearing
// values that would otherwise be persisted into a task config file.
fn secret_env_values() -> Vec<String> {
    std::env::vars()
        .filter(|(k, v)| {
            v.len() >= 8 && (k == "MODEL_API_KEY" || k.starts_with("LLM_HEADER_") || k.contains("TOKEN"))
        })
        .map(|(_, v)| v)
        .collect()
}
// mcpServers: [{id,command,args,env?}] — stdio MCP servers bound through the
// task-scoped managed config. Ids become TOML table keys and native server
// names; command/args are passed verbatim to the app-server. env names follow
// the allowlist rule and values must not carry a live credential: they are
// persisted in the managed home config on disk.
fn validate_mcp_servers(v: &Value) -> Result<Value> {
    let a = v.as_array().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
    if a.is_empty() || a.len() > 8 {
        bail!("INVALID_REQUEST")
    }
    let secrets = secret_env_values();
    let mut servers = vec![];
    let mut seen = HashSet::new();
    for s in a {
        exact(s, &["id", "command", "args", "env"])?;
        let id = string(s, "id")?;
        // The id is a TOML table key and the native server name.
        if id.len() > 64
            || !id.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
            || !id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            bail!("INVALID_ID")
        }
        if !seen.insert(id.to_owned()) {
            bail!("INVALID_REQUEST")
        }
        let command = string(s, "command")?;
        if command.len() > 512 || command.bytes().any(|b| b < 0x20) {
            bail!("INVALID_REQUEST")
        }
        let mut args = vec![];
        if let Some(list) = s["args"].as_array() {
            if list.len() > 32 {
                bail!("INVALID_REQUEST")
            }
            for arg in list {
                let arg = arg.as_str().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
                if arg.len() > 512 || arg.bytes().any(|b| b < 0x20) {
                    bail!("INVALID_REQUEST")
                }
                args.push(json!(arg));
            }
        }
        let mut env = serde_json::Map::new();
        if let Some(obj) = s["env"].as_object() {
            if obj.len() > 32 {
                bail!("INVALID_REQUEST")
            }
            for (name, value) in obj {
                env_name_ok(name)?;
                let value = value.as_str().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
                if value.len() > 512 {
                    bail!("INVALID_REQUEST")
                }
                if secrets.iter().any(|secret| value.contains(secret)) {
                    bail!("MCP_ENV_SECRET_VALUE")
                }
                env.insert(name.clone(), json!(value));
            }
        }
        if s.get("env").is_some() && s["env"].as_object().is_none() {
            bail!("INVALID_REQUEST")
        }
        servers.push(json!({"id":id,"command":command,"args":args,"env":env}));
    }
    Ok(json!(servers))
}
// skills: [{name,sourcePath}] — directories staged verbatim into the managed
// CODEX_HOME skills tree (user scope). SKILL.md presence is verified at intake
// so a mis-shaped binding is refused before dispatch.
fn validate_skills(v: &Value) -> Result<Value> {
    let a = v.as_array().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
    if a.is_empty() || a.len() > 16 {
        bail!("INVALID_REQUEST")
    }
    let mut skills = vec![];
    let mut seen = HashSet::new();
    for s in a {
        exact(s, &["name", "sourcePath"])?;
        let name = string(s, "name")?;
        if name.len() > 64
            || !name.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            bail!("INVALID_ID")
        }
        if !seen.insert(name.to_owned()) {
            bail!("INVALID_REQUEST")
        }
        let source = string(s, "sourcePath")?;
        if source.len() > 512 || !Path::new(source).is_absolute() {
            bail!("INVALID_REQUEST")
        }
        let dir = lexical(Path::new(source));
        let manifest = dir.join("SKILL.md");
        if !manifest.is_file() {
            bail!("SKILL_MANIFEST_MISSING")
        }
        skills.push(json!({"name":name,"sourcePath":dir.to_string_lossy()}));
    }
    Ok(json!(skills))
}
// Hook events the managed profile can honestly bind. Subagent events are
// refused because the managed config pins multi_agent=false: a hook that can
// never run is not a binding, it is a false claim.
const HOOK_EVENTS: &[(&str, &str, &str)] = &[
    ("preToolUse", "PreToolUse", "pre_tool_use"),
    ("postToolUse", "PostToolUse", "post_tool_use"),
    ("permissionRequest", "PermissionRequest", "permission_request"),
    ("preCompact", "PreCompact", "pre_compact"),
    ("postCompact", "PostCompact", "post_compact"),
    ("sessionStart", "SessionStart", "session_start"),
    ("sessionEnd", "SessionEnd", "session_end"),
    ("userPromptSubmit", "UserPromptSubmit", "user_prompt_submit"),
    ("stop", "Stop", "stop"),
    ("interrupt", "Interrupt", "interrupt"),
];
fn hook_event(name: &str) -> Option<(&'static str, &'static str)> {
    HOOK_EVENTS
        .iter()
        .find(|(n, _, _)| *n == name)
        .map(|(_, toml, snake)| (*toml, *snake))
}
// hooks: [{event,command,matcher?,timeoutSec?,async?,statusMessage?}] — command
// hooks written into the task config [hooks] tables and pre-trusted offline.
// Hook children inherit the managed peer process environment (the engine's
// shell_environment_policy does not cover them), so each command is wrapped in
// a generated script that first unsets model credentials.
fn validate_hooks(v: &Value) -> Result<Value> {
    let a = v.as_array().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
    if a.is_empty() || a.len() > 8 {
        bail!("INVALID_REQUEST")
    }
    let mut hooks = vec![];
    for s in a {
        exact(s, &["event", "command", "matcher", "timeoutSec", "async", "statusMessage"])?;
        let event = string(s, "event")?;
        if hook_event(event).is_none() {
            bail!("INVALID_HOOK_EVENT")
        }
        let command = string(s, "command")?;
        if command.len() > 1024 || command.bytes().any(|b| b < 0x20) {
            bail!("INVALID_REQUEST")
        }
        if let Some(m) = s["matcher"].as_str() {
            if m.is_empty() || m.len() > 256 {
                bail!("INVALID_REQUEST")
            }
        } else if !s["matcher"].is_null() {
            bail!("INVALID_REQUEST")
        }
        if let Some(t) = decimal(&s["timeoutSec"])? {
            if !(1..=600).contains(&t) {
                bail!("INVALID_REQUEST")
            }
        }
        if !s["async"].is_null() && !s["async"].is_boolean() {
            bail!("INVALID_REQUEST")
        }
        if let Some(m) = s["statusMessage"].as_str() {
            if m.len() > 256 {
                bail!("INVALID_REQUEST")
            }
        } else if !s["statusMessage"].is_null() {
            bail!("INVALID_REQUEST")
        }
        hooks.push(json!({
            "event": event,
            "command": command,
            "matcher": s["matcher"],
            "timeoutSec": s["timeoutSec"],
            "async": s["async"].as_bool().unwrap_or(false),
            "statusMessage": s["statusMessage"],
        }));
    }
    Ok(json!(hooks))
}
// The pinned app-server derives hook trust as "sha256:" + hex(sha256(canonical
// JSON of the normalized hook identity)) where the identity is
// {event_name, matcher?, hooks:[one normalized handler]} with key order
// canonicalized. Reproduced against hooks/list currentHash on the pinned
// binary (see tests). Computing it offline lets the host pre-trust exactly the
// hook text it generated — the same ceremony the native client performs.
fn hook_trust_hash(event_snake: &str, matcher: Option<&str>, handler: &Value) -> String {
    let mut identity = serde_json::Map::new();
    identity.insert("event_name".into(), json!(event_snake));
    if let Some(m) = matcher {
        identity.insert("matcher".into(), json!(m));
    }
    identity.insert("hooks".into(), json!([handler]));
    format!("sha256:{}", hash(serde_json::to_vec(&Value::Object(identity)).unwrap_or_default()))
}
// Normalized command-handler shape used by the trust hash: timeout defaults to
// 600 (engine normalization), async defaults to false, statusMessage only when
// configured.
fn hook_handler_identity(command: &str, timeout_sec: i64, async_: bool, status: Option<&str>) -> Value {
    let mut h = serde_json::Map::new();
    h.insert("async".into(), json!(async_));
    h.insert("command".into(), json!(command));
    if let Some(s) = status {
        h.insert("statusMessage".into(), json!(s));
    }
    h.insert("timeout".into(), json!(timeout_sec.max(1)));
    h.insert("type".into(), json!("command"));
    Value::Object(h)
}
// TOML basic string for arbitrary text (escapes backslash/quote/control).
fn toml_basic(s: &str) -> String {
    toml_string(s)
}
fn hook_script_path(home: &Path, index: usize) -> PathBuf {
    let ext = if cfg!(windows) { "bat" } else { "sh" };
    home.join("hooks").join(format!("hook-{}.{}", index, ext))
}
fn hook_invocation(script: &Path) -> String {
    if cfg!(windows) {
        format!("cmd.exe /d /c \"{}\"", script.display())
    } else {
        format!("sh \"{}\"", script.display())
    }
}
// Generated hook wrappers unset model credentials before running the caller's
// command: hook children inherit the managed peer environment verbatim (the
// engine's shell_environment_policy does not cover them), so the scrub mirrors
// exactly the credential names the policy excludes and the peer receives.
fn write_hook_wrappers(home: &Path, t: &Value) -> Result<()> {
    let Some(hooks) = t["hooks"].as_array().filter(|a| !a.is_empty()) else {
        return Ok(());
    };
    let dir = home.join("hooks");
    fs::create_dir_all(&dir)?;
    let mut scrub = vec!["MODEL_API_KEY".to_string()];
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("LLM_HEADER_")) {
        scrub.push(k);
    }
    for (i, hk) in hooks.iter().enumerate() {
        let command = hk["command"].as_str().unwrap_or("");
        let body = if cfg!(windows) {
            let mut s = String::from("@echo off\r\n");
            for name in &scrub {
                s.push_str(&format!("set \"{}=\"\r\n", name));
            }
            s.push_str(command);
            s.push_str("\r\n");
            s
        } else {
            let mut s = String::from("#!/bin/sh\n");
            s.push_str(&format!("unset {}\n", scrub.join(" ")));
            s.push_str(command);
            s.push('\n');
            s
        };
        fs::write(hook_script_path(home, i), body)?;
    }
    Ok(())
}
// Bounded verbatim staging of caller skill directories into the managed
// CODEX_HOME skills tree (the engine's "user" scope). Symlinks and non-file
// entries are refused rather than silently materialized outside the home.
fn stage_skills(home: &Path, t: &Value) -> Result<Vec<Value>> {
    let Some(skills) = t["skills"].as_array() else {
        return Ok(vec![]);
    };
    let mut staged = vec![];
    for skill in skills {
        let name = skill["name"].as_str().unwrap_or("");
        let source = PathBuf::from(skill["sourcePath"].as_str().unwrap_or(""));
        let target = home.join("skills").join(name);
        fs::create_dir_all(&target)?;
        let mut files = vec![];
        let mut total = 0usize;
        let mut count = 0usize;
        for entry in walk(&source, 4)? {
            let rel = entry.strip_prefix(&source).unwrap().to_path_buf();
            let meta = fs::symlink_metadata(&entry)?;
            if meta.file_type().is_symlink() || !meta.is_file() {
                continue;
            }
            count += 1;
            if count > 64 || meta.len() > 256 * 1024 || total >= 4 * 1024 * 1024 {
                bail!("SKILL_BOUNDS_EXCEEDED")
            }
            let bytes = fs::read(&entry)?;
            total += bytes.len();
            let dest = target.join(&rel);
            if let Some(p) = dest.parent() {
                fs::create_dir_all(p)?;
            }
            fs::write(&dest, &bytes)?;
            files.push(json!({"path":rel.to_string_lossy().replace('\\',"/"),"sha256":hash(&bytes),"size":bytes.len().to_string()}));
        }
        staged.push(json!({"name":name,"files":files}));
    }
    Ok(staged)
}
fn walk(dir: &Path, depth: usize) -> Result<Vec<PathBuf>> {
    let mut out = vec![];
    if depth == 0 {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.is_dir() && !meta.file_type().is_symlink() {
            out.extend(walk(&path, depth - 1)?);
        } else {
            out.push(path);
        }
    }
    Ok(out)
}
// Task-scoped managed config: the digest-pinned profile text plus append-only
// W4b sections ([mcp_servers.*], [hooks] groups and [hooks.state] trust). The
// profile line itself is never edited; verify() still rechecks its digest.
fn task_config(root: &Path, home: &Path, t: &Value) -> Result<String> {
    let mut text = fs::read_to_string(root.join("profile/config.toml"))?;
    if let Some(servers) = t["mcpServers"].as_array() {
        for s in servers {
            text.push_str(&format!(
                "\n[mcp_servers.{}]\ncommand = {}\n",
                string(s, "id")?,
                toml_basic(string(s, "command")?)
            ));
            if let Some(args) = s["args"].as_array().filter(|a| !a.is_empty()) {
                let args: Vec<String> = args
                    .iter()
                    .map(|a| toml_basic(a.as_str().unwrap_or("")))
                    .collect();
                text.push_str(&format!("args = [{}]\n", args.join(", ")));
            }
            if let Some(env) = s["env"].as_object().filter(|o| !o.is_empty()) {
                let pairs: Vec<String> = env
                    .iter()
                    .map(|(k, v)| format!("{} = {}", toml_basic(k), toml_basic(v.as_str().unwrap_or(""))))
                    .collect();
                text.push_str(&format!("env = {{ {} }}\n", pairs.join(", ")));
            }
        }
    }
    if let Some(hooks) = t["hooks"].as_array().filter(|a| !a.is_empty()) {
        // The engine gates hook loading behind features.hooks; the profile's
        // [features] table already exists, so the flag is injected in place
        // rather than appended as a duplicate TOML table.
        if let Some(at) = text.find("[features]\n") {
            let insert = at + "[features]\n".len();
            text.insert_str(insert, "hooks = true\n");
        }
        let cfg_key = canonical_path(home)?.join("config.toml");
        let cfg_key = cfg_key.to_string_lossy().to_string();
        text.push_str("\n[hooks]\n");
        let mut state = String::new();
        let mut counters: HashMap<String, usize> = HashMap::new();
        for (i, hk) in hooks.iter().enumerate() {
            let event = string(hk, "event")?;
            let (toml_ev, snake) = hook_event(event).unwrap();
            let command = hook_invocation(&hook_script_path(home, i));
            let timeout_sec = hk["timeoutSec"]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| hk["timeoutSec"].as_i64())
                .unwrap_or(600)
                .max(1);
            let async_ = hk["async"].as_bool().unwrap_or(false);
            let status = hk["statusMessage"].as_str();
            let matcher = hk["matcher"].as_str();
            let group = counters.entry(snake.to_string()).or_insert(0);
            let index = *group;
            *group += 1;
            text.push_str(&format!("[[hooks.{}]]\n", toml_ev));
            if let Some(m) = matcher {
                text.push_str(&format!("matcher = {}\n", toml_basic(m)));
            }
            text.push_str(&format!(
                "[[hooks.{}.hooks]]\ntype = \"command\"\ncommand = {}\ntimeout = {}\nasync = {}\n",
                toml_ev,
                toml_basic(&command),
                timeout_sec,
                async_
            ));
            if let Some(sm) = status {
                text.push_str(&format!("statusMessage = {}\n", toml_basic(sm)));
            }
            let handler = hook_handler_identity(&command, timeout_sec, async_, status);
            let trust = hook_trust_hash(snake, matcher, &handler);
            state.push_str(&format!(
                "[hooks.state.{}]\ntrusted_hash = {}\n",
                toml_basic(&format!("{}:{}:{}:0", cfg_key, snake, index)),
                toml_basic(&trust)
            ));
        }
        text.push_str(&state);
    }
    Ok(text)
}
// PREPARING admission gate: every required command resolves through where/which
// and every required directory exists and is readable. Failure is terminal.
fn binding_check(t: &Value) -> Vec<Value> {
    let mut failures = vec![];
    let tool = if cfg!(windows) { "where.exe" } else { "which" };
    if let Some(a) = t["environment"]["requiresCommands"].as_array() {
        for name in a.iter().filter_map(|c| c.as_str()) {
            let resolved = Command::new(tool)
                .arg(name)
                .output()
                .map(|o| o.status.success() && !o.stdout.is_empty())
                .unwrap_or(false);
            if !resolved {
                failures.push(json!({"kind":"command","name":name,"reason":"NOT_RESOLVED"}));
            }
        }
    }
    if let Some(a) = t["environment"]["requiresDirs"].as_array() {
        for d in a.iter().filter_map(|c| c.as_str()) {
            let p = Path::new(d);
            if !p.is_dir() {
                failures.push(json!({"kind":"dir","name":d,"reason":"MISSING_OR_NOT_DIRECTORY"}));
            } else if fs::read_dir(p).is_err() {
                failures.push(json!({"kind":"dir","name":d,"reason":"NOT_READABLE"}));
            }
        }
    }
    // A bound stdio MCP server whose program cannot resolve would silently
    // mark the server failed inside the engine; the binding fails admission
    // instead, like any other declared requirement.
    if let Some(a) = t["mcpServers"].as_array() {
        for server in a.iter().filter_map(|s| s["command"].as_str()) {
            let resolved = if server.contains(['/', '\\']) {
                Path::new(server).is_file()
            } else {
                Command::new(tool)
                    .arg(server)
                    .output()
                    .map(|o| o.status.success() && !o.stdout.is_empty())
                    .unwrap_or(false)
            };
            if !resolved {
                failures.push(json!({"kind":"mcpCommand","name":server,"reason":"NOT_RESOLVED"}));
            }
        }
    }
    failures
}
// Admit queued intents in durable FIFO order while a slot is free. The queue
// mutex serializes concurrent drains (STOP arm, budget arm, startup pass).
fn drain_queue(h: &Shared) {
    let _guard = h.queue.lock().unwrap();
    loop {
        let admitted = {
            let mut s = h.store.lock().unwrap();
            match s.active_count(None) {
                Ok(active) if active < h.max_active => {
                    let head = s
                        .queued()
                        .ok()
                        .and_then(|q| q.first().map(|(id, _)| id.clone()));
                    head.and_then(|id| {
                        // admit_queued rechecks launchState inside the tx so a
                        // concurrent STOP cannot resurrect a cancelled intent.
                        s.admit_queued(&id).ok().map(|t| {
                            (
                                id.clone(),
                                t["requestKey"].as_str().unwrap_or("").to_string(),
                            )
                        })
                    })
                }
                _ => None,
            }
        };
        let Some((task, key)) = admitted else { break };
        let text = h
            .store
            .lock()
            .unwrap()
            .request_body("create", &key)
            .ok()
            .flatten()
            .and_then(|b| b["instruction"].as_str().map(str::to_owned))
            .unwrap_or_default();
        if text.is_empty() {
            // A queued intent whose stored instruction is unreadable can never
            // dispatch; mark it terminal instead of spinning.
            let mut s = h.store.lock().unwrap();
            let _ = s.update(&task, |t| {
                t["state"] = json!("UNKNOWN");
                t["reasonCode"] = json!("OUTCOME_UNCONFIRMED")
            });
            if let Ok(t) = s.task(&task) {
                let _ = s.receipt("create", &key, "UNKNOWN", &t);
            }
            continue;
        }
        let h2 = h.clone();
        tokio::spawn(async move {
            if let Err(e) = launch(h2.clone(), &task, &key, &text).await {
                spawn_failed(&h2, &task, &key, &e).await;
            }
        });
    }
}
async fn spawn_failed(h: &Shared, task: &str, key: &str, e: &anyhow::Error) {
    let code = safe_code(e);
    let dead = match h.peers.lock().await.get(task) {
        // A dead root alone cannot prove a macOS process group/descendant
        // boundary. Only explicit STOP with the guardian's seal can do that.
        Some(p) => !cfg!(target_os = "macos") && !p.alive().await.unwrap_or(false),
        None => !h
            .store
            .lock()
            .unwrap()
            .task(task)
            .is_ok_and(|t| t["preparationState"] == "RUNNING"),
    };
    {
        let mut s = h.store.lock().unwrap();
        if s.task(task).is_ok_and(|t| {
            matches!(
                t["state"].as_str(),
                Some("STOP_REQUESTED" | "CANCELLED" | "BUDGET_EXCEEDED" | "BINDING_UNAVAILABLE")
            )
        }) {
            return;
        }
        let _ = s.update(task, |t| {
            t["state"] = json!("UNKNOWN");
            t["reasonCode"] = json!(code);
            if dead {
                // Nothing was ever dispatched or the writer is gone: the slot is
                // released so queued intents can be evaluated.
                t["quiescent"] = json!(true)
            }
        });
        if let Ok(t) = s.task(task) {
            let _ = s.receipt("create", key, "UNKNOWN", &t);
        }
        let _ = s.event(task, "execution.unconfirmed", json!({"code":code}));
    }
    drain_queue(h);
}
async fn create(h: Shared, b: &Value) -> Result<(u16, Value)> {
    exact(
        b,
        &[
            "requestKey",
            "projectId",
            "engineId",
            "optionsRevision",
            "instruction",
            "budget",
            "writeScope",
            "environment",
            "mcpServers",
            "skills",
            "hooks",
            "policyPresetId",
            "materials",
        ],
    )?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    let text = instruction(b, "instruction")?;
    let budget = b.get("budget").map(validate_budget).transpose()?;
    let write_scope = b.get("writeScope").map(validate_write_scope).transpose()?;
    let environment = b.get("environment").map(validate_environment).transpose()?;
    let mcp_servers = b.get("mcpServers").map(validate_mcp_servers).transpose()?;
    let skills = b.get("skills").map(validate_skills).transpose()?;
    let hooks = b.get("hooks").map(validate_hooks).transpose()?;
    // C7: an optional managed preset binds by id AND digest — the frozen copy
    // lands on the task record so later preset-list changes can never rewrite
    // this task's admitted policy.
    let preset: Option<Value> = policy_preset(&h, b)?;
    // C6: create-scoped materials[] refs bind to this create requestKey.
    let binds = {
        let s = h.store.lock().unwrap();
        crate::materials::validate_materials(&s, &b["materials"], &key)?
    };
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing("create", &key, b)? {
            return Ok((202, v));
        }
    }
    let checked = preflight(
        &h,
        &json!({"projectId":b["projectId"],"engineId":b["engineId"],"policyPresetId":b["policyPresetId"]}),
    )?;
    if b["optionsRevision"] != checked["optionsRevision"] {
        bail!("PREFLIGHT_CHANGED")
    }
    require_model_configuration(&h)?;
    let task = format!("native-{}", store::id());
    let workspace = h.root.join("workspaces").join(&task);
    let mut t = json!({"taskId":task,"requestKey":key,"engineId":"codex","engineVersion":h.registry.version,"storeId":identity(&h)["storeId"],"projectId":h.registry.project_id,"state":"PREPARING","launchState":"PREPARING","generation":store::id(),"ownerBootId":h.boot,"currentCommandId":key,"runId":format!("run-{}",store::id()),"segmentId":format!("segment-{}",store::id()),"stateVersion":"1","ownerEpoch":"1","workspace":workspace,"baseCommit":checked["headRevision"],"configuredModel":h.registry.model,"configuredProvider":"nwf","profileId":"MANAGED","createdAt":store::now(),"quiescent":false,"turns":{},"inputRequests":{},"output":"","publicItemIds":[],"capabilities":{"stop":true,"steer":true,"followUp":true,"pause":false,"resume":false}});
    t["nativeToolProfile"] = json!(h.registry.tool_profile);
    if let Some(binding) = crate::mac_tools::binding(&key)? {
        if h.registry.tool_profile != "macos-runner-v1" { bail!("MAC_TOOL_PROFILE_REQUIRED") }
        if b["environment"]["envAllowlist"].as_array().is_some_and(|a| a.contains(&binding["credentialEnvironment"])) { bail!("MAC_TASK_CREDENTIAL_NOT_EXPORTABLE") }
        t["macToolBinding"] = binding;
    }
    if let Some(v) = budget {
        t["budget"] = v;
    }
    if let Some(v) = write_scope {
        t["writeScope"] = v;
    }
    if let Some(v) = environment {
        t["environment"] = v;
    }
    if let Some(v) = mcp_servers {
        t["mcpServers"] = v;
    }
    if let Some(v) = skills {
        t["skills"] = v;
    }
    if let Some(v) = hooks {
        t["hooks"] = v;
    }
    if let Some(p) = preset {
        let mut frozen = p.clone();
        frozen["digest"] = json!(hash(store::canonical(&p)?));
        t["policyPreset"] = frozen;
    }
    {
        let mut s = h.store.lock().unwrap();
        if let Some(v) = s.existing("create", &key, b)? {
            return Ok((202, v));
        }
        // Over-cap intake writes a durable QUEUED intent with a FIFO ordinal; it
        // is never dispatched until a slot frees and drain_queue admits it.
        if s.active_count(None)? >= h.max_active {
            let ordinal = s.queued()?.last().map(|(_, o)| *o).unwrap_or(0) + 1;
            t["state"] = json!("QUEUED");
            t["launchState"] = json!("QUEUED");
            t["queueOrdinal"] = json!(ordinal);
        }
        s.accept_task(&t, &key, b, &binds)?;
    }
    let output = t.clone();
    if output["launchState"] != "QUEUED" {
        tokio::spawn(async move {
            if let Err(e) = launch(h.clone(), &task, &key, &text).await {
                spawn_failed(&h, &task, &key, &e).await;
            }
        });
    }
    Ok((202, output))
}
// envAllowlist admits validated ambient names into the managed process
// environment only; values are never read or persisted by the host. The pinned
// app-server has no shell-level include/set knob for environment policy, so
// propagation to tool-invoked shells is a recorded native gap.
fn env_allowlist(t: &Value) -> Vec<String> {
    t["environment"]["envAllowlist"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}
async fn launch(h: Shared, task: &str, key: &str, text: &str) -> Result<()> {
    let preparing = h.preparation.lock().await;
    ensure_running(&h, task)?;
    let t = h.store.lock().unwrap().task(task)?;
    // Environment binding is verified before any dispatch mark or peer exists;
    // a missing binding is terminal and releases the slot for queued intents.
    let failures = binding_check(&t);
    if !failures.is_empty() {
        {
            let mut s = h.store.lock().unwrap();
            s.update(task, |t| {
                t["state"] = json!("BINDING_UNAVAILABLE");
                t["launchState"] = json!("REJECTED");
                t["reasonCode"] = json!("BINDING_UNAVAILABLE");
                t["bindingFailures"] = json!(failures);
                t["quiescent"] = json!(true);
            })?;
            let t = s.task(task)?;
            s.receipt("create", key, "REJECTED", &t)?;
            s.event(task, "binding.unavailable", json!({"failures":failures}))?;
        }
        drain_queue(&h);
        return Ok(());
    }
    let workspace = PathBuf::from(string(&t, "workspace")?);
    let parent = workspace.parent().unwrap();
    fs::create_dir_all(parent)?;
    {
        let mut s = h.store.lock().unwrap();
        let mut r = s.lookup("create", key)?;
        r["state"] = json!("DISPATCHING");
        s.receipt("create", key, "DISPATCHING", &r)?;
    }
    #[cfg(target_os = "macos")]
    let clone = {
        use std::ffi::OsStr;
        h.store
            .lock()
            .unwrap()
            .update(task, |t| t["preparationState"] = json!("RUNNING"))?;
        let mut args: Vec<&OsStr> = [
            "-c",
            "core.longpaths=true",
            "clone",
            "--local",
            "--no-hardlinks",
            "--no-checkout",
        ]
        .iter()
        .map(OsStr::new)
        .collect();
        args.extend([h.registry.project.as_os_str(), workspace.as_os_str()]);
        let output =
            adapter::prepare_git(&args, &h.root, &h.root.join("preparation").join(task)).await?;
        h.store.lock().unwrap().update(task, |t| {
            t["preparationState"] = json!("DRAINED_MANAGED_GROUPS")
        })?;
        output
    };
    #[cfg(not(target_os = "macos"))]
    let clone = {
        let clone = tokio::process::Command::new("git")
            .args([
                "-c",
                "core.longpaths=true",
                "clone",
                "--no-hardlinks",
                "--no-checkout",
            ])
            .arg(&h.registry.project)
            .arg(&workspace)
            .kill_on_drop(true)
            .output();
        timeout(Duration::from_secs(60), clone)
            .await
            .map_err(|_| anyhow!("LOCAL_PROCESS_TIMEOUT"))??
    };
    if !clone.status.success() {
        bail!("LOCAL_CLONE_FAILED")
    }
    ensure_running(&h, task)?;
    #[cfg(target_os = "macos")]
    {
        use std::ffi::OsStr;
        h.store
            .lock()
            .unwrap()
            .update(task, |t| t["preparationState"] = json!("RUNNING"))?;
        let args = [
            OsStr::new("-C"),
            workspace.as_os_str(),
            OsStr::new("checkout"),
            OsStr::new("--detach"),
            OsStr::new(string(&t, "baseCommit")?),
        ];
        let output =
            adapter::prepare_git(&args, &h.root, &h.root.join("preparation").join(task)).await?;
        h.store.lock().unwrap().update(task, |t| {
            t["preparationState"] = json!("DRAINED_MANAGED_GROUPS")
        })?;
        if !output.status.success() {
            bail!("LOCAL_PROCESS_FAILED")
        }
    }
    #[cfg(not(target_os = "macos"))]
    git(
        &workspace,
        &["checkout", "--detach", string(&t, "baseCommit")?],
    )?;
    ensure_running(&h, task)?;
    verify(&h.root, &h.registry)?;
    let home = h.root.join("native").join(task);
    fs::create_dir_all(&home)?;
    // W4b staging precedes the write of the task-scoped managed config so the
    // config can embed hook wrapper paths and trust state, and precedes peer
    // spawn so the engine reads MCP/skills configuration on its own startup.
    write_hook_wrappers(&home, &t)?;
    let staged = stage_skills(&home, &t)?;
    fs::write(home.join("config.toml"), task_config(&h.root, &home, &t)?)?;
    if !staged.is_empty() {
        h.store.lock().unwrap().update(task, |t| {
            t["stagedSkills"] = json!(staged);
        })?;
    }
    let (peer, events) = Peer::spawn(
        &h.registry.executable,
        &home,
        &workspace,
        &env_allowlist(&t),
        bounded_profile(&h.registry.tool_profile),
    )
    .await?;
    h.peers.lock().await.insert(task.into(), peer.clone());
    drop(preparing);
    watch(&h, task, events);
    peer.initialize_with_experimental(h.registry.tool_profile == "macos-runner-v1")
        .await?;
    ensure_running(&h, task)?;
    let mut thread_params = json!({"model":h.registry.model,"modelProvider":"nwf","cwd":workspace,"approvalPolicy":"on-request","sandbox":"workspace-write","baseInstructions":"You are an implementation worker. Execute the user's task in the given owned workspace. Do not read credentials or environment secrets. Preserve user files. Report actual changes and tests.","config":{"features.multi_agent":false}});
    if h.registry.tool_profile == "macos-runner-v1" {
        thread_params["dynamicTools"] = crate::runner::tools();
        if !t["macToolBinding"].is_null() {
            let tools = crate::mac_tools::tools(&t["macToolBinding"]).await?;
            thread_params["dynamicTools"].as_array_mut().unwrap().extend(tools);
        }
    }
    let result = peer.rpc("thread/start", thread_params).await?;
    let thread = string(&result["thread"], "id")?.to_owned();
    ensure_running(&h, task)?;
    if string(&result, "model")? != h.registry.model || string(&result, "modelProvider")? != "nwf" {
        bail!("MODEL_BINDING_MISMATCH")
    }
    h.store.lock().unwrap().update(task, |t| {
        t["nativeSessionId"] = json!(thread);
        t["resolvedModel"] = result["model"].clone();
        t["resolvedProvider"] = result["modelProvider"].clone();
        t["modelBindingDigest"] = json!(h.registry.launch_config_digest);
    })?;
    bind_extensions(&h, task, &peer, &thread, &workspace).await;
    start_turn(h, task, key, text, peer).await
}
fn watch(h: &Shared, task: &str, mut events: tokio::sync::mpsc::Receiver<Value>) {
    let event_h = h.clone();
    let event_task = task.to_owned();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Err(e) = native_event(event_h.clone(), &event_task, event).await {
                let code = safe_code(&e);
                {
                    let mut s = event_h.store.lock().unwrap();
                    let _ = s.update(&event_task, |t| {
                        t["state"] = json!("UNKNOWN");
                        t["reasonCode"] = json!(code)
                    });
                    let _ = s.event(&event_task, "adapter.failure", json!({"code":code}));
                }
                let _ = stop_runners(&event_h, &event_task).await;
                if let Some(p) = event_h.peers.lock().await.get(&event_task).cloned() {
                    let _ = p.stop().await;
                }
                break;
            }
        }
    });
}
// Stopped-session reopen: the admitted new generation reattaches the persisted
// native thread through a fresh managed peer on the same per-task CODEX_HOME.
async fn reopen_session(h: Shared, task: &str, key: &str, text: &str) -> Result<()> {
    let preparing = h.preparation.lock().await;
    ensure_running(&h, task)?;
    verify(&h.root, &h.registry)?;
    let t = h.store.lock().unwrap().task(task)?;
    let workspace = PathBuf::from(string(&t, "workspace")?);
    let thread = string(&t, "nativeSessionId")?.to_owned();
    let home = h.root.join("native").join(task);
    fs::create_dir_all(&home)?;
    // Reopen regenerates the managed config from the persisted task record so
    // a fresh peer binds the same MCP servers, hooks and staged skills.
    write_hook_wrappers(&home, &t)?;
    let staged = stage_skills(&home, &t)?;
    fs::write(home.join("config.toml"), task_config(&h.root, &home, &t)?)?;
    if !staged.is_empty() {
        h.store.lock().unwrap().update(task, |t| {
            t["stagedSkills"] = json!(staged);
        })?;
    }
    let (peer, events) = Peer::spawn(
        &h.registry.executable,
        &home,
        &workspace,
        &env_allowlist(&t),
        bounded_profile(&h.registry.tool_profile),
    )
    .await?;
    h.peers.lock().await.insert(task.into(), peer.clone());
    drop(preparing);
    watch(&h, task, events);
    peer.initialize_with_experimental(h.registry.tool_profile == "macos-runner-v1")
        .await?;
    ensure_running(&h, task)?;
    let result = peer.rpc("thread/resume",json!({"threadId":thread,"model":h.registry.model,"modelProvider":"nwf","cwd":workspace,"approvalPolicy":"on-request","sandbox":"workspace-write","baseInstructions":"You are an implementation worker. Execute the user's task in the given owned workspace. Do not read credentials or environment secrets. Preserve user files. Report actual changes and tests.","config":{"features.multi_agent":false}})).await?;
    ensure_running(&h, task)?;
    if string(&result["thread"], "id")? != thread {
        bail!("NATIVE_SESSION_IDENTITY_CHANGED")
    }
    if string(&result, "model")? != h.registry.model || string(&result, "modelProvider")? != "nwf" {
        bail!("MODEL_BINDING_MISMATCH")
    }
    bind_extensions(&h, task, &peer, &thread, &workspace).await;
    start_turn(h, task, key, text, peer).await
}
// Extension binding verification, run once per peer generation immediately
// after thread/start or thread/resume. Each bound surface is verified by
// asking the engine to enumerate what it actually loaded; the recorded facts
// always reflect the engine's own report, never the request alone. Binding
// failures are recorded as facts (the task still runs); only dispatch-time
// failures (spawn, thread, model) are terminal.
async fn bind_extensions(h: &Shared, task: &str, peer: &Arc<Peer>, thread: &str, workspace: &Path) {
    let t = match h.store.lock().unwrap().task(task) {
        Ok(t) => t,
        Err(_) => return,
    };
    let servers = t["mcpServers"].as_array().cloned().unwrap_or_default();
    let skills = t["skills"].as_array().cloned().unwrap_or_default();
    let hooks = t["hooks"].as_array().cloned().unwrap_or_default();
    if !servers.is_empty() {
        let wanted: Vec<String> = servers.iter().filter_map(|s| s["id"].as_str().map(str::to_owned)).collect();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let mut rows: Option<Vec<Value>> = None;
        let mut last_error: Option<String>;
        loop {
            match peer
                .rpc("mcpServerStatus/list", json!({"threadId":thread}))
                .await
            {
                Ok(res) => {
                    let data = res["data"].as_array().cloned().unwrap_or_default();
                    let mut settled = true;
                    let mut report = vec![];
                    for id in &wanted {
                        let row = data.iter().find(|r| r["name"].as_str() == Some(id.as_str()));
                        let runtime = row
                            .and_then(|r| r["runtimeStatus"].as_str())
                            .unwrap_or("absent");
                        let tools: Vec<String> = row
                            .map(|r| {
                                r["tools"]
                                    .as_object()
                                    .map(|o| o.keys().cloned().collect())
                                    .or_else(|| {
                                        r["tools"].as_array().map(|a| {
                                            a.iter()
                                                .filter_map(|t| {
                                                    t["name"].as_str().or_else(|| t.as_str()).map(str::to_owned)
                                                })
                                                .collect()
                                        })
                                    })
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default();
                        let status = match runtime {
                            "connected" => "BOUND",
                            "failed" | "disabled" | "cancelled" => "FAILED",
                            _ => {
                                settled = false;
                                "PENDING"
                            }
                        };
                        report.push(json!({"id":id,"status":status,"runtimeStatus":runtime,"tools":tools}));
                    }
                    if settled || std::time::Instant::now() >= deadline {
                        rows = Some(report);
                        break;
                    }
                }
                Err(e) => {
                    // A single unconfirmed transport call must not discard the
                    // whole observation window; keep polling until the
                    // deadline, then record the last error as the failure.
                    last_error = Some(safe_code(&e));
                    if std::time::Instant::now() >= deadline {
                        let s = h.store.lock().unwrap();
                        let _ = s.event(
                            task,
                            "mcp.bind.failure",
                            json!({"code":last_error.clone().unwrap_or_default()}),
                        );
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if let Some(report) = rows {
            let mut s = h.store.lock().unwrap();
            let _ = s.update(task, |t| {
                t["mcpStatus"] = json!(report);
            });
            let _ = s.event(task, "mcp.bound", json!({"servers":report}));
        }
    }
    if !skills.is_empty() {
        let home = h.root.join("native").join(task);
        match peer
            .rpc(
                "skills/list",
                json!({"cwds":[workspace.to_string_lossy()],"forceReload":true}),
            )
            .await
        {
            Ok(res) => {
                let listed: Vec<&Value> = res["data"]
                    .as_array()
                    .map(|a| a.iter().flat_map(|e| e["skills"].as_array().into_iter().flatten()).collect())
                    .unwrap_or_default();
                let staged = t["stagedSkills"].as_array().cloned().unwrap_or_default();
                let mut report = vec![];
                for skill in &skills {
                    let name = skill["name"].as_str().unwrap_or("");
                    let expect = lexical(&home.join("skills").join(name))
                        .to_string_lossy()
                        .replace('\\', "/");
                    let found = listed.iter().find(|s| {
                        s["path"]
                            .as_str()
                            .map(|p| {
                                let p = lexical(Path::new(p)).to_string_lossy().replace('\\', "/");
                                p == expect || p.starts_with(&format!("{}/", expect))
                            })
                            .unwrap_or(false)
                    });
                    let files = staged
                        .iter()
                        .find(|f| f["name"].as_str() == Some(name))
                        .map(|f| f["files"].clone())
                        .unwrap_or(json!([]));
                    report.push(match found {
                        Some(s) => json!({
                            "name": name,
                            "status": if s["enabled"].as_bool().unwrap_or(false) { "BOUND" } else { "DISABLED" },
                            "scope": s["scope"],
                            "path": s["path"],
                            "files": files,
                        }),
                        None => json!({"name":name,"status":"MISSING","files":files}),
                    });
                }
                let mut s = h.store.lock().unwrap();
                let _ = s.update(task, |t| {
                    t["skillStatus"] = json!(report);
                });
                let _ = s.event(task, "skill.bound", json!({"skills":report}));
            }
            Err(e) => {
                let s = h.store.lock().unwrap();
                let _ = s.event(task, "skill.bind.failure", json!({"code":safe_code(&e)}));
            }
        }
    }
    if !hooks.is_empty() {
        let home = h.root.join("native").join(task);
        let cfg_key = canonical_path(&home)
            .map(|p| p.join("config.toml").to_string_lossy().to_string())
            .unwrap_or_default();
        match peer
            .rpc("hooks/list", json!({"cwds":[workspace.to_string_lossy()]}))
            .await
        {
            Ok(res) => {
                let listed: Vec<&Value> = res["data"]
                    .as_array()
                    .map(|a| a.iter().flat_map(|e| e["hooks"].as_array().into_iter().flatten()).collect())
                    .unwrap_or_default();
                let mut counters: HashMap<String, usize> = HashMap::new();
                let mut report = vec![];
                for hk in &hooks {
                    let event = hk["event"].as_str().unwrap_or("");
                    let snake = hook_event(event).map(|(_, s)| s).unwrap_or("");
                    let group = counters.entry(snake.to_string()).or_insert(0);
                    let key = format!("{}:{}:{}:0", cfg_key, snake, *group);
                    *group += 1;
                    let found = listed.iter().find(|r| r["key"].as_str() == Some(key.as_str()));
                    report.push(match found {
                        Some(r) => json!({
                            "key": key,
                            "event": event,
                            "status": if r["trustStatus"].as_str() == Some("trusted") && r["enabled"].as_bool().unwrap_or(true) {
                                "BOUND"
                            } else {
                                "UNTRUSTED"
                            },
                            "trustStatus": r["trustStatus"],
                            "enabled": r["enabled"],
                        }),
                        None => json!({"key":key,"event":event,"status":"MISSING"}),
                    });
                }
                let mut s = h.store.lock().unwrap();
                let _ = s.update(task, |t| {
                    t["hookStatus"] = json!(report);
                });
                let _ = s.event(task, "hook.bound", json!({"hooks":report}));
            }
            Err(e) => {
                let s = h.store.lock().unwrap();
                let _ = s.event(task, "hook.bind.failure", json!({"code":safe_code(&e)}));
            }
        }
    }
}
async fn start_turn(h: Shared, task: &str, key: &str, text: &str, peer: Arc<Peer>) -> Result<()> {
    ensure_running(&h, task)?;
    let t = h.store.lock().unwrap().task(task)?;
    // Boundary check: a cap already tripped by observed usage makes this task
    // terminally BUDGET_EXCEEDED and the fresh peer is shut down at once.
    let exceeded = {
        let mut s = h.store.lock().unwrap();
        if s.budget_check(task)?.is_empty() {
            false
        } else {
            let turn = t["nativeTurnId"].as_str().unwrap_or("").to_owned();
            s.budget_mark_exceeded(task, &turn)?
        }
    };
    if exceeded {
        let runner_proof = stop_runners(&h, task).await;
        let mac_proof = stop_mac_tools(&h, task).await;
        let runner_ok = runner_proof.is_ok() && mac_proof.is_ok();
        peer.stop().await?;
        let proven = peer.quiescence_proven();
        {
            let mut s = h.store.lock().unwrap();
            s.update(task, |t| {
                apply_stop_proof(t, proven && runner_ok, Some("BUDGET_EXCEEDED"));
                if let Ok(proof) = &runner_proof {
                    apply_runner_proof(t, proof);
                }
                if let Some(questions) = t["inputRequests"].as_object_mut() {
                    for q in questions.values_mut() {
                        if q["state"] == "PENDING" {
                            q["state"] = json!("EXPIRED")
                        }
                    }
                }
            })?;
            s.effects_unknown(task)?;
        }
        drain_queue(&h);
        bail!("BUDGET_EXCEEDED")
    }
    // C6: uploads bound to this request ride the same turn input — IMAGE refs
    // become localImage items over the staged bytes, REFERENCE_TEXT refs append
    // a bounded ATTACHED_MATERIAL section to the text item. The turn params
    // also carry the frozen policy preset (C7).
    let (mut input, material_text) = crate::materials::material_input(&h, task, key)?;
    let mut full_text = String::from(text);
    full_text.push_str(&material_text);
    input.insert(0, json!({"type":"text","text":full_text}));
    let mut params = json!({"threadId":t["nativeSessionId"],"model":h.registry.model,"input":input,"clientUserMessageId":key});
    // The pinned schema accepts sandboxPolicy on turn/start; extra writable
    // roots ride the same workspace-write policy the sandbox already enforces.
    // A policy preset's writableRoots extend the task writeScope set.
    let mut writable = vec![t["workspace"].clone()];
    if let Some(roots) = t["writeScope"]["extraWriteDirs"].as_array() {
        writable.extend(roots.iter().cloned());
    }
    if let Some(roots) = t["policyPreset"]["writableRoots"].as_array() {
        writable.extend(roots.iter().cloned());
    }
    if writable.len() > 1 {
        params["sandboxPolicy"] = json!({"type":"workspaceWrite","writableRoots":writable});
    }
    let result = peer.rpc("turn/start", params).await?;
    let native = string(&result["turn"], "id")?.to_owned();
    ensure_running(&h, task)?;
    let mut s = h.store.lock().unwrap();
    let t = s.update(task, |t| {
        t["nativeTurnId"] = json!(native);
        t["currentCommandId"] = json!(key);
        let already_completed = t["turns"][key]["nativeTurnId"] == native
            && matches!(
                t["turns"][key]["state"].as_str(),
                Some("SUCCEEDED" | "FAILED" | "CANCELLED")
            );
        if !already_completed {
            t["state"] = json!("RUNNING");
        }
        t["launchState"] = json!("DISPATCH_STARTED");
        if !already_completed {
            t["turns"][key] = json!({"state":"RUNNING","nativeTurnId":native});
        }
        t["quiescent"] = json!(false)
    })?;
    let scope = if t["requestKey"] == key {
        "create"
    } else {
        task
    };
    let receipt = if scope == "create" {
        t.clone()
    } else {
        json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","nativeTurnId":native,"detail":"Native turn accepted"})
    };
    s.receipt(scope, key, "APPLIED", &receipt)?;
    // Bound materials mark CONSUMED only once the native turn bound — the
    // durable consumption fact, never assumed at intake.
    s.materials_consume(task, key)?;
    s.event(
        task,
        "turn.bound",
        json!({"requestKey":key,"nativeTurnId":native}),
    )?;
    Ok(())
}

fn guards(t: &Value, b: &Value) -> Result<()> {
    for k in ["generation", "nativeSessionId", "nativeTurnId"] {
        if t[k] != b[k] {
            bail!("NATIVE_TARGET_CHANGED")
        }
    }
    if t["currentCommandId"] != b["targetCommandId"] {
        bail!("NATIVE_TARGET_CHANGED")
    }
    // Optional owner-epoch fence: transitions rotate the epoch, so a client
    // bound to the old owner is refused even inside the same native session.
    if b["ownerEpoch"].is_string()
        && t["ownerEpoch"].as_str() != b["ownerEpoch"].as_str()
    {
        bail!("NATIVE_TARGET_CHANGED")
    }
    Ok(())
}
fn ensure_running(h: &Host, task: &str) -> Result<()> {
    let t = h.store.lock().unwrap().task(task)?;
    match t["state"].as_str() {
        Some("STOP_REQUESTED" | "CANCELLED") => bail!("NATIVE_STOP_REQUESTED"),
        Some("BUDGET_EXCEEDED") => bail!("BUDGET_EXCEEDED"),
        Some("BINDING_UNAVAILABLE") => bail!("BINDING_UNAVAILABLE"),
        _ => {}
    }
    Ok(())
}
// Shared reopen gates: the last bound native turn of this exact generation and
// segment must carry a frozen completion fact, no effect may be unresolved, no
// question may be open, and an active slot must be free to host the dispatch.
fn reopen_gates(s: &Store, t: &Value, max_active: usize) -> Result<()> {
    let task = string(t, "taskId")?;
    let seal = s
        .turn_seal(task, string(t, "nativeTurnId")?)?
        .ok_or_else(|| anyhow!("EFFECT_RECONCILIATION_REQUIRED"))?;
    if seal["generation"] != t["generation"] || seal["segmentId"] != t["segmentId"] {
        bail!("EFFECT_RECONCILIATION_REQUIRED")
    }
    if s.open_effects(task)? != 0 {
        bail!("EFFECT_RECONCILIATION_REQUIRED")
    }
    if t["inputRequests"].as_object().is_some_and(|q| {
        q.values()
            .any(|q| matches!(q["state"].as_str(), Some("PENDING" | "ANSWERED")))
    }) {
        bail!("EFFECT_RECONCILIATION_REQUIRED")
    }
    if s.active_count(Some(task))? >= max_active {
        bail!("LOCAL_TASK_BUSY")
    }
    Ok(())
}
// The durable proof that this task's bound native turn completed cleanly in
// the CURRENT generation/segment: a frozen turn_seal row with the native
// "completed" status. A stale-generation seal never qualifies a new owner.
fn completed_turn_sealed(s: &Store, t: &Value) -> bool {
    let task = t["taskId"].as_str().unwrap_or("");
    let turn = t["nativeTurnId"].as_str().unwrap_or("");
    if task.is_empty() || turn.is_empty() {
        return false;
    }
    s.turn_seal(task, turn)
        .ok()
        .flatten()
        .is_some_and(|seal| {
            seal["status"] == "completed"
                && seal["generation"] == t["generation"]
                && seal["segmentId"] == t["segmentId"]
        })
}
// The full Pi canReopen seal check: the durable completed turn proof AND the
// recorded completedSessionSeal still naming the bound session/generation.
fn completed_session_ok(s: &Store, t: &Value) -> bool {
    t["completedSessionSeal"].is_object()
        && t["completedSessionSeal"]["nativeSessionId"] == t["nativeSessionId"]
        && t["completedSessionSeal"]["generation"] == t["generation"]
        && completed_turn_sealed(s, t)
}
// Pi sealCompleted: only a SUCCEEDED session whose last bound turn carries the
// frozen completed seal — and which has no unresolved native effects — earns a
// completedSessionSeal when its owner quiesces (managed STOP or an observed
// peer exit). The seal is the reopen/transition credential; the next admission
// deletes it. method honestly names how quiescence was proven.
fn maybe_seal_completed(t: &mut Value, seal_ok: bool, unresolved_effects: i64, method: &str) {
    if t["completedSessionSeal"].is_object()
        || t["state"] != "SUCCEEDED"
        || !seal_ok
        || unresolved_effects != 0
        || !t["nativeSessionId"].is_string()
        || !t["nativeTurnId"].is_string()
    {
        return;
    }
    t["completedSessionSeal"] = json!({
        "nativeSessionId": t["nativeSessionId"],
        "nativeTurnId": t["nativeTurnId"],
        "generation": t["generation"],
        "segmentId": t["segmentId"],
        "engineVersion": t["engineVersion"],
        "sealedAt": store::now(),
        "quiescenceMethod": method,
    });
}
// An explicit new follow-up may reuse a quiesced native terminal turn. A
// success still needs its completed-session seal; a stopped interruption or
// failure needs its matching durable native turn seal and owned stop proof.
// Nothing replays an old command or settles unknown effects on admission.
fn reopen_blockers(
    s: &Store,
    t: &Value,
    live_peer: bool,
    max_active: usize,
    engine_version: &str,
) -> Result<()> {
    // A live or still-owned session is not a reopen case: the ordinary
    // same-thread follow-up path applies instead (Pi NATIVE_NOT_IDLE parity).
    if live_peer || t["quiescent"] != true {
        bail!("NATIVE_NOT_IDLE")
    }
    if !matches!(t["state"].as_str(), Some("SUCCEEDED" | "CANCELLED" | "FAILED")) {
        bail!("COMPLETED_SESSION_REQUIRED")
    }
    let terminal_seal = match t["state"].as_str() {
        Some("SUCCEEDED") => completed_session_ok(s, t),
        Some("CANCELLED" | "FAILED") => {
            let expected = if t["state"] == "CANCELLED" { "interrupted" } else { "failed" };
            t["stopProof"].is_object() && s.turn_seal(
                t["taskId"].as_str().unwrap_or(""),
                t["nativeTurnId"].as_str().unwrap_or(""),
            )?.is_some_and(|seal| seal["status"] == expected)
        }
        _ => false,
    };
    // The durable proof bundle: bound native ids, same engine version, and the
    // recorded completedSessionSeal + frozen completed turn seal for this exact
    // generation/segment. Missing or mismatched proofs stay unreconciled.
    if !t["nativeSessionId"].is_string()
        || !t["nativeTurnId"].is_string()
        || t["engineVersion"].as_str() != Some(engine_version)
        || !terminal_seal
    {
        bail!("EFFECT_RECONCILIATION_REQUIRED")
    }
    reopen_gates(s, t, max_active)
}
// EXTEND_BUDGET admission: only a terminal quiescent BUDGET_EXCEEDED session can
// be reopened, under exactly the same provenance gates as a stopped follow-up.
fn extend_blockers(s: &Store, t: &Value, max_active: usize) -> Result<()> {
    let qualifies = t["nativeSessionId"].is_string()
        && t["nativeTurnId"].is_string()
        && t["state"] == "BUDGET_EXCEEDED"
        && t["quiescent"] == true;
    if !qualifies {
        bail!("NATIVE_NOT_IDLE")
    }
    reopen_gates(s, t, max_active)
}
fn runtime_read_stamp(root: &Path, registry: &Registry) -> Option<ReadVerificationStamp> {
    // Native-profile inputs are fixed paths. Other profiles can depend on
    // directory bundles or platform permissions and retain the short TTL.
    if registry.tool_profile != "native" {
        return None;
    }
    let mut paths = vec![registry.executable.clone(), root.join("profile/config.toml")];
    if let Some(catalog) = &registry.model_catalog {
        paths.push(catalog.clone());
    }
    paths.iter().map(|path| {
        let metadata = fs::metadata(path).ok()?;
        if !metadata.is_file() { return None; }
        Some((metadata.len(), metadata.modified().ok()?))
    }).collect()
}
async fn coalesced_read_verification(
    cache: &ReadVerificationCache,
    stamp: Option<ReadVerificationStamp>,
    check: impl FnOnce() -> bool + Send + 'static,
) -> bool {
    let mut observed = cache.lock().await;
    if let Some((at, previous_stamp, valid)) = observed.as_ref() {
        if previous_stamp == &stamp && ((stamp.is_some() && *valid) || at.elapsed() < Duration::from_secs(1)) {
            return *valid;
        }
    }
    // Metadata only caches an advisory capability observation. It is never
    // execution authorization: admission and launch still verify all bytes.
    let valid = tokio::task::spawn_blocking(check).await.unwrap_or(false);
    *observed = Some((std::time::Instant::now(), stamp, valid));
    valid
}
async fn reopen_ready(h: &Shared, t: &Value, live_peer: bool) -> bool {
    // Runtime verification reads the pinned executable (hundreds of MiB on
    // Windows). Never hold the global task ledger while performing that I/O:
    // STOP and unrelated observations must still be able to acquire the store.
    let eligible = {
        let s = h.store.lock().unwrap();
        reopen_blockers(&s, t, live_peer, h.max_active, h.registry.version.as_str()).is_ok()
    };
    if !eligible || !missing_model_env(h).is_ok_and(|names| names.is_empty()) {
        return false;
    }
    let verifier = h.clone();
    coalesced_read_verification(&h.read_verification, runtime_read_stamp(&h.root, &h.registry), move || {
        verify(&verifier.root, &verifier.registry).is_ok()
    })
    .await
}
// An EXTEND_BUDGET is only an extension when every cap it keeps sits strictly
// above the usage already observed; removing a cap is also a valid extension.
fn budget_extend_check(caps: &Value, usage: &Value) -> Result<()> {
    for (cap, dim) in [
        ("maxTotalTokens", "totalTokens"),
        ("maxInputTokens", "inputTokens"),
        ("maxOutputTokens", "outputTokens"),
    ] {
        let Some(new_cap) = caps["caps"][cap]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| caps["caps"][cap].as_u64())
        else {
            continue;
        };
        let observed = usage[dim]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| usage[dim].as_u64())
            .unwrap_or(0);
        if new_cap <= observed {
            bail!("BUDGET_STILL_EXCEEDED")
        }
    }
    Ok(())
}
async fn interact(h: Shared, task: &str, b: Value) -> Result<(u16, Value)> {
    let key = valid_id(string(&b, "requestKey")?)?.to_owned();
    let existing = { h.store.lock().unwrap().existing(task, &key, &b)? };
    if let Some(v) = existing {
        reconcile_mac_stop_receipt(&h, task, &key).await?;
        let observed = h.store.lock().unwrap().lookup(task, &key).unwrap_or(v);
        return Ok((202, observed));
    }
    let t = h.store.lock().unwrap().task(task)?;
    guards(&t, &b)?;
    let kind = b["kind"]
        .as_str()
        .or_else(|| b["intent"].as_str())
        .ok_or_else(|| anyhow!("NATIVE_ACTION_UNSUPPORTED"))?;
    if !matches!(
        kind,
        "NATIVE_FOLLOW_UP" | "NATIVE_STEER" | "INPUT_RESPONSE" | "STOP" | "EXTEND_BUDGET"
    ) {
        bail!("NATIVE_ACTION_UNSUPPORTED")
    }
    // C5: a sealed source refuses every new input/mutation; only the
    // idempotent STOP cleanup path stays admitted.
    if kind != "STOP" {
        ensure_not_sealed(&t)?;
    }
    // C6: materials[] may ride only text-carrying message kinds.
    let binds = if b["materials"].is_null() {
        vec![]
    } else {
        if !matches!(kind, "NATIVE_FOLLOW_UP" | "NATIVE_STEER") {
            bail!("INVALID_MATERIALS")
        }
        let s = h.store.lock().unwrap();
        crate::materials::validate_materials(&s, &b["materials"], task)?
    };
    // A terminally exceeded task accepts no work until an atomic guarded
    // EXTEND_BUDGET reopens it; STOP stays admitted for cleanup bookkeeping.
    if !matches!(kind, "STOP" | "EXTEND_BUDGET") {
        match t["state"].as_str() {
            Some("BUDGET_EXCEEDED") => bail!("BUDGET_EXCEEDED"),
            Some("BINDING_UNAVAILABLE") => bail!("BINDING_UNAVAILABLE"),
            _ => {}
        }
    }
    let peer = h.peers.lock().await.get(task).cloned();
    let live = match &peer {
        Some(p) => p.alive().await?,
        None => false,
    };
    // A follow-up rides the live turn on a bound peer; with no live writer the
    // request can only be admitted as a stopped-session reopen under its gates.
    let mut reopen = false;
    if kind == "NATIVE_FOLLOW_UP" {
        if t["state"] == "SUCCEEDED" && live {
        } else if !live {
            // No live writer: the only honest admission is a completed-session
            // reopen — reopen_blockers proves the seal durably at admit time.
            reopen = true;
        } else {
            bail!("NATIVE_NOT_IDLE")
        }
    }
    // EXTEND_BUDGET reuses the same atomic reopen admission: new caps, a new
    // generation/segment/epoch, and a guarded restart of the persisted session.
    if kind == "EXTEND_BUDGET" {
        let new_caps = validate_budget(b.get("budget").ok_or_else(|| anyhow!("INVALID_REQUEST"))?)?;
        if t["state"] != "BUDGET_EXCEEDED" {
            bail!("BUDGET_NOT_EXCEEDED")
        }
        budget_extend_check(&new_caps, &t["usageTotals"])?;
        extend_blockers(&h.store.lock().unwrap(), &t, h.max_active)?;
        reopen = true;
    }
    if kind != "STOP" {
        require_model_configuration(&h)?;
    }
    if reopen {
        // Executable and launch-config digests must still match what froze the session.
        verify(&h.root, &h.registry)?;
    }
    if kind == "NATIVE_STEER" && t["state"] != "RUNNING" {
        bail!("NATIVE_NOT_RUNNING")
    }
    let content = if matches!(kind, "NATIVE_FOLLOW_UP" | "NATIVE_STEER" | "EXTEND_BUDGET") {
        instruction(&b, "content")?
    } else {
        String::new()
    };
    if kind != "STOP" && !reopen {
        if peer.is_none() {
            bail!("OUTCOME_UNCONFIRMED")
        }
        if !live {
            bail!("NATIVE_WRITER_UNPROVEN")
        }
    }
    let answer = if kind == "INPUT_RESPONSE" {
        let q = &t["inputRequests"][string(&b, "inputRequestId")?];
        if q["state"] != "PENDING" {
            bail!("NATIVE_INPUT_EXPIRED")
        }
        guards(&t, q)?;
        Some((
            q["nativeRequestId"].clone(),
            adapter::response(string(q, "method")?, &q["params"], &b["response"])?,
        ))
    } else {
        None
    };
    let accepted = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"ACCEPTED","detail":"Durably accepted"});
    {
        let mut s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, &b)? {
            return Ok((202, v));
        }
        let current = s.task(task)?;
        // Seal-vs-admission TOCTOU: re-verify on the fresh row under the same
        // store lock that commits the durable message. STOP stays admitted so
        // the sealed source can still be terminated.
        if kind != "STOP" {
            ensure_not_sealed(&current)?;
        }
        guards(&current, &b)?;
        if reopen {
            if kind == "EXTEND_BUDGET" {
                extend_blockers(&s, &current, h.max_active)?;
                // Recheck under the lock: caps must still sit above observed usage.
                budget_extend_check(
                    &validate_budget(b.get("budget").ok_or_else(|| anyhow!("INVALID_REQUEST"))?)?,
                    &current["usageTotals"],
                )?;
            } else {
                reopen_blockers(&s, &current, live, h.max_active, h.registry.version.as_str())?;
            }
            // Seal the outgoing segment before its id moves so the stopped
            // workspace snapshot survives the new generation's writes.
            let segment = current["segmentId"]
                .as_str()
                .unwrap_or("segment-legacy")
                .to_owned();
            if s.segment_manifest(task, &segment)?.is_none() {
                seal_manifest(&mut s, &h.root, &current, &segment)?;
            }
            let extended = if kind == "EXTEND_BUDGET" {
                Some(validate_budget(
                    b.get("budget").ok_or_else(|| anyhow!("INVALID_REQUEST"))?,
                )?)
            } else {
                None
            };
            s.admit_reopen(task, &key, &b, &accepted, extended.as_ref().map(|v| &v["caps"]), if kind == "NATIVE_FOLLOW_UP" { Some(content.as_str()) } else { None }, &binds, |t| {
                t["reopenFrom"] = json!({"generation":t["generation"],"segmentId":t["segmentId"],"nativeTurnId":t["nativeTurnId"],"commandId":t["currentCommandId"],"stopProof":t["stopProof"],"completedSessionSeal":t["completedSessionSeal"]});
                t["generation"] = json!(store::id());
                t["segmentId"] = json!(format!("segment-{}", store::id()));
                let epoch = t["ownerEpoch"]
                    .as_str()
                    .unwrap_or("0")
                    .parse::<u64>()
                    .unwrap_or(0)
                    + 1;
                t["ownerEpoch"] = json!(epoch.to_string());
                t["ownerBootId"] = json!(h.boot);
                t["currentCommandId"] = json!(key);
                t["state"] = json!("UNKNOWN");
                t["launchState"] = json!("RESTORING");
                t["quiescent"] = json!(false);
                t.as_object_mut().unwrap().remove("stopProof");
                t.as_object_mut().unwrap().remove("reasonCode");
                t.as_object_mut().unwrap().remove("completedSessionSeal");
            })?;
            s.receipt(
                task,
                &key,
                "DISPATCHING",
                &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"DISPATCHING"}),
            )?;
        } else if matches!(kind, "NATIVE_FOLLOW_UP" | "NATIVE_STEER") {
            // User admissions carry exactly one durable ACCEPTED user message
            // in the same transaction; a follow-up's re-engagement task state
            // joins that commit instead of riding a second write.
            s.accept_user(
                task,
                &key,
                &b,
                &accepted,
                &content,
                kind == "NATIVE_FOLLOW_UP",
                &binds,
            )?;
            s.receipt(
                task,
                &key,
                "DISPATCHING",
                &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"DISPATCHING"}),
            )?;
        } else {
            s.accept(task, &key, &b, &accepted, task)?;
            s.receipt(
                task,
                &key,
                "DISPATCHING",
                &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"DISPATCHING"}),
            )?;
            if kind == "STOP" {
                s.update(task, |t| { mark_stop_requested(t); if !t["macToolBinding"].is_null() { t["macStopParentRequestKey"] = json!(key); } })?;
            }
        }
    }
    let task = task.to_owned();
    let kind = kind.to_owned();
    let output = accepted.clone();
    tokio::spawn(async move {
        let result:Result<Value>=async{match kind.as_str(){
    "STOP"=>{let affected=stop_owner(&h,&task,Some(("TASK_STOPPED","PARENT_STOP"))).await?;drain_queue(&h);Ok(json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","affectedChildren":affected,"detail":"Owned preparation drained and process tree exited; remote effects remain separately unproven"}))},
    "NATIVE_FOLLOW_UP" | "EXTEND_BUDGET"=>{if reopen{reopen_session(h.clone(),&task,&key,&content).await?;}else{start_turn(h.clone(),&task,&key,&content,peer.unwrap()).await?;}Ok(h.store.lock().unwrap().lookup(&task,&key)?)},
    "NATIVE_STEER"=>{let(mut items,extra)=crate::materials::material_input(&h,&task,&key)?;let mut text=content.clone();text.push_str(&extra);items.insert(0,json!({"type":"text","text":text}));peer.unwrap().rpc("turn/steer",json!({"threadId":t["nativeSessionId"],"expectedTurnId":t["nativeTurnId"],"input":items,"clientUserMessageId":key})).await?;{let mut s=h.store.lock().unwrap();let _=s.materials_consume(&task,&key);}Ok(json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"REQUESTED","detail":"Native turn/steer accepted; consumption awaits native evidence"}))},
    "INPUT_RESPONSE"=>{let(id,response)=answer.unwrap();h.store.lock().unwrap().update(&task,|t|{let q=&mut t["inputRequests"][b["inputRequestId"].as_str().unwrap()];q["state"]=json!("ANSWERED");q["responseRequestKey"]=json!(key);})?;peer.unwrap().send(&json!({"id":id,"result":response})).await?;Ok(json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"REQUESTED","detail":"Bound response sent; native consumption unconfirmed"}))},_=>unreachable!()}}.await;
        let mut s = h.store.lock().unwrap();
        match result {
            Ok(v) => {
                let phase = v["state"].as_str().unwrap_or("UNKNOWN");
                let _ = s.receipt(&task, &key, phase, &v);
                let _ = s.event(&task, "request.observed", v);
            }
            Err(e) => {
                let code = safe_code(&e);
                if kind == "STOP" {
                    let _ = s.update(&task, |t| {
                        t["state"] = json!("UNKNOWN");
                        t["quiescent"] = json!(false);
                        t["reasonCode"] = json!(code);
                    });
                }
                // A failed reopen leaves the task in its admitted UNKNOWN state;
                // record why without reviving the cancelled/succeeded past.
                if reopen {
                    let _ = s.update(&task, |t| t["reasonCode"] = json!(code));
                }
                let _=s.receipt(&task,&key,"UNKNOWN",&json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"UNKNOWN","reasonCode":code}));
                let _ = s.event(
                    &task,
                    "request.unconfirmed",
                    json!({"requestKey":key,"code":code}),
                );
            }
        }
    });
    Ok((202, output))
}

// argv for a task terminal: a bounded list of non-empty bounded strings. NUL is
// the only byte that cannot cross a process argv on any platform.
fn validate_argv(v: &Value) -> Result<Vec<String>> {
    let a = v.as_array().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
    if a.is_empty() || a.len() > 32 {
        bail!("INVALID_REQUEST")
    }
    let mut argv = vec![];
    for e in a {
        let s = e.as_str().ok_or_else(|| anyhow!("INVALID_REQUEST"))?;
        if s.is_empty() || s.len() > 1024 || s.contains('\0') {
            bail!("INVALID_REQUEST")
        }
        argv.push(s.to_owned());
    }
    Ok(argv)
}
fn file_approval_paths(t: &Value, params: &Value) -> Option<Value> {
    let change = &t["nativeFileChanges"][params["itemId"].as_str()?];
    if change["nativeSessionId"] != params["threadId"]
        || change["nativeTurnId"] != params["turnId"]
        || change["generation"] != t["generation"]
        || change["nativeSessionId"] != t["nativeSessionId"]
        || change["nativeTurnId"] != t["nativeTurnId"]
    {
        return None;
    }
    let paths = change["paths"].as_array()?;
    if paths.is_empty() || paths.iter().any(|p| p.as_str().is_none_or(str::is_empty)) {
        return None;
    }
    Some(json!(paths))
}
// Dynamic calls are separate server RPC requests, never native approval grants.
fn dynamic_binding(t: &Value, p: &Value) -> Result<()> {
    let call = string(p, "callId")?;
    if call.len() > 256 || t["nativeSessionId"] != p["threadId"] || t["nativeTurnId"] != p["turnId"]
    {
        bail!("NATIVE_TARGET_CHANGED")
    }
    let observed = &t["nativeDynamicCalls"][call];
    if observed["generation"] != t["generation"]
        || observed["nativeSessionId"] != p["threadId"]
        || observed["nativeTurnId"] != p["turnId"]
        || observed["tool"] != p["tool"]
        || observed["arguments"] != p["arguments"]
    {
        bail!("NATIVE_TOOL_CALL_UNBOUND")
    }
    Ok(())
}
fn runner_key(task: &str, p: &Value) -> String {
    format!(
        "runner-{}",
        hash(json!([task, p["threadId"], p["turnId"], p["callId"]]).to_string())
    )
}
fn runner_context(t: &Value) -> String {
    format!(
        "ctx-{}",
        hash(
            json!([
                t["taskId"],
                t["generation"],
                t["segmentId"],
                t["nativeSessionId"],
                t["nativeTurnId"],
                t["currentCommandId"]
            ])
            .to_string()
        )
    )
}
async fn runner_dispatch(h: &Shared, task: &str, p: &Value) -> Result<Value> {
    // STOP records STOP_REQUESTED before waiting on this setup/dispatch gate.
    // Once the gate is released, closing all contexts excludes late admission.
    let gate = h.runner_admission.lock().await;
    let t = h.store.lock().unwrap().task(task)?;
    dynamic_binding(&t, p)?;
    if !matches!(t["state"].as_str(), Some("RUNNING" | "STARTING")) {
        bail!("RUNNER_INTAKE_CLOSED")
    }
    verify(&h.root, &h.registry)?;
    let key = runner_key(task, p);
    let context = runner_context(&t);
    let command = crate::runner::command(p, &context, &key)?;
    let body = json!({"native":p,"generation":t["generation"],"segmentId":t["segmentId"],"currentCommandId":t["currentCommandId"],"command":command});
    let existing = h.store.lock().unwrap().existing(task, &key, &body)?;
    if let Some(receipt) = &existing {
        if receipt["state"] == "APPLIED" {
            return Ok(receipt["runner"].clone());
        }
        if receipt["state"] == "UNKNOWN" {
            bail!("OUTCOME_UNCONFIRMED")
        }
    }
    let existing_client = { h.runners.lock().await.get(task).map(|r| r.client.clone()) };
    let client = match existing_client {
        Some(client) => client,
        None => {
            let client = crate::runner::Runner::start(
                h.registry
                    .command_runner
                    .as_ref()
                    .ok_or_else(|| anyhow!("RUNNER_CONFIGURATION_REQUIRED"))?,
                h.registry
                    .runner_node
                    .as_ref()
                    .ok_or_else(|| anyhow!("RUNNER_CONFIGURATION_REQUIRED"))?,
                &h.root.join("runners").join(task),
                &env_allowlist(&t),
            )
            .await?;
            h.runners.lock().await.insert(
                task.into(),
                TaskRunner {
                    client: client.clone(),
                    contexts: HashSet::new(),
                },
            );
            client
        }
    };
    // Call identity is in the command's original requestKey, and the immutable
    // context supplies task, generation, segment, thread, turn and command guard.
    let registration = json!({"contextId":context,"workspace":t["workspace"],"binding":{"taskId":task,"generation":t["generation"],"segmentId":t["segmentId"],"nativeSessionId":t["nativeSessionId"],"nativeTurnId":t["nativeTurnId"],"currentCommandId":t["currentCommandId"],"executionProfile":"macos-runner-v1","engineBindingDigest":h.registry.launch_config_digest,"runnerRuntimeDigest":h.registry.command_runner_digest,"runnerNodeDigest":h.registry.runner_node_digest},"envAllowlist":env_allowlist(&t)});
    h.runners
        .lock()
        .await
        .get_mut(task)
        .unwrap()
        .contexts
        .insert(context.clone());
    let registered=match client.request("POST", "/v1/contexts", Some(&registration)).await {
        Ok(value)=>value,
        Err(_)=>client.request("GET",&format!("/v1/contexts/{context}"),None).await?,
    };
    if registered["binding"]!=registration["binding"] || registered["contextId"]!=context || registered["closed"]!=false {bail!("RUNNER_CONTEXT_CHANGED")}
    if existing.is_none() {
        let mut s = h.store.lock().unwrap();
        // Recheck STOP's persisted fence after runner startup/registration.
        let fresh = s.task(task)?;
        dynamic_binding(&fresh, p)?;
        if !matches!(fresh["state"].as_str(), Some("RUNNING" | "STARTING")) {
            bail!("RUNNER_INTAKE_CLOSED")
        }
        let receipt = json!({"requestKey":key,"taskId":task,"state":"DISPATCHING","nativeSessionId":p["threadId"],"nativeTurnId":p["turnId"],"nativeCallId":p["callId"],"contextId":context});
        s.accept(task, &key, &body, &receipt, task)?;
        s.receipt(task, &key, "DISPATCHING", &receipt)?;
        s.event(task,"runner.dispatching",json!({"requestKey":key,"contextId":context,"tool":p["tool"],"nativeCallId":p["callId"],"nativeSessionId":p["threadId"],"nativeTurnId":p["turnId"]}))?;
    }
    // Exactly one POST. Any lost response is reconciled only with original-key
    // GET lookup; absence or ambiguity never authorizes a replacement dispatch.
    let mut result = if existing.is_none() {
        client
            .request("POST", "/v1/commands", Some(&command))
            .await
            .ok()
    } else {
        None
    };
    drop(gate);
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(command["timeoutMs"].as_u64().unwrap_or(120000) + 30000);
    while result.as_ref().is_none_or(|v| !crate::runner::terminal(v)) {
        if tokio::time::Instant::now() >= deadline {
            bail!("RUNNER_RESPONSE_UNCONFIRMED")
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        result = client
            .request("GET", &format!("/v1/requests/{key}"), None)
            .await
            .ok();
        let state = h.store.lock().unwrap().lookup(task, &key)?;
        if state["state"] == "APPLIED" {
            return Ok(state["runner"].clone());
        }
        if state["state"] == "UNKNOWN" {
            bail!("OUTCOME_UNCONFIRMED")
        }
    }
    let result = redact_value(&result.unwrap());
    // Context binding and original request identity must survive every response.
    if result["requestKey"] != key || result["contextId"] != context {
        bail!("RUNNER_IDENTITY_CHANGED")
    }
    let proven = crate::runner::settled(&result);
    let receipt = json!({"requestKey":key,"taskId":task,"state":if proven{"APPLIED"}else{"UNKNOWN"},"nativeSessionId":p["threadId"],"nativeTurnId":p["turnId"],"nativeCallId":p["callId"],"contextId":context,"runner":result});
    let mut s = h.store.lock().unwrap();
    s.runner_result(task, &key, string(p, "callId")?, &receipt, proven)?;
    s.event(task, "runner.completed", receipt)?;
    if !proven {
        bail!("RUNNER_EFFECT_UNPROVEN")
    }
    Ok(result)
}
async fn mac_dynamic_call(h: Shared, task: String, event: Value) {
    let p = &event["params"];
    let result: Result<Value> = async {
        let t = h.store.lock().unwrap().task(&task)?;
        dynamic_binding(&t, p)?;
        if t["macToolBinding"].is_null() || t["macToolsClosed"] == true || t["intakeClosed"] == true { bail!("MAC_TOOLS_CLOSED") }
        let binding = &t["macToolBinding"];
        let name = p["tool"].as_str().or_else(||p["name"].as_str()).unwrap_or("");
        let input = &p["arguments"];
        let key = format!("mac-{}",hash(format!("{}:{}:{}",task,t["segmentId"],p["callId"])));
        let input_digest = hash(json!({"name":name,"arguments":input}).to_string());
        if let Some(old) = t["macOperations"].get(&key) {
            if old["inputDigest"] != input_digest { bail!("IDEMPOTENCY_CONFLICT") }
            if !old["receipt"].is_null() { return Ok(old["receipt"].clone()) }
            return crate::mac_tools::call(binding,"GET",&format!("/operations/{key}"),None).await;
        }
        let context = crate::mac_tools::call(binding,"GET","/context",None).await?;
        let capability = context["capabilities"].as_array().and_then(|a|a.iter().find(|c|crate::mac_tools::tool_name(c["capabilityId"].as_str().unwrap_or(""))==name)).ok_or_else(||anyhow!("MAC_CAPABILITY_UNAVAILABLE"))?;
        let body=json!({"schemaVersion":"mac-native-call/1","requestKey":key,"capabilityId":capability["capabilityId"],"capabilityVersion":capability["version"],"guard":context["guard"],"arguments":input});
        { let mut s=h.store.lock().unwrap(); let current=s.task(&task)?; dynamic_binding(&current,p)?;
          if current["macToolsClosed"]==true || current["intakeClosed"]==true { bail!("MAC_TOOLS_CLOSED") }
          s.update(&task,|t| { if !t["macOperations"].is_object() {t["macOperations"]=json!({});} t["macOperations"][&key]=json!({"inputDigest":input_digest,"body":body,"state":"DISPATCHING"}); })?;
        }
        let receipt=crate::mac_tools::call(binding,"POST","/operations",Some(&body)).await?;
        if receipt["requestKey"]!=key || receipt["taskId"]!=binding["taskID"] { bail!("MAC_RECEIPT_BINDING_CHANGED") }
        h.store.lock().unwrap().update(&task,|t|{t["macOperations"][&key]["receipt"]=receipt.clone();})?;
        Ok(receipt)
    }.await;
    let response = match result {
        Ok(receipt) => {
            if receipt["state"] == "APPLIED" && receipt["result"]["verification"] == "PASSED" {
                let _ = h.store.lock().unwrap().effect_settled(&task, p["callId"].as_str().unwrap_or(""), "MAC_NATIVE_APPLIED_VERIFIED");
            }
            json!({"success":receipt["state"]=="APPLIED" && receipt["result"]["verification"]=="PASSED","contentItems":[{"type":"inputText","text":receipt.to_string()}]})
        }
        Err(_) => {
            json!({"success":false,"contentItems":[{"type":"inputText","text":"MAC_OPERATION_UNCONFIRMED"}]})
        }
    };
    if let Some(peer) = h.peers.lock().await.get(&task).cloned() {
        let _ = peer
            .send(&json!({"id":event["id"],"result":response}))
            .await;
    }
}
async fn reconcile_mac_stop_receipt(h: &Shared, task: &str, key: &str) -> Result<()> {
    let t = h.store.lock().unwrap().task(task)?;
    if t["macStopParentRequestKey"] != key || t["macStopIntent"] != true || t["macRemoteQuiescent"] == true { return Ok(()) }
    stop_mac_tools(h, task).await?;
    let mut s = h.store.lock().unwrap(); let observed = s.task(task)?;
    if observed["localQuiescent"] == true && observed["macRemoteQuiescent"] == true {
        s.update(task, |t| { settle_stop_state(t); t["quiescent"] = json!(true); })?;
        s.receipt(task, key, "APPLIED", &json!({"kind":"WORKER_LOCAL","requestKey":key,"taskId":task,"state":"APPLIED","detail":"Owned model and Mac child are both quiescent"}))?;
    }
    Ok(())
}
async fn stop_mac_tools(h: &Shared, task: &str) -> Result<()> {
    let t = h.store.lock().unwrap().task(task)?;
    if t["macToolBinding"].is_null() {
        return Ok(());
    }
    let binding = &t["macToolBinding"];
    let key = format!(
        "mac-stop-{}",
        hash(
            json!({"requestKey":binding["workerRequestKey"],"taskID":binding["taskID"]})
                .to_string()
        )
    );
    let original = t["macStopIntent"] == true;
    h.store.lock().unwrap().update(task, |t| {
        t["macToolsClosed"] = json!(true);
        t["macStopIntent"] = json!(true);
    })?;
    let mut receipt = if original {
        crate::mac_tools::call(binding, "GET", &format!("/requests/{key}"), None).await?
    } else {
        crate::mac_tools::call(binding, "POST", "/stop", Some(&json!({"requestKey":key}))).await?
    };
    for _ in 0..80 {
        if receipt["remoteQuiescent"] == true || receipt["quiescent"] == true { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
        receipt = crate::mac_tools::call(binding, "GET", &format!("/requests/{key}"), None).await?;
    }
    if receipt["requestKey"] != key || receipt["taskId"] != binding["taskID"] { bail!("MAC_STOP_BINDING_CHANGED") }
    let quiet = receipt["remoteQuiescent"] == true || receipt["quiescent"] == true;
    h.store.lock().unwrap().update(task, |t| {
        t["macStopReceipt"] = receipt;
        t["macRemoteQuiescent"] = json!(quiet);
    })?;
    if !quiet {
        bail!("MAC_STOP_UNCONFIRMED")
    }
    Ok(())
}
async fn dynamic_call(h: Shared, task: String, event: Value) {
    let p = &event["params"];
    if p["tool"].as_str().or_else(||p["name"].as_str()).is_some_and(|n|n.starts_with("mac__")) {
        mac_dynamic_call(h, task, event).await; return;
    }
    let result = runner_dispatch(&h, &task, p).await;
    let response = match result {
        Ok(mut result) => {
            for field in ["stdout", "stderr"] {
                if let Some(text) = result[field].as_str().filter(|s| s.len() > 128 * 1024) {
                    let mut start = text.len() - 128 * 1024;
                    while !text.is_char_boundary(start) {
                        start += 1;
                    }
                    result[field] = json!(text[start..].to_owned());
                    result["hostOutputTruncated"] = json!(true);
                }
            }
            json!({"success":result["status"]=="SUCCEEDED" && result["exitCode"]==0,"contentItems":[{"type":"inputText","text":result.to_string()}]})
        }
        Err(error) => {
            let code = safe_code(&error);
            let key = runner_key(&task, p);
            let mut s = h.store.lock().unwrap();
            if let Ok(receipt) = s.lookup(&task, &key) {
                if receipt["state"] != "APPLIED" {
                    let unknown = json!({"requestKey":key,"taskId":task,"state":"UNKNOWN","nativeCallId":p["callId"],"reasonCode":code});
                    let _ = s.runner_result(
                        &task,
                        &key,
                        p["callId"].as_str().unwrap_or(""),
                        &unknown,
                        false,
                    );
                    let _ = s.update(&task, |t| {
                        t["state"] = json!("UNKNOWN");
                        t["quiescent"] = json!(false);
                        t["reasonCode"] = json!(code);
                    });
                }
            } else if s.task(&task).is_ok_and(|t| dynamic_binding(&t, p).is_ok()) {
                // Rejected before dispatch: there is provably no command effect.
                let _ = s.effect_settled(
                    &task,
                    p["callId"].as_str().unwrap_or(""),
                    "HOST_REJECTED_BEFORE_DISPATCH",
                );
            }
            let _ = s.event(
                &task,
                "runner.refused",
                json!({"code":code,"requestKey":key,"nativeCallId":p["callId"]}),
            );
            json!({"success":false,"contentItems":[{"type":"inputText","text":json!({"status":"REFUSED","code":code}).to_string()}]})
        }
    };
    if let Some(peer) = h.peers.lock().await.get(&task).cloned() {
        let _ = peer
            .send(&json!({"id":event["id"],"result":response}))
            .await;
    }
}
async fn stop_runners(h: &Shared, task: &str) -> Result<Value> {
    if h.registry.tool_profile != "macos-runner-v1" {
        return Ok(Value::Null);
    }
    let _gate = h.runner_admission.lock().await;
    let owned = h.runners.lock().await.get(task).map(|r| {
        (
            r.client.clone(),
            r.contexts.iter().cloned().collect::<Vec<_>>(),
        )
    });
    let Some((client, contexts)) = owned else {
        if h.store.lock().unwrap().runner_requests(task)?.is_empty() {
            return Ok(
                json!({"scope":"REGISTERED_PROCESS_GROUP","closed":true,"activeCommands":0,"unknownCommands":0,"groupExited":true,"arbitraryEscapedProcesses":"NOT_PROVEN","contexts":[]}),
            );
        }
        bail!("RUNNER_WRITER_UNPROVEN")
    };
    let mut proofs = vec![];
    for context in contexts {
        let key = format!("stop-{}", hash(format!("{}:{task}:{context}", h.boot)));
        // Context stop is itself original-keyed. After response loss use GET only.
        let mut proof = client
            .request(
                "POST",
                &format!("/v1/contexts/{context}/stop"),
                Some(&json!({"requestKey":key})),
            )
            .await
            .ok();
        if proof.is_none() {
            proof = client
                .request("GET", &format!("/v1/contexts/{context}"), None)
                .await
                .ok();
        }
        let proof = proof.ok_or_else(|| anyhow!("RUNNER_STOP_UNCONFIRMED"))?;
        if !crate::runner::context_proven(&proof) {
            bail!("RUNNER_WRITER_UNPROVEN")
        }
        proofs.push(proof);
    }
    let requests = h.store.lock().unwrap().runner_requests(task)?;
    for (key, body, receipt) in requests {
        if receipt["state"] == "UNKNOWN" {
            bail!("RUNNER_EFFECT_UNPROVEN")
        }
        let result = redact_value(
            &client
                .request("GET", &format!("/v1/requests/{key}"), None)
                .await?,
        );
        if result["requestKey"] != key
            || result["contextId"] != body["command"]["contextId"]
            || !crate::runner::settled(&result)
        {
            bail!("RUNNER_EFFECT_UNPROVEN")
        }
        let receipt = json!({"requestKey":key,"taskId":task,"state":"APPLIED","nativeSessionId":body["native"]["threadId"],"nativeTurnId":body["native"]["turnId"],"nativeCallId":body["native"]["callId"],"contextId":result["contextId"],"runner":result});
        h.store.lock().unwrap().runner_result(
            task,
            &key,
            body["native"]["callId"].as_str().unwrap_or(""),
            &receipt,
            true,
        )?;
    }
    Ok(
        json!({"scope":"REGISTERED_PROCESS_GROUP","closed":true,"activeCommands":0,"unknownCommands":0,"groupExited":true,"arbitraryEscapedProcesses":"NOT_PROVEN","contexts":proofs}),
    )
}
fn apply_runner_proof(t: &mut Value, proof: &Value) {
    if !proof.is_null() {
        t["stopProof"]["commandRunner"] = proof.clone();
        t["stopProof"]["scope"] = json!("MODEL_NO_FORK_AND_REGISTERED_COMMAND_GROUPS");
        t["stopProof"]["modelProcessTree"] = t["stopProof"]["managedProcessTree"].clone();
        t["stopProof"]["managedProcessTree"] =
            json!("MODEL_NO_FORK_AND_REGISTERED_RUNNER_GROUPS_EXITED");
        t["stopProof"]["escapedProcesses"] = json!("NOT_PROVEN_FOR_COMMAND_GROUPS");
        t["stopProof"]["arbitraryEscapedProcesses"] = json!("NOT_PROVEN");
    }
}


// The deferred `command/exec` response is the session's real exit fact: the
// pinned app-server resolves it only when the spawned process settles. A dead
// transport resolves with NATIVE_PROCESS_EXITED instead — the session is then
// LOST, never claimed to have exited.
async fn terminal_settle(
    h: &Shared,
    task: &str,
    tid: &str,
    process_id: &str,
    outcome: Result<Value>,
) {
    let bytes = h
        .terminals
        .lock()
        .unwrap()
        .get(task)
        .and_then(|m| m.get(process_id))
        .map(|l| l.base + l.buf.len() as u64)
        .unwrap_or(0) as i64;
    let s = h.store.lock().unwrap();
    match &outcome {
        Ok(res) => {
            let code = res["exitCode"].as_i64();
            if s
                .terminal_finish(task, tid, "EXITED", code, None, Some(bytes))
                .unwrap_or(false)
            {
                let _ = s.event(
                    task,
                    "terminal.exited",
                    json!({"terminalId":tid,"exitCode":code,"bytesOut":bytes.to_string()}),
                );
            }
        }
        Err(e) => {
            let code = safe_code(e);
            // A dead transport during a managed kill (STOP/budget job
            // termination) is a *proven* termination — the owning job was
            // closed on purpose and its emptying was verified. Only a
            // spontaneously dead transport makes the session truly LOST.
            let task_state = s
                .task(task)
                .ok()
                .and_then(|t| t["state"].as_str().map(str::to_owned))
                .unwrap_or_default();
            let (state, reason) = match task_state.as_str() {
                "STOP_REQUESTED" | "CANCELLED" => ("TERMINATED", "STOP_JOB_TERMINATED"),
                "BUDGET_EXCEEDED" => ("TERMINATED", "BUDGET_JOB_TERMINATED"),
                _ => {
                    if code == "NATIVE_PROCESS_EXITED" {
                        ("LOST", code.as_str())
                    } else {
                        ("EXITED", code.as_str())
                    }
                }
            };
            if s
                .terminal_finish(task, tid, state, None, Some(reason), Some(bytes))
                .unwrap_or(false)
            {
                let _ = s.event(
                    task,
                    "terminal.exited",
                    json!({"terminalId":tid,"reason":reason,"bytesOut":bytes.to_string()}),
                );
            }
        }
    }
}
// Task-scoped terminal sessions ride the pinned app-server's `command/exec`:
// streamed stdout/stderr over the connection, streamed stdin through
// command/exec/write, native ConPTY only when the caller asked for `tty:true`.
// Sessions are connection-scoped — a peer generation owns them, so task STOP
// (job kill) and peer loss both settle them honestly.
async fn terminal_route(
    h: Shared,
    task: &str,
    t: &Value,
    rest: &[&str],
    method: &str,
    b: Value,
    query: &HashMap<String, String>,
) -> Result<(u16, Value)> {
    if rest.is_empty() {
        match method {
            "GET" => {
                return Ok((
                    200,
                    json!({"items":h.store.lock().unwrap().terminals(task)?}),
                ));
            }
            "POST" => {}
            _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
        }
        exact(&b, &["argv", "tty"])?;
        // A sealed source refuses every new write effect — a fresh terminal
        // spawn is one. DELETE/signal stay open as stop-class cleanup.
        ensure_not_sealed(t)?;
        let argv = validate_argv(&b["argv"])?;
        // PTY is claimed only for the native tty flag the pinned schema
        // supports; anything else runs over the engine's own pipes.
        let pty = b["tty"].as_bool().unwrap_or(false);
        if !b["tty"].is_null() && !b["tty"].is_boolean() {
            bail!("INVALID_REQUEST")
        }
        let peer = h.peers.lock().await.get(task).cloned();
        let peer = match peer {
            Some(p) if p.alive().await.unwrap_or(false) => p,
            _ => bail!("TERMINAL_UNAVAILABLE"),
        };
        {
            let s = h.store.lock().unwrap();
            if s.open_terminal_count(task)? >= TERMINAL_MAX_PER_TASK {
                bail!("TERMINAL_LIMIT")
            }
        }
        let tid = format!("term-{}", store::id());
        let process_id = tid.clone();
        {
            let mut terms = h.terminals.lock().unwrap();
            terms.entry(task.to_owned()).or_default().insert(
                process_id.clone(),
                TermLive {
                    buf: std::collections::VecDeque::new(),
                    base: 0,
                    capped: false,
                    _waiter: None,
                },
            );
        }
        {
            let s = h.store.lock().unwrap();
            // Re-verify inside the same lock that commits the row: a seal
            // landing between the snapshot check and this write must still win.
            if s.task(task)?["sealedFor"].is_object() {
                bail!("SOURCE_SEALED")
            }
            s.terminal_put(task, &tid, &process_id, &json!(argv), pty)?;
            s.event(
                task,
                "terminal.created",
                json!({"terminalId":tid,"processId":process_id,"argv":argv,"pty":pty,"cwd":t["workspace"]}),
            )?;
        }
        let params = json!({
            "command": argv,
            "processId": process_id,
            "cwd": t["workspace"],
            "tty": pty,
            "streamStdin": true,
            "streamStdoutStderr": true,
            "disableTimeout": true,
        });
        let h2 = h.clone();
        let task2 = task.to_owned();
        let tid2 = tid.clone();
        let pid2 = process_id.clone();
        let waiter = tokio::spawn(async move {
            let outcome = peer.rpc_deferred("command/exec", params).await;
            terminal_settle(&h2, &task2, &tid2, &pid2, outcome).await;
        });
        if let Some(live) = h
            .terminals
            .lock()
            .unwrap()
            .get_mut(task)
            .and_then(|m| m.get_mut(&process_id))
        {
            live._waiter = Some(waiter);
        }
        return Ok((
            200,
            json!({"schemaVersion":"worker-terminal/1","terminalId":tid,"state":"OPEN","pty":pty,"argv":argv,"cursor":"0"}),
        ));
    }
    let tid = valid_id(rest[0])?;
    let row = h.store.lock().unwrap().terminal(task, tid)?;
    let process_id = row["processId"].as_str().unwrap_or("").to_owned();
    match (method, rest.get(1).copied()) {
        ("GET", None) => {
            let mut v = row;
            v["schemaVersion"] = json!("worker-terminal/1");
            let end = h
                .terminals
                .lock()
                .unwrap()
                .get(task)
                .and_then(|m| m.get(&process_id))
                .map(|l| l.base + l.buf.len() as u64);
            v["cursor"] = json!(end.unwrap_or(0).to_string());
            Ok((200, v))
        }
        ("DELETE", None) => {
            if row["state"] == "OPEN" {
                if let Some(peer) = h.peers.lock().await.get(task).cloned() {
                    let _ = peer
                        .rpc(
                            "command/exec/terminate",
                            json!({"processId":process_id}),
                        )
                        .await;
                }
                let s = h.store.lock().unwrap();
                if s
                    .terminal_finish(task, tid, "TERMINATED", None, Some("DELETED"), None)
                    .unwrap_or(false)
                {
                    s.event(
                        task,
                        "terminal.closed",
                        json!({"terminalId":tid,"reason":"DELETED"}),
                    )?;
                }
            }
            h.terminals
                .lock()
                .unwrap()
                .get_mut(task)
                .and_then(|m| m.remove(&process_id));
            let row = h.store.lock().unwrap().terminal(task, tid)?;
            Ok((200, row))
        }
        ("POST", Some("input")) => {
            // stdin writes are mutation-class: sealed sources refuse them.
            ensure_not_sealed(t)?;
            if row["state"] != "OPEN" {
                bail!("TERMINAL_NOT_OPEN")
            }
            exact(&b, &["dataBase64", "text"])?;
            let bytes = if let Some(d) = b["dataBase64"].as_str() {
                STANDARD.decode(d).map_err(|_| anyhow!("INVALID_REQUEST"))?
            } else if let Some(s) = b["text"].as_str() {
                s.as_bytes().to_vec()
            } else {
                bail!("INVALID_REQUEST")
            };
            if bytes.is_empty() || bytes.len() > TERMINAL_OUTPUT_MAX as usize {
                bail!("INVALID_REQUEST")
            }
            let peer = h.peers.lock().await.get(task).cloned();
            let peer = match peer {
                Some(p) if p.alive().await.unwrap_or(false) => p,
                _ => bail!("TERMINAL_UNAVAILABLE"),
            };
            peer.rpc(
                "command/exec/write",
                json!({"processId":process_id,"deltaBase64":STANDARD.encode(&bytes)}),
            )
            .await?;
            let s = h.store.lock().unwrap();
            s.terminal_input(task, tid, bytes.len() as i64)?;
            s.event(
                task,
                "terminal.input",
                json!({"terminalId":tid,"bytes":bytes.len().to_string()}),
            )?;
            Ok((
                200,
                json!({"schemaVersion":"worker-terminal/1","terminalId":tid,"state":"APPLIED","bytesIn":bytes.len().to_string()}),
            ))
        }
        ("GET", Some("output")) => {
            if query.keys().any(|k| k != "cursor") {
                bail!("INVALID_QUERY")
            }
            let cursor: u64 = match query.get("cursor") {
                Some(c) => c.parse().map_err(|_| anyhow!("INVALID_QUERY"))?,
                None => 0,
            };
            let (data, base, end, capped) = {
                let terms = h.terminals.lock().unwrap();
                match terms.get(task).and_then(|m| m.get(&process_id)) {
                    Some(l) => {
                        let end = l.base + l.buf.len() as u64;
                        (l.buf.iter().cloned().collect::<Vec<u8>>(), l.base, end, l.capped)
                    }
                    None => (vec![], cursor, cursor, false),
                }
            };
            let truncated = cursor < base;
            let start = cursor.max(base) as usize - base as usize;
            let slice = if start < data.len() {
                &data[start..]
            } else {
                &[][..]
            };
            Ok((
                200,
                json!({
                    "schemaVersion":"worker-terminal/1",
                    "terminalId":tid,
                    "state":row["state"],
                    "dataBase64":STANDARD.encode(slice),
                    "cursor":cursor.to_string(),
                    "nextCursor":end.to_string(),
                    "truncated":truncated,
                    "ringCapped":capped,
                }),
            ))
        }
        ("POST", Some("signal")) => {
            if row["state"] != "OPEN" {
                bail!("TERMINAL_NOT_OPEN")
            }
            exact(&b, &["signal"])?;
            // The pinned surface exposes terminate only; a POSIX-style signal
            // vocabulary would be a false claim.
            if b["signal"].as_str() != Some("terminate") {
                bail!("TERMINAL_SIGNAL_UNSUPPORTED")
            }
            let peer = h.peers.lock().await.get(task).cloned();
            let peer = match peer {
                Some(p) if p.alive().await.unwrap_or(false) => p,
                _ => bail!("TERMINAL_UNAVAILABLE"),
            };
            peer.rpc(
                "command/exec/terminate",
                json!({"processId":process_id}),
            )
            .await?;
            let s = h.store.lock().unwrap();
            s.event(
                task,
                "terminal.signal",
                json!({"terminalId":tid,"signal":"terminate"}),
            )?;
            Ok((
                200,
                json!({"schemaVersion":"worker-terminal/1","terminalId":tid,"state":"SIGNALLED"}),
            ))
        }
        _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
    }
}
// ---------------------------------------------------------------------------
// W5a — task-scoped context packages, auxiliary sessions and assignment
// transitions (P11/P12/P13, N16/N17/N18). Bounds are deliberate and honest:
// package source bytes are frozen into SQLite at creation, aux sessions run a
// separate peer on an isolated workspace copy under a read-only sandbox, and
// transitions only admit a new owner on the SAME engine + SAME native thread.
const PACKAGE_MAX_SOURCES: usize = 20;
const PACKAGE_MAX_FILES: usize = 20;
const PACKAGE_MAX_FILE_BYTES: usize = 256 * 1024;
const PACKAGE_MAX_TOTAL_BYTES: usize = 1024 * 1024;
// Pi PACKAGE_LIMIT parity: 32 durable packages per task.
const PACKAGE_MAX_PER_TASK: i64 = 32;
const PACKAGE_MAX_TEXT_BYTES: usize = 64 * 1024;
const AUX_MAX_PER_TASK: i64 = 4;
const AUX_MAX_PACKAGES: usize = 8;
const AUX_MAX_QUESTION: usize = 16384;
// Pi bounds: the snapshot copy is bounded to 4096 files / 32 MiB, the durable
// answer to 64 KiB and the inlined fixed material to 128 KiB.
const AUX_COPY_MAX_BYTES: u64 = 32 * 1024 * 1024;
const AUX_COPY_MAX_FILES: u64 = 4096;
const AUX_ANSWER_MAX: usize = 64 * 1024;
const AUX_INLINE_MAX: usize = 128 * 1024;
// An auxiliary session runs at most ten minutes from acceptance; the durable
// deadline survives the host so a sweep can cancel it even after a restart.
const AUX_DEADLINE_MS: u64 = 10 * 60 * 1000;
// A file source must resolve inside the task workspace using the same lexical +
// realpath discipline as writeScope: the path is normalized, canonicalized
// (symlinks resolved) and required to stay under the canonical workspace.
fn package_file_bytes(ws_real: &Path, deny: &[String], given: &str) -> Result<(String, Vec<u8>)> {
    let raw = Path::new(given);
    let candidate = if raw.is_absolute() {
        lexical(raw)
    } else {
        lexical(&ws_real.join(raw))
    };
    // Best-effort realpath: an existing path resolves through symlinks; a
    // missing file keeps its lexical form so scope checks still decide first.
    let real = canonical_path(&candidate).unwrap_or_else(|_| candidate.clone());
    if !real.starts_with(ws_real) {
        bail!("PACKAGE_SOURCE_OUT_OF_SCOPE")
    }
    let rel = real
        .strip_prefix(ws_real)
        .map(|p| p.to_string_lossy().replace('\\', "/"))
        .unwrap_or_default();
    // Workspace-relative prefix match against the declared deny paths — the
    // same component-prefix rule the writeScope check applies.
    if rel.is_empty()
        || deny.iter().any(|d| {
            let d = d.trim_end_matches('/');
            !d.is_empty() && (rel == d || rel.starts_with(&format!("{d}/")))
        })
    {
        bail!("PACKAGE_SOURCE_OUT_OF_SCOPE")
    }
    if !real.is_file() {
        bail!("PACKAGE_SOURCE_UNAVAILABLE")
    }
    let bytes = fs::read(&real).map_err(|_| anyhow!("PACKAGE_SOURCE_UNAVAILABLE"))?;
    if bytes.len() > PACKAGE_MAX_FILE_BYTES {
        bail!("PACKAGE_SOURCE_TOO_LARGE")
    }
    Ok((rel, bytes))
}
// Frozen package metadata: per-source {kind,path?,sha256,size} plus the frozen
// bytes, bounded per file and in total. The returned digest is sha256 over the
// canonical metadata list — re-verifiable from the recorded hashes alone.
fn package_sources(
    v: &Value,
    ws_real: &Path,
    deny: &[String],
) -> Result<(Vec<Value>, Vec<Vec<u8>>, i64, String)> {
    let a = v
        .as_array()
        .ok_or_else(|| anyhow!("INVALID_PACKAGE_SOURCES"))?;
    if a.is_empty() || a.len() > PACKAGE_MAX_SOURCES {
        bail!("INVALID_PACKAGE_SOURCES")
    }
    let mut metas = vec![];
    let mut blobs = vec![];
    let mut total = 0usize;
    let mut files = 0usize;
    for s in a {
        if !s.is_object() {
            bail!("INVALID_PACKAGE_SOURCES")
        }
        let (meta, bytes) = match s["kind"].as_str().unwrap_or("") {
            "file" => {
                exact(s, &["kind", "path"])?;
                files += 1;
                if files > PACKAGE_MAX_FILES {
                    bail!("PACKAGE_SOURCE_TOO_LARGE")
                }
                let given = s["path"]
                    .as_str()
                    .filter(|p| !p.trim().is_empty() && p.len() <= 1024 && !p.contains('\0'))
                    .ok_or_else(|| anyhow!("INVALID_PACKAGE_SOURCES"))?;
                let (rel, bytes) = package_file_bytes(ws_real, deny, given)?;
                if total + bytes.len() > PACKAGE_MAX_TOTAL_BYTES {
                    bail!("PACKAGE_SOURCE_TOO_LARGE")
                }
                total += bytes.len();
                (
                    json!({"kind":"file","path":rel,"sha256":hash(&bytes),"size":bytes.len()}),
                    bytes,
                )
            }
            "text" | "summary" => {
                exact(s, &["kind", "text"])?;
                let text = s["text"]
                    .as_str()
                    .ok_or_else(|| anyhow!("INVALID_PACKAGE_SOURCES"))?;
                if text.as_bytes().len() > PACKAGE_MAX_TEXT_BYTES
                    || total + text.as_bytes().len() > PACKAGE_MAX_TOTAL_BYTES
                {
                    bail!("PACKAGE_SOURCE_TOO_LARGE")
                }
                total += text.as_bytes().len();
                let bytes = text.as_bytes().to_vec();
                (
                    json!({"kind":s["kind"],"sha256":hash(&bytes),"size":bytes.len()}),
                    bytes,
                )
            }
            _ => bail!("INVALID_PACKAGE_SOURCES"),
        };
        metas.push(meta);
        blobs.push(bytes);
    }
    let digest = hash(store::canonical(&json!({"sources":metas}))?);
    Ok((metas, blobs, total as i64, digest))
}
async fn package_route(
    h: &Shared,
    task: &str,
    t: &Value,
    rest: &[&str],
    method: &str,
    b: Value,
) -> Result<(u16, Value)> {
    match (method, rest.len()) {
        ("GET", 0) => Ok((
            200,
            json!({"items":h.store.lock().unwrap().context_packages(task)?,"coverage":"FROZEN_AT_CREATION"}),
        )),
        ("GET", 1) => {
            let pid = valid_id(rest[0])?;
            Ok((
                200,
                h.store.lock().unwrap().context_package(task, pid)?,
            ))
        }
        ("POST", 0) => {
            exact(&b, &["requestKey", "name", "sources"])?;
            ensure_not_sealed(t)?;
            let key = valid_id(string(&b, "requestKey")?)?.to_owned();
            let name = if b["name"].is_null() {
                None
            } else {
                let n = b["name"]
                    .as_str()
                    .filter(|s| {
                        !s.trim().is_empty()
                            && s.len() <= 128
                            && !s.chars().any(|c| (c as u32) < 0x20)
                    })
                    .ok_or_else(|| anyhow!("INVALID_PACKAGE_NAME"))?;
                Some(n.to_owned())
            };
            {
                let s = h.store.lock().unwrap();
                if let Some(v) = s.existing(task, &key, &b)? {
                    return Ok((200, v));
                }
                if s.context_package_count(task)? >= PACKAGE_MAX_PER_TASK {
                    bail!("PACKAGE_LIMIT")
                }
            }
            // The workspace realpath must resolve before any source freezes —
            // a task without a materialized workspace cannot host packages.
            let ws_real = canonical_path(Path::new(t["workspace"].as_str().unwrap_or("")))
                .map_err(|_| anyhow!("PACKAGE_WORKSPACE_UNAVAILABLE"))?;
            let deny: Vec<String> = t["writeScope"]["denyPaths"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect())
                .unwrap_or_default();
            let (metas, blobs, total, digest) =
                package_sources(&b["sources"], &ws_real, &deny)?;
            let package = json!({
                "packageId": format!("pkg-{}", store::id()),
                "name": name,
                "digest": digest,
                "size": total,
                "sourceCount": metas.len(),
                "fileCount": metas.iter().filter(|m| m["kind"] == "file").count(),
                "sources": metas,
                "createdAt": store::now(),
            });
            let mut s = h.store.lock().unwrap();
            s.accept_context_package(
                task,
                &key,
                &b,
                &package,
                &metas,
                &blobs,
                &package,
            )?;
            Ok((200, package))
        }
        _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
    }
}
// Bounded recursive snapshot of the task workspace into the aux session's own
// directory (Pi parity): .git directories are skipped so an aux-side git
// command can never touch the main object store, symlinks are skipped rather
// than materialized outside the copy, and the copy is bounded by file count
// and total bytes — an exceeded bound is AUX_SNAPSHOT_TOO_LARGE.
fn copy_snapshot(src: &Path, dst: &Path, budget: &mut (u64, u64)) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_name() == ".git" {
            continue;
        }
        let path = entry.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            continue;
        }
        let target = dst.join(entry.file_name());
        if meta.is_dir() {
            copy_snapshot(&path, &target, budget)?;
        } else if meta.is_file() {
            budget.0 += 1;
            budget.1 += meta.len();
            if budget.0 > AUX_COPY_MAX_FILES || budget.1 > AUX_COPY_MAX_BYTES {
                bail!("AUX_SNAPSHOT_TOO_LARGE")
            }
            fs::copy(&path, &target)?;
        }
    }
    Ok(())
}
// The aux research prompt: the question plus the frozen context package
// contents inlined, bounded so the synthesized instruction always fits the
// adapter frame. Package text that would exceed the bound is cut with an
// explicit marker — never silently dropped.
fn aux_prompt(question: &str, packages: &[Value]) -> Result<String> {
    let mut text = format!(
        "You are a bounded read-only research session for an implementation task. Answer the question using the frozen context below and by reading files inside this workspace copy. Do not modify files, do not run mutating commands, do not read credentials. Report findings concisely.\n\nQuestion:\n{}\n",
        question
    );
    for package in packages {
        text.push_str(&format!(
            "\n--- context package {} ({}) ---\n",
            package["packageId"].as_str().unwrap_or(""),
            package["digest"].as_str().unwrap_or("")
        ));
        if let Some(sources) = package["sources"].as_array() {
            for src in sources {
                // File sources carry base64 bytes; text/summary carry text.
                // Pi parity: each source inlines at most 32KiB.
                let decoded = if src["kind"].as_str() == Some("file") {
                    STANDARD
                        .decode(src["content"].as_str().unwrap_or(""))
                        .unwrap_or_default()
                } else {
                    src["text"].as_str().unwrap_or("").as_bytes().to_vec()
                };
                let body: String = String::from_utf8_lossy(&decoded)
                    .chars()
                    .take(32768)
                    .collect();
                let header = format!(
                    "[source {} {} sha256:{} size:{}]\n",
                    src["kind"].as_str().unwrap_or(""),
                    src["path"].as_str().map(|p| format!("path={p} ")).unwrap_or_default(),
                    src["sha256"].as_str().unwrap_or(""),
                    src["size"].as_str().unwrap_or("")
                );
                let remaining = AUX_INLINE_MAX.saturating_sub(text.len());
                if body.len() + header.len() > remaining {
                    text.push_str(&header);
                    text.push_str("[context source omitted: inline budget exhausted]\n");
                    break;
                }
                text.push_str(&header);
                text.push_str(&body);
                text.push('\n');
            }
        }
        if text.len() >= AUX_INLINE_MAX {
            break;
        }
    }
    Ok(text)
}
async fn aux_route(
    h: Shared,
    task: &str,
    t: &Value,
    rest: &[&str],
    method: &str,
    b: Value,
    query: &HashMap<String, String>,
) -> Result<(u16, Value)> {
    match (method, rest.len()) {
        ("GET", 0) => Ok((
            200,
            json!({"items":h.store.lock().unwrap().aux_sessions(task)?,"coverage":"ISOLATED_SNAPSHOT_READ_ONLY"}),
        )),
        ("GET", 1) => Ok((
            200,
            h.store.lock().unwrap().aux_session(task, valid_id(rest[0])?)?,
        )),
        ("POST", 0) => aux_create(h, task, t, &b).await,
        ("POST", 2) => {
            let aid = valid_id(rest[0])?;
            match rest[1] {
                "stop" => aux_stop(h, task, aid, &b).await,
                "deliver" => aux_deliver(h, task, aid, &b).await,
                _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
            }
        }
        // C2: per-stream durable public message stream (same GET shape as the
        // collaboration-child route — reads never take a body).
        ("GET", 2) if rest[1] == "messages" => {
            let aid = valid_id(rest[0])?;
            {
                let s = h.store.lock().unwrap();
                s.aux_session(task, aid)?; // AUX_SESSION_NOT_FOUND
            }
            child_messages_page(&h, task, None, aid, query)
        }
        _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
    }
}
// child-messages/1 page reader (C2): resolves coverage from the stream meta —
// a captured stream is FULL_SINCE_CAPTURE (PARTIAL when page items carry
// truncated bodies); an owning row that predates capture degrades honestly to
// SUMMARY_ONLY with the cumulative text attached, never a fabricated
// timeline. `streamId` is the immutable stream ref.
fn child_messages_page(
    h: &Shared,
    task: &str,
    collab: Option<&str>,
    child: &str,
    query: &HashMap<String, String>,
) -> Result<(u16, Value)> {
    let (cursor, before, limit) = message_page_query(query)?;
    let (mut page, legacy) = {
        let s = h.store.lock().unwrap();
        let page = s.child_messages(task, collab, child, cursor, before, limit)?;
        let legacy = if page["hasMeta"] == true {
            Value::Null
        } else {
            // No captured stream: the owning row's cumulative output is the
            // honest legacy summary when one exists.
            let text = match collab {
                Some(cid) => s
                    .collab_child(task, cid, child)
                    .map(|c| c["output"].as_str().unwrap_or("").to_owned())
                    .unwrap_or_default(),
                None => s
                    .aux_session(task, child)
                    .map(|a| a["answer"].as_str().unwrap_or("").to_owned())
                    .unwrap_or_default(),
            };
            if text.is_empty() {
                Value::Null
            } else {
                json!({"text":text,"source":"CUMULATIVE_OUTPUT","truncated":false})
            }
        };
        (page, legacy)
    };
    let captured = page["hasMeta"] == true;
    page["schemaVersion"] = json!("child-messages/1");
    page["parentTaskId"] = json!(task);
    page["collaborationId"] = json!(collab);
    page["childId"] = json!(child);
    // GAP-07 convergence: the canonical token is SUMMARY_ONLY (design enum +
    // Pi host); `legacy` keeps the provenance object while `summaryText`
    // mirrors the Pi carrier shape so consumers read one field. A captured
    // page containing truncated item bodies reports PARTIAL — the flag was
    // always in the row, it just never reached coverage.
    let degraded = page["items"]
        .as_array()
        .map(|items| items.iter().any(|i| i["textTruncated"] == true))
        .unwrap_or(false);
    page["coverage"] = json!(if captured && degraded {
        "PARTIAL"
    } else if captured {
        "FULL_SINCE_CAPTURE"
    } else if legacy.is_object() {
        "SUMMARY_ONLY"
    } else {
        "UNAVAILABLE"
    });
    if legacy.is_object() {
        page["summaryText"] = legacy["text"].clone();
        page["legacy"] = legacy;
    }
    page.as_object_mut().unwrap().remove("hasMeta");
    Ok((200, page))
}
// Aux-session states the host may still act on; terminal facts are immutable.
// Pi vocabulary: the only open state is RUNNING — a session row exists as
// RUNNING from acceptance until a terminal SUCCEEDED/FAILED/CANCELLED lands.
const AUX_OPEN: &[&str] = &["RUNNING"];
async fn aux_create(h: Shared, task: &str, t: &Value, b: &Value) -> Result<(u16, Value)> {
    exact(b, &["requestKey", "question", "contextPackageIds", "budget"])?;
    ensure_not_sealed(t)?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    // Pi intake vocabulary: question and package-list failures get aux-specific
    // codes; the budget object mirrors the Pi SOFT-cap shape.
    let question = b["question"]
        .as_str()
        .filter(|q| !q.trim().is_empty() && q.len() <= AUX_MAX_QUESTION)
        .ok_or_else(|| anyhow!("INVALID_AUX_QUESTION"))?
        .to_owned();
    let mut packages = vec![];
    if !b["contextPackageIds"].is_null() {
        let a = b["contextPackageIds"]
            .as_array()
            .ok_or_else(|| anyhow!("INVALID_AUX_PACKAGES"))?;
        if a.len() > AUX_MAX_PACKAGES
            || a.iter()
                .any(|v| v.as_str().map(|s| s.is_empty()).unwrap_or(true))
        {
            bail!("INVALID_AUX_PACKAGES")
        }
        for pid in a {
            packages.push(pid.as_str().unwrap_or("").to_owned());
        }
    }
    let budget = if b["budget"].is_null() {
        None
    } else {
        let spec = &b["budget"];
        if !spec.is_object() {
            bail!("INVALID_BUDGET")
        }
        exact(spec, &["maxTotalTokens", "enforcement"])?;
        if !spec["enforcement"].is_null() && spec["enforcement"] != "SOFT" {
            bail!("BUDGET_ENFORCEMENT_UNSUPPORTED")
        }
        let Some(cap) = decimal(&spec["maxTotalTokens"])? else {
            bail!("INVALID_BUDGET")
        };
        if cap < 0 {
            bail!("INVALID_BUDGET")
        }
        Some(cap)
    };
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
        if s.aux_count(task)? >= AUX_MAX_PER_TASK {
            bail!("AUX_SESSION_LIMIT")
        }
        // Host-wide live-aux bound: AUX_SESSION_BUSY mirrors the Pi
        // NWF_PI_MAX_ACTIVE_AUX admission refusal.
        if s.aux_active_count()? as usize >= h.max_active_aux {
            bail!("AUX_SESSION_BUSY")
        }
        // Named packages must already exist — their frozen bytes are inlined at
        // spawn time, so a missing id is an intake refusal, not a spawn failure.
        for pid in &packages {
            s.context_package(task, pid)?;
        }
    }
    match t["state"].as_str() {
        Some("BUDGET_EXCEEDED") => bail!("BUDGET_EXCEEDED"),
        Some("BINDING_UNAVAILABLE" | "REJECTED" | "STOP_REQUESTED" | "UNKNOWN") => {
            bail!("NATIVE_NOT_IDLE")
        }
        _ => {}
    }
    // Aux sessions run real model turns on a separate managed peer.
    require_model_configuration(&h)?;
    verify(&h.root, &h.registry)?;
    // The snapshot needs a materialized workspace to copy; a queued/preparing
    // task has none yet.
    let ws = PathBuf::from(t["workspace"].as_str().unwrap_or(""));
    if !ws.is_dir() {
        bail!("AUX_WORKSPACE_UNAVAILABLE")
    }
    let aux_id = format!("aux-{}", store::id());
    let aux_ws = h
        .root
        .join("auxiliary")
        .join(task)
        .join(&aux_id)
        .join("workspace");
    // Frozen copy of the task workspace taken BEFORE the session row is
    // accepted: a snapshot that cannot be produced refuses intake without
    // leaving a phantom session behind (Pi parity).
    let mut copy_bound = (0u64, 0u64);
    if let Err(e) = copy_snapshot(&ws, &aux_ws, &mut copy_bound) {
        let code = safe_code(&e);
        if code == "AUX_SNAPSHOT_TOO_LARGE" {
            bail!("AUX_SNAPSHOT_TOO_LARGE")
        }
        bail!("AUX_SNAPSHOT_FAILED")
    }
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64 + AUX_DEADLINE_MS)
        .unwrap_or(0)
        .to_string();
    let aux = json!({
        "auxSessionId": aux_id,
        "question": question,
        "contextPackageIds": packages,
        "budget": budget.map(|b| json!({"caps":{"maxTotalTokens":b.to_string()},"enforcement":"SOFT","basis":"ENGINE_REPORTED_POST_HOC","costCoverage":"UNKNOWN"})),
        "workspace": aux_ws,
        "deadlineEpochMillis": deadline,
        "createdAt": store::now(),
    });
    {
        let mut s = h.store.lock().unwrap();
        s.accept_aux(task, &aux, &key, b, &aux)?;
        // C2: the durable ASSIGNMENT fact — the question as admitted, bound to
        // this stream before any native traffic exists.
        let _ = s.child_message(
            task,
            None,
            &aux_id,
            &store::ChildMsg {
                kind: "ASSIGNMENT",
                text: &question,
                status: "PENDING",
                event_ref: None,
            },
        );
        s.receipt(
            task,
            &key,
            "DISPATCHING",
            &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"DISPATCHING","auxSessionId":aux_id}),
        )?;
        s.event(
            task,
            "aux-session.started",
            json!({"auxSessionId":aux_id,"contextPackageIds":aux["contextPackageIds"],"budget":aux["budget"]}),
        )?;
    }
    let out = h.store.lock().unwrap().aux_session(task, &aux_id)?;
    let h2 = h.clone();
    let task2 = task.to_owned();
    let aid2 = aux_id.clone();
    let package_ids = packages.clone();
    tokio::spawn(async move {
        if let Err(e) = aux_spawn(h2.clone(), &task2, &aid2, package_ids).await {
            aux_spawn_failed(&h2, &task2, &aid2, &e).await;
        }
    });
    Ok((202, out))
}
// Aux-session driver: snapshot the workspace, spawn a separate managed peer on
// its own CODEX_HOME, start a read-only thread and dispatch one bounded turn.
// Every step rechecks the durable row so a stop or a peer loss can never be
// resurrected by a later spawn step.
async fn aux_spawn(h: Shared, task: &str, aux_id: &str, package_ids: Vec<String>) -> Result<()> {
    let preparing = h.preparation.lock().await;
    let (t, aux) = {
        let s = h.store.lock().unwrap();
        (s.task(task)?, s.aux_session(task, aux_id)?)
    };
    let aux_root = h.root.join("auxiliary").join(task).join(aux_id);
    // The workspace snapshot was already copied at intake (aux_create); the
    // spawn driver only builds the isolated managed home on top of it.
    let aux_ws = aux_root.join("workspace");
    let home = aux_root.join("home");
    fs::create_dir_all(&home)?;
    // Same digest-pinned managed profile as the task home; task-bound MCP
    // servers, skills and hooks are deliberately NOT replicated — a read-only
    // researcher binds no task extensions.
    fs::write(
        home.join("config.toml"),
        fs::read_to_string(h.root.join("profile/config.toml"))?,
    )?;
    let (peer, events) = Peer::spawn(
        &h.registry.executable,
        &home,
        &aux_ws,
        &env_allowlist(&t),
        bounded_profile(&h.registry.tool_profile),
    )
    .await?;
    {
        let mut peers = h.aux_peers.lock().await;
        peers
            .entry(task.to_owned())
            .or_default()
            .insert(aux_id.to_owned(), peer.clone());
    }
    drop(preparing);
    // If a stop landed while the peer was spawning, kill it now instead of
    // dispatching a turn the durable record already refuses.
    if {
        let s = h.store.lock().unwrap();
        s.aux_session(task, aux_id)?["state"] != "RUNNING"
    } {
        let _ = peer.stop().await;
        h.aux_peers
            .lock()
            .await
            .get_mut(task)
            .and_then(|m| m.remove(aux_id));
        bail!("AUX_SESSION_CLOSED")
    }
    aux_watch(&h, task, aux_id, events);
    peer.initialize_with_experimental(false).await?;
    let result = peer
        .rpc(
            "thread/start",
            json!({"model":h.registry.model,"modelProvider":"nwf","cwd":aux_ws,"approvalPolicy":"never","sandbox":"read-only","baseInstructions":"You are a bounded read-only research assistant. Answer the question using the provided frozen context and by reading files in this workspace copy. Do not modify files. Do not read credentials or environment secrets. Report findings concisely.","config":{"features.multi_agent":false}}),
        )
        .await?;
    let thread = string(&result["thread"], "id")?.to_owned();
    if string(&result, "model")? != h.registry.model || string(&result, "modelProvider")? != "nwf" {
        bail!("MODEL_BINDING_MISMATCH")
    }
    {
        let s = h.store.lock().unwrap();
        // Still open -> record the bound native session; a stop or failure
        // that won the race makes this driver bail instead of resurrecting.
        if !s.aux_transition(task, aux_id, AUX_OPEN, "RUNNING", None, |r| {
            r["nativeSessionId"] = json!(thread)
        })? {
            bail!("AUX_SESSION_CLOSED")
        }
        let _ = s.child_message(
            task,
            None,
            aux_id,
            &store::ChildMsg {
                kind: "STATUS",
                text: "RUNNING: native thread bound",
                status: "RUNNING",
                event_ref: Some(&thread),
            },
        );
        s.event(
            task,
            "aux-session.bound",
            json!({"auxSessionId":aux_id,"nativeSessionId":thread}),
        )?;
    }
    // The frozen context is inlined into the single aux instruction; packages
    // named at intake read back their frozen bytes from the durable store.
    let packages = {
        let s = h.store.lock().unwrap();
        let mut out = vec![];
        for pid in &package_ids {
            out.push(s.context_package(task, pid)?);
        }
        out
    };
    let prompt = aux_prompt(aux["question"].as_str().unwrap_or(""), &packages)?;
    let result = peer
        .rpc(
            "turn/start",
            json!({"threadId":thread,"model":h.registry.model,"input":[{"type":"text","text":prompt}],"clientUserMessageId":aux["requestKey"]}),
        )
        .await?;
    let turn = string(&result["turn"], "id")?.to_owned();
    {
        let mut s = h.store.lock().unwrap();
        s.aux_transition(task, aux_id, AUX_OPEN, "RUNNING", None, |r| {
            r["nativeTurnId"] = json!(turn)
        })?;
        let _ = s.child_message(
            task,
            None,
            aux_id,
            &store::ChildMsg {
                kind: "USER_INPUT",
                text: &prompt,
                status: "RUNNING",
                event_ref: Some(&turn),
            },
        );
        s.receipt(
            task,
            aux["requestKey"].as_str().unwrap_or(""),
            "APPLIED",
            &json!({"kind":"WORKER_LOCAL","requestKey":aux["requestKey"],"state":"APPLIED","detail":"Auxiliary turn bound","auxSessionId":aux_id,"nativeTurnId":turn}),
        )?;
    }
    Ok(())
}
// Aux spawn failure: the durable row becomes FAILED honestly, the peer is
// never left running unattested, and the intake receipt reports UNKNOWN. A row
// that already went terminal (stop/deadline won the race) is never rewritten.
async fn aux_spawn_failed(h: &Shared, task: &str, aux_id: &str, e: &anyhow::Error) {
    let code = safe_code(e);
    if let Some(peer) = h
        .aux_peers
        .lock()
        .await
        .get(task)
        .and_then(|m| m.get(aux_id))
        .cloned()
    {
        let _ = peer.stop().await;
    }
    {
        let mut s = h.store.lock().unwrap();
        let marked = s
            .aux_transition(task, aux_id, AUX_OPEN, "FAILED", Some(&code), |_| {})
            .unwrap_or(false);
        if marked {
            let text = format!("FAILED: {code}");
            let _ = s.child_message(
                task,
                None,
                aux_id,
                &store::ChildMsg {
                    kind: "STATUS",
                    text: &text,
                    status: "FAILED",
                    event_ref: None,
                },
            );
            let _ = s.event(
                task,
                "aux-session.completed",
                json!({"auxSessionId":aux_id,"state":"FAILED","reasonCode":code}),
            );
            if let Ok(aux) = s.aux_session(task, aux_id) {
                let _ = s.receipt(
                    task,
                    aux["requestKey"].as_str().unwrap_or(""),
                    "UNKNOWN",
                    &json!({"kind":"WORKER_LOCAL","requestKey":aux["requestKey"],"state":"UNKNOWN","reasonCode":code}),
                );
            }
        }
    }
    h.aux_peers
        .lock()
        .await
        .get_mut(task)
        .and_then(|m| m.remove(aux_id));
}
fn aux_watch(h: &Shared, task: &str, aux_id: &str, mut events: tokio::sync::mpsc::Receiver<Value>) {
    let h2 = h.clone();
    let task2 = task.to_owned();
    let aid = aux_id.to_owned();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Err(e) = aux_event(&h2, &task2, &aid, event).await {
                let code = safe_code(&e);
                {
                    let s = h2.store.lock().unwrap();
                    if s
                        .aux_transition(&task2, &aid, AUX_OPEN, "FAILED", Some(&code), |_| {})
                        .unwrap_or(false)
                    {
                        let _ = s.event(
                            &task2,
                            "aux-session.completed",
                            json!({"auxSessionId":aid,"state":"FAILED","reason":code}),
                        );
                    }
                }
                if let Some(peer) = h2
                    .aux_peers
                    .lock()
                    .await
                    .get(&task2)
                    .and_then(|m| m.get(&aid))
                    .cloned()
                {
                    let _ = peer.stop().await;
                }
                break;
            }
        }
    });
}
// Native events for an aux peer: they only ever touch the aux row and its own
// event facts — the main task record, its input queue and its budget are never
// written by this path (N17 isolation by construction).
async fn aux_event(h: &Shared, task: &str, aux_id: &str, event: Value) -> Result<()> {
    let method = string(&event, "method")?;
    let p = &event["params"];
    if event.get("id").is_some() {
        // A read-only approval-free researcher should never raise native
        // questions; anything that still arrives is refused, not answered.
        if let Some(peer) = h
            .aux_peers
            .lock()
            .await
            .get(task)
            .and_then(|m| m.get(aux_id))
            .cloned()
        {
            peer.send(&json!({"id":event["id"],"error":{"code":-32601,"message":"AUX_INPUT_UNSUPPORTED"}}))
                .await?;
        }
        return Ok(());
    }
    match method {
        "adapter/failure" => bail!(adapter::failure_code(&event)),
        "adapter/exited" => {
            {
                let s = h.store.lock().unwrap();
                // A peer that exited before its completion fact arrived leaves
                // the aux FAILED with the honest unconfirmed-exit reason.
                if s.aux_transition(task, aux_id, AUX_OPEN, "FAILED", Some("AUX_PROCESS_EXITED_UNCONFIRMED"), |_| {})? {
                    let _ = s.child_message(
                        task,
                        None,
                        aux_id,
                        &store::ChildMsg {
                            kind: "STATUS",
                            text: "FAILED: process exited before a completion fact arrived",
                            status: "FAILED",
                            event_ref: None,
                        },
                    );
                    s.event(
                        task,
                        "aux-session.completed",
                        json!({"auxSessionId":aux_id,"state":"FAILED","reasonCode":"AUX_PROCESS_EXITED_UNCONFIRMED"}),
                    )?;
                }
            }
            h.aux_peers
                .lock()
                .await
                .get_mut(task)
                .and_then(|m| m.remove(aux_id));
        }
        "item/started" => {
            // C2 TOOL_CALL: the item type + id only — command text and tool
            // arguments are never public stream facts.
            let kind = p["item"]["type"].as_str().unwrap_or("");
            if adapter::EFFECT_KINDS.contains(&kind) {
                let s = h.store.lock().unwrap();
                let text = format!("tool started: {kind}");
                let _ = s.child_message(
                    task,
                    None,
                    aux_id,
                    &store::ChildMsg {
                        kind: "TOOL_CALL",
                        text: &text,
                        status: "RUNNING",
                        event_ref: p["item"]["id"].as_str(),
                    },
                );
            }
        }
        "item/completed" => {
            let item = &p["item"];
            if item["type"].as_str() == Some("agentMessage") {
                if let Some(text) = item["text"].as_str() {
                    let s = h.store.lock().unwrap();
                    s.aux_transition(task, aux_id, AUX_OPEN, "RUNNING", None, |r| {
                        let mut answer = r["answer"].as_str().unwrap_or("").to_owned();
                        if !answer.is_empty() {
                            answer.push('\n')
                        }
                        answer.push_str(text);
                        if answer.len() > AUX_ANSWER_MAX {
                            answer.truncate(AUX_ANSWER_MAX);
                            while !answer.is_char_boundary(answer.len()) {
                                answer.pop();
                            }
                        }
                        r["answer"] = json!(answer);
                    })?;
                    let _ = s.child_message(
                        task,
                        None,
                        aux_id,
                        &store::ChildMsg {
                            kind: "ASSISTANT_MESSAGE",
                            text,
                            status: "RUNNING",
                            event_ref: item["id"].as_str(),
                        },
                    );
                }
            } else if let Some(kind) = item["type"].as_str() {
                // Non-message completed items are TOOL_RESULT facts; the text
                // carries only the item type, never raw output.
                let text = format!("tool completed: {kind}");
                let s = h.store.lock().unwrap();
                let _ = s.child_message(
                    task,
                    None,
                    aux_id,
                    &store::ChildMsg {
                        kind: "TOOL_RESULT",
                        text: &text,
                        status: "RUNNING",
                        event_ref: item["id"].as_str(),
                    },
                );
            }
        }
        "turn/completed" => {
            let status = string(&p["turn"], "status")?;
            let (mut state, mut reason) = match status {
                "completed" => ("SUCCEEDED", None),
                "failed" => ("FAILED", Some("TURN_FAILED")),
                _ => ("CANCELLED", Some("INTERRUPTED")),
            };
            {
                let mut s = h.store.lock().unwrap();
                // Final accounting boundary, same rule as the parent budget: a
                // cap the observed usage already crossed settles
                // BUDGET_EXCEEDED even when the turn's own completion fact
                // reaches the durable row before aux_budget_stop's mark does.
                // Usage is taken from the row's recorded totals plus any
                // cumulative total the completion notification itself carries.
                let mut return_text = String::new();
                if let Ok(aux) = s.aux_session(task, aux_id) {
                    return_text = aux["answer"].as_str().unwrap_or("").to_owned();
                    let cap = aux["budget"]["maxTotalTokens"]
                        .as_str()
                        .and_then(|v| v.parse::<u64>().ok());
                    let observed = [
                        &aux["usage"]["totalTokens"],
                        &p["tokenUsage"]["total"]["totalTokens"],
                    ]
                    .iter()
                    .filter_map(|v| {
                        v.as_str()
                            .and_then(|s| s.parse::<u64>().ok())
                            .or_else(|| v.as_u64())
                    })
                    .max()
                    .unwrap_or(0);
                    if cap.is_some_and(|c| observed >= c) {
                        // Pi parity: a soft-budget breach settles FAILED with
                        // BUDGET_EXCEEDED as the reasonCode, never its own state.
                        state = "FAILED";
                        reason = Some("BUDGET_EXCEEDED");
                    }
                }
                // C2 terminal fact commits INSIDE the state transition: the
                // durable row and its RETURN/STATUS stream entry atomically
                // agree or neither lands.
                let status_text = format!("{}: {}", state, reason.unwrap_or(""));
                let msgs = [store::ChildMsg {
                    kind: if state == "SUCCEEDED" { "RETURN" } else { "STATUS" },
                    text: if state == "SUCCEEDED" {
                        &return_text
                    } else {
                        &status_text
                    },
                    status: state,
                    event_ref: None,
                }];
                if s.aux_transition_msgs(task, aux_id, AUX_OPEN, state, reason, |_| {}, &msgs)? {
                    s.event(
                        task,
                        "aux-session.completed",
                        json!({"auxSessionId":aux_id,"state":state,"reasonCode":reason}),
                    )?;
                }
            }
            // The answer is durable; the research peer is released instead of
            // holding an idle managed process.
            if let Some(peer) = h
                .aux_peers
                .lock()
                .await
                .get(task)
                .and_then(|m| m.get(aux_id))
                .cloned()
            {
                let _ = peer.stop().await;
            }
            h.aux_peers
                .lock()
                .await
                .get_mut(task)
                .and_then(|m| m.remove(aux_id));
        }
        "thread/tokenUsage/updated" => {
            // Independent aux ledger: cumulative counters on the aux row only;
            // the parent task's usageTotals and budget row are untouched. A
            // usage report can arrive before OR after its turn's completion
            // notification, so the accounting and the cap check run in both
            // orderings: RUNNING sessions are interrupted mid-turn, and a
            // completed/stopped row whose late report crossed the cap still
            // settles BUDGET_EXCEEDED at this final accounting boundary.
            let usage = &p["tokenUsage"]["total"];
            if usage.is_object() {
                let mut totals = serde_json::Map::new();
                for d in ["inputTokens", "cachedInputTokens", "outputTokens", "reasoningOutputTokens", "totalTokens"] {
                    totals.insert(d.into(), usage[d].clone());
                }
                let aux = {
                    let s = h.store.lock().unwrap();
                    s.aux_record_usage(task, aux_id, &Value::from(totals))?;
                    s.aux_session(task, aux_id)?
                };
                let cap = aux["budget"]["maxTotalTokens"]
                    .as_str()
                    .and_then(|v| v.parse::<u64>().ok());
                let observed = usage["totalTokens"]
                    .as_str()
                    .and_then(|v| v.parse::<u64>().ok())
                    .or_else(|| usage["totalTokens"].as_u64())
                    .unwrap_or(0);
                // Pi parity: a soft cap only interrupts a live RUNNING turn; a
                // durable terminal fact is never upgraded by a late report.
                if cap.is_some_and(|c| observed >= c)
                    && aux["state"].as_str() == Some("RUNNING")
                {
                    let h2 = h.clone();
                    let task2 = task.to_owned();
                    let aid = aux_id.to_owned();
                    tokio::spawn(async move { aux_budget_stop(h2, task2, aid).await });
                }
            }
        }
        _ => {}
    }
    Ok(())
}
// Soft aux budget: the cap can only trip at a real observed usage boundary —
// the aux turn is interrupted and the managed peer is terminated, mirroring the
// parent budget rule without touching the parent ledger. Pi parity: the
// terminal state is FAILED and BUDGET_EXCEEDED is the reasonCode.
async fn aux_budget_stop(h: Shared, task: String, aux_id: String) {
    // The durable mark runs FIRST: if a real turn completion won the race to
    // the row, the turn's own terminal fact stands and nothing is rewritten;
    // otherwise an INTERRUPTED seal can never overwrite the budget reason.
    {
        let s = h.store.lock().unwrap();
        if s
            .aux_transition(&task, &aux_id, AUX_OPEN, "FAILED", Some("BUDGET_EXCEEDED"), |_| {})
            .unwrap_or(false)
        {
            let _ = s.child_message(
                &task,
                None,
                &aux_id,
                &store::ChildMsg {
                    kind: "STATUS",
                    text: "FAILED: BUDGET_EXCEEDED",
                    status: "FAILED",
                    event_ref: None,
                },
            );
            let _ = s.event(
                &task,
                "aux-session.completed",
                json!({"auxSessionId":aux_id,"state":"FAILED","reasonCode":"BUDGET_EXCEEDED"}),
            );
        }
    }
    let peer = h
        .aux_peers
        .lock()
        .await
        .get(&task)
        .and_then(|m| m.get(&aux_id))
        .cloned();
    if let Some(peer) = peer {
        let (thread, turn) = {
            let s = h.store.lock().unwrap();
            s.aux_session(&task, &aux_id)
                .map(|a| {
                    (
                        a["nativeSessionId"].as_str().map(str::to_owned),
                        a["nativeTurnId"].as_str().map(str::to_owned),
                    )
                })
                .unwrap_or((None, None))
        };
        if let (Some(thread), Some(turn)) = (thread, turn) {
            let _ = timeout(
                Duration::from_secs(3),
                peer.rpc("turn/interrupt", json!({"threadId":thread,"turnId":turn})),
            )
            .await;
        }
        let _ = peer.stop().await;
    }
    h.aux_peers
        .lock()
        .await
        .get_mut(&task)
        .and_then(|m| m.remove(&aux_id));
}
// Stop kills only the aux peer; the parent task is untouched. Idempotent: an
// already-terminal session just reports its durable state.
async fn aux_stop(h: Shared, task: &str, aux_id: &str, b: &Value) -> Result<(u16, Value)> {
    exact(b, &["requestKey"])?;
    let aux = h.store.lock().unwrap().aux_session(task, aux_id)?;
    if !AUX_OPEN.contains(&aux["state"].as_str().unwrap_or("")) {
        return Ok((200, aux));
    }
    let peer = h
        .aux_peers
        .lock()
        .await
        .get(task)
        .and_then(|m| m.get(aux_id))
        .cloned();
    if let Some(peer) = peer {
        // The kill-on-close job drain proves the aux process tree is gone
        // before the CANCELLED fact is written; an unproven kill is an honest
        // FAILED, never a claimed cancel.
        if peer.stop().await.is_err() {
            let s = h.store.lock().unwrap();
            s.aux_transition(task, aux_id, AUX_OPEN, "FAILED", Some("NATIVE_WRITER_UNPROVEN"), |_| {})?;
            s.event(
                task,
                "aux-session.completed",
                json!({"auxSessionId":aux_id,"state":"FAILED","reasonCode":"NATIVE_WRITER_UNPROVEN"}),
            )?;
            return Ok((200, h.store.lock().unwrap().aux_session(task, aux_id)?));
        }
    }
    // Peerless (the spawn driver has not registered yet): marking CANCELLED
    // makes the spawn driver abort its next step.
    {
        let s = h.store.lock().unwrap();
        if s.aux_transition(task, aux_id, AUX_OPEN, "CANCELLED", Some("AUX_STOP"), |_| {})? {
            let _ = s.child_message(
                task,
                None,
                aux_id,
                &store::ChildMsg {
                    kind: "STATUS",
                    text: "CANCELLED: AUX_STOP",
                    status: "CANCELLED",
                    event_ref: None,
                },
            );
            s.event(
                task,
                "aux-session.completed",
                json!({"auxSessionId":aux_id,"state":"CANCELLED","reasonCode":"AUX_STOP"}),
            )?;
        }
    }
    h.aux_peers
        .lock()
        .await
        .get_mut(task)
        .and_then(|m| m.remove(aux_id));
    Ok((200, h.store.lock().unwrap().aux_session(task, aux_id)?))
}
// Deadline sweep: a RUNNING aux row past its durable deadlineEpochMillis is
// cancelled and its peer released — the same cadence and vocabulary as the Pi
// host's sweepAuxDeadlines.
async fn sweep_aux_deadlines(h: &Shared) {
    let expired: Vec<(String, String)> = {
        let s = h.store.lock().unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut out = vec![];
        if let Ok(tasks) = s.tasks() {
            for t in tasks {
                let tid = t["taskId"].as_str().unwrap_or("").to_owned();
                if let Ok(list) = s.aux_sessions(&tid) {
                    for aux in list {
                        if aux["state"].as_str() == Some("RUNNING")
                            && aux["deadlineEpochMillis"]
                                .as_str()
                                .and_then(|v| v.parse::<u64>().ok())
                                .is_some_and(|d| now_ms > d)
                        {
                            out.push((
                                tid.clone(),
                                aux["auxSessionId"].as_str().unwrap_or("").to_owned(),
                            ));
                        }
                    }
                }
            }
        }
        out
    };
    for (task, aux_id) in expired {
        {
            let s = h.store.lock().unwrap();
            if s
                .aux_transition(&task, &aux_id, AUX_OPEN, "CANCELLED", Some("AUX_DEADLINE"), |_| {})
                .unwrap_or(false)
            {
                let _ = s.child_message(
                    &task,
                    None,
                    &aux_id,
                    &store::ChildMsg {
                        kind: "STATUS",
                        text: "CANCELLED: AUX_DEADLINE",
                        status: "CANCELLED",
                        event_ref: None,
                    },
                );
                let _ = s.event(
                    &task,
                    "aux-session.completed",
                    json!({"auxSessionId":aux_id,"state":"CANCELLED","reasonCode":"AUX_DEADLINE"}),
                );
            }
        }
        if let Some(peer) = h
            .aux_peers
            .lock()
            .await
            .get(&task)
            .and_then(|m| m.get(&aux_id))
            .cloned()
        {
            let _ = peer.stop().await;
        }
        h.aux_peers
            .lock()
            .await
            .get_mut(&task)
            .and_then(|m| m.remove(&aux_id));
    }
}
// Stop every open aux peer of a task (task STOP arm); parent-owned kills carry
// their own reason and never overwrite a terminal row.
async fn aux_stop_all(h: &Shared, task: &str, reason: &str) -> Vec<Value> {
    let mut affected: Vec<Value> = vec![];
    let peers: Vec<Arc<Peer>> = h
        .aux_peers
        .lock()
        .await
        .get(task)
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    for peer in peers {
        let _ = peer.stop().await;
    }
    {
        let s = h.store.lock().unwrap();
        if let Ok(list) = s.aux_sessions(task) {
            for aux in list {
                if AUX_OPEN.contains(&aux["state"].as_str().unwrap_or("")) {
                    let id = aux["auxSessionId"].as_str().unwrap_or("").to_owned();
                    if s.aux_transition(task, &id, AUX_OPEN, "CANCELLED", Some(reason), |_| {})
                        .unwrap_or(false)
                    {
                        let text = format!("CANCELLED: {reason}");
                        let _ = s.child_message(
                            task,
                            None,
                            &id,
                            &store::ChildMsg {
                                kind: "STATUS",
                                text: &text,
                                status: "CANCELLED",
                                event_ref: None,
                            },
                        );
                        affected.push(json!({
                            "kind":"AUXILIARY_SESSION",
                            "auxSessionId":id,
                            "childId":id,
                            "collaborationId":Value::Null,
                            "newState":"CANCELLED",
                            "reasonCode":reason,
                        }));
                        let _ = s.event(
                            task,
                            "aux-session.completed",
                            json!({"auxSessionId":id,"state":"CANCELLED","reasonCode":reason}),
                        );
                    }
                }
            }
        }
    }
    h.aux_peers.lock().await.remove(task);
    affected
}
// ======================= W5b — collaborations (P14/P15) =======================
// Pi parity vocabulary. Each child is a separate managed app-server peer bound
// to its own writable checkout COPY of the parent workspace; durable ordering,
// baseline, candidate and adoption facts live in collab_children rows. Child
// bytes never reach the parent workspace except through an explicit guarded
// adoption.
const COLLAB_MAX_CHILDREN: usize = 8;
const COLLAB_MAX_PER_TASK: i64 = 4;
const COLLAB_MAX_GOAL: usize = 8192;
const COLLAB_MAX_PATHS: usize = 64;
const COLLAB_MAX_ADOPTIONS: usize = 32;
const COLLAB_CANDIDATE_MAX_FILES: usize = 8192;
const COLLAB_DEADLINE_MS: u64 = 10 * 60 * 1000;
const COLLAB_MAX_NAME: usize = 64;
const COLLAB_PACKAGE_MAX: usize = 8;
// Open child states the host may still act on. Terminal facts
// (SUCCEEDED/FAILED/CANCELLED/BLOCKED) are immutable once written.
const COLLAB_OPEN: &[&str] = &["PENDING", "RUNNING"];
fn collab_key(collab_id: &str, child_id: &str) -> String {
    format!("{collab_id}/{child_id}")
}
// ^[a-z0-9-]{1,64}$ — the contract's collaborationChild.childId pattern.
fn collab_child_id_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
async fn collab_route(
    h: Shared,
    task: &str,
    t: &Value,
    rest: &[&str],
    method: &str,
    b: Value,
    query: &HashMap<String, String>,
) -> Result<(u16, Value)> {
    match (method, rest.len()) {
        ("GET", 0) => Ok((
            200,
            json!({"items":h.store.lock().unwrap().collaborations(task)?,"coverage":"ISOLATED_CHECKOUT_DAG"}),
        )),
        ("GET", 1) => Ok((
            200,
            h.store.lock().unwrap().collaboration(task, valid_id(rest[0])?)?,
        )),
        // Adoption read surface (Pi parity): the flat {childId, ...record}
        // item list the PAC adoption read route depends on.
        ("GET", 2) if rest[1] == "adoptions" => {
            let c = h
                .store
                .lock()
                .unwrap()
                .collaboration(task, valid_id(rest[0])?)?;
            let mut items = Vec::new();
            for ch in c["children"].as_array().cloned().unwrap_or_default() {
                for mut a in ch["adoptions"].as_array().cloned().unwrap_or_default() {
                    a["childId"] = ch["childId"].clone();
                    items.push(a);
                }
            }
            Ok((200, json!({"items": items})))
        }
        ("POST", 0) => collab_create(h, task, t, &b).await,
        // C3: validation-only frozen plan — no collaboration row is created.
        ("POST", 1) if rest[0] == "preflight" => collab_preflight(h, task, t, &b).await,
        ("POST", 2) => {
            let cid = valid_id(rest[0])?;
            match rest[1] {
                "stop" => collab_stop(h, task, cid, &b).await,
                "adoptions" => collab_adopt(h, task, cid, &b).await,
                "previews" => collab_preview_create(h, task, cid, &b).await,
                _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
            }
        }
        ("GET", 3) if rest[1] == "previews" => Ok((
            200,
            h.store
                .lock()
                .unwrap()
                .collab_preview(task, valid_id(rest[2])?)?,
        )),
        // C2: per-child durable public message stream.
        ("GET", 4) if rest[1] == "children" && rest[3] == "messages" => {
            let cid = valid_id(rest[0])?;
            let ch = valid_id(rest[2])?;
            {
                let s = h.store.lock().unwrap();
                s.collab_child(task, cid, ch)?; // COLLABORATION_CHILD_NOT_FOUND
            }
            child_messages_page(&h, task, Some(cid), ch, query)
        }
        _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
    }
}
// Writable child instruction (Pi collabInstruction): goal + frozen context
// package material inlined, bounded so the synthesized prompt always fits the
// adapter frame.
fn collab_instruction(goal: &str, packages: &[Value]) -> Result<String> {
    let mut text = format!(
        "You are a writable child execution inside an isolated checkout of the parent task workspace. Nothing you write reaches the parent workspace directly: your finished checkout is a candidate the parent task may adopt explicitly, in whole or in part. Work only inside this checkout, write all result files here, then finish with a short summary. Do not ask the user questions.\n\n<<GOAL>>\n{}\n<</GOAL>>\n\n<<FIXED_MATERIAL>>\n",
        goal
    );
    // Same inlining recipe as the aux prompt, bounded to Pi's 128KiB material
    // ceiling.
    let mut material = String::new();
    for package in packages {
        material.push_str(&format!(
            "\n--- context package {} ({}) ---\n",
            package["packageId"].as_str().unwrap_or(""),
            package["digest"].as_str().unwrap_or("")
        ));
        if let Some(sources) = package["sources"].as_array() {
            for src in sources {
                let decoded = if src["kind"].as_str() == Some("file") {
                    STANDARD
                        .decode(src["content"].as_str().unwrap_or(""))
                        .unwrap_or_default()
                } else {
                    src["text"].as_str().unwrap_or("").as_bytes().to_vec()
                };
                let body: String = String::from_utf8_lossy(&decoded)
                    .chars()
                    .take(32768)
                    .collect();
                let header = format!(
                    "[source {} {} sha256:{} size:{}]\n",
                    src["kind"].as_str().unwrap_or(""),
                    src["path"].as_str().map(|p| format!("path={p} ")).unwrap_or_default(),
                    src["sha256"].as_str().unwrap_or(""),
                    src["size"].as_str().unwrap_or("")
                );
                let remaining = 131072usize.saturating_sub(material.len());
                if body.len() + header.len() > remaining {
                    material.push_str(&header);
                    material.push_str("[context source omitted: inline budget exhausted]\n");
                    break;
                }
                material.push_str(&header);
                material.push_str(&body);
                material.push('\n');
            }
        }
        if material.len() >= 131072 {
            break;
        }
    }
    text.push_str(&material);
    text.push_str("\n<</FIXED_MATERIAL>>\n\nWhen finished, list the files you wrote.");
    Ok(text)
}
// Writable-checkout snapshot + per-file baseline: a bounded byte copy of the
// task workspace (.git and symlinks skipped, non-regular entries recorded as
// skipped) plus the sha256 of every copied file — the adoption drift anchor.
fn snapshot_collab_checkout(src: &Path, dst: &Path) -> Result<Value> {
    let mut files = serde_json::Map::new();
    let mut budget = (0u64, 0u64);
    let mut skipped = 0u64;
    fn walk(
        src: &Path,
        dst: &Path,
        rel: &str,
        files: &mut serde_json::Map<String, Value>,
        budget: &mut (u64, u64),
        skipped: &mut u64,
    ) -> Result<()> {
        fs::create_dir_all(dst)?;
        let mut entries: Vec<_> = fs::read_dir(src)?.collect::<std::result::Result<_, _>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let name = entry.file_name();
            if name == ".git" {
                *skipped += 1;
                continue;
            }
            let meta = fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() {
                *skipped += 1;
                continue;
            }
            let child_rel = if rel.is_empty() {
                name.to_string_lossy().to_string()
            } else {
                format!("{rel}/{}", name.to_string_lossy())
            };
            let target = dst.join(&name);
            if meta.is_dir() {
                walk(&path, &target, &child_rel, files, budget, skipped)?;
            } else if meta.is_file() {
                budget.0 += 1;
                budget.1 += meta.len();
                if budget.0 > AUX_COPY_MAX_FILES || budget.1 > AUX_COPY_MAX_BYTES {
                    bail!("COLLAB_SNAPSHOT_TOO_LARGE")
                }
                let bytes = fs::read(&path)?;
                fs::write(&target, &bytes)?;
                files.insert(
                    child_rel.replace('\\', "/"),
                    json!({"sha256":hash(&bytes),"size":bytes.len().to_string()}),
                );
            } else {
                *skipped += 1;
            }
        }
        Ok(())
    }
    walk(src, dst, "", &mut files, &mut budget, &mut skipped)?;
    let files_v = Value::from(files);
    Ok(json!({
        "files": files_v,
        "digest": hash(canonical_collab(&files_v)?),
        "fileCount": budget.0.to_string(),
        "bytes": budget.1.to_string(),
        "skipped": skipped.to_string(),
        "snapshotAt": store::now()
    }))
}
// Canonical-bytes helper mirroring store::canonical but local to the host
// (object-order-stable sha256 for baseline/candidate digests).
fn canonical_collab(v: &Value) -> Result<Vec<u8>> {
    store::canonical(v)
}
// Live candidate diff: current regular files of the child checkout vs the
// sealed baseline. New/changed files are candidates; baseline files that
// disappeared are deletions. Recomputed at adoption time so a leaked write
// after completion is still adopted-or-conflicted honestly.
fn collab_candidate(child: &Value) -> Result<Value> {
    let ws = child["workspace"]
        .as_str()
        .ok_or_else(|| anyhow!("ADOPTION_CHECKOUT_UNAVAILABLE"))?;
    let ws_path = Path::new(ws);
    if !ws_path.is_dir() {
        bail!("ADOPTION_CHECKOUT_UNAVAILABLE")
    }
    let mut current = serde_json::Map::new();
    fn walk(
        dir: &Path,
        rel: &str,
        out: &mut serde_json::Map<String, Value>,
        count: &mut u64,
    ) -> Result<()> {
        let mut entries: Vec<_> = fs::read_dir(dir)?.collect::<std::result::Result<_, _>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let name = entry.file_name();
            let meta = fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() {
                continue;
            }
            let child_rel = if rel.is_empty() {
                name.to_string_lossy().to_string()
            } else {
                format!("{rel}/{}", name.to_string_lossy())
            };
            if meta.is_dir() {
                walk(&path, &child_rel, out, count)?;
            } else if meta.is_file() {
                *count += 1;
                if *count > COLLAB_CANDIDATE_MAX_FILES as u64 {
                    bail!("CANDIDATE_TOO_LARGE")
                }
                let bytes = fs::read(&path)?;
                out.insert(
                    child_rel.replace('\\', "/"),
                    json!({"sha256":hash(&bytes),"size":bytes.len().to_string()}),
                );
            }
        }
        Ok(())
    }
    let mut count = 0u64;
    walk(ws_path, "", &mut current, &mut count)?;
    let baseline = &child["baseline"]["files"];
    let mut files = serde_json::Map::new();
    for (rel, meta) in &current {
        if baseline[rel]["sha256"] != meta["sha256"] {
            files.insert(rel.clone(), meta.clone());
        }
    }
    let mut deletions = vec![];
    if let Some(base) = baseline.as_object() {
        for rel in base.keys() {
            if !current.contains_key(rel) {
                deletions.push(json!(rel));
            }
        }
    }
    let files_v = Value::from(files);
    let digest = hash(canonical_collab(&json!({"files":files_v,"deletions":deletions}))?);
    Ok(json!({
        "files": files_v,
        "deletions": deletions,
        "digest": digest,
        "fileCount": files_v.as_object().map(|o|o.len()).unwrap_or(0).to_string(),
        "deletionCount": deletions.len().to_string(),
        "computedAt": store::now()
    }))
}
// Shared collaboration plan validation (C3): name + children DAG + per-child
// goal/scope/budget/package checks produce the canonical spec list both the
// preflight resource and the create path bind. Deterministic for a fixed
// request body — the plan digest is stable across both call sites.
fn collab_plan(b: &Value) -> Result<(Option<String>, Vec<Value>)> {
    let name = if b["name"].is_null() {
        None
    } else {
        match b["name"]
            .as_str()
            .filter(|n| !n.trim().is_empty() && n.len() <= COLLAB_MAX_NAME)
        {
            Some(n) => Some(n.to_owned()),
            None => bail!("INVALID_COLLABORATION_NAME"),
        }
    };
    let raw = b["children"]
        .as_array()
        .ok_or_else(|| anyhow!("INVALID_COLLABORATION_CHILDREN"))?;
    if raw.is_empty() || raw.len() > COLLAB_MAX_CHILDREN {
        bail!("INVALID_COLLABORATION_CHILDREN")
    }
    let mut declared = HashSet::new();
    for c in raw {
        if !c.is_object() {
            bail!("INVALID_COLLABORATION_CHILDREN")
        }
        exact(
            c,
            &[
                "childId",
                "goal",
                "dependsOn",
                "budget",
                "writeScope",
                "contextPackageIds",
            ],
        )?;
        if let Some(id) = c.get("childId") {
            let id = id
                .as_str()
                .filter(|s| collab_child_id_ok(s))
                .ok_or_else(|| anyhow!("INVALID_COLLABORATION_CHILD_ID"))?;
            if !declared.insert(id.to_owned()) {
                bail!("INVALID_COLLABORATION_CHILD_ID")
            }
        }
    }
    let mut specs: Vec<Value> = vec![];
    for (i, c) in raw.iter().enumerate() {
        let child_id = c["childId"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("child-{}", i + 1));
        let goal = c["goal"]
            .as_str()
            .filter(|g| !g.trim().is_empty() && g.len() <= COLLAB_MAX_GOAL)
            .ok_or_else(|| anyhow!("INVALID_COLLABORATION_GOAL"))?
            .to_owned();
        let mut depends_on: Vec<String> = vec![];
        let mut seen = HashSet::new();
        if !c["dependsOn"].is_null() {
            let a = c["dependsOn"]
                .as_array()
                .ok_or_else(|| anyhow!("INVALID_COLLABORATION_DEPENDENCY"))?;
            if a.len() > COLLAB_MAX_CHILDREN {
                bail!("INVALID_COLLABORATION_DEPENDENCY")
            }
            for d in a {
                let d = d
                    .as_str()
                    .filter(|s| collab_child_id_ok(s))
                    .ok_or_else(|| anyhow!("INVALID_COLLABORATION_DEPENDENCY"))?;
                if seen.insert(d.to_owned()) {
                    depends_on.push(d.to_owned())
                }
            }
        }
        let mut package_ids: Vec<String> = vec![];
        if !c["contextPackageIds"].is_null() {
            let a = c["contextPackageIds"]
                .as_array()
                .ok_or_else(|| anyhow!("INVALID_COLLABORATION_PACKAGES"))?;
            if a.len() > COLLAB_PACKAGE_MAX
                || a
                    .iter()
                    .any(|v| v.as_str().map(|s| s.is_empty()).unwrap_or(true))
            {
                bail!("INVALID_COLLABORATION_PACKAGES")
            }
            for pid in a {
                package_ids.push(pid.as_str().unwrap_or("").to_owned())
            }
        }
        // Child scope/budget reuse the task-level validators: the child's
        // workspace-relative deny prefixes and SOFT-only caps carry identical
        // semantics inside its own checkout.
        let write_scope = if c["writeScope"].is_null() {
            None
        } else {
            Some(validate_write_scope(&c["writeScope"])?)
        };
        let budget = if c["budget"].is_null() {
            None
        } else {
            Some(validate_budget(&c["budget"])?)
        };
        specs.push(json!({
            "childId": child_id,
            "goal": goal,
            "goalSha256": hash(goal.as_bytes()),
            "dependsOn": depends_on,
            "contextPackageIds": package_ids,
            "writeScope": write_scope,
            "budget": budget,
        }));
    }
    // DAG: final ids must be unique (an auto `child-N` name can still collide
    // with an explicit one); a self-dep is a cycle; an unknown dep names its
    // own code; a real cycle is caught by the DFS below.
    let ids: HashSet<String> = specs
        .iter()
        .map(|s| s["childId"].as_str().unwrap_or("").to_owned())
        .collect();
    if ids.len() != specs.len() {
        bail!("INVALID_COLLABORATION_CHILD_ID")
    }
    for s in &specs {
        for d in s["dependsOn"].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
            let d = d.as_str().unwrap_or("");
            if d == s["childId"].as_str().unwrap_or("") {
                bail!("COLLABORATION_DEPENDENCY_CYCLE")
            }
            if !ids.contains(d) {
                bail!("COLLABORATION_DEPENDENCY_UNKNOWN")
            }
        }
    }
    {
        fn visit(id: &str, specs: &[Value], color: &mut HashMap<String, u8>) -> Result<()> {
            match color.get(id) {
                Some(1) => bail!("COLLABORATION_DEPENDENCY_CYCLE"),
                Some(2) => return Ok(()),
                _ => {}
            }
            color.insert(id.to_owned(), 1);
            if let Some(s) = specs.iter().find(|s| s["childId"].as_str() == Some(id)) {
                for d in s["dependsOn"].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
                    if let Some(d) = d.as_str() {
                        visit(d, specs, color)?;
                    }
                }
            }
            color.insert(id.to_owned(), 2);
            Ok(())
        }
        let mut color: HashMap<String, u8> = HashMap::new();
        for s in &specs {
            visit(s["childId"].as_str().unwrap_or(""), &specs, &mut color)?;
        }
    }
    Ok((name, specs))
}
// A stopped intake, a dead task state or a sealed source refuses new
// collaboration work; a completed-but-unstopped parent stays writable.
fn collab_intake_open(t: &Value) -> Result<()> {
    ensure_not_sealed(t)?;
    if t["stopProof"].is_object()
        || matches!(
            t["state"].as_str(),
            Some(
                "STOP_REQUESTED"
                    | "CANCELLED"
                    | "FAILED"
                    | "BUDGET_EXCEEDED"
                    | "BINDING_UNAVAILABLE"
                    | "REJECTED"
                    | "UNKNOWN"
            )
        )
    {
        bail!("COLLABORATION_TASK_INACTIVE")
    }
    Ok(())
}
// The canonical plan document a preflight/collab-create digest binds.
fn collab_plan_doc(name: &Option<String>, specs: &[Value]) -> Value {
    json!({
        "schema": "collab-plan/1",
        "name": name,
        "children": specs,
        "limits": {"maxChildren":COLLAB_MAX_CHILDREN.to_string(),"maxGoalBytes":COLLAB_MAX_GOAL.to_string()},
    })
}
// C3 — collaboration preflight: validate the plan against the durable parent
// state and freeze it under a digest + expiry. No collaboration row, no
// spawn; create later re-checks the digest and the frozen generation.
async fn collab_preflight(h: Shared, task: &str, t: &Value, b: &Value) -> Result<(u16, Value)> {
    exact(b, &["requestKey", "name", "children"])?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
    }
    collab_intake_open(t)?;
    let (name, specs) = collab_plan(b)?;
    // Named packages must exist — same intake refusal class as create.
    {
        let s = h.store.lock().unwrap();
        for ch in &specs {
            for pid in ch["contextPackageIds"]
                .as_array()
                .map(|a| a.as_slice())
                .unwrap_or(&[])
            {
                s.context_package(task, pid.as_str().unwrap_or(""))?;
            }
        }
    }
    let plan = collab_plan_doc(&name, &specs);
    let plan_digest = hash(store::canonical(&plan)?);
    let row = json!({
        "preflightId": format!("pf-{}", store::id()),
        "planDigest": plan_digest,
        "plan": plan,
        "sourceBindings": {
            "taskId": t["taskId"],
            "projectId": t["projectId"],
            "engineId": "codex",
            "engineVersion": h.registry.version,
            "generation": t["generation"],
            "segmentId": t["segmentId"],
            "baseCommit": t["baseCommit"],
        },
        "allowedScopes": specs.iter().map(|c| json!({"childId":c["childId"],"writeScope":c["writeScope"]})).collect::<Vec<_>>(),
        "budgetTerms": specs.iter().map(|c| json!({"childId":c["childId"],"budget":c["budget"]})).collect::<Vec<_>>(),
        "expiresAt": store::now_plus_ms(COLLAB_PLAN_TTL_MS),
        "expiresEpochMillis": (store::epoch_ms() + COLLAB_PLAN_TTL_MS).to_string(),
    });
    let result = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","operation":"COLLABORATION_PREFLIGHT","preflightId":row["preflightId"],"planDigest":plan_digest,"expiresAt":row["expiresAt"],"sourceBindings":row["sourceBindings"]});
    {
        let mut s = h.store.lock().unwrap();
        s.accept_preflight(task, &row, &key, b, &result)?;
    }
    Ok((202, result))
}
async fn collab_create(h: Shared, task: &str, t: &Value, b: &Value) -> Result<(u16, Value)> {
    exact(
        b,
        &[
            "requestKey",
            "name",
            "children",
            "expectedPreflightId",
            "expectedPlanDigest",
        ],
    )?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
    }
    collab_intake_open(t)?;
    let (name, specs) = collab_plan(b)?;
    let plan_digest = hash(store::canonical(&collab_plan_doc(&name, &specs))?);
    // Stale-plan guards (C3): a named preflight must exist, be unexpired,
    // carry this exact plan digest and still match the parent's generation —
    // anything else refuses before any mutation.
    if !b["expectedPreflightId"].is_null() || !b["expectedPlanDigest"].is_null() {
        if let Some(d) = b["expectedPlanDigest"].as_str() {
            if d != plan_digest {
                bail!("PREFLIGHT_MISMATCH")
            }
        }
        if let Some(pid) = b["expectedPreflightId"].as_str() {
            let pf = {
                let s = h.store.lock().unwrap();
                s.collab_preflight(task, valid_id(pid)?)?
                    .ok_or_else(|| anyhow!("PREFLIGHT_NOT_FOUND"))?
            };
            if pf["expired"] == true {
                bail!("PREFLIGHT_EXPIRED")
            }
            if pf["planDigest"].as_str() != Some(plan_digest.as_str()) {
                bail!("PREFLIGHT_MISMATCH")
            }
            if pf["sourceBindings"]["generation"] != t["generation"]
                || pf["sourceBindings"]["segmentId"] != t["segmentId"]
            {
                bail!("PREFLIGHT_STALE")
            }
        }
    }
    let collab_id = format!("collab-{}", store::id());
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64 + COLLAB_DEADLINE_MS)
        .unwrap_or(0)
        .to_string();
    let created_at = store::now();
    let collab = json!({"collaborationId":collab_id,"name":name,"createdAt":created_at});
    let children: Vec<Value> = specs
        .iter()
        .map(|s| {
            let mut ch = s.clone();
            ch["workspace"] = json!(
                h.root
                    .join("collab")
                    .join(&collab_id)
                    .join(s["childId"].as_str().unwrap_or(""))
                    .join("workspace")
                    .to_string_lossy()
                    .to_string()
            );
            ch["createdAt"] = json!(created_at);
            ch["deadlineEpochMillis"] = json!(deadline);
            ch
        })
        .collect();
    // Durable created fact carries goalSha256 per child — never the goal body.
    let created_event = json!({
        "collaborationId": collab_id,
        "name": name,
        "childCount": children.len().to_string(),
        "children": children.iter().map(|ch| json!({"childId":ch["childId"],"dependsOn":ch["dependsOn"],"goalSha256":ch["goalSha256"],"budget":ch["budget"]})).collect::<Vec<_>>()
    });
    let result = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"ACCEPTED","collaborationId":collab_id,"operation":"COLLABORATION_CREATE","detail":"Durably accepted"});
    {
        let mut s = h.store.lock().unwrap();
        // Per-task active bound — Pi maxPerTask maps to COLLABORATION_LIMIT.
        if s.collab_running_on_task(task)? >= COLLAB_MAX_PER_TASK {
            bail!("COLLABORATION_LIMIT")
        }
        // Named packages must exist — their frozen bytes inline at child spawn.
        for ch in &children {
            for pid in ch["contextPackageIds"]
                .as_array()
                .map(|a| a.as_slice())
                .unwrap_or(&[])
            {
                s.context_package(task, pid.as_str().unwrap_or(""))?;
            }
        }
        s.accept_collaboration(task, &collab, &children, &key, b, &result, &created_event)?;
        // C2: each child's ASSIGNMENT — the admitted goal text — is its
        // stream's first durable fact.
        for ch in &children {
            let (cid, goal) = (
                ch["childId"].as_str().unwrap_or("").to_owned(),
                ch["goal"].as_str().unwrap_or("").to_owned(),
            );
            let _ = s.child_message(
                task,
                Some(&collab_id),
                &cid,
                &store::ChildMsg {
                    kind: "ASSIGNMENT",
                    text: &goal,
                    status: "PENDING",
                    event_ref: None,
                },
            );
        }
        s.receipt(
            task,
            &key,
            "DISPATCHING",
            &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"DISPATCHING","collaborationId":collab_id,"operation":"COLLABORATION_CREATE"}),
        )?;
    }
    let h2 = h.clone();
    let task2 = task.to_owned();
    let cid = collab_id.clone();
    tokio::spawn(async move { pump_collab(&h2, &task2, &cid).await });
    Ok((202, result))
}
// Pump the durable DAG: blocked-by-failed-deps marks, pool-bounded launches and
// the all-terminal completion seal all commit atomically in the store; this
// host function only applies the receipt and drives the launches.
async fn pump_collab(h: &Shared, task: &str, collab_id: &str) {
    let (launch, completed) = {
        let mut s = h.store.lock().unwrap();
        s.collab_pump(task, collab_id, h.max_collab_children)
            .unwrap_or_default()
    };
    if let Some((key, result)) = completed {
        let mut s = h.store.lock().unwrap();
        let _ = s.receipt(
            task,
            &key,
            "APPLIED",
            &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","collaborationId":collab_id,"operation":"COLLABORATION_CREATE","detail":"Collaboration completed","result":result}),
        );
    }
    for child_id in launch {
        let h2 = h.clone();
        let task2 = task.to_owned();
        let cid = collab_id.to_owned();
        tokio::spawn(async move {
            if let Err(e) = collab_launch(h2.clone(), &task2, &cid, &child_id).await {
                collab_spawn_failed(&h2, &task2, &cid, &child_id, &e).await;
            }
        });
    }
}
// The pump -> launch -> spawn_failed -> pump chain is a recursive future type
// (rustc surfaces it as !Send), so the edge back into the pump goes through a
// type-erased detached spawn. The pump is idempotent and durable, so detaching
// changes nothing semantically.
fn pump_collab_detached(h: &Shared, task: &str, collab_id: &str) {
    let (h2, t2, c2) = (h.clone(), task.to_owned(), collab_id.to_owned());
    let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async move { pump_collab(&h2, &t2, &c2).await });
    tokio::spawn(fut);
}
// Child driver: snapshot the parent workspace into the child's own checkout
// (sealing its adoption baseline), spawn a separate managed peer on its own
// CODEX_HOME, bind a workspace-write thread and dispatch one bounded turn.
// Every step rechecks the durable row so a stop can never be resurrected.
async fn collab_launch(h: Shared, task: &str, collab_id: &str, child_id: &str) -> Result<()> {
    let preparing = h.preparation.lock().await;
    let (t, collab, child) = {
        let s = h.store.lock().unwrap();
        (
            s.task(task)?,
            s.collaboration(task, collab_id)?,
            s.collab_child(task, collab_id, child_id)?,
        )
    };
    if collab["state"] != "RUNNING" || child["state"] != "RUNNING" {
        return Ok(());
    }
    let src = PathBuf::from(t["workspace"].as_str().unwrap_or(""));
    let root = h.root.join("collab").join(collab_id).join(child_id);
    let child_ws = root.join("workspace");
    let baseline = snapshot_collab_checkout(&src, &child_ws)?;
    {
        let s = h.store.lock().unwrap();
        if !s.collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, "RUNNING", None, |r| {
            r["baseline"] = baseline.clone();
        })? {
            bail!("COLLABORATION_CHILD_CLOSED")
        }
    }
    // Same digest-pinned managed profile as the task home; task-bound MCP
    // servers, skills and hooks are NOT replicated on a child — it writes code,
    // it does not inherit task extensions.
    let home = root.join("home");
    fs::create_dir_all(&home)?;
    fs::write(
        home.join("config.toml"),
        fs::read_to_string(h.root.join("profile/config.toml"))?,
    )?;
    let (peer, events) = Peer::spawn(
        &h.registry.executable,
        &home,
        &child_ws,
        &env_allowlist(&t),
        bounded_profile(&h.registry.tool_profile),
    )
    .await?;
    {
        let mut peers = h.collab_peers.lock().await;
        peers
            .entry(task.to_owned())
            .or_default()
            .insert(collab_key(collab_id, child_id), peer.clone());
    }
    drop(preparing);
    // If a stop landed while the peer was spawning, kill it now instead of
    // dispatching a turn the durable record already refuses.
    if {
        let s = h.store.lock().unwrap();
        s.collab_child(task, collab_id, child_id)?["state"] != "RUNNING"
    } {
        let _ = peer.stop().await;
        h.collab_peers
            .lock()
            .await
            .get_mut(task)
            .and_then(|m| m.remove(&collab_key(collab_id, child_id)));
        bail!("COLLABORATION_CHILD_CLOSED")
    }
    collab_watch(&h, task, collab_id, child_id, events);
    peer.initialize_with_experimental(false).await?;
    // Writable child: workspace-write sandbox with the child's declared
    // extraWriteDirs as additional writable roots. Approvals stay "on-request"
    // (the same app-server path parent tasks use): the pinned Windows sandbox
    // cannot auto-approve commands under "never" — every exec would be refused
    // as blocked-by-policy — so the host answers the child's approval requests
    // itself, granting only what is provably inside the child's own checkout
    // (see collab_event). Autonomy is preserved; nothing reaches a caller.
    let mut writable: Vec<Value> = vec![json!(child_ws)];
    if let Some(roots) = child["writeScope"]["extraWriteDirs"].as_array() {
        writable.extend(roots.iter().cloned());
    }
    let result = peer
        .rpc(
            "thread/start",
            json!({"model":h.registry.model,"modelProvider":"nwf","cwd":child_ws,"approvalPolicy":"on-request","sandbox":"workspace-write","baseInstructions":"You are a writable child execution inside an isolated checkout of the parent task workspace. Nothing you write reaches the parent workspace directly: your finished checkout is a candidate the parent task may adopt explicitly, in whole or in part. Work only inside this checkout, write all result files here, then finish with a short summary. Do not ask the user questions.","config":{"features.multi_agent":false}}),
        )
        .await?;
    let thread = string(&result["thread"], "id")?.to_owned();
    if string(&result, "model")? != h.registry.model || string(&result, "modelProvider")? != "nwf" {
        bail!("MODEL_BINDING_MISMATCH")
    }
    {
        let s = h.store.lock().unwrap();
        if !s.collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, "RUNNING", None, |r| {
            r["nativeSessionId"] = json!(thread)
        })? {
            bail!("COLLABORATION_CHILD_CLOSED")
        }
        let _ = s.child_message(
            task,
            Some(collab_id),
            child_id,
            &store::ChildMsg {
                kind: "STATUS",
                text: "RUNNING: native thread bound",
                status: "RUNNING",
                event_ref: Some(&thread),
            },
        );
        s.event(
            task,
            "collaboration.child.bound",
            json!({"collaborationId":collab_id,"childId":child_id,"nativeSessionId":thread}),
        )?;
    }
    // Frozen context packages named at intake read back their durable bytes now.
    let packages = {
        let s = h.store.lock().unwrap();
        let mut out = vec![];
        for pid in child["contextPackageIds"]
            .as_array()
            .map(|a| a.as_slice())
            .unwrap_or(&[])
        {
            out.push(s.context_package(task, pid.as_str().unwrap_or(""))?);
        }
        out
    };
    let prompt = collab_instruction(child["goal"].as_str().unwrap_or(""), &packages)?;
    let result = peer
        .rpc(
            "turn/start",
            json!({"threadId":thread,"model":h.registry.model,"input":[{"type":"text","text":prompt}],"clientUserMessageId":format!("collab-prompt-{}-{}",collab_id,child_id),"sandboxPolicy":{"type":"workspaceWrite","writableRoots":writable}}),
        )
        .await?;
    let turn = string(&result["turn"], "id")?.to_owned();
    {
        let s = h.store.lock().unwrap();
        s.collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, "RUNNING", None, |r| {
            r["nativeTurnId"] = json!(turn)
        })?;
        // C2 USER_INPUT: the dispatched instruction (goal + frozen context) —
        // the same text the child turn received, truncated by the store bound.
        let _ = s.child_message(
            task,
            Some(collab_id),
            child_id,
            &store::ChildMsg {
                kind: "USER_INPUT",
                text: &prompt,
                status: "RUNNING",
                event_ref: Some(&turn),
            },
        );
    }
    Ok(())
}
// Child launch failure: the durable row becomes FAILED honestly, its peer is
// killed before the mark, and dependents are blocked by the next pump — the
// collaboration stays RUNNING for its remaining children (Pi parity: a single
// child failure does not fail the collab).
async fn collab_spawn_failed(
    h: &Shared,
    task: &str,
    collab_id: &str,
    child_id: &str,
    e: &anyhow::Error,
) {
    let code = safe_code(e);
    let key = collab_key(collab_id, child_id);
    if let Some(peer) = h
        .collab_peers
        .lock()
        .await
        .get(task)
        .and_then(|m| m.get(&key))
        .cloned()
    {
        let _ = peer.stop().await;
    }
    {
        let s = h.store.lock().unwrap();
        if s
            .collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, "FAILED", Some(&code), |_| {})
            .unwrap_or(false)
        {
            let text = format!("FAILED: {code}");
            let _ = s.child_message(
                task,
                Some(collab_id),
                child_id,
                &store::ChildMsg {
                    kind: "STATUS",
                    text: &text,
                    status: "FAILED",
                    event_ref: None,
                },
            );
            let _ = s.event(
                task,
                "collaboration.child.failed",
                json!({"collaborationId":collab_id,"childId":child_id,"state":"FAILED","reasonCode":code}),
            );
        }
    }
    h.collab_peers
        .lock()
        .await
        .get_mut(task)
        .and_then(|m| m.remove(&key));
    pump_collab_detached(h, task, collab_id);
}
fn collab_watch(
    h: &Shared,
    task: &str,
    collab_id: &str,
    child_id: &str,
    mut events: tokio::sync::mpsc::Receiver<Value>,
) {
    let h2 = h.clone();
    let task2 = task.to_owned();
    let cid = collab_id.to_owned();
    let ch = child_id.to_owned();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Err(e) = collab_event(&h2, &task2, &cid, &ch, event).await {
                let code = safe_code(&e);
                {
                    let s = h2.store.lock().unwrap();
                    if s
                        .collab_child_transition(&task2, &cid, &ch, COLLAB_OPEN, "FAILED", Some(&code), |_| {})
                        .unwrap_or(false)
                    {
                        let text = format!("FAILED: {code}");
                        let _ = s.child_message(
                            &task2,
                            Some(&cid),
                            &ch,
                            &store::ChildMsg {
                                kind: "STATUS",
                                text: &text,
                                status: "FAILED",
                                event_ref: None,
                            },
                        );
                        let _ = s.event(
                            &task2,
                            "collaboration.child.failed",
                            json!({"collaborationId":cid,"childId":ch,"state":"FAILED","reasonCode":code}),
                        );
                    }
                }
                if let Some(peer) = h2
                    .collab_peers
                    .lock()
                    .await
                    .get(&task2)
                    .and_then(|m| m.get(&collab_key(&cid, &ch)))
                    .cloned()
                {
                    let _ = peer.stop().await;
                }
                h2.collab_peers
                    .lock()
                    .await
                    .get_mut(&task2)
                    .and_then(|m| m.remove(&collab_key(&cid, &ch)));
                pump_collab(&h2, &task2, &cid).await;
                break;
            }
        }
    });
}
// Child scope check: identical semantics to the task write_scope_check, rooted
// at the child's own checkout with its own declared extraWriteDirs/denyPaths.
fn collab_scope_check(child: &Value, path: &str) -> Option<&'static str> {
    write_scope_check(
        &json!({"workspace":child["workspace"],"writeScope":child["writeScope"]}),
        path,
    )
}
// Host-side approval policy for an autonomous writable child: a command is
// granted when its declared working directory is inside the child's checkout
// (the checkout the child fully owns), a file change when every reported path
// passes the child's write scope. Fail-closed on missing scope evidence.
fn collab_approval_grant(child: &Value, method: &str, p: &Value) -> bool {
    match method {
        "item/commandExecution/requestApproval" => p["cwd"]
            .as_str()
            .is_some_and(|cwd| collab_scope_check(child, cwd).is_none()),
        "item/fileChange/requestApproval" => p["paths"].as_array().is_some_and(|paths| {
            !paths.is_empty()
                && paths
                    .iter()
                    .all(|pth| pth.as_str().is_some_and(|pth| collab_scope_check(child, pth).is_none()))
        }),
        _ => false,
    }
}
// Native events for a child peer: they only ever touch the child row and its
// own durable facts — the parent task record, its input queue and its budget
// are never written by this path (P14 isolation by construction).
async fn collab_event(
    h: &Shared,
    task: &str,
    collab_id: &str,
    child_id: &str,
    event: Value,
) -> Result<()> {
    let method = string(&event, "method")?;
    let p = &event["params"];
    let key = collab_key(collab_id, child_id);
    if event.get("id").is_some() {
        // A writable autonomous child has no caller to answer questions — but
        // the pinned Windows sandbox routes commands and file changes through
        // on-request approvals, which the host must settle itself. The grant
        // is scope-checked against the child's own checkout: a command is
        // granted only when its declared cwd sits inside that checkout, a file
        // change only when every named path passes the child write scope
        // (checkout + declared extraWriteDirs, deny paths refused). Out-of-
        // scope approvals decline with a durable scope_violation fact;
        // genuine questions (requestUserInput, elicitation) still refuse
        // outright because no answer channel exists.
        if let Some(peer) = h
            .collab_peers
            .lock()
            .await
            .get(task)
            .and_then(|m| m.get(&key))
            .cloned()
        {
            match method {
                "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                    let child = {
                        let s = h.store.lock().unwrap();
                        s.collab_child(task, collab_id, child_id)?
                    };
                    let grant = collab_approval_grant(&child, method, p);
                    peer.send(
                        &json!({"id":event["id"],"result":{"decision":if grant {"accept"} else {"decline"}}}),
                    )
                    .await?;
                    if !grant {
                        let s = h.store.lock().unwrap();
                        s.event(
                            task,
                            "collaboration.child.scope_violation",
                            json!({"collaborationId":collab_id,"childId":child_id,"phase":"approval","method":method,"cwd":p["cwd"],"paths":p["paths"]}),
                        )?;
                    }
                }
                _ => {
                    peer.send(&json!({"id":event["id"],"error":{"code":-32601,"message":"COLLAB_INPUT_UNSUPPORTED"}}))
                        .await?;
                }
            }
        }
        return Ok(());
    }
    match method {
        "adapter/failure" => bail!(adapter::failure_code(&event)),
        "adapter/exited" => {
            {
                let s = h.store.lock().unwrap();
                if s.collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, "FAILED", Some("COLLAB_PROCESS_EXITED_UNCONFIRMED"), |_| {})? {
                    let _ = s.child_message(
                        task,
                        Some(collab_id),
                        child_id,
                        &store::ChildMsg {
                            kind: "STATUS",
                            text: "FAILED: process exited before a completion fact arrived",
                            status: "FAILED",
                            event_ref: None,
                        },
                    );
                    s.event(
                        task,
                        "collaboration.child.failed",
                        json!({"collaborationId":collab_id,"childId":child_id,"state":"FAILED","reasonCode":"COLLAB_PROCESS_EXITED_UNCONFIRMED"}),
                    )?;
                }
            }
            h.collab_peers
                .lock()
                .await
                .get_mut(task)
                .and_then(|m| m.remove(&key));
            pump_collab(h, task, collab_id).await;
        }
        "item/started" => {
            let kind = string(&p["item"], "type").unwrap_or("");
            if adapter::EFFECT_KINDS.contains(&kind) {
                let s = h.store.lock().unwrap();
                s.collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, "RUNNING", None, |r| {
                    let n: u64 = r["toolCalls"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    r["toolCalls"] = json!((n + 1).to_string());
                })?;
                // C2 TOOL_CALL: item type + ref only — command text and tool
                // arguments are never public stream facts.
                let text = format!("tool started: {kind}");
                let _ = s.child_message(
                    task,
                    Some(collab_id),
                    child_id,
                    &store::ChildMsg {
                        kind: "TOOL_CALL",
                        text: &text,
                        status: "RUNNING",
                        event_ref: p["item"]["id"].as_str(),
                    },
                );
            }
        }
        "item/completed" => {
            let item = &p["item"];
            match item["type"].as_str() {
                Some("agentMessage") => {
                    if let Some(text) = item["text"].as_str() {
                        let s = h.store.lock().unwrap();
                        s.collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, "RUNNING", None, |r| {
                            let mut output = r["output"].as_str().unwrap_or("").to_owned();
                            if !output.is_empty() {
                                output.push('\n')
                            }
                            output.push_str(text);
                            if output.len() > AUX_ANSWER_MAX {
                                output.truncate(AUX_ANSWER_MAX);
                                while !output.is_char_boundary(output.len()) {
                                    output.pop();
                                }
                            }
                            r["output"] = json!(output);
                        })?;
                        let _ = s.child_message(
                            task,
                            Some(collab_id),
                            child_id,
                            &store::ChildMsg {
                                kind: "ASSISTANT_MESSAGE",
                                text,
                                status: "RUNNING",
                                event_ref: item["id"].as_str(),
                            },
                        );
                    }
                }
                Some("fileChange") => {
                    // Host-observed deny-path check inside the child's own
                    // checkout: codex writeScope enforcement is post-hoc, so a
                    // violation is a durable fact, not a refused write.
                    let paths: Vec<String> = item["changes"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|c| c["path"].as_str().map(str::to_owned))
                                .collect()
                        })
                        .unwrap_or_default();
                    let child = h.store.lock().unwrap().collab_child(task, collab_id, child_id)?;
                    for path in &paths {
                        if let Some(vkind) = collab_scope_check(&child, path) {
                            h.store.lock().unwrap().event(
                                task,
                                "collaboration.child.scope_violation",
                                json!({"collaborationId":collab_id,"childId":child_id,"path":path,"kind":vkind,"enforcement":"HOST_OBSERVED"}),
                            )?;
                        }
                    }
                    // C2 TOOL_RESULT: the file-change fact names its public
                    // workspace-relative paths, never file bytes.
                    let s = h.store.lock().unwrap();
                    let text = format!("tool completed: fileChange [{}]", paths.join(", "));
                    let _ = s.child_message(
                        task,
                        Some(collab_id),
                        child_id,
                        &store::ChildMsg {
                            kind: "TOOL_RESULT",
                            text: &text,
                            status: "RUNNING",
                            event_ref: item["id"].as_str(),
                        },
                    );
                }
                Some(other) => {
                    let s = h.store.lock().unwrap();
                    let text = format!("tool completed: {other}");
                    let _ = s.child_message(
                        task,
                        Some(collab_id),
                        child_id,
                        &store::ChildMsg {
                            kind: "TOOL_RESULT",
                            text: &text,
                            status: "RUNNING",
                            event_ref: item["id"].as_str(),
                        },
                    );
                }
                _ => {}
            }
        }
        "turn/completed" => {
            let status = string(&p["turn"], "status")?;
            let (mut state, mut reason) = match status {
                "completed" => ("SUCCEEDED", None),
                "failed" => ("FAILED", Some("TURN_FAILED")),
                _ => ("CANCELLED", Some("INTERRUPTED")),
            };
            {
                let mut s = h.store.lock().unwrap();
                // Final accounting boundary (same rule as the parent budget):
                // a cap the observed usage already crossed settles FAILED with
                // reasonCode BUDGET_EXCEEDED — Pi parity, never its own state.
                let mut return_text = String::new();
                if let Ok(child) = s.collab_child(task, collab_id, child_id) {
                    return_text = child["output"].as_str().unwrap_or("").to_owned();
                    let cap = child["budget"]["caps"]["maxTotalTokens"]
                        .as_str()
                        .and_then(|v| v.parse::<u64>().ok());
                    let observed = [
                        &child["usage"]["totalTokens"],
                        &p["tokenUsage"]["total"]["totalTokens"],
                    ]
                    .iter()
                    .filter_map(|v| {
                        v.as_str()
                            .and_then(|s| s.parse::<u64>().ok())
                            .or_else(|| v.as_u64())
                    })
                    .max()
                    .unwrap_or(0);
                    if cap.is_some_and(|c| observed >= c) {
                        state = "FAILED";
                        reason = Some("BUDGET_EXCEEDED");
                    }
                }
                // C2 terminal fact commits INSIDE the transition: RETURN (or
                // STATUS) and the state row agree atomically.
                let status_text = format!("{}: {}", state, reason.unwrap_or(""));
                let msgs = [store::ChildMsg {
                    kind: if state == "SUCCEEDED" { "RETURN" } else { "STATUS" },
                    text: if state == "SUCCEEDED" {
                        &return_text
                    } else {
                        &status_text
                    },
                    status: state,
                    event_ref: None,
                }];
                let marked = s.collab_child_transition_msgs(task, collab_id, child_id, COLLAB_OPEN, state, reason, |r| {
                    if state == "SUCCEEDED" {
                        // Candidate seal: the durable diff the guarded adoption
                        // later validates. A diff failure degrades the child to
                        // a non-adoptable SUCCEEDED state honestly.
                        r["candidate"] = collab_candidate(r).unwrap_or_else(|e| {
                            json!({"error":safe_code(&e),"computedAt":store::now()})
                        });
                    }
                }, &msgs)?;
                if marked {
                    let child = s.collab_child(task, collab_id, child_id)?;
                    let kind = if state == "SUCCEEDED" {
                        "collaboration.child.completed"
                    } else {
                        "collaboration.child.failed"
                    };
                    s.event(
                        task,
                        kind,
                        json!({"collaborationId":collab_id,"childId":child_id,"state":state,"reasonCode":reason,"usage":child["usage"],"outputBytes":child["output"].as_str().map(|o|o.len().to_string()),"toolCalls":child["toolCalls"],"candidateFiles":child["candidate"]["fileCount"],"candidateDigest":child["candidate"]["digest"]}),
                    )?;
                }
            }
            if let Some(peer) = h
                .collab_peers
                .lock()
                .await
                .get(task)
                .and_then(|m| m.get(&key))
                .cloned()
            {
                let _ = peer.stop().await;
            }
            h.collab_peers
                .lock()
                .await
                .get_mut(task)
                .and_then(|m| m.remove(&key));
            pump_collab(h, task, collab_id).await;
        }
        "thread/tokenUsage/updated" => {
            // Independent child ledger: cumulative counters on the child row
            // only; the parent usageTotals/budget row are untouched. A usage
            // report can arrive before OR after the turn's completion fact.
            let usage = &p["tokenUsage"]["total"];
            if usage.is_object() {
                let mut totals = serde_json::Map::new();
                for d in ["inputTokens", "cachedInputTokens", "outputTokens", "reasoningOutputTokens", "totalTokens"] {
                    totals.insert(d.into(), usage[d].clone());
                }
                let child = {
                    let s = h.store.lock().unwrap();
                    s.collab_record_usage(task, collab_id, child_id, &Value::from(totals))?;
                    s.collab_child(task, collab_id, child_id)?
                };
                let cap = child["budget"]["caps"]["maxTotalTokens"]
                    .as_str()
                    .and_then(|v| v.parse::<u64>().ok());
                let observed = usage["totalTokens"]
                    .as_str()
                    .and_then(|v| v.parse::<u64>().ok())
                    .or_else(|| usage["totalTokens"].as_u64())
                    .unwrap_or(0);
                if cap.is_some_and(|c| observed >= c)
                    && child["state"].as_str() == Some("RUNNING")
                {
                    let h2 = h.clone();
                    let task2 = task.to_owned();
                    let cid = collab_id.to_owned();
                    let ch = child_id.to_owned();
                    tokio::spawn(async move { collab_budget_stop(h2, task2, cid, ch).await });
                }
            }
        }
        _ => {}
    }
    Ok(())
}
// Soft child budget: the cap can only trip at a real observed usage boundary —
// the turn is interrupted and the managed peer terminated, mirroring the
// parent budget rule without touching the parent ledger. Pi parity: the
// terminal state is FAILED and BUDGET_EXCEEDED is the reasonCode.
async fn collab_budget_stop(h: Shared, task: String, collab_id: String, child_id: String) {
    {
        let s = h.store.lock().unwrap();
        if s
            .collab_child_transition(&task, &collab_id, &child_id, COLLAB_OPEN, "FAILED", Some("BUDGET_EXCEEDED"), |_| {})
            .unwrap_or(false)
        {
            let _ = s.child_message(
                &task,
                Some(&collab_id),
                &child_id,
                &store::ChildMsg {
                    kind: "STATUS",
                    text: "FAILED: BUDGET_EXCEEDED",
                    status: "FAILED",
                    event_ref: None,
                },
            );
            let _ = s.event(
                &task,
                "collaboration.child.budget_exceeded",
                json!({"collaborationId":collab_id,"childId":child_id,"field":"maxTotalTokens","state":"FAILED","reasonCode":"BUDGET_EXCEEDED"}),
            );
            let _ = s.event(
                &task,
                "collaboration.child.failed",
                json!({"collaborationId":collab_id,"childId":child_id,"state":"FAILED","reasonCode":"BUDGET_EXCEEDED"}),
            );
        }
    }
    let key = collab_key(&collab_id, &child_id);
    let peer = h
        .collab_peers
        .lock()
        .await
        .get(&task)
        .and_then(|m| m.get(&key))
        .cloned();
    if let Some(peer) = peer {
        let (thread, turn) = {
            let s = h.store.lock().unwrap();
            s.collab_child(&task, &collab_id, &child_id)
                .map(|c| {
                    (
                        c["nativeSessionId"].as_str().map(str::to_owned),
                        c["nativeTurnId"].as_str().map(str::to_owned),
                    )
                })
                .unwrap_or((None, None))
        };
        if let (Some(thread), Some(turn)) = (thread, turn) {
            let _ = timeout(
                Duration::from_secs(3),
                peer.rpc("turn/interrupt", json!({"threadId":thread,"turnId":turn})),
            )
            .await;
        }
        let _ = peer.stop().await;
    }
    h.collab_peers
        .lock()
        .await
        .get_mut(&task)
        .and_then(|m| m.remove(&key));
    pump_collab(&h, &task, &collab_id).await;
}
// Deadline sweep: a RUNNING child past its durable deadlineEpochMillis is
// cancelled and its peer released — the same cadence and vocabulary as the Pi
// host's sweepCollabDeadlines.
async fn sweep_collab_deadlines(h: &Shared) {
    let expired: Vec<(String, String, String)> = {
        let s = h.store.lock().unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut out = vec![];
        if let Ok(tasks) = s.tasks() {
            for t in tasks {
                let tid = t["taskId"].as_str().unwrap_or("").to_owned();
                if let Ok(collabs) = s.collaborations(&tid) {
                    for collab in collabs {
                        let cid = collab["collaborationId"].as_str().unwrap_or("").to_owned();
                        for ch in collab["children"].as_array().map(|a|a.as_slice()).unwrap_or(&[]) {
                            if ch["state"].as_str() == Some("RUNNING")
                                && ch["deadlineEpochMillis"]
                                    .as_str()
                                    .and_then(|v| v.parse::<u64>().ok())
                                    .is_some_and(|d| now_ms > d)
                            {
                                out.push((
                                    tid.clone(),
                                    cid.clone(),
                                    ch["childId"].as_str().unwrap_or("").to_owned(),
                                ));
                            }
                        }
                    }
                }
            }
        }
        out
    };
    for (task, cid, ch) in expired {
        collab_finish_child(h, &task, &cid, &ch, "CANCELLED", "COLLAB_DEADLINE", true).await;
    }
}
// Shared child settle path: kill the managed peer (proof first), mark the
// guarded terminal fact, emit the completion event, release the peer, pump.
async fn collab_finish_child(
    h: &Shared,
    task: &str,
    collab_id: &str,
    child_id: &str,
    state: &str,
    reason: &str,
    kill_peer: bool,
) {
    let key = collab_key(collab_id, child_id);
    let peer = h
        .collab_peers
        .lock()
        .await
        .get(task)
        .and_then(|m| m.get(&key))
        .cloned();
    let mut stopped = false;
    if kill_peer {
        if let Some(peer) = peer.clone() {
            stopped = peer.stop().await.is_ok();
        }
    }
    {
        let s = h.store.lock().unwrap();
        let marked = s
            .collab_child_transition(task, collab_id, child_id, COLLAB_OPEN, state, Some(reason), |r| {
                if stopped {
                    r["stopProof"] = json!({"ownerBootId":h.boot,"managedProcessTree":"EXITED","method":"WINDOWS_JOB_ACTIVE_ZERO","observedAt":store::now()});
                }
            })
            .unwrap_or(false);
        if marked {
            // C2: every host-driven terminal write lands on the public stream.
            let text = format!("{state}: {reason}");
            let _ = s.child_message(
                task,
                Some(collab_id),
                child_id,
                &store::ChildMsg {
                    kind: "STATUS",
                    text: &text,
                    status: state,
                    event_ref: None,
                },
            );
            let kind = if state == "SUCCEEDED" {
                "collaboration.child.completed"
            } else if state == "BLOCKED" {
                "collaboration.child.blocked"
            } else {
                "collaboration.child.failed"
            };
            let _ = s.event(
                task,
                kind,
                json!({"collaborationId":collab_id,"childId":child_id,"state":state,"reasonCode":reason}),
            );
        }
    }
    h.collab_peers
        .lock()
        .await
        .get_mut(task)
        .and_then(|m| m.remove(&key));
    pump_collab(h, task, collab_id).await;
}
// Collaboration STOP: PENDING children become BLOCKED, the collaboration seals
// CANCELLED, RUNNING children are drained through the shared finish path with
// the caller's stop reason.
async fn collab_stop(h: Shared, task: &str, collab_id: &str, b: &Value) -> Result<(u16, Value)> {
    exact(b, &["requestKey"])?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        s.collaboration(task, collab_id)?; // COLLABORATION_NOT_FOUND -> 404
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
    }
    let accepted = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"ACCEPTED","collaborationId":collab_id,"operation":"COLLABORATION_STOP"});
    let running: Vec<String>;
    {
        let mut s = h.store.lock().unwrap();
        s.accept(task, &key, b, &accepted, task)?;
        let collab = s.collaboration(task, collab_id)?;
        if collab["state"].as_str() != Some("RUNNING") {
            s.receipt(
                task,
                &key,
                "APPLIED",
                &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","collaborationId":collab_id,"operation":"COLLABORATION_STOP","detail":format!("Collaboration already {}",collab["state"].as_str().unwrap_or("TERMINAL"))}),
            )?;
            return Ok((
                202,
                json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","collaborationId":collab_id,"operation":"COLLABORATION_STOP","detail":format!("Collaboration already {}",collab["state"].as_str().unwrap_or("TERMINAL"))}),
            ));
        }
        // Pending dependents settle BLOCKED + the collab seals CANCELLED in one
        // transaction before any child is killed, so the pump can never launch
        // another child for this collaboration.
        running = s.collab_stop_mark(task, collab_id, "COLLABORATION_STOP")?;
    }
    // C7: report every child this stop touched — CANCELLED runners and the
    // dependents the mark transaction settled BLOCKED under the same reason.
    let mut affected: Vec<Value> = running
        .iter()
        .map(|ch| {
            json!({"kind":"COLLABORATION_CHILD","collaborationId":collab_id,"childId":ch,"newState":"CANCELLED","reasonCode":"COLLABORATION_STOP"})
        })
        .collect();
    {
        let s = h.store.lock().unwrap();
        for c in s.collab_children(task, collab_id).unwrap_or_default() {
            if c["state"].as_str() == Some("BLOCKED")
                && c["reasonCode"].as_str() == Some("COLLABORATION_STOP")
            {
                affected.push(json!({"kind":"COLLABORATION_CHILD","collaborationId":collab_id,"childId":c["childId"],"newState":"BLOCKED","reasonCode":"COLLABORATION_STOP"}));
            }
        }
    }
    for ch in running {
        collab_finish_child(&h, task, collab_id, &ch, "CANCELLED", "COLLABORATION_STOP", true).await;
    }
    let applied = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","collaborationId":collab_id,"operation":"COLLABORATION_STOP","affectedChildren":affected,"detail":"Collaboration cancelled"});
    {
        let mut s = h.store.lock().unwrap();
        s.receipt(task, &key, "APPLIED", &applied)?;
    }
    Ok((202, applied))
}
// Parent-owner drain: every RUNNING collaboration on the task settles
// CANCELLED with the caller's reason; pending dependents become BLOCKED. Used
// by every real owner stop — task STOP and transition quiesce alike.
async fn collab_stop_all(h: &Shared, task: &str, reason: &str) -> Vec<Value> {
    let mut affected: Vec<Value> = vec![];
    let collab_ids: Vec<String> = {
        let s = h.store.lock().unwrap();
        s.collaborations(task)
            .unwrap_or_default()
            .iter()
            .filter(|c| c["state"].as_str() == Some("RUNNING"))
            .map(|c| c["collaborationId"].as_str().unwrap_or("").to_owned())
            .collect()
    };
    for cid in collab_ids {
        let running: Vec<String> = {
            let mut s = h.store.lock().unwrap();
            s.collab_stop_mark(task, &cid, reason).unwrap_or_default()
        };
        for ch in &running {
            affected.push(json!({
                "kind":"COLLABORATION_CHILD",
                "collaborationId":cid,
                "childId":ch,
                "auxSessionId":Value::Null,
                "newState":"CANCELLED",
                "reasonCode":reason,
            }));
            collab_finish_child(h, task, &cid, ch, "CANCELLED", reason, true).await;
        }
        // Pending dependents settled BLOCKED inside the same mark transaction
        // are affected children too — report them under the same reason.
        let blocked: Vec<String> = {
            let s = h.store.lock().unwrap();
            s.collab_children(task, &cid)
                .unwrap_or_default()
                .iter()
                .filter(|c| {
                    c["state"].as_str() == Some("BLOCKED")
                        && c["reasonCode"].as_str() == Some(reason)
                })
                .map(|c| c["childId"].as_str().unwrap_or("").to_owned())
                .collect()
        };
        for ch in blocked {
            affected.push(json!({
                "kind":"COLLABORATION_CHILD",
                "collaborationId":cid,
                "childId":ch,
                "auxSessionId":Value::Null,
                "newState":"BLOCKED",
                "reasonCode":reason,
            }));
        }
    }
    affected
}
// ======================= W5b — guarded adoption (P15) =======================
// Adoption path normalization: forward-slash relative path, no absolute or
// drive-qualified forms, no empty/. /.. segments. Mirrors Pi adoptRelPath.
fn adopt_rel_path(raw: &Value) -> Result<String> {
    let s = raw
        .as_str()
        .filter(|s| !s.trim().is_empty() && s.len() <= 1024 && !s.contains('\0'))
        .ok_or_else(|| anyhow!("INVALID_ADOPTION_PATHS"))?;
    let norm = s.replace('\\', "/");
    if norm.starts_with('/') || norm.len() >= 3 && norm.as_bytes()[1] == b':' && norm.as_bytes()[2] == b'/' {
        bail!("INVALID_ADOPTION_PATHS")
    }
    let parts: Vec<&str> = norm
        .split('/')
        .filter(|p| !p.is_empty() && *p != ".")
        .collect();
    if parts.is_empty() || parts.iter().any(|p| *p == "..") {
        bail!("INVALID_ADOPTION_PATHS")
    }
    Ok(parts.join("/"))
}
// Best-effort canonicalization: canonicalize the deepest existing ancestor and
// append the unresolved tail — mirrors Pi realpathBestEffort so conflict checks
// on a not-yet-existing target still bind to a real path.
fn realpath_best_effort(p: &Path) -> PathBuf {
    let mut cur = lexical(p);
    let mut tail: Vec<std::ffi::OsString> = vec![];
    for _ in 0..64 {
        match canonical_path(&cur) {
            Ok(mut real) => {
                for t in tail.iter().rev() {
                    real.push(t)
                }
                return real;
            }
            Err(_) => {
                let Some(parent) = cur.parent().map(PathBuf::from) else {
                    return cur;
                };
                if parent == cur {
                    return cur;
                }
                if let Some(name) = cur.file_name() {
                    tail.push(name.to_owned());
                }
                cur = parent;
            }
        }
    }
    cur
}
// Path key: separators normalized, trailing slashes trimmed, case-folded on
// Windows — mirrors Pi pathKey for within() comparisons.
fn path_key(p: &Path) -> String {
    let n = p.to_string_lossy().replace('\\', "/");
    let n = n.trim_end_matches('/').to_string();
    if cfg!(windows) { n.to_lowercase() } else { n }
}
fn within_key(child: &str, root: &str) -> bool {
    child == root || child.starts_with(&format!("{root}/"))
}
// Guarded file-level adoption. Baseline vs live candidate decides the file
// set; the parent's current bytes vs the child baseline decides drift. Zero
// writes on any conflict.
// One planned file move for an adoption; `parent_sha`/`kind` are captured at
// plan time so a preview can report the exact effect the apply path will run.
struct AdoptPlanned {
    rel: String,
    meta: Option<Value>,
    base_sha: Option<String>,
    parent_sha: Option<String>,
    kind: &'static str,
}
struct AdoptionPlan {
    t: Value,
    child: Value,
    checkout_real: PathBuf,
    ws_real: PathBuf,
    cand_files: serde_json::Map<String, Value>,
    adopt: Vec<String>,
    plan: Vec<AdoptPlanned>,
    conflicts: Vec<Value>,
    already: Vec<String>,
    candidate_digest: Value,
}
// Shared adoption planner (C4): computes the exact file set an apply would
// move — candidate diff vs sealed baseline, parent drift/scope/escape
// conflicts — without writing. collab_adopt applies it; collab_preview_create
// freezes it. Read-only by construction: every fs access is a stat or read.
fn adoption_plan(
    h: &Shared,
    task: &str,
    collab_id: &str,
    child_id: &str,
    mode: &str,
    paths: &Value,
) -> Result<AdoptionPlan> {
    let (t, child) = {
        let s = h.store.lock().unwrap();
        (
            s.task(task)?,
            s.collab_child(task, collab_id, child_id)?, // 404 COLLABORATION_CHILD_NOT_FOUND
        )
    };
    if child["state"].as_str() != Some("SUCCEEDED") {
        bail!("COLLABORATION_CHILD_NOT_FINISHED")
    }
    let mut selected: Option<Vec<String>> = None;
    if mode == "FILES" {
        let raw = paths
            .as_array()
            .filter(|a| !a.is_empty() && a.len() <= COLLAB_MAX_PATHS)
            .ok_or_else(|| anyhow!("INVALID_ADOPTION_PATHS"))?;
        let mut paths = vec![];
        let mut seen = HashSet::new();
        for v in raw {
            let rel = adopt_rel_path(v)?;
            if seen.insert(rel.clone()) {
                paths.push(rel)
            }
        }
        selected = Some(paths);
    } else if !paths.is_null() {
        bail!("INVALID_ADOPTION_PATHS")
    }
    // Conflict checks need real anchors even when targets do not exist yet.
    let checkout_real = realpath_best_effort(Path::new(
        child["workspace"]
            .as_str()
            .ok_or_else(|| anyhow!("ADOPTION_CHECKOUT_UNAVAILABLE"))?,
    ));
    let ws_real = realpath_best_effort(Path::new(
        t["workspace"]
            .as_str()
            .ok_or_else(|| anyhow!("ADOPTION_CHECKOUT_UNAVAILABLE"))?,
    ));
    if !ws_real.is_dir() || !checkout_real.is_dir() {
        bail!("ADOPTION_CHECKOUT_UNAVAILABLE")
    }
    let candidate = collab_candidate(&child)?;
    if candidate["error"].is_string() {
        bail!("{}", candidate["error"].as_str().unwrap_or("CANDIDATE_TOO_LARGE"))
    }
    let baseline = &child["baseline"]["files"];
    let cand_files = candidate["files"].as_object().cloned().unwrap_or_default();
    let mut candidates: Vec<String> = cand_files.keys().cloned().collect();
    for d in candidate["deletions"].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
        if let Some(rel) = d.as_str() {
            candidates.push(rel.to_owned())
        }
    }
    candidates.sort();
    let cand_set: HashSet<String> = candidates.iter().cloned().collect();
    let adopt: Vec<String> = match &selected {
        Some(sel) => {
            for rel in sel {
                if !cand_set.contains(rel) {
                    bail!("ADOPTION_PATH_NOT_CANDIDATE")
                }
            }
            sel.clone()
        }
        None => candidates.clone(),
    };
    // Per-file plan: conflict names PARENT_DRIFT, scope breach SCOPE_DENIED,
    // realpath escape CANDIDATE_ESCAPE/PATH_ESCAPE. Checks run BEFORE writes.
    let mut plan: Vec<AdoptPlanned> = vec![];
    let mut conflicts: Vec<Value> = vec![];
    let mut already: Vec<String> = vec![];
    for rel in &adopt {
        let meta = cand_files.get(rel).cloned();
        let base_sha = baseline[rel]["sha256"].as_str().map(str::to_owned);
        let target_checkout = checkout_real.join(rel);
        if meta.is_some() && !within_key(&path_key(&realpath_best_effort(&target_checkout)), &path_key(&checkout_real)) {
            conflicts.push(json!({"path":rel,"code":"CANDIDATE_ESCAPE"}));
            continue;
        }
        let target = ws_real.join(rel);
        if !within_key(&path_key(&realpath_best_effort(&target)), &path_key(&ws_real)) {
            conflicts.push(json!({"path":rel,"code":"PATH_ESCAPE"}));
            continue;
        }
        // Parent writeScope deny paths are workspace-relative prefixes; the
        // adopted rel path must not land under one.
        if t["writeScope"]["denyPaths"]
            .as_array()
            .is_some_and(|a| {
                a.iter().any(|d| {
                    let d = d.as_str().unwrap_or("").replace('\\', "/");
                    let d = d.trim_end_matches('/');
                    !d.is_empty() && (rel == d || rel.starts_with(&format!("{d}/")))
                })
            })
        {
            conflicts.push(json!({"path":rel,"code":"SCOPE_DENIED"}));
            continue;
        }
        // Parent current state vs the child's sealed baseline.
        let target_meta = fs::symlink_metadata(&target).ok();
        let (parent_is_file, parent_other, parent_sha) = match &target_meta {
            Some(m) if m.is_file() => {
                let bytes = fs::read(&target)?;
                (true, false, Some(hash(&bytes)))
            }
            Some(_) => (false, true, None),
            None => (false, false, None),
        };
        let identical = match (&meta, &parent_sha) {
            (Some(m), Some(ps)) => m["sha256"].as_str() == Some(ps.as_str()),
            (None, None) => base_sha.is_none() && !parent_other && !parent_is_file,
            _ => false,
        };
        if identical {
            already.push(rel.clone());
            continue;
        }
        let clean = if parent_is_file {
            parent_sha.as_deref() == base_sha.as_deref()
        } else {
            !parent_other && base_sha.is_none()
        };
        if !clean {
            conflicts.push(json!({
                "path": rel,
                "code": "PARENT_DRIFT",
                "baselineSha256": base_sha,
                "parentSha256": parent_sha,
                "candidateSha256": meta.as_ref().map(|m| m["sha256"].clone()),
            }));
            continue;
        }
        plan.push(AdoptPlanned {
            rel: rel.clone(),
            kind: if meta.is_some() {
                if parent_is_file {
                    "MODIFY"
                } else {
                    "CREATE"
                }
            } else {
                "DELETE"
            },
            parent_sha,
            meta,
            base_sha,
        });
    }
    Ok(AdoptionPlan {
        t,
        child,
        checkout_real,
        ws_real,
        cand_files,
        adopt,
        plan,
        conflicts,
        already,
        candidate_digest: candidate["digest"].clone(),
    })
}
// C4 — adoption preview: run the shared planner and freeze its outcome as a
// durable preview row keyed by (task, previewId). No parent write happens
// here; the apply path later re-verifies the preview digest and staleness.
async fn collab_preview_create(
    h: Shared,
    task: &str,
    collab_id: &str,
    b: &Value,
) -> Result<(u16, Value)> {
    exact(b, &["requestKey", "childId", "mode", "paths"])?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        s.collaboration(task, collab_id)?;
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
    }
    let child_id = b["childId"]
        .as_str()
        .filter(|s| collab_child_id_ok(s))
        .ok_or_else(|| anyhow!("INVALID_COLLABORATION_CHILD_ID"))?
        .to_owned();
    let mode = match b["mode"].as_str() {
        Some("ALL") => "ALL",
        Some("FILES") => "FILES",
        _ => bail!("INVALID_ADOPTION_MODE"),
    };
    let p = adoption_plan(&h, task, collab_id, &child_id, mode, &b["paths"])?;
    ensure_not_sealed(&p.t)?;
    let files: Vec<Value> = p
        .plan
        .iter()
        .map(|f| {
            json!({
                "path": f.rel,
                "kind": f.kind,
                "fromSha256": f.parent_sha,
                "toSha256": f.meta.as_ref().map(|m| m["sha256"].clone()),
                "baselineSha256": f.base_sha,
            })
        })
        .collect();
    // The preview's plan digest binds everything the apply path re-verifies:
    // child + mode + selected paths + the per-file effect list + candidate.
    let plan_digest = hash(store::canonical(&json!({
        "schema": "adoption-preview/1",
        "collaborationId": collab_id,
        "childId": child_id,
        "mode": mode,
        "paths": p.adopt,
        "files": files,
        "conflicts": p.conflicts,
        "candidateDigest": p.candidate_digest,
    }))?);
    let row = json!({
        "previewId": format!("pv-{}", store::id()),
        "collaborationId": collab_id,
        "childId": child_id,
        "planDigest": plan_digest,
        "files": files,
        "paths": if mode == "FILES" { json!(p.adopt) } else { Value::Null },
        "candidateDigest": p.candidate_digest,
        "expiresAt": store::now_plus_ms(COLLAB_PLAN_TTL_MS),
    });
    let mut result = json!({
        "kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED",
        "operation":"COLLABORATION_PREVIEW",
        "previewId":row["previewId"],"collaborationId":collab_id,"childId":child_id,
        "mode":mode,"planDigest":plan_digest,"candidateDigest":p.candidate_digest,
        "expiresAt":row["expiresAt"],
        "fileEffect":{"mode":mode,"files":files,"conflicts":p.conflicts,"alreadyApplied":p.already},
        "verification":{"status":"NOT_RUN","detail":"PREVIEW_ONLY"},
        "contextContinuation":{"mode":"INHERIT_PARENT","contextPackageIds":p.child["contextPackageIds"]},
    });
    result["wouldConflict"] = json!(!p.conflicts.is_empty());
    {
        let mut s = h.store.lock().unwrap();
        s.accept_preview(task, &row, &key, b, &result)?;
    }
    Ok((202, result))
}
async fn collab_adopt(h: Shared, task: &str, collab_id: &str, b: &Value) -> Result<(u16, Value)> {
    exact(
        b,
        &[
            "requestKey",
            "childId",
            "mode",
            "paths",
            "expectedPreviewId",
            "expectedPreviewDigest",
        ],
    )?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        s.collaboration(task, collab_id)?; // COLLABORATION_NOT_FOUND -> 404
        if let Some(mut v) = s.existing(task, &key, b)? {
            v["replayed"] = json!(true);
            return Ok((if v["outcome"] == "CONFLICT" { 409 } else { 202 }, v));
        }
    }
    let child_id = b["childId"]
        .as_str()
        .filter(|s| collab_child_id_ok(s))
        .ok_or_else(|| anyhow!("INVALID_COLLABORATION_CHILD_ID"))?
        .to_owned();
    let mode = match b["mode"].as_str() {
        Some("ALL") => "ALL",
        Some("FILES") => "FILES",
        _ => bail!("INVALID_ADOPTION_MODE"),
    };
    let p = adoption_plan(&h, task, collab_id, &child_id, mode, &b["paths"])?;
    // A sealed parent is read-only: no adoption may move bytes into it.
    ensure_not_sealed(&p.t)?;
    // Stale-preview guards (C4): a named preview must exist, be unexpired and
    // match the plan this call just re-derived — the digest recompute catches
    // parent drift even when the preview row is still fresh.
    if !b["expectedPreviewId"].is_null() || !b["expectedPreviewDigest"].is_null() {
        let files: Vec<Value> = p
            .plan
            .iter()
            .map(|f| {
                json!({
                    "path": f.rel, "kind": f.kind,
                    "fromSha256": f.parent_sha,
                    "toSha256": f.meta.as_ref().map(|m| m["sha256"].clone()),
                    "baselineSha256": f.base_sha,
                })
            })
            .collect();
        let plan_digest = hash(store::canonical(&json!({
            "schema": "adoption-preview/1",
            "collaborationId": collab_id,
            "childId": child_id,
            "mode": mode,
            "paths": p.adopt,
            "files": files,
            "conflicts": p.conflicts,
            "candidateDigest": p.candidate_digest,
        }))?);
        if let Some(d) = b["expectedPreviewDigest"].as_str() {
            if d != plan_digest {
                bail!("PREVIEW_MISMATCH")
            }
        }
        if let Some(pid) = b["expectedPreviewId"].as_str() {
            let pv = {
                let s = h.store.lock().unwrap();
                s.collab_preview(task, valid_id(pid)?)?
            };
            if pv["expired"] == true {
                bail!("PREVIEW_EXPIRED")
            }
            if pv["planDigest"].as_str() != Some(plan_digest.as_str())
                || pv["collaborationId"].as_str() != Some(collab_id)
                || pv["childId"].as_str() != Some(child_id.as_str())
            {
                bail!("PREVIEW_MISMATCH")
            }
        }
    }
    let AdoptionPlan {
        t: _,
        child: _,
        checkout_real,
        ws_real,
        cand_files,
        adopt,
        plan,
        conflicts,
        already,
        candidate_digest,
    } = p;
    let record = |outcome: &str, extra: Value| -> Result<(u16, Value)> {
        let adoption = json!({
            "requestKey": key, "mode": mode, "outcome": outcome,
            "paths": adopt, "candidateDigest": candidate_digest.clone(), "at": store::now()
        });
        let mut result = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","collaborationId":collab_id,"childId":child_id,"operation":"COLLABORATION_ADOPT","mode":mode,"outcome":outcome});
        let mut payload = serde_json::Map::new();
        payload.insert("collaborationId".into(), json!(collab_id));
        payload.insert("childId".into(), json!(child_id));
        payload.insert("requestKey".into(), json!(key));
        payload.insert("mode".into(), json!(mode));
        payload.insert("outcome".into(), json!(outcome));
        if let Some(obj) = extra.as_object() {
            for (k, v) in obj {
                result[k] = v.clone();
                payload.insert(k.clone(), v.clone());
            }
        }
        let ev = json!({
            "type": if outcome == "CONFLICT" { "collaboration.adoption.conflict" } else { "collaboration.adopted" },
            "payload": Value::Object(payload),
        });
        let mut s = h.store.lock().unwrap();
        s.record_adoption(task, collab_id, &child_id, &adoption, &key, b, &result, &ev)?;
        Ok((if outcome == "CONFLICT" { 409 } else { 202 }, result))
    };
    if !conflicts.is_empty() {
        return record("CONFLICT", json!({"conflicts":conflicts}));
    }
    if plan.is_empty() {
        return record("NO_CHANGES", json!({"alreadyApplied":already}));
    }
    // Zero-write guarantee: all candidate bytes are preloaded and verified
    // before the first parent write; a failing read aborts with no change.
    let mut writes: Vec<(String, Vec<u8>)> = vec![];
    let mut deletes: Vec<String> = vec![];
    for p in &plan {
        if let Some(meta) = &p.meta {
            let source = checkout_real.join(&p.rel);
            let bytes = fs::read(&source).map_err(|_| anyhow!("ADOPTION_CHECKOUT_UNAVAILABLE"))?;
            if hash(&bytes) != meta["sha256"].as_str().unwrap_or("") {
                bail!("ADOPTION_CANDIDATE_CHANGED")
            }
            writes.push((p.rel.clone(), bytes));
        } else {
            deletes.push(p.rel.clone());
        }
    }
    // Second guard pass (TOCTOU): every planned path must still match its
    // sealed baseline right before the first byte moves — drift inside the
    // check/apply window still yields zero writes and a 409.
    let mut late_conflicts: Vec<Value> = vec![];
    for p in &plan {
        let target = ws_real.join(&p.rel);
        let target_meta = fs::symlink_metadata(&target).ok();
        let (is_file, other, sha) = match &target_meta {
            Some(m) if m.is_file() => (true, false, Some(hash(&fs::read(&target)?))),
            Some(_) => (false, true, None),
            None => (false, false, None),
        };
        let identical = match (&p.meta, &sha) {
            (Some(m), Some(ps)) => m["sha256"].as_str() == Some(ps.as_str()),
            (None, None) => p.base_sha.is_none() && !other && !is_file,
            _ => false,
        };
        let clean = if is_file {
            sha.as_deref() == p.base_sha.as_deref()
        } else {
            !other && p.base_sha.is_none()
        };
        if !clean && !identical {
            late_conflicts.push(json!({
                "path": p.rel,
                "code": "PARENT_DRIFT",
                "baselineSha256": p.base_sha.clone(),
                "parentSha256": sha,
                "candidateSha256": p.meta.as_ref().map(|m| m["sha256"].clone()),
            }));
        }
    }
    if !late_conflicts.is_empty() {
        return record("CONFLICT", json!({"conflicts":late_conflicts}));
    }
    let mut applied: Vec<Value> = vec![];
    for (rel, bytes) in &writes {
        let target = ws_real.join(rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&target, bytes)?;
        applied.push(json!({"path":rel,"sha256":meta_sha(&cand_files, rel),"action":"WROTE","bytes":bytes.len().to_string()}));
    }
    for rel in &deletes {
        let target = ws_real.join(rel);
        if target.is_file() {
            fs::remove_file(&target)?;
        }
        applied.push(json!({"path":rel,"sha256":Value::Null,"action":"DELETED"}));
    }
    record("ADOPTED", json!({"files":applied}))
}
fn meta_sha(cand_files: &serde_json::Map<String, Value>, rel: &str) -> Value {
    cand_files
        .get(rel)
        .map(|m| m["sha256"].clone())
        .unwrap_or(Value::Null)
}
// Deliver: inject the completed aux answer into the MAIN task through the same
// follow-up path a client would use — same-thread turn/start on a live peer,
// or the stopped-session reopen admission when the main peer is gone. The aux
// row's DELIVERED mark commits in the same transaction as the admission, so an
// answer can only enter the main thread once.
async fn aux_deliver(h: Shared, task: &str, aux_id: &str, b: &Value) -> Result<(u16, Value)> {
    exact(b, &["requestKey"])?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
        let aux = s.aux_session(task, aux_id)?;
        // Pi vocabulary: already-delivered and unfinished sessions are
        // distinct intake refusals.
        if aux["deliveredAt"].is_string() {
            bail!("AUX_ALREADY_DELIVERED")
        }
        if aux["state"] != "SUCCEEDED" {
            bail!("AUX_NOT_FINISHED")
        }
    }
    let t = h.store.lock().unwrap().task(task)?;
    // Delivering an answer writes a follow-up into the parent thread — a
    // sealed source refuses it like any other mutation.
    ensure_not_sealed(&t)?;
    match t["state"].as_str() {
        Some("BUDGET_EXCEEDED") => bail!("BUDGET_EXCEEDED"),
        Some("BINDING_UNAVAILABLE") => bail!("BINDING_UNAVAILABLE"),
        _ => {}
    }
    let peer = h.peers.lock().await.get(task).cloned();
    let live = match &peer {
        Some(p) => p.alive().await?,
        None => false,
    };
    // The same guarded follow-up admission a client would use: a live
    // SUCCEEDED session takes a normal turn; without a live writer the only
    // honest admission is the completed-session reopen (Pi N17 parity).
    let reopen = if t["state"] == "SUCCEEDED" && live {
        false
    } else if !live {
        true
    } else {
        bail!("NATIVE_NOT_IDLE")
    };
    require_model_configuration(&h)?;
    if reopen {
        verify(&h.root, &h.registry)?;
    }
    let aux = h.store.lock().unwrap().aux_session(task, aux_id)?;
    let content = format!(
        "Auxiliary session {} completed its read-only research.\nQuestion: {}\n\nAnswer:\n{}",
        aux_id,
        aux["question"].as_str().unwrap_or(""),
        aux["answer"].as_str().unwrap_or("").trim()
    );
    let content: String = content.chars().take(16000).collect();
    let accepted = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"ACCEPTED","detail":"Durably accepted","auxDelivery":aux_id});
    {
        let mut s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, b)? {
            return Ok((202, v));
        }
        let current = s.task(task)?;
        if reopen {
            // Same provenance gates as a stopped-session follow-up.
            reopen_blockers(&s, &current, live, h.max_active, h.registry.version.as_str())?;
            let segment = current["segmentId"]
                .as_str()
                .unwrap_or("segment-legacy")
                .to_owned();
            if s.segment_manifest(task, &segment)?.is_none() {
                seal_manifest(&mut s, &h.root, &current, &segment)?;
            }
            let turn = current["nativeTurnId"].as_str().unwrap_or("").to_owned();
            s.admit_aux_deliver(task, aux_id, &key, b, &accepted, |t| {
                t["reopenFrom"] = json!({"generation":t["generation"],"segmentId":t["segmentId"],"nativeTurnId":t["nativeTurnId"],"commandId":t["currentCommandId"],"stopProof":t["stopProof"],"completedSessionSeal":t["completedSessionSeal"]});
                t["generation"] = json!(store::id());
                t["segmentId"] = json!(format!("segment-{}", store::id()));
                let epoch = t["ownerEpoch"]
                    .as_str()
                    .unwrap_or("0")
                    .parse::<u64>()
                    .unwrap_or(0)
                    + 1;
                t["ownerEpoch"] = json!(epoch.to_string());
                t["ownerBootId"] = json!(h.boot);
                t["currentCommandId"] = json!(key);
                t["state"] = json!("UNKNOWN");
                t["launchState"] = json!("RESTORING");
                t["quiescent"] = json!(false);
                t["auxDeliverFrom"] = json!({"auxSessionId":aux_id,"nativeTurnId":turn});
                t.as_object_mut().unwrap().remove("stopProof");
                t.as_object_mut().unwrap().remove("reasonCode");
                t.as_object_mut().unwrap().remove("completedSessionSeal");
            })?;
        } else {
            if current["state"] != "SUCCEEDED" {
                bail!("NATIVE_NOT_IDLE")
            }
            s.admit_aux_deliver(task, aux_id, &key, b, &accepted, |t| {
                t["state"] = json!("STARTING");
                t["currentCommandId"] = json!(key);
                t["quiescent"] = json!(false);
            })?;
        }
        s.receipt(
            task,
            &key,
            "DISPATCHING",
            &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"DISPATCHING"}),
        )?;
    }
    let task_id = task.to_owned();
    let output = accepted.clone();
    let kind_peer = peer.clone();
    tokio::spawn(async move {
        let result: Result<Value> = async {
            if reopen {
                reopen_session(h.clone(), &task_id, &key, &content).await?;
            } else {
                match kind_peer {
                    Some(p) => start_turn(h.clone(), &task_id, &key, &content, p).await?,
                    None => bail!("NATIVE_NOT_IDLE"),
                }
            }
            Ok(h.store.lock().unwrap().lookup(&task_id, &key)?)
        }
        .await;
        let mut s = h.store.lock().unwrap();
        match result {
            Ok(v) => {
                let phase = v["state"].as_str().unwrap_or("UNKNOWN");
                let _ = s.receipt(&task_id, &key, phase, &v);
                let _ = s.event(&task_id, "request.observed", v);
            }
            Err(e) => {
                let code = safe_code(&e);
                if reopen {
                    let _ = s.update(&task_id, |t| t["reasonCode"] = json!(code));
                }
                let _ = s.receipt(
                    &task_id,
                    &key,
                    "UNKNOWN",
                    &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"UNKNOWN","reasonCode":code}),
                );
                let _ = s.event(
                    &task_id,
                    "request.unconfirmed",
                    json!({"requestKey":key,"code":code}),
                );
            }
        }
    });
    Ok((202, json!({"delivered":output,"auxSessionId":aux_id})))
}
// Assignment transition compatibility: same engine only, the frozen model
// binding only, and no tool-allowlist change — the pinned kernel has no
// per-turn tool allowlist knob, so a requested restriction is refused instead
// of silently ignored.
fn transition_compat(h: &Host, b: &Value) -> Result<Value> {
    if b["targetProfile"].is_null() {
        return Ok(json!({"engineId":"codex","model":h.registry.model,"toolAllowlist":null,"sameEngine":true,"crossEngine":false,"sameNativeSession":true}));
    }
    let p = &b["targetProfile"];
    if !p.is_object() {
        bail!("INVALID_TARGET_PROFILE")
    }
    exact(p, &["engineId", "model", "toolAllowlist"])?;
    if p["engineId"].as_str().unwrap_or("codex") != "codex" {
        bail!("INCOMPATIBLE_TRANSITION")
    }
    if let Some(m) = p["model"].as_str() {
        if m != h.registry.model {
            bail!("INCOMPATIBLE_TRANSITION")
        }
    }
    if !p["toolAllowlist"].is_null() {
        // The pinned kernel exposes no per-turn tool allowlist to rebind, so a
        // requested change is incompatible — never silently ignored.
        let a = p["toolAllowlist"]
            .as_array()
            .ok_or_else(|| anyhow!("INVALID_TARGET_PROFILE"))?;
        if a.is_empty()
            || a.len() > 64
            || a.iter()
                .any(|v| v.as_str().is_none_or(|s| s.is_empty() || s.len() > 128))
        {
            bail!("INVALID_TARGET_PROFILE")
        }
        bail!("INCOMPATIBLE_TRANSITION")
    }
    Ok(json!({"engineId":"codex","model":h.registry.model,"toolAllowlist":null,"sameEngine":true,"crossEngine":false,"sameNativeSession":true}))
}
// Shared owned-writer shutdown (task STOP arm): interrupt the bound turn,
// drain the managed peer, settle open terminals, cancel aux sessions, expire
// pending questions and mark effects unresolved. Terminal states survive
// honestly — only a live or live-ish state becomes CANCELLED (Pi parity) —
// and a provably completed session gains completedSessionSeal so it may still
// reopen or transition under a new owner.
fn mark_stop_requested(t: &mut Value) {
    if t["state"] != "STOP_REQUESTED" {
        t["stopRequestedFromState"] = t["state"].clone();
    }
    t["state"] = json!("STOP_REQUESTED");
}
// Terminal task states: only in these does a detail read report when the
// state itself was observed terminal. UNKNOWN is deliberately absent — it
// names an honesty gap, not a settled end.
const TASK_TERMINAL_STATES: &[&str] = &[
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "BUDGET_EXCEEDED",
    "BINDING_UNAVAILABLE",
    "REJECTED",
];
// The recorded fact that observed the current terminal state, chosen in the
// order of how specifically it names the settle: the durable exceed mark for
// BUDGET_EXCEEDED, the binding-refusal event for BINDING_UNAVAILABLE, the
// bound turn's frozen seal for any settle that came with a completed turn,
// and the stop's own applied-observation for a CANCELLED whose turn never
// sealed. A terminal state with no recorded fact stays absent — the read
// never borrows an unrelated timestamp.
fn terminal_observed_at(s: &Store, t: &Value) -> Result<Option<String>> {
    let state = t["state"].as_str().unwrap_or("");
    let task = t["taskId"].as_str().unwrap_or("");
    match state {
        "BUDGET_EXCEEDED" => {
            if let Some(at) = s
                .budget(task)?
                .and_then(|b| b["exceededAt"].as_str().map(str::to_owned))
            {
                return Ok(Some(at));
            }
        }
        "BINDING_UNAVAILABLE" => {
            if let Some(at) = s.earliest_event_at(task, &["binding.unavailable"])? {
                return Ok(Some(at));
            }
        }
        _ => {}
    }
    let turn = t["nativeTurnId"].as_str().unwrap_or("");
    if !turn.is_empty() {
        if let Some(seal) = s.turn_seal(task, turn)? {
            if let Some(at) = seal["sealedAt"].as_str() {
                return Ok(Some(at.to_owned()));
            }
        }
    }
    if state == "CANCELLED" {
        return s.latest_applied_observation_at(task);
    }
    Ok(None)
}
// Task timing projection for the detail read: every surfaced value is copied
// verbatim from a durable recorded fact — the journal's occurredAt stamps, the
// frozen turn seal and the budget exceed mark. A task that never dispatched a
// turn reports no runStartedAt; only a terminal state reports
// endedAt/completedAt.
fn task_timing(s: &Store, task: &str, v: &mut Value) -> Result<()> {
    if let Some(at) = s.earliest_event_at(task, &["turn.bound", "turn.started"])? {
        v["runStartedAt"] = json!(at);
    }
    if TASK_TERMINAL_STATES.contains(&v["state"].as_str().unwrap_or("")) {
        if let Some(at) = terminal_observed_at(s, v)? {
            v["endedAt"] = json!(at);
            v["completedAt"] = json!(at);
        }
    }
    Ok(())
}
fn settle_stop_state(t: &mut Value) {
    // STOP intake remains fenced until owned writers drain. A completed
    // outcome captured at admission survives that fence and cleanup.
    if matches!(t["state"].as_str(), Some("STOP_REQUESTED" | "CANCELLED"))
        && matches!(t["stopRequestedFromState"].as_str(), Some("SUCCEEDED" | "FAILED" | "BUDGET_EXCEEDED" | "BINDING_UNAVAILABLE")) {
        t["state"] = t["stopRequestedFromState"].clone();
    }
    if !matches!(t["state"].as_str(), Some("SUCCEEDED" | "FAILED" | "BUDGET_EXCEEDED" | "BINDING_UNAVAILABLE")) {
        t["state"] = json!("CANCELLED");
    }
}
fn native_stop_drained(s: &Store, task: &str, turn: &str) -> Result<bool> {
    let t = s.task(task)?;
    let Some(seal) = s.turn_seal(task, turn)? else {
        return Ok(false);
    };
    Ok(!turn.is_empty()
        && t["nativeTurnId"] == turn
        && seal["generation"] == t["generation"]
        && seal["segmentId"] == t["segmentId"]
        && seal["commandId"] == t["currentCommandId"]
        && matches!(
            seal["status"].as_str(),
            Some("completed" | "failed" | "interrupted")
        )
        && s.open_effects(task)? == 0)
}
// Returns the durable affected-children inventory for the drain stage: every
// aux session and collaboration child this quiesce actually stopped, or an
// empty list when the caller asked for no child drain (`child_reasons` None).
async fn stop_owner(h: &Shared, task: &str, child_reasons: Option<(&str, &str)>) -> Result<Vec<Value>> {
    let _preparing = h.preparation.lock().await;
    let t = h.store.lock().unwrap().task(task)?;
    if t["ownerBootId"] != h.boot && t["quiescent"] != true {
        bail!("NATIVE_WRITER_UNPROVEN")
    }
    if t["preparationState"] == "RUNNING" { bail!("NATIVE_PREPARATION_UNPROVEN") }
    let runner_proof = stop_runners(h, task).await;
    let mac_proof = stop_mac_tools(h, task).await;
    let runner_ok = runner_proof.is_ok() && mac_proof.is_ok();
    let mut proven = true;
    let peer = h.peers.lock().await.get(task).cloned();
    let mut interrupt_ack = false;
    // Sessions OPEN at STOP time all live inside the owning job: the job-empty
    // wait in peer.stop() proves their kill even when a racing deferred exec
    // settle marks the durable row first, so the proof names the pre-kill set.
    let was_open: Vec<String> = h
        .store
        .lock()
        .unwrap()
        .terminals(task)?
        .iter()
        .filter(|r| r["state"] == "OPEN")
        .map(|r| r["terminalId"].as_str().unwrap_or("").to_owned())
        .collect();
    if let Some(peer) = peer {
        #[cfg(windows)]
        {
            let has_commands = {
                let s = h.store.lock().unwrap();
                s.active_native_commands(task).unwrap_or(0) > 0
            };
            if has_commands {
                // Closing the producer first loses interrupted exec-session
                // completion facts. Stop only its verified Job children first;
                // native facts, not this exit proof, must settle the effects.
                if let Ok(count) = peer.stop_owned_children().await {
                    if count > 0 {
                        let _ = h.store.lock().unwrap().event(task, "stop.children.exited", json!({"count":count,"nativeEffectsVerified":false}));
                        for _ in 0..20 {
                            if h.store.lock().unwrap().open_effects(task).unwrap_or(-1) == 0 { break; }
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }
        if t["nativeSessionId"].is_string() && t["nativeTurnId"].is_string() {
            interrupt_ack = matches!(timeout(
                Duration::from_secs(2),
                peer.rpc(
                    "turn/interrupt",
                    json!({"threadId":t["nativeSessionId"],"turnId":t["nativeTurnId"]}),
                ),
            )
            .await, Ok(Ok(_)));
        }
        // Interrupt is acknowledged before its item/completed notifications
        // necessarily reach the ledger. Drain their native facts before killing
        // the event producer; the deadline still bounds an unresponsive peer.
        if interrupt_ack {
            for _ in 0..40 {
                let drained = {
                    let s = h.store.lock().unwrap();
                    native_stop_drained(&s, task, t["nativeTurnId"].as_str().unwrap_or("")).unwrap_or(false)
                };
                if drained { break; }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        peer.stop().await?;
        proven = peer.quiescence_proven();
        {
            let s = h.store.lock().unwrap();
            for id in s.terminals_close_open(task, if proven { "TERMINATED" } else { "LOST" }, if proven { "STOP_JOB_TERMINATED" } else { "NATIVE_WRITER_UNPROVEN" })? {
                let _ = s.event(task, "terminal.closed", json!({"terminalId":id,"reason":"STOP_JOB_TERMINATED"}));
            }
        }
        // The interrupted turn's completion seal travels the event channel; a
        // short poll lets it land before the quiescence fact is written.
        let turn = t["nativeTurnId"].as_str().unwrap_or("").to_owned();
        for _ in 0..60 {
            {
                let s = h.store.lock().unwrap();
                if turn.is_empty() || s.turn_seal(task, &turn).ok().flatten().is_some() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    } else {
        let s = h.store.lock().unwrap();
        for id in s.terminals_close_open(task, "LOST", "TRANSPORT_GONE")? {
            let _ = s.event(task, "terminal.closed", json!({"terminalId":id,"reason":"TRANSPORT_GONE"}));
        }
    }
    let mut affected_children: Vec<Value> = vec![];
    if let Some((aux_reason, collab_reason)) = child_reasons {
        affected_children.extend(aux_stop_all(h, task, aux_reason).await);
        // W5b: a real owner stop also drains the task's collaborations — Pi
        // parity: pending dependents become BLOCKED, running children CANCELLED
        // with the caller's reason, never launched silently afterwards.
        affected_children.extend(collab_stop_all(h, task, collab_reason).await);
    }
    {
        let mut s = h.store.lock().unwrap();
        s.update(task, |t| {
            // Pi parity: terminal task states survive STOP untouched; only a
            // live or live-ish state settles CANCELLED.
            settle_stop_state(t);
            if !t["macToolBinding"].is_null() { t["localQuiescent"] = json!(proven && runner_proof.is_ok()); }
            apply_stop_proof(t, proven && runner_ok, None);
            if let Ok(proof) = &runner_proof { apply_runner_proof(t, proof); }
            t["stopProof"]["terminals"] = json!(was_open);
            // C7: the durable stop proof names every child the drain touched.
            t["stopProof"]["affectedChildren"] = json!(affected_children);
            if let Some(questions) = t["inputRequests"].as_object_mut() {
                for q in questions.values_mut() {
                    if q["state"] == "PENDING" {
                        q["state"] = json!("EXPIRED")
                    }
                }
            }
        })?;
        s.effects_unknown(task)?;
        // Pi ordering: effects are marked unknown BEFORE the seal check, so an
        // interrupted effect set can never earn a completed-session seal.
        let cur = s.task(task)?;
        let seal_ok = cur["state"] == "SUCCEEDED" && completed_turn_sealed(&s, &cur);
        let unresolved = s.open_effects(task)?;
        s.update(task, |t| {
            maybe_seal_completed(t, seal_ok, unresolved, "MANAGED_PEER_EXITED")
        })?;
    }
    if !proven { bail!("NATIVE_WRITER_UNPROVEN") }
    runner_proof?;
    mac_proof?;
    Ok(affected_children)
}
async fn transition_route(
    h: Shared,
    task: &str,
    rest: &[&str],
    method: &str,
    b: Value,
) -> Result<(u16, Value)> {
    if method == "GET" && rest.is_empty() {
        return Ok((
            200,
            json!({"items":h.store.lock().unwrap().transitions(task)?}),
        ));
    }
    if method != "POST" || !rest.is_empty() {
        bail!("FIXED_LOCAL_ROUTE_REQUIRED")
    }
    // Same binding surface as a guarded command: requestKey plus the full
    // generation/session/turn/command/ownerEpoch binding. A stale binding is
    // fenced before any compatibility or state evaluation (Pi parity).
    exact(
        &b,
        &[
            "requestKey",
            "targetProfile",
            "generation",
            "nativeSessionId",
            "nativeTurnId",
            "targetCommandId",
            "ownerEpoch",
        ],
    )?;
    let key = valid_id(string(&b, "requestKey")?)?.to_owned();
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, &b)? {
            return Ok((202, v));
        }
    }
    let t = h.store.lock().unwrap().task(task)?;
    guards(&t, &b)?;
    // A sealed source is immutable: it admits reads, never a new transition.
    ensure_not_sealed(&t)?;
    // Compatibility is evaluated before any state inspection: an incompatible
    // target refuses without touching task state (N18).
    let compat = transition_compat(&h, &b)?;
    if t["state"] == "BUDGET_EXCEEDED" {
        bail!("BUDGET_EXCEEDED")
    }
    let peer = h.peers.lock().await.get(task).cloned();
    let live = match &peer {
        Some(p) => p.alive().await?,
        None => false,
    };
    let transition_id = format!("tr-{}", store::id());
    // Codex exposes no boundary pause/checkpoint, so a LIVE owner is quiesced
    // by the owned-writer STOP machinery itself — interrupt the bound turn,
    // drain the managed peer (kill-on-close job proves the tree), close
    // terminals, expire questions, mark effects unknown. That is the honest
    // codex equivalent of Pi's boundary checkpoint (MANAGED_STOP_QUIESCE).
    // An already-quiesced task follows Pi Path C: only a PROVEN completed
    // session admits (COMPLETED_SESSION). Any other shape refuses.
    let mut method_name = "COMPLETED_SESSION";
    let mut already_quiescent = true;
    // Children the live quiesce actually drained; a Path-C completed session
    // had its drain proven earlier, so the fact list stays honestly empty.
    let mut affected_children: Vec<Value> = vec![];
    if live || t["quiescent"] != true {
        if matches!(
            t["state"].as_str(),
            Some("FAILED" | "CANCELLED" | "REJECTED" | "BINDING_UNAVAILABLE" | "UNKNOWN")
        ) {
            bail!("TRANSITION_STATE_INCOMPATIBLE")
        }
        if t["ownerBootId"].as_str() != Some(h.boot.as_str()) {
            // A restarted task's owner is gone — nothing left to quiesce.
            bail!("NATIVE_NOT_IDLE")
        }
        // A live-owner quiesce stops exactly what a real STOP stops: the owner
        // peer AND the aux sessions + collaboration children — a child can
        // never outlive the owner epoch that launched it. The returned
        // inventory lands on the transition facts below.
        affected_children = stop_owner(&h, task, Some(("TRANSITION_QUIESCE", "PARENT_TRANSITION"))).await?;
        method_name = "MANAGED_STOP_QUIESCE";
        already_quiescent = false;
        let cur = h.store.lock().unwrap().task(task)?;
        if cur["quiescent"] != true {
            let _ = h.store.lock().unwrap().event(
                task,
                "transition.failed",
                json!({"transitionId":transition_id,"requestKey":key,"reasonCode":"TRANSITION_QUIESCE_UNPROVEN"}),
            );
            bail!("TRANSITION_STATE_INCOMPATIBLE")
        }
    }
    let t = h.store.lock().unwrap().task(task)?;
    if already_quiescent {
        // Pi Path C: only a PROVEN completed session continues — SUCCEEDED,
        // sealed, same engine version, no open questions.
        let qualifies = t["nativeSessionId"].is_string()
            && t["nativeTurnId"].is_string()
            && t["state"] == "SUCCEEDED"
            && t["engineVersion"].as_str() == Some(h.registry.version.as_str())
            && completed_session_ok(&h.store.lock().unwrap(), &t)
            && !t["inputRequests"].as_object().is_some_and(|q| {
                q.values()
                    .any(|q| q["state"] == "PENDING" || q["state"] == "DISPATCH_STARTED")
            });
        if !qualifies {
            bail!("TRANSITION_STATE_INCOMPATIBLE")
        }
    }
    require_model_configuration(&h)?;
    verify(&h.root, &h.registry)?;
    let from_epoch = t["ownerEpoch"].as_str().unwrap_or("1").to_owned();
    let to_epoch = (from_epoch.parse::<u64>().unwrap_or(0) + 1).to_string();
    let from_segment = t["segmentId"].as_str().unwrap_or("").to_owned();
    let from_generation = t["generation"].clone();
    let from_command = t["currentCommandId"].clone();
    // The continuation is the fixed Pi Path-C text: the new owner continues
    // the original task on the same native session.
    let continuation = "Assignment transitioned to a new owner epoch on the same native session (same engine codex). The previous owner stopped after proven completion. Continue the original task.".to_string();
    {
        let s = h.store.lock().unwrap();
        s.event(
            task,
            "transition.quiesced",
            json!({"transitionId":transition_id,"requestKey":key,"method":method_name,"alreadyQuiescent":already_quiescent,"proof":t["completedSessionSeal"]["quiescenceMethod"].as_str().unwrap_or("SEALED_COMPLETED_SESSION"),"affectedChildren":affected_children}),
        )?;
    }
    // One atomic admission: rotated generation/segment/epoch on the same
    // thread + durable transition row + request + both facts.
    let accepted = json!({"kind":"TRANSITION","requestKey":key,"state":"ACCEPTED","transitionId":transition_id,"fromEpoch":from_epoch,"toEpoch":to_epoch});
    {
        let mut s = h.store.lock().unwrap();
        if let Some(v) = s.existing(task, &key, &b)? {
            return Ok((202, v));
        }
        let current = s.task(task)?;
        // The task must still be the exact quiesced owner we inspected: a
        // racing admission that rotated the generation or epoch is fenced out.
        if current["generation"] != from_generation
            || current["ownerEpoch"].as_str() != Some(from_epoch.as_str())
            || current["currentCommandId"] != from_command
        {
            bail!("NATIVE_TARGET_CHANGED")
        }
        let still = if method_name == "COMPLETED_SESSION" {
            current["state"] == "SUCCEEDED"
                && current["quiescent"] == true
                && !live
                && completed_session_ok(&s, &current)
        } else {
            current["quiescent"] == true
        };
        if !still {
            let _ = s.event(
                task,
                "transition.failed",
                json!({"transitionId":transition_id,"requestKey":key,"reasonCode":"TRANSITION_STATE_INCOMPATIBLE"}),
            );
            bail!("TRANSITION_STATE_INCOMPATIBLE")
        }
        let gate = if method_name == "COMPLETED_SESSION" {
            reopen_blockers(&s, &current, live, h.max_active, h.registry.version.as_str()).err()
        } else if s.active_count(Some(task))? >= h.max_active {
            Some(anyhow!("LOCAL_TASK_BUSY"))
        } else {
            None
        };
        if let Some(e) = gate {
            let code = safe_code(&e);
            let _ = s.event(
                task,
                "transition.failed",
                json!({"transitionId":transition_id,"requestKey":key,"reasonCode":code}),
            );
            return Err(e);
        }
        let segment = current["segmentId"]
            .as_str()
            .unwrap_or("segment-legacy")
            .to_owned();
        if s.segment_manifest(task, &segment)?.is_none() {
            seal_manifest(&mut s, &h.root, &current, &segment)?;
        }
        let to_segment = format!("segment-{}", store::id());
        let mut compat = compat;
        compat["method"] = json!(method_name);
        // The durable transition record names every child the quiesce drained,
        // so post-transition reads report what was actually stopped.
        compat["affectedChildren"] = json!(affected_children);
        let row = json!({"transitionId":transition_id,"fromEpoch":from_epoch,"toEpoch":to_epoch,"fromSegment":from_segment,"toSegment":to_segment,"compatibility":compat});
        s.admit_transition(task, &key, &b, &accepted, &row, |t| {
            t["transitionedFrom"] = json!({"segmentId":from_segment,"fromEpoch":from_epoch,"transitionId":transition_id,"method":method_name,"affectedChildren":affected_children});
            t["reopenFrom"] = json!({"nativeSessionId":t["nativeSessionId"],"generation":t["generation"],"segmentId":t["segmentId"],"nativeTurnId":t["nativeTurnId"],"commandId":t["currentCommandId"],"checkpoint":false,"transition":transition_id});
            t["generation"] = json!(store::id());
            t["segmentId"] = json!(to_segment);
            t["ownerEpoch"] = json!(to_epoch);
            t["ownerBootId"] = json!(h.boot);
            t["currentCommandId"] = json!(key);
            t["state"] = json!("UNKNOWN");
            t["launchState"] = json!("RESTORING");
            t["quiescent"] = json!(false);
            t.as_object_mut().unwrap().remove("stopProof");
            t.as_object_mut().unwrap().remove("reasonCode");
            t.as_object_mut().unwrap().remove("completedSessionSeal");
        })?;
        s.receipt(
            task,
            &key,
            "DISPATCHING",
            &json!({"kind":"TRANSITION","requestKey":key,"state":"DISPATCHING","transitionId":transition_id,"fromEpoch":from_epoch,"toEpoch":to_epoch}),
        )?;
    }
    let task_id = task.to_owned();
    let output = accepted.clone();
    tokio::spawn(async move {
        let result: Result<Value> = async {
            reopen_session(h.clone(), &task_id, &key, &continuation).await?;
            Ok(h.store.lock().unwrap().lookup(&task_id, &key)?)
        }
        .await;
        let mut s = h.store.lock().unwrap();
        match result {
            Ok(v) => {
                let phase = v["state"].as_str().unwrap_or("UNKNOWN");
                let _ = s.receipt(&task_id, &key, phase, &v);
                let _ = s.event(&task_id, "request.observed", v);
            }
            Err(e) => {
                let code = safe_code(&e);
                let _ = s.update(&task_id, |t| t["reasonCode"] = json!(code));
                let _ = s.receipt(
                    &task_id,
                    &key,
                    "UNKNOWN",
                    &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"UNKNOWN","reasonCode":code}),
                );
                let _ = s.event(
                    &task_id,
                    "request.unconfirmed",
                    json!({"requestKey":key,"code":code}),
                );
            }
        }
    });
    Ok((202, output))
}
fn redact(s: &str) -> String {
    let mut out = s.to_owned();
    for (k, v) in std::env::vars().filter(|(k, v)| {
        v.len() >= 8
            && (k == "MODEL_API_KEY" || k.starts_with("LLM_HEADER_") || k.contains("TOKEN"))
    }) {
        let _ = k;
        out = out.replace(&v, "[REDACTED]");
    }
    out
}
fn redact_value(v: &Value) -> Value {
    match v {
        Value::String(s) => json!(redact(s)),
        Value::Array(a) => json!(a.iter().map(redact_value).collect::<Vec<_>>()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), redact_value(v)))
                .collect(),
        ),
        _ => v.clone(),
    }
}
async fn native_event(h: Shared, task: &str, event: Value) -> Result<()> {
    let method = string(&event, "method")?;
    let p = &event["params"];
    let t = h.store.lock().unwrap().task(task)?;
    if event.get("id").is_some() {
        if method == "item/tool/call" && h.registry.tool_profile == "macos-runner-v1" {
            let h = h.clone();
            let task = task.to_owned();
            let event = event.clone();
            tokio::spawn(async move {
                dynamic_call(h, task, event).await;
            });
            return Ok(());
        }
        let method = method.to_owned();
        if !matches!(
            method.as_str(),
            "item/commandExecution/requestApproval"
                | "item/fileChange/requestApproval"
                | "item/tool/requestUserInput"
                | "mcpServer/elicitation/request"
        ) {
            if let Some(peer) = h.peers.lock().await.get(task).cloned() {
                peer.send(&json!({"id":event["id"],"error":{"code":-32601,"message":"NATIVE_INPUT_TYPE_UNSUPPORTED"}})).await?;
            }
            h.store
                .lock()
                .unwrap()
                .event(task, "input.unsupported", json!({"method":method}))?;
            return Ok(());
        }
        let native_thread = string(p, "threadId")?;
        let native_turn = string(p, "turnId")?;
        if t["nativeSessionId"] != native_thread || t["nativeTurnId"] != native_turn {
            bail!("NATIVE_TARGET_CHANGED")
        }
        let qid = format!("input-{}", store::id());
        let mut params = redact_value(p);
        if let Some(o) = params.as_object_mut() {
            o.remove("command");
            o.remove("reason");
        }
        // Elicitation carriers: `_meta.tool_params`/`tool_params_display`
        // embed the MCP call's raw arguments (body carriers). The durable
        // event keeps the approval kind + persist hints but not the payload.
        if let Some(m) = params
            .pointer_mut("/_meta")
            .and_then(|v| v.as_object_mut())
        {
            m.remove("tool_params");
            m.remove("tool_params_display");
        }
        let file_scope = if method == "item/fileChange/requestApproval" {
            file_approval_paths(&t, p)
        } else {
            None
        };
        let mut q = json!({"inputRequestId":qid,"nativeRequestId":event["id"],"nativeMethod":method,"method":method,"title":if method=="item/tool/requestUserInput"{"Worker question"}else{"Worker approval"},"options":if method=="item/tool/requestUserInput"{json!([])}else{json!(["accept","decline","cancel"])},"state":"PENDING","generation":t["generation"],"nativeSessionId":native_thread,"nativeTurnId":native_turn,"targetCommandId":t["currentCommandId"],"scope":{"cwd":params["cwd"].as_str().unwrap_or(""),"workspace":t["workspace"].as_str().unwrap_or(""),"commandActions":params["commandActions"].to_string(),"proposedExecpolicyAmendment":params["proposedExecpolicyAmendment"].to_string()},"params":params});
        if let Some(paths) = file_scope {
            q["scope"]["cwd"] = t["workspace"].clone();
            q["scope"]["paths"] = json!(paths.to_string());
            q["scope"]["nativeItemId"] = p["itemId"].clone();
        }
        let mut s = h.store.lock().unwrap();
        s.update(task, |t| t["inputRequests"][&qid] = q.clone())?;
        s.event(task, "input.requested", q)?;
        return Ok(());
    }
    match method {
        "adapter/exited" => {
            {
                let mut s = h.store.lock().unwrap();
                if !matches!(
                    t["state"].as_str(),
                    Some("STOP_REQUESTED" | "CANCELLED" | "SUCCEEDED" | "FAILED" | "BUDGET_EXCEEDED")
                ) {
                    s.update(task, |t| {
                        t["state"] = json!("UNKNOWN");
                        t["reasonCode"] = json!("NATIVE_PROCESS_EXITED")
                    })?;
                }
                // A dead transport cannot report further settlements: whatever was
                // still in flight is now an unresolved effect, never assumed done.
                s.effects_unknown(task)?;
                s.event(task, "engine.exited", json!({"observed":true}))?;
                // Pi parity: a peer that exits AFTER the bound turn completed
                // leaves a provably quiescent completed session — it gains the
                // completedSessionSeal that reopen/transition later requires.
                if t["state"] == "SUCCEEDED" {
                    let seal_ok = completed_turn_sealed(&s, &t);
                    let unresolved = s.open_effects(task).unwrap_or(0);
                    s.update(task, |t| {
                        t["quiescent"] = json!(true);
                        maybe_seal_completed(t, seal_ok, unresolved, "NATIVE_PROCESS_EXITED")
                    })?;
                }
                // exec sessions are connection-scoped: with the transport gone they
                // are uncontrollable and their outcome is unproven — LOST, not
                // exited. A managed STOP owns its own marking and skips this path.
                if !matches!(t["state"].as_str(), Some("STOP_REQUESTED" | "CANCELLED")) {
                    let lost = s.terminals_close_open(task, "LOST", "NATIVE_PROCESS_EXITED")?;
                    for id in &lost {
                        s.event(
                            task,
                            "terminal.closed",
                            json!({"terminalId":id,"reason":"NATIVE_PROCESS_EXITED"}),
                        )?;
                    }
                }
            }
            // Dropping the peer closes the job handle: KILL_ON_JOB_CLOSE then
            // reaps any exec children the crashed engine left inside the job.
            h.peers.lock().await.remove(task);
        }
        "adapter/failure" => bail!(adapter::failure_code(&event)),
        "turn/started" => {
            if p["threadId"] != t["nativeSessionId"] {
                return Ok(());
            }
            let native = string(&p["turn"], "id")?;
            let mut s = h.store.lock().unwrap();
            s.update(task, |t| {
                t["nativeTurnId"] = json!(native);
                if !matches!(t["state"].as_str(), Some("STOP_REQUESTED" | "CANCELLED")) {
                    t["state"] = json!("RUNNING")
                }
            })?;
            s.event(task, "turn.started", json!({"nativeTurnId":native}))?;
        }
        "turn/completed" => {
            let turn = &p["turn"];
            let native = string(turn, "id")?;
            let status = string(turn, "status")?;
            if !matches!(status, "completed" | "interrupted" | "failed") {
                bail!("PROTOCOL_UNKNOWN_TURN_STATUS")
            }
            if p["threadId"] != t["nativeSessionId"] || t["nativeTurnId"] != native {
                return Ok(());
            }
            let state = match status {
                "completed" => "SUCCEEDED",
                "failed" => "FAILED",
                _ => "CANCELLED",
            };
            let mut s = h.store.lock().unwrap();
            // Final accounting boundary: if cumulative usage tripped a cap — even
            // exactly at completion — the task ends BUDGET_EXCEEDED, not SUCCEEDED.
            let _ = s.budget_mark_exceeded(task, native)?;
            let fresh = s.task(task)?;
            let budget_terminal = fresh["state"] == "BUDGET_EXCEEDED";
            let runner_unproven =
                h.registry.tool_profile == "macos-runner-v1" && s.open_effects(task)? != 0;
            // Frozen turn-completion fact, bound to this exact
            // generation/segment/command and immune to later task state.
            s.seal_turn(
                task,
                native,
                fresh["currentCommandId"].as_str().unwrap_or(""),
                fresh["generation"].as_str().unwrap_or(""),
                fresh["segmentId"].as_str().unwrap_or(""),
                status,
                if budget_terminal {
                    Some("BUDGET_EXCEEDED")
                } else {
                    None
                },
            )?;
            // Workspace-diff deny-path check: paths the turn actually touched are
            // verified against the bound write scope at this real boundary.
            if let Some(deny) = fresh["writeScope"]["denyPaths"]
                .as_array()
                .filter(|a| !a.is_empty())
            {
                if let (Ok(ws), Ok(base)) = (
                    fresh["workspace"].as_str().map(PathBuf::from).ok_or(()),
                    string(&fresh, "baseCommit"),
                ) {
                    if let Ok(names) = workspace_changed(&ws, base) {
                        for name in names {
                            if deny.iter().any(|d| matches_denied(&name, d)) {
                                record_violation(&mut s, task, &name, "DENIED_PATH")?;
                            }
                        }
                    }
                }
            }
            s.update(task, |t| {
                if !matches!(
                    t["state"].as_str(),
                    Some("STOP_REQUESTED" | "CANCELLED" | "BUDGET_EXCEEDED")
                ) {
                    t["state"] = json!(if runner_unproven { "UNKNOWN" } else { state });
                    if runner_unproven {
                        t["reasonCode"] = json!("RUNNER_EFFECT_UNPROVEN");
                    } else if state == "FAILED" {
                        t["reasonCode"] = json!(native_failure_reason(&turn["error"]));
                    }
                }
                let key = t["currentCommandId"].as_str().unwrap_or("").to_owned();
                t["turns"][&key] = json!({"state":state,"nativeTurnId":native});
                // Turn completion is not evidence that detached tool writers exited.
                // Only a managed-tree stop may set the public quiescence fact.
                if t["state"] != "CANCELLED" && t["state"] != "BUDGET_EXCEEDED" {
                    t["quiescent"] = json!(false);
                }
            })?;
            s.event(
                task,
                "turn.completed",
                json!({"nativeTurnId":native,"status":status}),
            )?;
            drop(s);
            if budget_terminal {
                let h2 = h.clone();
                let task2 = task.to_owned();
                tokio::spawn(async move { budget_shutdown(h2, task2).await });
            }
        }
        "item/started" => {
            let item = &p["item"];
            let kind = item["type"].as_str().unwrap_or("");
            if adapter::EFFECT_KINDS.contains(&kind) {
                adapter::validate_item(item)?;
                let mut s = h.store.lock().unwrap();
                let fresh = s.task(task)?;
                if fresh["nativeSessionId"] == p["threadId"] && fresh["nativeTurnId"] == p["turnId"]
                {
                    if kind == "dynamicToolCall" && h.registry.tool_profile == "macos-runner-v1" {
                        let call_id = string(item, "id")?;
                        if let Some(previous)=fresh["nativeDynamicCalls"].get(call_id) {
                            if previous["generation"]!=fresh["generation"] || previous["nativeSessionId"]!=p["threadId"] || previous["nativeTurnId"]!=p["turnId"] || previous["tool"]!=item["tool"] || previous["arguments"]!=item["arguments"] {bail!("NATIVE_CALL_ID_REUSED")}
                        }
                        s.update(task, |t| {
                            t["nativeDynamicCalls"][call_id]=json!({"nativeSessionId":p["threadId"],"nativeTurnId":p["turnId"],"generation":t["generation"],"tool":item["tool"],"arguments":item["arguments"]});
                        })?;
                    }
                    if kind == "fileChange" {
                        let paths = item["changes"]
                            .as_array()
                            .ok_or_else(|| anyhow!("PROTOCOL_REQUIRED_FIELD"))?
                            .iter()
                            .map(|c| string(c, "path").map(|s| json!(redact(s))))
                            .collect::<Result<Vec<_>>>()?;
                        let item_id = string(item, "id")?;
                        s.update(task, |t| {
                            t["nativeFileChanges"][item_id] = json!({"nativeSessionId":p["threadId"],"nativeTurnId":p["turnId"],"generation":t["generation"],"paths":paths});
                        })?;
                    }
                    s.effect_started(task, string(item, "id")?, kind, string(p, "turnId")?)?;
                    s.event(
                        task,
                        "tool.started",
                        json!({"nativeItemId":item["id"],"type":kind,"nativeTurnId":p["turnId"]}),
                    )?;
                }
            }
        }
        "item/completed" => {
            let item = &p["item"];
            adapter::validate_item(item)?;
            let item_type = string(item, "type")?;
            if adapter::EFFECT_KINDS.contains(&item_type)
                && !(item_type == "dynamicToolCall" && h.registry.tool_profile == "macos-runner-v1")
            {
                h.store.lock().unwrap().effect_settled(
                    task,
                    string(item, "id")?,
                    item["status"].as_str().unwrap_or("unknown"),
                )?;
            }
            if item_type == "agentMessage" {
                let native = string(p, "turnId")?;
                if t["nativeTurnId"] != native || t["nativeSessionId"] != p["threadId"] {
                    return Ok(());
                }
                let text = redact(string(item, "text")?);
                let item_id = string(item, "id")?;
                let thread = string(p, "threadId")?;
                let mut s = h.store.lock().unwrap();
                // One transaction: (task,thread,item) dedup, the assistant row,
                // accumulated output + legacy publicItemIds, the public event
                // and the transcript revision commit or roll back together.
                s.record_agent_message(task, thread, native, item_id, &text)?;
            } else if item_type == "userMessage" {
                // A native user item only confirms an already-durable user row:
                // the clientId must equal an original request key of this task
                // on the same thread (and the bound turn when one is recorded).
                // It never inserts a second user message; unbound evidence is
                // reported for diagnostics instead of guessing by text or time.
                let thread = p["threadId"].as_str().unwrap_or("");
                let turn = p["turnId"].as_str().unwrap_or("");
                let item_id = item["id"].as_str().unwrap_or("");
                let mut s = h.store.lock().unwrap();
                match item["clientId"].as_str() {
                    None => {
                        s.event(
                            task,
                            "message.unbound",
                            json!({"nativeItemId":item_id,"nativeTurnId":turn,"reason":"CLIENT_ID_ABSENT"}),
                        )?;
                    }
                    Some(key) => {
                        match s.record_user_consumption(task, key, thread, turn, item_id)? {
                            store::Consumption::Bound => {}
                            store::Consumption::Unbound(reason) => {
                                s.event(
                                task,
                                "message.unbound",
                                json!({"nativeItemId":item_id,"nativeTurnId":turn,"clientId":key,"reason":reason}),
                            )?;
                            }
                        }
                    }
                }
            } else if matches!(item_type, "commandExecution" | "fileChange") {
                let mut s = h.store.lock().unwrap();
                s.event(
                    task,
                    "tool.completed",
                    json!({"nativeItemId":item["id"],"type":item_type,"status":item["status"]}),
                )?;
                // Host-observed write-scope verification: every path a fileChange
                // reports is checked against workspace+extraWriteDirs+denyPaths,
                // and a native rejection of an attempt is surfaced as a fact.
                let item_status = item["status"].as_str().unwrap_or("");
                if item_type == "fileChange" {
                    if let Some(changes) = item["changes"].as_array() {
                        for change in changes {
                            if let Some(path) = change["path"].as_str() {
                                if let Some(kind) = write_scope_check(&t, path) {
                                    record_violation(&mut s, task, path, kind)?;
                                }
                            }
                        }
                    }
                }
                if item_status == "declined" {
                    record_violation(
                        &mut s,
                        task,
                        "",
                        if item_type == "fileChange" {
                            "NATIVE_FILE_REJECTED"
                        } else {
                            "NATIVE_EXEC_REJECTED"
                        },
                    )?;
                } else if item_type == "commandExecution" && item_status == "failed" {
                    let out = item["aggregatedOutput"]
                        .as_str()
                        .unwrap_or("")
                        .to_lowercase();
                    if ["sandbox", "denied", "permission", "outside the workspace"]
                        .iter()
                        .any(|m| out.contains(m))
                    {
                        record_violation(&mut s, task, "", "NATIVE_SANDBOX_REJECTED")?;
                    }
                }
                let mut settled = vec![];
                s.update(task, |t| {
                    if let Some(qs) = t["inputRequests"].as_object_mut() {
                        for q in qs.values_mut() {
                            if q["params"]["itemId"] == item["id"] && q["state"] == "ANSWERED" {
                                q["state"] = json!("CONSUMED");
                                q["consumedNativeItemId"] = item["id"].clone();
                                if let Some(key) = q["responseRequestKey"].as_str() {
                                    settled.push(key.to_owned())
                                }
                            }
                        }
                    }
                })?;
                for key in settled {
                    let v = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","detail":"Native item completed after its bound answer","consumptionEvidence":{"nativeItemId":item["id"],"nativeTurnId":p["turnId"]}});
                    s.receipt(task, &key, "APPLIED", &v)?;
                    s.event(task, "input.consumed", v)?;
                }
            } else if item_type == "mcpToolCall" {
                // MCP tool-call fact: the effect row already tracks settlement;
                // the event carries the call identity but never its arguments.
                let s = h.store.lock().unwrap();
                s.event(
                    task,
                    "tool.completed",
                    json!({"nativeItemId":item["id"],"type":item_type,"status":item["status"],"server":item["server"],"tool":item["tool"]}),
                )?;
            }
        }
        "thread/tokenUsage/updated" => {
            let usage = &p["tokenUsage"]["total"];
            if usage.is_object() {
                let thread = p["threadId"].as_str().unwrap_or("").to_owned();
                let turn = p["turnId"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| t["nativeTurnId"].as_str().unwrap_or("").to_owned());
                let hit = h
                    .store
                    .lock()
                    .unwrap()
                    .usage_observed(task, &thread, &turn, usage)?;
                // A cap tripped mid-turn: interrupt the real native turn, seal it,
                // then release the slot through a managed shutdown.
                if hit.is_some() {
                    let h2 = h.clone();
                    let task2 = task.to_owned();
                    tokio::spawn(async move { budget_shutdown(h2, task2).await });
                }
            }
        }
        // Streamed exec output is routed by processId into the per-terminal
        // bounded ring; no bytes are persisted — the durable record keeps only
        // counters and the exit fact.
        "command/exec/outputDelta" => {
            let pid = p["processId"].as_str().unwrap_or("");
            if let Some(b64) = p["deltaBase64"].as_str() {
                if let Ok(bytes) = STANDARD.decode(b64) {
                    let mut terms = h.terminals.lock().unwrap();
                    if let Some(live) = terms.get_mut(task).and_then(|m| m.get_mut(pid)) {
                        live.buf.extend(bytes);
                        if live.buf.len() > TERMINAL_RING {
                            live.capped = true;
                            let drop = live.buf.len() - TERMINAL_RING;
                            live.buf.drain(..drop);
                            live.base += drop as u64;
                        }
                    }
                }
            }
            if p["capReached"].as_bool() == Some(true) {
                h.store.lock().unwrap().event(
                    task,
                    "terminal.output.capped",
                    json!({"processId":pid}),
                )?;
            }
        }
        "hook/started" | "hook/completed" => {
            let kind = if method == "hook/started" {
                "hook.started"
            } else {
                "hook.completed"
            };
            h.store
                .lock()
                .unwrap()
                .event(task, kind, redact_value(p))?;
        }
        "mcpServer/startupStatus/updated" => {
            h.store
                .lock()
                .unwrap()
                .event(task, "mcp.status", redact_value(p))?;
        }
        "error" => {
            h.store.lock().unwrap().event(
                task,
                "engine.error",
                json!({"willRetry":p["willRetry"],"code":p["error"]["codexErrorInfo"]}),
            )?;
        }
        // Private reasoning, raw tool output, config/environment and unknown notifications
        // are deliberately excluded from the public projection.
        _ => {}
    }
    Ok(())
}

// Keep only a public classification, never an upstream URL/body or raw error.
fn native_failure_reason(error: &Value) -> &'static str {
    if error["message"].as_str().is_some_and(|m| m.contains("stream disconnected before completion")) {
        "MODEL_STREAM_DISCONNECTED"
    } else {
        "NATIVE_TURN_FAILED"
    }
}

// Mid-turn budget exceed: interrupt the real native turn, wait briefly for its
// interrupted seal so a later EXTEND_BUDGET can prove it, then terminate the
// managed tree and release the slot for queued intents.
async fn budget_shutdown(h: Shared, task: String) {
    let runner_proof = stop_runners(&h, &task).await;
    let mac_proof = stop_mac_tools(&h, &task).await;
    let runner_ok = runner_proof.is_ok() && mac_proof.is_ok();
    let mut proven = true;
    let peer = h.peers.lock().await.get(&task).cloned();
    if let Some(peer) = peer {
        let (thread, turn) = {
            let s = h.store.lock().unwrap();
            s.task(&task)
                .map(|t| {
                    (
                        t["nativeSessionId"].as_str().map(str::to_owned),
                        t["nativeTurnId"].as_str().map(str::to_owned),
                    )
                })
                .unwrap_or((None, None))
        };
        if let (Some(thread), Some(turn)) = (thread, turn) {
            let _ = timeout(
                Duration::from_secs(3),
                peer.rpc("turn/interrupt", json!({"threadId":thread,"turnId":turn})),
            )
            .await;
            for _ in 0..80 {
                {
                    let s = h.store.lock().unwrap();
                    if s.turn_seal(&task, &turn).ok().flatten().is_some() {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        proven = peer.stop().await.is_ok() && peer.quiescence_proven();
        {
            let s = h.store.lock().unwrap();
            let _ = s.terminals_close_open(&task, if proven { "TERMINATED" } else { "LOST" }, if proven { "BUDGET_JOB_TERMINATED" } else { "NATIVE_WRITER_UNPROVEN" });
        }
    }
    {
        let mut s = h.store.lock().unwrap();
        let _ = s.update(&task, |t| {
            apply_stop_proof(t, proven && runner_ok, Some("BUDGET_EXCEEDED"));
            if let Ok(proof) = &runner_proof {
                apply_runner_proof(t, proof);
            }
            if let Some(questions) = t["inputRequests"].as_object_mut() {
                for q in questions.values_mut() {
                    if q["state"] == "PENDING" {
                        q["state"] = json!("EXPIRED")
                    }
                }
            }
        });
        let _ = s.effects_unknown(&task);
    }
    drain_queue(&h);
}
// Workspace-relative names the turn actually touched: tracked diffs against the
// bound base plus untracked files, the same evidence a sealed manifest uses.
fn workspace_changed(ws: &Path, base: &str) -> Result<Vec<String>> {
    let changed = git(ws, &["diff", "--name-only", "-z", base])?;
    let untracked = git(ws, &["ls-files", "--others", "--exclude-standard", "-z"])?;
    Ok(changed
        .split('\0')
        .chain(untracked.split('\0'))
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect())
}
fn matches_denied(rel: &str, deny: &Value) -> bool {
    let Some(d) = deny.as_str() else {
        return false;
    };
    Path::new(rel).starts_with(Path::new(d))
}
// Durable write-scope violation fact + bounded task-visible record; duplicate
// (path,kind) pairs are not appended twice.
fn record_violation(s: &mut Store, task: &str, path: &str, kind: &str) -> Result<()> {
    s.event(
        task,
        "write.scope.violation",
        json!({"kind":kind,"path":path,"enforcement":"HOST_OBSERVED"}),
    )?;
    s.update(task, |t| {
        let list = t["writeScopeViolations"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if list.iter().any(|v| v["path"] == path && v["kind"] == kind) {
            return;
        }
        let mut list = list;
        if list.len() < 64 {
            list.push(json!({"path":path,"kind":kind,"at":store::now()}));
            t["writeScopeViolations"] = json!(list);
        }
    })?;
    Ok(())
}
// Scope membership check for one reported path. Returns Some(kind) when the
// path is outside workspace+extraWriteDirs or under a bound deny prefix.
fn write_scope_check(t: &Value, path: &str) -> Option<&'static str> {
    let ws = Path::new(t["workspace"].as_str()?);
    let raw = Path::new(path);
    let abs = if raw.is_absolute() {
        lexical(raw)
    } else {
        lexical(&ws.join(raw))
    };
    let ws_n = lexical(ws);
    let in_ws = abs.starts_with(&ws_n);
    let in_extra = t["writeScope"]["extraWriteDirs"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .any(|d| abs.starts_with(Path::new(d)))
        })
        .unwrap_or(false);
    if !in_ws && !in_extra {
        return Some("OUT_OF_SCOPE");
    }
    if in_ws {
        if let Ok(rel) = abs.strip_prefix(&ws_n) {
            if t["writeScope"]["denyPaths"].as_array().is_some_and(|a| {
                a.iter()
                    .any(|d| rel.starts_with(Path::new(d.as_str().unwrap_or(""))))
            }) {
                return Some("DENIED_PATH");
            }
        }
    }
    None
}
// Diagnostics redaction: body-bearing keys become {sha256,size}; every other
// string passes the environment redactor; absolute paths outside the workspace
// are hashed rather than emitted.
fn diagnostics_value(v: &Value, ws: &str) -> Value {
    const BODY_KEYS: &[&str] = &[
        "instruction",
        "content",
        "text",
        "response",
        "output",
        "command",
        "commandActions",
        "proposedExecpolicyAmendment",
        "parsedCmd",
        "parsedCommand",
        "argv",
        "arguments",
        "cmd",
        "cmdline",
        "commandLine",
        "question",
        "questions",
        "answer",
        "answers",
        "prompt",
        "input",
        "body",
        "data",
        "value",
        "patch",
        "params",
        "diff",
        "aggregatedOutput",
        "stdout",
        "stderr",
        "message",
    ];
    match v {
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, x)| {
                    let v = if BODY_KEYS.contains(&k.as_str()) {
                        let bytes = serde_json::to_vec(x).unwrap_or_default();
                        json!({"sha256":hash(&bytes),"size":bytes.len().to_string()})
                    } else {
                        diagnostics_value(x, ws)
                    };
                    (k.clone(), v)
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(|x| diagnostics_value(x, ws)).collect()),
        Value::String(s) => {
            let p = Path::new(s);
            if p.is_absolute() && !p.starts_with(ws) {
                json!({"sha256":hash(s.as_bytes()),"size":s.len().to_string()})
            } else {
                json!(redact(s))
            }
        }
        other => other.clone(),
    }
}
// Segment-scoped opaque ids keep a stopped segment's manifest resolvable after a
// later generation changed the same workspace paths.
fn artifact_id(name: &str, segment: &str) -> String {
    format!("file-{}", hash(format!("{}\0{}", name, segment)))
}
// Immutable snapshot of the workspace diff for one sealed segment. Bytes and the
// manifest row commit atomically; a published manifest is never rewritten.
fn seal_manifest(s: &mut Store, root: &Path, t: &Value, segment: &str) -> Result<Value> {
    let task = string(t, "taskId")?;
    let ws = canonical_path(root.join("workspaces").join(task))?;
    if ws != canonical_path(string(t, "workspace")?)? {
        bail!("OWNED_WORKSPACE_REQUIRED")
    }
    let base = string(t, "baseCommit")?;
    git(&ws, &["cat-file", "-e", &format!("{}^{{commit}}", base)])?;
    let changed = git(&ws, &["diff", "--name-only", "-z", base])?;
    let untracked = git(&ws, &["ls-files", "--others", "--exclude-standard", "-z"])?;
    let names: HashSet<&str> = changed
        .split('\0')
        .chain(untracked.split('\0'))
        .filter(|s| !s.is_empty())
        .collect();
    let bounded = names.len() > 100;
    let mut rows = vec![];
    let mut objects = vec![];
    let mut total = 0;
    let mut sorted: Vec<_> = names.into_iter().collect();
    sorted.sort();
    for name in sorted.into_iter().take(100) {
        let aid = artifact_id(name, segment);
        let path = ws.join(name);
        let mut row = json!({"artifactId":aid,"path":name});
        if !path.exists() {
            row["status"] = json!("DELETED")
        } else if fs::symlink_metadata(&path)?.file_type().is_symlink()
            || !path.is_file()
            || !canonical_path(&path)?.starts_with(&ws)
        {
            row["status"] = json!("WITHHELD_PATH")
        } else if fs::metadata(&path)?.len() > 512 * 1024 || total >= 4 * 1024 * 1024 {
            row["status"] = json!("WITHHELD_SIZE")
        } else {
            let bytes = fs::read(&path)?;
            let text = String::from_utf8_lossy(&bytes);
            let unsafe_name = name.to_lowercase().split('/').any(|p| {
                p == ".env"
                    || p.contains("credential")
                    || p.contains("secret")
                    || p == "auth.json"
                    || p == "env.properties"
            });
            let secret = redact(&text) != text
                || text.contains("-----BEGIN PRIVATE KEY-----")
                || text.contains("-----BEGIN RSA PRIVATE KEY-----");
            if unsafe_name || secret {
                row["status"] = json!("WITHHELD_SECRET")
            } else {
                total += bytes.len();
                row["status"] = json!("AVAILABLE");
                row["size"] = json!(bytes.len().to_string());
                row["sha256"] = json!(hash(&bytes));
                objects.push((aid.clone(), row.clone(), bytes));
            }
        }
        rows.push(row);
    }
    let manifest = json!({"items":rows,"bounded":bounded,"coverage":"CHANGED_OWNED_FILES","baseline":base,"segmentId":segment});
    let tx = s.db.transaction()?;
    for (id, row, bytes) in objects {
        tx.execute(
            "INSERT INTO artifacts VALUES(?1,?2,?3,?4)",
            rusqlite::params![task, id, row.to_string(), bytes],
        )?;
    }
    tx.execute(
        "INSERT INTO artifact_manifests VALUES(?1,?2,?3,?4)",
        rusqlite::params![task, segment, manifest.to_string(), store::now()],
    )?;
    tx.commit()?;
    Ok(manifest)
}
// Latest sealed segment manifest, with every sealed segment still listed.
fn artifacts(h: &Host, task: &str) -> Result<Value> {
    let mut s = h.store.lock().unwrap();
    let t = s.task(task)?;
    let segment = t["segmentId"]
        .as_str()
        .unwrap_or("segment-legacy")
        .to_owned();
    if t["quiescent"] == true
        && s.segment_manifest(task, &segment)?.is_none()
        && !(h.registry.tool_profile == "macos-runner-v1" && s.open_effects(task)? != 0)
    {
        seal_manifest(&mut s, &h.root, &t, &segment)?;
    }
    let manifests = s.segment_manifests(task)?;
    let Some(latest) = manifests.last().cloned() else {
        return Ok(
            json!({"items":[],"reasonCode":"STOP_TO_SEAL_RESULTS","segmentId":segment,"segments":[]}),
        );
    };
    let mut v = latest;
    v["segments"] =
        json!(manifests
        .iter()
        .map(|m| json!({"segmentId":m["segmentId"],"sealedAt":m["sealedAt"],"items":m["items"]}))
        .collect::<Vec<_>>());
    Ok(v)
}
fn artifact_read(h: &Host, task: &str, id: &str) -> Result<Value> {
    if !id.starts_with("file-") || id.len() != 69 || !id[5..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        bail!("INVALID_ID")
    }
    artifacts(h, task)?;
    let s = h.store.lock().unwrap();
    // Resolve across every sealed segment manifest, not only the latest one.
    let row = s
        .segment_manifests(task)?
        .iter()
        .flat_map(|m| m["items"].as_array().cloned().unwrap_or_default())
        .find(|r| r["artifactId"] == id)
        .ok_or_else(|| anyhow!("ARTIFACT_NOT_AVAILABLE"))?;
    if row["status"] != "AVAILABLE" {
        bail!("ARTIFACT_NOT_AVAILABLE")
    }
    let bytes: Vec<u8> =
        s.db.query_row(
            "SELECT bytes FROM artifacts WHERE task=?1 AND id=?2",
            rusqlite::params![task, id],
            |r| r.get(0),
        )
        .map_err(|_| anyhow!("ARTIFACT_NOT_AVAILABLE"))?;
    if hash(&bytes) != row["sha256"] {
        bail!("ARTIFACT_CHANGED")
    }
    Ok(
        json!({"artifactId":id,"path":row["path"],"sha256":row["sha256"],"encoding":"base64","bytes":STANDARD.encode(bytes)}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn capability_reads_share_verification_without_blocking_async_work() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = super::AsyncMutex::new(None);
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let checked = calls.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let verify = super::coalesced_read_verification(&cache, None, move || {
            checked.fetch_add(1, Ordering::SeqCst);
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            true
        });
        let concurrent = async {
            started_rx.await.unwrap();
            release_tx.send(()).unwrap();
            super::coalesced_read_verification(&cache, None, || panic!("duplicate binary read")).await
        };
        let (first, second) = tokio::join!(verify, concurrent);
        assert!(first && second);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn capability_verification_expires_and_preserves_rejection() {
        let cache = super::AsyncMutex::new(Some((
            std::time::Instant::now() - std::time::Duration::from_secs(2), None, true,
        )));
        assert!(!super::coalesced_read_verification(&cache, None, || false).await);
        assert!(!super::coalesced_read_verification(&cache, None, || panic!("cached rejection lost")).await);
    }

    #[tokio::test]
    async fn native_read_stamp_reuses_startup_proof_and_invalidates_changed_files() {
        let stamp = vec![(10, std::time::SystemTime::UNIX_EPOCH)];
        let cache = super::AsyncMutex::new(Some((
            std::time::Instant::now() - std::time::Duration::from_secs(20), Some(stamp.clone()), true,
        )));
        assert!(super::coalesced_read_verification(&cache, Some(stamp), || panic!("unchanged startup proof rehashed")).await);
        let changed = vec![(11, std::time::SystemTime::UNIX_EPOCH)];
        assert!(!super::coalesced_read_verification(&cache, Some(changed.clone()), || false).await);
        *cache.lock().await = Some((
            std::time::Instant::now() - std::time::Duration::from_secs(2), Some(changed.clone()), false,
        ));
        assert!(super::coalesced_read_verification(&cache, Some(changed), || true).await);
    }
    #[test]
    fn streaming_file_digest_matches_bytes_across_chunks_and_refuses_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pinned.bin");
        for bytes in [vec![], (0..(3 * 64 * 1024 + 17)).map(|n| (n % 251) as u8).collect()] {
            fs::write(&path, &bytes).unwrap();
            assert_eq!(file_hash(&path).unwrap(), hash(&bytes));
        }
        fs::remove_file(&path).unwrap();
        assert!(file_hash(&path).is_err());
    }
    #[test]
    fn runner_callback_requires_observed_call_and_exact_generation_thread_turn_tool_arguments() {
        let p=json!({"threadId":"thread","turnId":"turn","callId":"call","tool":"run_tests","arguments":{"command":"node --test"}});
        let t=json!({"taskId":"task","generation":"g","segmentId":"seg","nativeSessionId":"thread","nativeTurnId":"turn","currentCommandId":"cmd","nativeDynamicCalls":{"call":{"generation":"g","nativeSessionId":"thread","nativeTurnId":"turn","tool":"run_tests","arguments":{"command":"node --test"}}}});
        assert!(dynamic_binding(&t,&p).is_ok());
        for (field,value) in [("threadId",json!("other")),("turnId",json!("old")),("callId",json!("missing")),("tool",json!("run_build")),("arguments",json!({"command":"different"}))] {
            let mut changed=p.clone();changed[field]=value;assert!(dynamic_binding(&t,&changed).is_err());
        }
        let mut stale=t.clone();stale["generation"]=json!("new");assert!(dynamic_binding(&stale,&p).is_err());
        assert_eq!(runner_key("task",&p),runner_key("task",&p));
        assert_ne!(runner_key("other-task",&p),runner_key("task",&p));
        let mut next=t.clone();next["nativeTurnId"]=json!("next-turn");assert_ne!(runner_context(&t),runner_context(&next));
    }
    #[test]
    fn exact_guards_and_stop_does_not_need_cas() {
        let t = json!({"generation":"g","nativeSessionId":"s","nativeTurnId":"t","currentCommandId":"c","stateVersion":"9"});
        let b = json!({"generation":"g","nativeSessionId":"s","nativeTurnId":"t","targetCommandId":"c","stateVersion":"1"});
        assert!(guards(&t, &b).is_ok());
        let mut wrong = b;
        wrong["nativeTurnId"] = json!("other");
        assert!(guards(&t, &wrong).is_err());
    }
    #[test]
    fn unknown_fields_and_empty_content_refused() {
        assert!(exact(&json!({"evil":1}), &["requestKey"]).is_err());
        assert!(instruction(&json!({"content":" "}), "content").is_err());
    }
    fn stopped_task() -> Value {
        json!({"taskId":"native-stop","state":"SUCCEEDED","quiescent":true,"nativeSessionId":"thread-1","nativeTurnId":"turn-1","generation":"g1","segmentId":"seg-1","currentCommandId":"cmd-0","engineVersion":"v-test","completedSessionSeal":{"nativeSessionId":"thread-1","nativeTurnId":"turn-1","generation":"g1","segmentId":"seg-1","engineVersion":"v-test","quiescenceMethod":"MANAGED_PEER_EXITED"},"inputRequests":{},"stateVersion":"1"})
    }
    #[test]
    fn reopen_guard_matrix_refuses_each_missing_prerequisite_without_dispatch() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        let t = stopped_task();
        s.save(&t).unwrap();
        // Non-qualifying states never reach the reconciliation gate.
        let mut running = t.clone();
        running["state"] = json!("RUNNING");
        running["quiescent"] = json!(false);
        assert_eq!(
            reopen_blockers(&s, &running, false, 1, "v-test").unwrap_err().to_string(),
            "NATIVE_NOT_IDLE"
        );
        let mut early = t.clone();
        early["nativeSessionId"] = json!(null);
        early["nativeTurnId"] = json!(null);
        assert_eq!(
            reopen_blockers(&s, &early, false, 1, "v-test").unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        // No sealed completion fact for the bound turn.
        assert_eq!(
            reopen_blockers(&s, &t, false, 1, "v-test").unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        // Seal bound to a different generation does not count.
        s.seal_turn(
            "native-stop",
            "turn-1",
            "cmd-0",
            "other-generation",
            "seg-1",
            "completed",
            None,
        )
        .unwrap();
        assert_eq!(
            reopen_blockers(&s, &t, false, 1, "v-test").unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        s.db.execute("DELETE FROM turn_seals", []).unwrap();
        s.seal_turn("native-stop", "turn-1", "cmd-0", "g1", "seg-1", "completed", None)
            .unwrap();
        assert!(reopen_blockers(&s, &t, false, 1, "v-test").is_ok());
        // An unresolved started or unknown effect blocks admission.
        s.effect_started("native-stop", "item-1", "commandExecution", "turn-1")
            .unwrap();
        assert_eq!(
            reopen_blockers(&s, &t, false, 1, "v-test").unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        s.effect_settled("native-stop", "item-1", "completed").unwrap();
        assert!(reopen_blockers(&s, &t, false, 1, "v-test").is_ok());
        s.effect_started("native-stop", "item-2", "fileChange", "turn-1")
            .unwrap();
        s.effects_unknown("native-stop").unwrap();
        assert_eq!(
            reopen_blockers(&s, &t, false, 1, "v-test").unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        s.db.execute("DELETE FROM native_effects", []).unwrap();
        // A pending or answered-but-unconsumed question blocks admission.
        let mut pending = t.clone();
        pending["inputRequests"]["q1"] = json!({"state":"PENDING"});
        assert_eq!(
            reopen_blockers(&s, &pending, false, 1, "v-test").unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        let mut answered = t.clone();
        answered["inputRequests"]["q1"] = json!({"state":"ANSWERED"});
        assert_eq!(
            reopen_blockers(&s, &answered, false, 1, "v-test").unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        let mut consumed = t.clone();
        consumed["inputRequests"]["q1"] = json!({"state":"CONSUMED"});
        consumed["inputRequests"]["q2"] = json!({"state":"EXPIRED"});
        assert!(reopen_blockers(&s, &consumed, false, 1, "v-test").is_ok());
        // A peerless SUCCEEDED session also qualifies; a live SUCCEEDED session
        // takes the ordinary same-thread path instead.
        let mut succeeded = t.clone();
        succeeded["state"] = json!("SUCCEEDED");
        assert!(reopen_blockers(&s, &succeeded, false, 1, "v-test").is_ok());
        assert_eq!(
            reopen_blockers(&s, &succeeded, true, 1, "v-test").unwrap_err().to_string(),
            "NATIVE_NOT_IDLE"
        );
        // Another admitted writer closes the single slot.
        s.save(&json!({"taskId":"native-other","state":"RUNNING","quiescent":false}))
            .unwrap();
        assert_eq!(
            reopen_blockers(&s, &t, false, 1, "v-test").unwrap_err().to_string(),
            "LOCAL_TASK_BUSY"
        );
    }
    #[test]
    fn stop_drain_requires_bound_native_completion_and_settled_effects() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        let mut t = stopped_task();
        t["currentCommandId"] = json!("cmd-0");
        s.save(&t).unwrap();
        assert!(!native_stop_drained(&s,"native-stop","turn-1").unwrap());
        s.seal_turn("native-stop","turn-1","cmd-0","g1","seg-1","interrupted",None).unwrap();
        s.effect_started("native-stop","executing","commandExecution","turn-1").unwrap();
        assert!(!native_stop_drained(&s,"native-stop","turn-1").unwrap());
        s.effect_settled("native-stop","executing","failed").unwrap();
        assert!(native_stop_drained(&s,"native-stop","turn-1").unwrap());
        for field in ["nativeTurnId","generation","segmentId","currentCommandId"] {
            let mut stale=t.clone();stale[field]=json!("different");s.save(&stale).unwrap();
            assert!(!native_stop_drained(&s,"native-stop","turn-1").unwrap(),"stale {field}");
        }
        s.save(&t).unwrap();
        s.effect_started("native-stop","opaque","mcpToolCall","turn-1").unwrap();
        s.effects_unknown("native-stop").unwrap();
        assert!(!native_stop_drained(&s,"native-stop","turn-1").unwrap());
    }
    #[test]
    fn segment_scoped_artifact_ids_are_stable_and_distinct() {
        let a = artifact_id("src/lib.rs", "segment-1");
        let b = artifact_id("src/lib.rs", "segment-2");
        assert_eq!(a, artifact_id("src/lib.rs", "segment-1"));
        assert_ne!(a, b);
        assert_eq!(a.len(), 69);
        assert!(a.starts_with("file-"));
    }
    #[test]
    fn stopped_failed_and_interrupted_turns_require_bound_proofs_and_settled_effects() {
        for (state, status) in [("FAILED", "failed"), ("CANCELLED", "interrupted")] {
            let s = Store::open(Path::new(":memory:")).unwrap();
            let mut t = stopped_task();
            t["state"] = json!(state);
            t.as_object_mut().unwrap().remove("completedSessionSeal");
            apply_stop_proof(&mut t, true, None);
            s.save(&t).unwrap();
            assert!(reopen_blockers(&s, &t, false, 1, "v-test").is_err());
            s.seal_turn("native-stop", "turn-1", "cmd-0", "g1", "seg-1", status, None).unwrap();
            assert!(reopen_blockers(&s, &t, false, 1, "v-test").is_ok());
            let mut unproven = t.clone();
            unproven.as_object_mut().unwrap().remove("stopProof");
            assert!(reopen_blockers(&s, &unproven, false, 1, "v-test").is_err());
            let mut stale = t.clone();
            stale["generation"] = json!("newer-owner");
            assert!(reopen_blockers(&s, &stale, false, 1, "v-test").is_err());
            assert!(reopen_blockers(&s, &t, true, 1, "v-test").is_err());
            let mut pending = t.clone();
            pending["inputRequests"]["q"] = json!({"state":"ANSWERED"});
            assert!(reopen_blockers(&s, &pending, false, 1, "v-test").is_err());
            s.effect_started("native-stop", "unfinished", "commandExecution", "turn-1").unwrap();
            s.effects_unknown("native-stop").unwrap();
            assert!(reopen_blockers(&s, &t, false, 1, "v-test").is_err());
        }
    }
    #[test]
    fn native_failure_reason_never_exposes_upstream_details() {
        assert_eq!(native_failure_reason(&json!({"message":"stream disconnected before completion: private endpoint"})), "MODEL_STREAM_DISCONNECTED");
        assert_eq!(native_failure_reason(&json!({"message":"private endpoint"})), "NATIVE_TURN_FAILED");
    }
    #[test]
    fn budget_spec_validation_is_strict_and_soft_only() {
        let good = validate_budget(
            &json!({"maxTotalTokens":"5000","maxCostMicroUsd":"20000","enforcement":"SOFT"}),
        )
        .unwrap();
        assert_eq!(good["caps"]["maxTotalTokens"], "5000");
        assert_eq!(good["enforcement"], "SOFT");
        // Hard/preemptive enforcement is not implementable from post-hoc usage.
        assert_eq!(
            validate_budget(&json!({"maxTotalTokens":"1","enforcement":"HARD"}))
                .unwrap_err()
                .to_string(),
            "BUDGET_ENFORCEMENT_UNSUPPORTED"
        );
        assert!(validate_budget(&json!({"enforcement":"SOFT"})).is_err());
        assert!(validate_budget(&json!({"maxTotalTokens":-3,"enforcement":"SOFT"})).is_err());
        assert!(validate_budget(&json!({"maxTotalTokens":"abc","enforcement":"SOFT"})).is_err());
    }
    #[test]
    fn write_scope_and_environment_validation() {
        let absolute = std::env::temp_dir().join("nwf-extra");
        let ws = validate_write_scope(
            &json!({"extraWriteDirs":[absolute],"denyPaths":["secrets",".ssh"]}),
        )
        .unwrap();
        assert_eq!(
            ws["enforcement"],
            "APP_SERVER_WRITABLE_ROOTS+HOST_OBSERVED_DENYLIST"
        );
        assert!(validate_write_scope(&json!({"extraWriteDirs":["relative/dir"]})).is_err());
        assert!(validate_write_scope(&json!({"denyPaths":["../escape"]})).is_err());
        assert!(validate_write_scope(&json!({"denyPaths":[absolute]})).is_err());
        let env = validate_environment(&json!({"requiresCommands":["node"],"requiresDirs":[absolute],"envAllowlist":["JAVA_HOME","GOPRIVATE"]})).unwrap();
        assert_eq!(env["envAllowlist"], json!(["JAVA_HOME", "GOPRIVATE"]));
        assert_eq!(
            validate_environment(&json!({"envAllowlist":["lowercase"]}))
                .unwrap_err()
                .to_string(),
            "INVALID_ENV_NAME"
        );
        for secret in [
            "MY_API_KEY",
            "SERVICE_TOKEN",
            "DB_PASSWORD",
            "MODEL_API_KEY",
            "LLM_HEADER_X",
        ] {
            assert_eq!(
                validate_environment(&json!({"envAllowlist":[secret]}))
                    .unwrap_err()
                    .to_string(),
                "ENV_ALLOWLIST_DENIED",
                "{secret}"
            );
        }
    }
    #[test]
    fn budget_extend_check_refuses_non_extension() {
        let usage = json!({"totalTokens":"120","inputTokens":"70","outputTokens":"50"});
        // Raising the tripped cap above observed usage is a real extension.
        assert!(budget_extend_check(&json!({"caps":{"maxTotalTokens":"1000"}}), &usage).is_ok());
        // A cap at or below observed usage extends nothing.
        assert_eq!(
            budget_extend_check(&json!({"caps":{"maxTotalTokens":"100"}}), &usage)
                .unwrap_err()
                .to_string(),
            "BUDGET_STILL_EXCEEDED"
        );
        assert_eq!(
            budget_extend_check(&json!({"caps":{"maxInputTokens":"70"}}), &usage)
                .unwrap_err()
                .to_string(),
            "BUDGET_STILL_EXCEEDED"
        );
    }
    #[test]
    fn spawn_env_admits_only_validated_names() {
        let allow = vec!["JAVA_HOME".to_string(), "GOPRIVATE".to_string()];
        assert!(adapter::spawn_env_allowed("JAVA_HOME", &allow));
        assert!(adapter::spawn_env_allowed("GOPRIVATE", &allow));
        assert!(adapter::spawn_env_allowed("PATH", &allow));
        assert!(adapter::spawn_env_allowed("MODEL_API_KEY", &allow));
        assert!(adapter::spawn_env_allowed("NO_PROXY", &[]));
        assert!(adapter::spawn_env_allowed("no_proxy", &[]));
        assert!(!adapter::spawn_env_allowed("HTTP_PROXY", &[]));
        assert!(adapter::spawn_env_allowed("LLM_HEADER_x", &allow));
        assert!(!adapter::spawn_env_allowed("AWS_SECRET", &allow));
        assert!(!adapter::spawn_env_allowed("HOME_DIR_CUSTOM", &allow));
        // The empty allowlist inherits only the fixed core set.
        assert!(!adapter::spawn_env_allowed("JAVA_HOME", &[]));
    }
    #[test]
    fn diagnostics_hashes_bodies_and_masks_foreign_paths() {
        let root = std::env::temp_dir().join("nwf-diagnostics");
        let workspace = root.join("task");
        let outside = root.join("other/file.txt");
        let inside = workspace.join("src/x.rs");
        let ws = workspace.to_str().unwrap();
        let event = json!({"type":"tool.completed","payload":{"command":"rm -rf /secret/path","nativeItemId":"item-1","detail":outside}});
        let v = diagnostics_value(&event, ws);
        assert!(v["payload"]["command"]["sha256"].is_string());
        assert_eq!(v["payload"]["command"]["size"], "21");
        assert!(diagnostics_value(&json!(outside), ws)["sha256"].is_string());
        assert!(diagnostics_value(&json!(inside), ws).is_string());
    }
    #[test]
    fn write_scope_check_flags_out_of_scope_and_denied() {
        let root = std::env::temp_dir().join("nwf-scope");
        let workspace = root.join("task");
        let extra = root.join("extra");
        let t = json!({"workspace":workspace,"writeScope":{"extraWriteDirs":[extra],"denyPaths":["secrets"],"enforcement":"APP_SERVER_WRITABLE_ROOTS+HOST_OBSERVED_DENYLIST"}});
        assert_eq!(write_scope_check(&t, "src/x.rs"), None);
        assert_eq!(
            write_scope_check(&t, extra.join("y.txt").to_str().unwrap()),
            None
        );
        assert_eq!(
            write_scope_check(&t, "secrets/key.txt"),
            Some("DENIED_PATH")
        );
        assert_eq!(
            write_scope_check(&t, "../outside.txt"),
            Some("OUT_OF_SCOPE")
        );
        assert_eq!(
            write_scope_check(&t, root.join("elsewhere/z.txt").to_str().unwrap()),
            Some("OUT_OF_SCOPE")
        );
    }
    #[test]
    fn file_approval_scope_requires_exact_item_turn_thread_generation_and_paths() {
        let t = json!({"generation":"g1","nativeSessionId":"th1","nativeTurnId":"tu1","nativeFileChanges":{"i1":{"generation":"g1","nativeSessionId":"th1","nativeTurnId":"tu1","paths":["one.txt"]}}});
        let p = json!({"itemId":"i1","threadId":"th1","turnId":"tu1"});
        assert_eq!(file_approval_paths(&t, &p), Some(json!(["one.txt"])));
        for (key, value) in [
            ("itemId", "unknown"),
            ("threadId", "other"),
            ("turnId", "old"),
        ] {
            let mut wrong = p.clone();
            wrong[key] = json!(value);
            assert!(file_approval_paths(&t, &wrong).is_none());
        }
        for key in ["generation", "nativeSessionId", "nativeTurnId"] {
            let mut wrong = t.clone();
            wrong[key] = json!("new");
            assert!(file_approval_paths(&wrong, &p).is_none());
        }
        for paths in [Value::Null, json!([]), json!([null]), json!([""])] {
            let mut wrong = t.clone();
            wrong["nativeFileChanges"]["i1"]["paths"] = paths;
            assert!(file_approval_paths(&wrong, &p).is_none());
        }
    }
    #[test]
    fn stop_intake_fence_preserves_completed_outcome_after_cleanup() {
        for (before, after) in [("SUCCEEDED", "SUCCEEDED"), ("FAILED", "FAILED"), ("RUNNING", "CANCELLED"), ("UNKNOWN", "CANCELLED")] {
            let mut task = json!({"state":before});
            mark_stop_requested(&mut task);
            assert_eq!(task["state"], "STOP_REQUESTED");
            // Repeated STOP admissions must not erase the original terminal fact.
            mark_stop_requested(&mut task);
            settle_stop_state(&mut task);
            apply_stop_proof(&mut task, true, None);
            assert_eq!(task["state"], after);
            assert_eq!(task["quiescent"], true);
            apply_stop_proof(&mut task, false, None);
            assert_eq!(task["state"], "UNKNOWN");
        }
    }
    #[test]
    fn platform_pin_and_quiescence_fail_closed() {
        let pin: Value = serde_json::from_str(include_str!("../protocol/codex.lock.json")).unwrap();
        let platform = if cfg!(target_os = "macos") {
            "macos-arm64"
        } else if cfg!(target_os = "linux") {
            "linux-x86_64"
        } else {
            "windows-x86_64"
        };
        assert!(
            verify_pin(
                &pin,
                pin["version"].as_str().unwrap(),
                pin["platforms"][platform]["executableSha256"]
                    .as_str()
                    .unwrap()
            )
            .is_ok()
        );
        assert_eq!(
            verify_pin(&pin, pin["version"].as_str().unwrap(), "changed")
                .unwrap_err()
                .to_string(),
            "ENGINE_BINARY_CHANGED"
        );
        let mut task = json!({"state":"CANCELLED"});
        apply_stop_proof(&mut task, false, None);
        assert_eq!(task["state"], "UNKNOWN");
        assert_eq!(task["quiescent"], false);
    }
    #[test]
    fn extend_blockers_require_terminal_budget_state() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"native-x","state":"BUDGET_EXCEEDED","quiescent":true,"nativeSessionId":"th","nativeTurnId":"tu","generation":"g","segmentId":"seg","currentCommandId":"c","inputRequests":{}});
        s.save(&t).unwrap();
        // No seal yet: the exceeded turn must prove its boundary first.
        assert_eq!(
            extend_blockers(&s, &t, 1).unwrap_err().to_string(),
            "EFFECT_RECONCILIATION_REQUIRED"
        );
        s.seal_turn(
            "native-x",
            "tu",
            "c",
            "g",
            "seg",
            "interrupted",
            Some("BUDGET_EXCEEDED"),
        )
        .unwrap();
        assert!(extend_blockers(&s, &t, 1).is_ok());
        // Wrong terminal state refuses admission rather than reopening.
        let mut other = t.clone();
        other["state"] = json!("SUCCEEDED");
        assert_eq!(
            extend_blockers(&s, &other, 1).unwrap_err().to_string(),
            "NATIVE_NOT_IDLE"
        );
    }
    #[test]
    fn argv_bounds_and_empty_lists_refused() {
        assert_eq!(
            validate_argv(&json!(["cmd.exe", "/d", "/c", "echo hi"])).unwrap(),
            vec!["cmd.exe", "/d", "/c", "echo hi"]
        );
        assert!(validate_argv(&json!([])).is_err());
        assert!(validate_argv(&json!([""])).is_err());
        assert!(validate_argv(&json!(["a\0b"])).is_err());
        let many: Vec<String> = (0..33).map(|i| format!("arg{i}")).collect();
        assert!(validate_argv(&json!(many)).is_err());
        assert!(validate_argv(&json!("cmd /c echo")).is_err());
    }
    #[test]
    fn mcp_servers_validation_rejects_bad_shapes_and_secrets() {
        let ok = validate_mcp_servers(&json!([{
            "id": "fake-1", "command": "node", "args": ["server.js"],
            "env": {"NWF_MARKER": "yes"}
        }]))
        .unwrap();
        assert_eq!(ok[0]["id"], "fake-1");
        assert_eq!(ok[0]["env"]["NWF_MARKER"], "yes");
        assert_eq!(
            validate_mcp_servers(&json!([{"id": "9bad", "command": "node"}]))
                .unwrap_err()
                .to_string(),
            "INVALID_ID"
        );
        assert!(
            validate_mcp_servers(&json!([
                {"id": "a", "command": "node"},
                {"id": "a", "command": "node"}
            ]))
            .is_err()
        );
        assert!(
            validate_mcp_servers(&json!([{"id": "x", "command": "node", "extra": 1}])).is_err()
        );
        // Credential-shaped env names are refused exactly like envAllowlist.
        assert_eq!(
            validate_mcp_servers(&json!([{"id": "x", "command": "node",
                "env": {"API_TOKEN": "abc"}}]))
            .unwrap_err()
            .to_string(),
            "ENV_ALLOWLIST_DENIED"
        );
        // A value carrying live credential material must never reach disk.
        unsafe {
            std::env::set_var("LLM_HEADER_x_nwf_w4b_test", "w4b-secret-marker-9876");
        }
        assert_eq!(
            validate_mcp_servers(&json!([{"id": "x", "command": "node",
                "env": {"NWF_MARKER": "prefix w4b-secret-marker-9876 suffix"}}]))
            .unwrap_err()
            .to_string(),
            "MCP_ENV_SECRET_VALUE"
        );
        // An unrelated env value is fine.
        assert!(
            validate_mcp_servers(&json!([{"id": "x", "command": "node",
            "env": {"NWF_MARKER": "harmless"}}]))
            .is_ok()
        );
        unsafe {
            std::env::remove_var("LLM_HEADER_x_nwf_w4b_test");
        }
    }
    #[test]
    fn skills_validation_requires_absolute_dir_with_manifest() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("SKILL.md"), "# skill\n").unwrap();
        let ok = validate_skills(&json!([{
            "name": "nwf-skill",
            "sourcePath": dir.path().to_string_lossy()
        }]))
        .unwrap();
        assert_eq!(ok[0]["name"], "nwf-skill");
        assert_eq!(
            validate_skills(&json!([{"name": "x", "sourcePath": "relative/dir"}]))
                .unwrap_err()
                .to_string(),
            "INVALID_REQUEST"
        );
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(
            validate_skills(&json!([{"name": "x", "sourcePath": empty.path().to_string_lossy()}]))
                .unwrap_err()
                .to_string(),
            "SKILL_MANIFEST_MISSING"
        );
        assert_eq!(
            validate_skills(&json!([{"name": "bad name!", "sourcePath": dir.path().to_string_lossy()}]))
                .unwrap_err()
                .to_string(),
            "INVALID_ID"
        );
    }
    #[test]
    fn hooks_validation_accepts_bound_events_and_refuses_the_rest() {
        let ok = validate_hooks(&json!([{
            "event": "sessionStart",
            "command": "cmd.exe /d /c echo hi >> hook.log",
            "timeoutSec": "30"
        }]))
        .unwrap();
        assert_eq!(ok[0]["event"], "sessionStart");
        assert_eq!(
            validate_hooks(&json!([{"event": "subagentStart", "command": "x"}]))
                .unwrap_err()
                .to_string(),
            "INVALID_HOOK_EVENT"
        );
        assert_eq!(
            validate_hooks(&json!([{"event": "sessionStart", "command": "x", "extra": 1}]))
                .unwrap_err()
                .to_string(),
            "UNEXPECTED_FIELD"
        );
        assert!(
            validate_hooks(&json!([{"event": "sessionStart", "command": "x", "timeoutSec": "0"}]))
                .is_err()
        );
        assert!(
            validate_hooks(
                &json!([{"event": "sessionStart", "command": "x", "timeoutSec": "601"}])
            )
            .is_err()
        );
    }
    #[test]
    fn hook_trust_hash_reproduces_engine_accepted_scheme() {
        // Known answer: the pinned app-server accepted this exact trusted_hash
        // for a SessionStart command hook (probe run, hooks/list trustStatus).
        let handler = hook_handler_identity(
            "cmd.exe /d /c echo NWF-HOOK-RAN >> hook-ran.log",
            5,
            false,
            None,
        );
        assert_eq!(
            hook_trust_hash("session_start", None, &handler),
            "sha256:8dc3caff36c80cdef168f16efc96996bb3f6b17c1ec78bec5ffa89550aa277e7"
        );
        // A matcher participates in the hashed identity.
        let with_matcher = hook_trust_hash("pre_tool_use", Some(".*"), &handler);
        assert_ne!(with_matcher, hook_trust_hash("pre_tool_use", None, &handler));
    }
    #[test]
    fn task_config_appends_mcp_and_hook_sections() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::write(
            profile.join("config.toml"),
            "model = \"m\"\n[features]\nmulti_agent = false\n[analytics]\nenabled = false\n",
        )
        .unwrap();
        let home = root.path().join("native").join("task-1");
        std::fs::create_dir_all(&home).unwrap();
        let t = json!({
            "mcpServers": [{"id":"nwffake","command":"node","args":["srv.js"],"env":{"NWF_MARKER":"yes"}}],
            "hooks": [{"event":"sessionStart","command":"echo hi","timeoutSec":"30","async":true}],
        });
        let text = task_config(root.path(), &home, &t).unwrap();
        assert!(text.contains("[features]\nhooks = true\nmulti_agent = false"));
        assert!(text.contains("[mcp_servers.nwffake]"));
        assert!(text.contains("command = \"node\""));
        assert!(text.contains("args = [\"srv.js\"]"));
        assert!(text.contains("NWF_MARKER"));
        assert!(text.contains("[[hooks.SessionStart]]"));
        assert!(text.contains("[hooks.state."));
        assert!(text.contains(":session_start:0:0"));
        assert!(text.contains("trusted_hash = \"sha256:"));
        // No bindings: the profile text comes back untouched.
        let bare = task_config(root.path(), &home, &json!({})).unwrap();
        assert!(!bare.contains("[mcp_servers."));
        assert!(!bare.contains("[hooks]"));
    }
    #[test]
    fn skill_staging_copies_files_with_sha256_facts() {
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("SKILL.md"), "# s\n").unwrap();
        std::fs::create_dir(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("sub").join("a.txt"), "data").unwrap();
        let home = tempfile::tempdir().unwrap();
        let t = json!({"skills":[{"name":"sk","sourcePath":src.path().to_string_lossy()}]});
        let staged = stage_skills(home.path(), &t).unwrap();
        assert_eq!(staged[0]["name"], "sk");
        assert_eq!(staged[0]["files"].as_array().unwrap().len(), 2);
        assert!(home.path().join("skills/sk/SKILL.md").is_file());
        assert!(home.path().join("skills/sk/sub/a.txt").is_file());
        let md = staged[0]["files"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["path"] == "SKILL.md")
            .unwrap();
        assert_eq!(md["sha256"], hash(b"# s\n"));
    }
    #[test]
    fn hook_wrapper_scrubs_model_credentials() {
        let home = tempfile::tempdir().unwrap();
        let t = json!({"hooks":[{"event":"sessionStart","command":"echo RUN"}]});
        write_hook_wrappers(home.path(), &t).unwrap();
        let body = std::fs::read_to_string(hook_script_path(home.path(), 0)).unwrap();
        assert!(body.contains("MODEL_API_KEY"));
        assert!(body.contains("echo RUN"));
    }
    // C12 (store side is covered by the query helper): cursor/before/limit
    // accept only mutual-exclusive decimal bounds in range.
    #[test]
    fn message_page_query_validation_is_strict() {
        let q = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<String, String>>()
        };
        // Defaults: recent page, limit 50.
        assert_eq!(message_page_query(&q(&[])).unwrap(), (None, None, 50));
        assert_eq!(
            message_page_query(&q(&[("cursor", "0")])).unwrap(),
            (Some(0), None, 50)
        );
        assert_eq!(
            message_page_query(&q(&[("cursor", "9223372036854775807")])).unwrap(),
            (Some(i64::MAX), None, 50)
        );
        assert_eq!(
            message_page_query(&q(&[("before", "1")])).unwrap(),
            (None, Some(1), 50)
        );
        assert_eq!(
            message_page_query(&q(&[("limit", "100")])).unwrap(),
            (None, None, 100)
        );
        // cursor+before, unknown keys, non-decimal and out-of-range all fail.
        assert!(message_page_query(&q(&[("cursor", "1"), ("before", "2")])).is_err());
        assert!(message_page_query(&q(&[("foo", "1")])).is_err());
        assert!(message_page_query(&q(&[("cursor", "-1")])).is_err());
        assert!(message_page_query(&q(&[("cursor", "abc")])).is_err());
        assert!(message_page_query(&q(&[("cursor", "1.5")])).is_err());
        assert!(message_page_query(&q(&[("cursor", " 1")])).is_err());
        assert!(message_page_query(&q(&[("cursor", "9223372036854775808")])).is_err());
        assert!(message_page_query(&q(&[("before", "0")])).is_err());
        assert!(message_page_query(&q(&[("limit", "0")])).is_err());
        assert!(message_page_query(&q(&[("limit", "101")])).is_err());
        assert!(message_page_query(&q(&[("limit", "30x")])).is_err());
        assert!(message_page_query(&q(&[("cursor", "")])).is_err());
    }
    fn test_host() -> Shared {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        std::mem::forget(dir);
        Arc::new(Host {
            root: root.clone(),
            registry: Registry {
                executable: root.join("codex.exe"),
                version: "v-test".into(),
                executable_digest: "d".into(),
                schema_digest: "d".into(),
                launch_config_digest: "d".into(),
                project: root.clone(),
                project_id: "proj-test".into(),
                model: "m".into(),
                base_url: "http://127.0.0.1".into(),
                tool_profile: "native".into(),
                model_catalog: None,
                model_catalog_digest: None,
                experimental_schema_digest: None,
                command_runner: None,
                command_runner_digest: None,
                runner_node: None,
                runner_node_digest: None,
                policy_presets: vec![],
            },
            store: Mutex::new(Store::open(Path::new(":memory:")).unwrap()),
            peers: AsyncMutex::new(HashMap::new()),
            aux_peers: AsyncMutex::new(HashMap::new()),
            collab_peers: AsyncMutex::new(HashMap::new()),
            terminals: Mutex::new(HashMap::new()),
            preparation: AsyncMutex::new(()),
            read_verification: AsyncMutex::new(None),
            runners: AsyncMutex::new(HashMap::new()),
            runner_admission: AsyncMutex::new(()),
            queue: Mutex::new(()),
            max_active: 4,
            max_active_aux: 2,
            max_collab_children: 4,
            boot: "boot-test".into(),
            token: "t".repeat(24),
            _lock: tempfile::NamedTempFile::new().unwrap().into_file(),
        })
    }
    // Transition quiesce drains the same children a real STOP drains — aux
    // sessions and collaboration children — and returns the honest inventory
    // for the transition/epoch facts. Regression: the quiesce used to leave
    // them running with aux_reason:None.
    #[tokio::test]
    async fn owner_quiesce_with_transition_reasons_drains_and_names_every_child() {
        let h = test_host();
        let task = "native-drain";
        {
            let mut s = h.store.lock().unwrap();
            s.save(&json!({"taskId":task,"requestKey":"k-create","state":"RUNNING","quiescent":false,"ownerBootId":"boot-test","generation":"g1","segmentId":"seg-1","currentCommandId":"cmd-1","nativeSessionId":"thread-1","nativeTurnId":"turn-1","inputRequests":{},"engineVersion":"v-test","stateVersion":"1"})).unwrap();
            s.accept_aux(
                task,
                &json!({"auxSessionId":"aux-1","question":"q","workspace":"D:/aux","createdAt":store::now()}),
                "k-aux",
                &json!({"requestKey":"k-aux","question":"q"}),
                &json!({"auxSessionId":"aux-1","state":"DISPATCHING"}),
            )
            .unwrap();
            let collab = json!({"collaborationId":"collab-1","name":Value::Null,"createdAt":store::now()});
            let children = vec![
                json!({"childId":"writer","goal":"g","goalSha256":"sha-w","dependsOn":[],"contextPackageIds":[],"writeScope":Value::Null,"budget":Value::Null,"workspace":"D:/ws/w","createdAt":store::now(),"deadlineEpochMillis":"9999999999999"}),
                json!({"childId":"after","goal":"g","goalSha256":"sha-a","dependsOn":["writer"],"contextPackageIds":[],"writeScope":Value::Null,"budget":Value::Null,"workspace":"D:/ws/a","createdAt":store::now(),"deadlineEpochMillis":"9999999999999"}),
            ];
            s.accept_collaboration(
                task,
                &collab,
                &children,
                "k-collab",
                &json!({"requestKey":"k-collab","children":[]}),
                &json!({"kind":"WORKER_LOCAL","requestKey":"k-collab","state":"ACCEPTED","collaborationId":"collab-1"}),
                &json!({"collaborationId":"collab-1","childCount":"2"}),
            )
            .unwrap();
            let (launch, _) = s.collab_pump(task, "collab-1", 4).unwrap();
            assert_eq!(launch, vec!["writer".to_string()]);
        }
        let affected = stop_owner(&h, task, Some(("TRANSITION_QUIESCE", "PARENT_TRANSITION")))
            .await
            .unwrap();
        // Every child the quiesce stopped is named with its settlement reason.
        let aux = affected.iter().find(|c| c["auxSessionId"] == "aux-1").unwrap();
        assert_eq!(aux["kind"], "AUXILIARY_SESSION");
        assert_eq!(aux["newState"], "CANCELLED");
        assert_eq!(aux["reasonCode"], "TRANSITION_QUIESCE");
        let writer = affected.iter().find(|c| c["childId"] == "writer").unwrap();
        assert_eq!(writer["kind"], "COLLABORATION_CHILD");
        assert_eq!(writer["newState"], "CANCELLED");
        assert_eq!(writer["reasonCode"], "PARENT_TRANSITION");
        let after = affected.iter().find(|c| c["childId"] == "after").unwrap();
        assert_eq!(after["newState"], "BLOCKED");
        assert_eq!(after["reasonCode"], "PARENT_TRANSITION");
        {
            let s = h.store.lock().unwrap();
            // Durable rows carry the same settlement — nothing keeps running.
            assert_eq!(s.aux_session(task, "aux-1").unwrap()["state"], "CANCELLED");
            assert_eq!(
                s.collab_child(task, "collab-1", "writer").unwrap()["state"],
                "CANCELLED"
            );
            assert_eq!(
                s.collab_child(task, "collab-1", "after").unwrap()["state"],
                "BLOCKED"
            );
            assert_eq!(
                s.collaboration(task, "collab-1").unwrap()["state"],
                "CANCELLED"
            );
            let t = s.task(task).unwrap();
            assert_eq!(t["state"], "CANCELLED");
            assert_eq!(t["quiescent"], true);
            // The durable stop proof carries the same inventory the transition
            // facts copy from.
            assert_eq!(
                t["stopProof"]["affectedChildren"].as_array().unwrap().len(),
                affected.len()
            );
        }
        // A second drain has nothing left to stop — the inventory is honest.
        assert!(
            stop_owner(&h, task, Some(("TRANSITION_QUIESCE", "PARENT_TRANSITION")))
                .await
                .unwrap()
                .is_empty()
        );
    }
    // A quiesce that must not touch children (no reasons) leaves them running.
    #[tokio::test]
    async fn owner_quiesce_without_reasons_leaves_children_untouched() {
        let h = test_host();
        let task = "native-nodrain";
        {
            let mut s = h.store.lock().unwrap();
            s.save(&json!({"taskId":task,"requestKey":"k-create","state":"RUNNING","quiescent":false,"ownerBootId":"boot-test","generation":"g1","segmentId":"seg-1","currentCommandId":"cmd-1","nativeSessionId":"thread-1","nativeTurnId":"turn-1","inputRequests":{},"engineVersion":"v-test","stateVersion":"1"})).unwrap();
            s.accept_aux(
                task,
                &json!({"auxSessionId":"aux-1","question":"q","workspace":"D:/aux","createdAt":store::now()}),
                "k-aux",
                &json!({"requestKey":"k-aux","question":"q"}),
                &json!({"auxSessionId":"aux-1","state":"DISPATCHING"}),
            )
            .unwrap();
        }
        let affected = stop_owner(&h, task, None).await.unwrap();
        assert!(affected.is_empty());
        let s = h.store.lock().unwrap();
        assert_eq!(s.aux_session(task, "aux-1").unwrap()["state"], "RUNNING");
    }
    // F33: the source seal requires main-task quiescence — a live
    // RUNNING/PREPARING owner or a still-dispatchable QUEUED intent refuses
    // TASK_NOT_QUIESCENT before any child drain, terminal kill or write
    // fence; handoff.seal reports the same verdict as BLOCKED_BY_STATE.
    #[tokio::test]
    async fn source_seal_requires_main_task_quiescence() {
        let h = test_host();
        // A live RUNNING owner with drainable children refuses up front.
        {
            let mut s = h.store.lock().unwrap();
            s.save(&json!({"taskId":"native-seal-live","requestKey":"k-create","state":"RUNNING","launchState":"DISPATCH_STARTED","quiescent":false,"ownerBootId":"boot-test","generation":"g1","segmentId":"seg-1","currentCommandId":"cmd-1","nativeSessionId":"thread-1","nativeTurnId":"turn-1","inputRequests":{},"engineVersion":"v-test","stateVersion":"1"})).unwrap();
            s.accept_aux(
                "native-seal-live",
                &json!({"auxSessionId":"aux-live","question":"q","workspace":"D:/aux","createdAt":store::now()}),
                "k-aux",
                &json!({"requestKey":"k-aux","question":"q"}),
                &json!({"auxSessionId":"aux-live","state":"DISPATCHING"}),
            )
            .unwrap();
            let collab = json!({"collaborationId":"collab-live","name":Value::Null,"createdAt":store::now()});
            let children = vec![
                json!({"childId":"writer","goal":"g","goalSha256":"sha-w","dependsOn":[],"contextPackageIds":[],"writeScope":Value::Null,"budget":Value::Null,"workspace":"D:/ws/w","createdAt":store::now(),"deadlineEpochMillis":"9999999999999"}),
            ];
            s.accept_collaboration(
                "native-seal-live",
                &collab,
                &children,
                "k-collab",
                &json!({"requestKey":"k-collab","children":[]}),
                &json!({"kind":"WORKER_LOCAL","requestKey":"k-collab","state":"ACCEPTED","collaborationId":"collab-live"}),
                &json!({"collaborationId":"collab-live","childCount":"1"}),
            )
            .unwrap();
            let (launch, _) = s.collab_pump("native-seal-live", "collab-live", 4).unwrap();
            assert_eq!(launch, vec!["writer".to_string()]);
        }
        let body = json!({"requestKey":"k-seal-live"});
        let snapshot = h.store.lock().unwrap().task("native-seal-live").unwrap();
        let err = seal_route(h.clone(), "native-seal-live", &snapshot, &[], "POST", &body)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "TASK_NOT_QUIESCENT");
        {
            let s = h.store.lock().unwrap();
            let cur = s.task("native-seal-live").unwrap();
            // Refusal lands no half-applied fence, seal row or request receipt.
            assert!(cur["sealedFor"].is_null());
            assert!(s.task_seal("native-seal-live").unwrap().is_none());
            assert!(s.existing("native-seal-live", "k-seal-live", &body).unwrap().is_none());
            // No drain fired: open children keep their honest live states.
            assert_eq!(s.aux_session("native-seal-live", "aux-live").unwrap()["state"], "RUNNING");
            assert_eq!(s.collab_child("native-seal-live", "collab-live", "writer").unwrap()["state"], "RUNNING");
            let ev = s.events("native-seal-live", 0, 100).unwrap();
            let kinds: Vec<&str> = ev["items"]
                .as_array().unwrap().iter().filter_map(|e| e["type"].as_str()).collect();
            assert!(!kinds.contains(&"source.sealed"));
            // handoff.seal projects the same verdict, not a false AVAILABLE.
            let sc = source_capabilities(&h, &cur);
            assert_eq!(sc["actions"]["handoff.seal"]["state"], "BLOCKED_BY_STATE");
            assert_eq!(sc["actions"]["handoff.seal"]["reasonCode"], "TASK_NOT_QUIESCENT");
        }
        // PREPARING refuses the same way — a dispatch in flight owns writes.
        {
            let s = h.store.lock().unwrap();
            s.save(&json!({"taskId":"native-seal-prep","requestKey":"k-create","state":"PREPARING","launchState":"PREPARING","quiescent":false,"generation":"g1","segmentId":"seg-1","inputRequests":{},"stateVersion":"1"})).unwrap();
            let t = s.task("native-seal-prep").unwrap();
            drop(s);
            let err = seal_route(h.clone(), "native-seal-prep", &t, &[], "POST", &json!({"requestKey":"k-seal-prep"}))
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), "TASK_NOT_QUIESCENT");
        }
    }
    // A still-dispatchable queue intent must not seal: codex admission selects
    // on launchState alone, so a QUEUED launchState refuses even when the
    // recorded quiescent flag or a settled-looking state says otherwise.
    #[tokio::test]
    async fn source_seal_refuses_still_schedulable_queue_intent() {
        let h = test_host();
        let body = json!({"requestKey":"k-seal-q"});
        for (state, quiescent) in [("QUEUED", true), ("CANCELLED", true)] {
            let task = format!("native-seal-q-{state}");
            {
                let s = h.store.lock().unwrap();
                s.save(&json!({"taskId":task,"requestKey":"k-create","state":state,"launchState":"QUEUED","queueOrdinal":1,"quiescent":quiescent,"generation":"g1","segmentId":"seg-1","inputRequests":{},"stateVersion":"1"})).unwrap();
                let t = s.task(&task).unwrap();
                drop(s);
                let err = seal_route(h.clone(), &task, &t, &[], "POST", &body)
                    .await
                    .unwrap_err();
                assert_eq!(err.to_string(), "TASK_NOT_QUIESCENT", "{state} still schedulable");
                let s = h.store.lock().unwrap();
                assert!(s.task(&task).unwrap()["sealedFor"].is_null());
                let sc = source_capabilities(&h, &s.task(&task).unwrap());
                assert_eq!(sc["actions"]["handoff.seal"]["reasonCode"], "TASK_NOT_QUIESCENT");
            }
        }
        // Once the queue intent is spent the same row seals honestly.
        {
            let mut s = h.store.lock().unwrap();
            s.update("native-seal-q-CANCELLED", |t| {
                t["launchState"] = json!("REJECTED");
            })
            .unwrap();
            let t = s.task("native-seal-q-CANCELLED").unwrap();
            drop(s);
            let (status, v) = seal_route(h.clone(), "native-seal-q-CANCELLED", &t, &[], "POST", &body)
                .await
                .unwrap();
            assert_eq!(status, 202);
            assert_eq!(v["state"], "APPLIED");
            let s = h.store.lock().unwrap();
            assert_eq!(s.task("native-seal-q-CANCELLED").unwrap()["sealedFor"]["sealId"], v["sealId"]);
        }
    }
    // A terminal verdict seals while children still run: the drain is recorded
    // in the seal's affectedChildren, then the fence refuses a second seal.
    #[tokio::test]
    async fn source_seal_terminal_task_drains_children_and_fences() {
        let h = test_host();
        let task = "native-seal-done";
        {
            let mut s = h.store.lock().unwrap();
            s.save(&json!({"taskId":task,"requestKey":"k-create","state":"SUCCEEDED","launchState":"DISPATCH_STARTED","quiescent":false,"ownerBootId":"boot-test","generation":"g1","segmentId":"seg-1","currentCommandId":"cmd-1","nativeSessionId":"thread-1","nativeTurnId":"turn-1","inputRequests":{},"engineVersion":"v-test","stateVersion":"1"})).unwrap();
            s.accept_aux(
                task,
                &json!({"auxSessionId":"aux-done","question":"q","workspace":"D:/aux","createdAt":store::now()}),
                "k-aux",
                &json!({"requestKey":"k-aux","question":"q"}),
                &json!({"auxSessionId":"aux-done","state":"DISPATCHING"}),
            )
            .unwrap();
            let collab = json!({"collaborationId":"collab-done","name":Value::Null,"createdAt":store::now()});
            let children = vec![
                json!({"childId":"writer","goal":"g","goalSha256":"sha-w","dependsOn":[],"contextPackageIds":[],"writeScope":Value::Null,"budget":Value::Null,"workspace":"D:/ws/w","createdAt":store::now(),"deadlineEpochMillis":"9999999999999"}),
                json!({"childId":"after","goal":"g","goalSha256":"sha-a","dependsOn":["writer"],"contextPackageIds":[],"writeScope":Value::Null,"budget":Value::Null,"workspace":"D:/ws/a","createdAt":store::now(),"deadlineEpochMillis":"9999999999999"}),
            ];
            s.accept_collaboration(
                task,
                &collab,
                &children,
                "k-collab",
                &json!({"requestKey":"k-collab","children":[]}),
                &json!({"kind":"WORKER_LOCAL","requestKey":"k-collab","state":"ACCEPTED","collaborationId":"collab-done"}),
                &json!({"collaborationId":"collab-done","childCount":"2"}),
            )
            .unwrap();
            let (launch, _) = s.collab_pump(task, "collab-done", 4).unwrap();
            assert_eq!(launch, vec!["writer".to_string()]);
        }
        let body = json!({"requestKey":"k-seal-done","targetRef":{"kind":"HANDOFF"}});
        let t = h.store.lock().unwrap().task(task).unwrap();
        let (status, v) = seal_route(h.clone(), task, &t, &[], "POST", &body).await.unwrap();
        assert_eq!(status, 202);
        assert_eq!(v["state"], "APPLIED");
        {
            let s = h.store.lock().unwrap();
            let cur = s.task(task).unwrap();
            assert_eq!(cur["sealedFor"]["sealId"], v["sealId"]);
            // The drain really fired and is named in the seal facts.
            assert_eq!(s.aux_session(task, "aux-done").unwrap()["state"], "CANCELLED");
            assert_eq!(s.collab_child(task, "collab-done", "writer").unwrap()["state"], "CANCELLED");
            assert_eq!(s.collab_child(task, "collab-done", "after").unwrap()["state"], "BLOCKED");
            let affected = v["affectedChildren"].as_array().unwrap();
            assert!(affected.iter().any(|c| c["auxSessionId"] == "aux-done" && c["newState"] == "CANCELLED"));
            assert!(affected.iter().any(|c| c["childId"] == "writer" && c["newState"] == "CANCELLED"));
            assert!(affected.iter().any(|c| c["childId"] == "after" && c["newState"] == "BLOCKED"));
            assert_eq!(s.task_seal(task).unwrap().unwrap()["sealId"], v["sealId"]);
            let sc = source_capabilities(&h, &cur);
            assert_eq!(sc["actions"]["handoff.seal"]["reasonCode"], "SOURCE_ALREADY_SEALED");
        }
        // A different key on the sealed source refuses the one-way fence.
        let t = h.store.lock().unwrap().task(task).unwrap();
        let err = seal_route(h.clone(), task, &t, &[], "POST", &json!({"requestKey":"k-seal-again"}))
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "SOURCE_ALREADY_SEALED");
    }
    // Recorded quiescence without a terminal verdict still seals: the fence is
    // what prevents a dead writer's task from resuming (F33), and the same
    // gate inside the committing transaction refuses a task that went live.
    #[tokio::test]
    async fn source_seal_commit_rechecks_quiescence() {
        let h = test_host();
        {
            let s = h.store.lock().unwrap();
            s.save(&json!({"taskId":"native-seal-unknown","requestKey":"k-create","state":"UNKNOWN","launchState":"DISPATCH_STARTED","quiescent":true,"generation":"g1","segmentId":"seg-1","inputRequests":{},"stateVersion":"1"})).unwrap();
        }
        let t = h.store.lock().unwrap().task("native-seal-unknown").unwrap();
        let (status, _) = seal_route(h.clone(), "native-seal-unknown", &t, &[], "POST", &json!({"requestKey":"k-seal-unk"}))
            .await
            .unwrap();
        assert_eq!(status, 202);
        // Commit-time gate: a row live again between route check and commit
        // refuses inside the same transaction that would land the fence.
        let mut s = h.store.lock().unwrap();
        s.save(&json!({"taskId":"native-seal-race","requestKey":"k-create","state":"RUNNING","launchState":"DISPATCH_STARTED","quiescent":false,"generation":"g1","segmentId":"seg-1","inputRequests":{},"stateVersion":"1"})).unwrap();
        let seal = json!({"sealId":"seal-race","handoffId":"wh-race","targetRef":Value::Null,"proof":{},"affectedChildren":[],"sealedAt":store::now()});
        let err = s
            .seal_source(
                "native-seal-race",
                &seal,
                "k-seal-race",
                &json!({"requestKey":"k-seal-race"}),
                &json!({"kind":"WORKER_LOCAL","requestKey":"k-seal-race","state":"APPLIED"}),
            )
            .unwrap_err();
        assert_eq!(err.to_string(), "TASK_NOT_QUIESCENT");
        assert!(s.task("native-seal-race").unwrap()["sealedFor"].is_null());
        assert!(s.task_seal("native-seal-race").unwrap().is_none());
    }
    // Task detail timing fields surface only real recorded facts: runStartedAt
    // is the first turn dispatch/binding fact, endedAt/completedAt the
    // recorded terminal observation, and absent whenever nothing was recorded.
    #[test]
    fn task_timing_surfaces_only_recorded_facts() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"tm-1","state":"RUNNING","nativeTurnId":"turn-1","generation":"g1","segmentId":"seg-1","createdAt":"2026-01-01T00:00:00Z"});
        s.save(&t).unwrap();
        let mut v = s.task("tm-1").unwrap();
        task_timing(&s, "tm-1", &mut v).unwrap();
        // No turn fact and a live state: both fields honestly absent.
        assert!(v.get("runStartedAt").is_none());
        assert!(v.get("endedAt").is_none());
        assert!(v.get("completedAt").is_none());
        // The first dispatch/binding fact becomes runStartedAt; a later
        // turn.started observation cannot move it.
        let bound = s
            .event("tm-1", "turn.bound", json!({"nativeTurnId":"turn-1"}))
            .unwrap();
        let bound_at = bound["occurredAt"].as_str().unwrap().to_owned();
        s.event("tm-1", "turn.started", json!({"nativeTurnId":"turn-1"}))
            .unwrap();
        let mut v = s.task("tm-1").unwrap();
        task_timing(&s, "tm-1", &mut v).unwrap();
        assert_eq!(v["runStartedAt"], bound_at);
        // SUCCEEDED reports the frozen turn seal's sealedAt.
        s.seal_turn("tm-1", "turn-1", "cmd-1", "g1", "seg-1", "completed", None)
            .unwrap();
        let seal_at = s
            .turn_seal("tm-1", "turn-1")
            .unwrap()
            .unwrap()["sealedAt"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut v = s.task("tm-1").unwrap();
        v["state"] = json!("SUCCEEDED");
        task_timing(&s, "tm-1", &mut v).unwrap();
        assert_eq!(v["endedAt"], seal_at);
        assert_eq!(v["completedAt"], seal_at);
        // CANCELLED with no sealed turn falls back to the stop's own applied
        // observation — and to nothing at all when even that was never
        // recorded.
        s.save(&json!({"taskId":"tm-2","state":"CANCELLED","nativeTurnId":"turn-9","generation":"g1","segmentId":"seg-1","createdAt":"2026-01-01T00:00:00Z"})).unwrap();
        let mut v = s.task("tm-2").unwrap();
        task_timing(&s, "tm-2", &mut v).unwrap();
        assert!(v.get("endedAt").is_none());
        let stop_observed = s
            .event(
                "tm-2",
                "request.observed",
                json!({"kind":"WORKER_LOCAL","requestKey":"k-stop","state":"APPLIED"}),
            )
            .unwrap()["occurredAt"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut v = s.task("tm-2").unwrap();
        task_timing(&s, "tm-2", &mut v).unwrap();
        assert_eq!(v["endedAt"], stop_observed);
        assert_eq!(v["completedAt"], stop_observed);
        // BUDGET_EXCEEDED reports the durable exceed mark, not a guess.
        s.accept_task(
            &json!({"taskId":"tm-3","requestKey":"k-b","state":"RUNNING","budget":{"caps":{"maxTotalTokens":"100"},"enforcement":"SOFT"}}),
            "k-b",
            &json!({"requestKey":"k-b"}),
            &[],
        )
        .unwrap();
        s.usage_observed(
            "tm-3",
            "th",
            "tu-1",
            &json!({"inputTokens":110,"outputTokens":10,"totalTokens":120,"cachedInputTokens":0,"reasoningOutputTokens":0}),
        )
        .unwrap();
        let exceeded_at = s.budget("tm-3").unwrap().unwrap()["exceededAt"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut v = s.task("tm-3").unwrap();
        assert_eq!(v["state"], "BUDGET_EXCEEDED");
        task_timing(&s, "tm-3", &mut v).unwrap();
        assert_eq!(v["endedAt"], exceeded_at);
        // BINDING_UNAVAILABLE reports the refusal event's observation.
        s.save(&json!({"taskId":"tm-4","state":"BINDING_UNAVAILABLE","createdAt":"2026-01-01T00:00:00Z"})).unwrap();
        let refused_at = s
            .event("tm-4", "binding.unavailable", json!({"failures":["x"]}))
            .unwrap()["occurredAt"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut v = s.task("tm-4").unwrap();
        task_timing(&s, "tm-4", &mut v).unwrap();
        assert_eq!(v["endedAt"], refused_at);
        assert_eq!(v["completedAt"], refused_at);
    }
    // GAP-01: sourceCapabilities action keys must match the dotted grammar
    // PAC reads (PersonalMobileControls) and the Pi host emits — camelCase
    // keys made every Codex extension action project NOT_INTEGRATED.
    #[test]
    fn source_capabilities_uses_pac_pi_dotted_vocabulary() {
        let h = test_host();
        let sc = source_capabilities(&h, &json!({"taskId":"t-1","state":"SUCCEEDED"}));
        assert_eq!(sc["schemaVersion"], "source-capabilities/1");
        let actions = sc["actions"].as_object().unwrap();
        // The exact keys PersonalMobileControls looks up under
        // sourceCapabilities.actions for a native task.
        for key in [
            "sideQuestion.create",
            "collaboration.create",
            "adoption.apply",
            "handoff.export",
            "handoff.seal",
            "materials.upload",
        ] {
            assert!(actions.contains_key(key), "PAC-read action {key} missing");
        }
        // The remaining names shared with the Pi host's projection.
        for key in [
            "childMessages.read",
            "collaboration.preflight",
            "adoption.preview",
            "policy.presets",
            "stop.affectedChildren",
        ] {
            assert!(actions.contains_key(key), "Pi-shared action {key} missing");
        }
        // Honest source-local rows keep their own names — no borrowed
        // contract name for facts the boundary does not read.
        for key in ["messageSend", "stop", "pause", "resume", "transitions"] {
            assert!(actions.contains_key(key), "source-local action {key} missing");
        }
        // The old camelCase contract names are gone and every state stays
        // inside the mobile-controls seven-state model.
        for key in [
            "childMessages",
            "collaborationPreflight",
            "collaborationCreate",
            "adoptionPreview",
            "adoptionCreate",
            "auxiliarySessionCreate",
            "sourceSeal",
            "handoffExport",
            "materialUploads",
            "policyPresets",
            "affectedChildrenStop",
        ] {
            assert!(!actions.contains_key(key), "camelCase contract key survived: {key}");
        }
        let states = [
            "AVAILABLE",
            "NOT_INTEGRATED",
            "UNSUPPORTED",
            "DENIED",
            "BLOCKED_BY_STATE",
            "TEMPORARILY_UNAVAILABLE",
            "UNKNOWN",
        ];
        for (key, row) in actions {
            assert!(
                states.contains(&row["state"].as_str().unwrap_or("")),
                "action {key} state outside contract vocabulary"
            );
        }
        // PAUSE/RESUME stay the honest gap; empty presets report
        // NOT_INTEGRATED rather than a fabricated AVAILABLE.
        assert_eq!(actions["pause"]["state"], "UNSUPPORTED");
        assert_eq!(actions["resume"]["state"], "UNSUPPORTED");
        assert_eq!(actions["policy.presets"]["state"], "NOT_INTEGRATED");
        // A sealed source blocks every write-family action with the real
        // reason codes PAC passes through.
        let sealed = source_capabilities(
            &h,
            &json!({"taskId":"t-1","state":"SUCCEEDED","sealedFor":{"kind":"HANDOFF"}}),
        );
        let sealed_actions = sealed["actions"].as_object().unwrap();
        for key in [
            "sideQuestion.create",
            "collaboration.create",
            "collaboration.preflight",
            "adoption.preview",
            "adoption.apply",
            "messageSend",
            "transitions",
        ] {
            assert_eq!(sealed_actions[key]["state"], "BLOCKED_BY_STATE", "{key} on sealed task");
            assert_eq!(sealed_actions[key]["reasonCode"], "SOURCE_SEALED", "{key} reason");
        }
        assert_eq!(sealed_actions["handoff.seal"]["state"], "BLOCKED_BY_STATE");
        assert_eq!(sealed_actions["handoff.seal"]["reasonCode"], "SOURCE_ALREADY_SEALED");
        // Reads stay admitted on a sealed source.
        assert_eq!(sealed_actions["handoff.export"]["state"], "AVAILABLE");
        assert_eq!(sealed_actions["materials.upload"]["state"], "AVAILABLE");
    }
    // L1-VOC-01 / GAP-01 complement: the COMPLETE action key set is pinned,
    // not just the names a known consumer reads — a stray rename, a dropped
    // row or a resurrected camelCase contract key fails here.
    // GAP-01 witness: f3bb7c4 emitted camelCase names PAC/Pi never read;
    // cb9f132 converged the PAC-read/Pi-shared subset to the dotted grammar
    // and the remaining names are deliberate source-local facts with no
    // boundary consumer.
    #[test]
    fn source_capabilities_action_key_set_is_complete_and_stable() {
        let h = test_host();
        let sc = source_capabilities(
            &h,
            &json!({"taskId":"t-1","state":"RUNNING","launchState":"DISPATCH_STARTED","quiescent":false}),
        );
        let mut keys: Vec<&str> = sc["actions"]
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "adoption.apply",
                "adoption.preview",
                "childMessages.read",
                "collaboration.create",
                "collaboration.preflight",
                "handoff.export",
                "handoff.seal",
                "materials.upload",
                "messageSend",
                "pause",
                "policy.presets",
                "resume",
                "sideQuestion.create",
                "stop",
                "stop.affectedChildren",
                "transitions",
            ]
        );
        // A live owner reports the same quiescence refusal the POST gate
        // enforces (F33) instead of advertising a seal that would 409.
        assert_eq!(sc["actions"]["handoff.seal"]["state"], "BLOCKED_BY_STATE");
        assert_eq!(sc["actions"]["handoff.seal"]["reasonCode"], "TASK_NOT_QUIESCENT");
    }
    // L1-VOC-02 / GAP-07 convergence: child-messages/1 pages report the
    // shared SUMMARY_ONLY token for legacy aggregates (with the Pi-shaped
    // `summaryText` carrier plus the Codex `legacy` provenance object),
    // PARTIAL is reachable through truncated item bodies, and the advertised
    // `coverage` list matches the emitted set.
    #[test]
    fn child_messages_page_reports_converged_coverage_vocabulary() {
        let h = test_host();
        let task = "native-coverage";
        {
            let mut s = h.store.lock().unwrap();
            s.save(&json!({"taskId":task,"requestKey":"k-create","state":"RUNNING","launchState":"DISPATCH_STARTED","quiescent":false,"generation":"g1","segmentId":"seg-1","inputRequests":{},"stateVersion":"1"})).unwrap();
            for (aux, answer) in [
                ("aux-legacy", "recovered cumulative answer"),
                ("aux-empty", ""),
                ("aux-captured", ""),
            ] {
                s.accept_aux(
                    task,
                    &json!({"auxSessionId":aux,"question":"q","workspace":"D:/aux","createdAt":store::now()}),
                    &format!("k-{aux}"),
                    &json!({"requestKey":format!("k-{aux}"),"question":"q"}),
                    &json!({"auxSessionId":aux,"state":"DISPATCHING"}),
                )
                .unwrap();
                if !answer.is_empty() {
                    s.aux_transition(task, aux, AUX_OPEN, "RUNNING", None, |r| {
                        r["answer"] = json!(answer)
                    })
                    .unwrap();
                }
            }
            // A captured public stream row: type + ref only, as item/started
            // records it — never command text or tool arguments.
            s.child_message(
                task,
                None,
                "aux-captured",
                &store::ChildMsg {
                    kind: "TOOL_CALL",
                    text: "tool started: commandExecution",
                    status: "RUNNING",
                    event_ref: Some("item-9"),
                },
            )
            .unwrap();
            s.child_message(
                task,
                None,
                "aux-captured",
                &store::ChildMsg {
                    kind: "ASSISTANT_MESSAGE",
                    text: "captured public line",
                    status: "SUCCEEDED",
                    event_ref: None,
                },
            )
            .unwrap();
        }
        // Advertised vocabulary on the source-capability row now matches the
        // emitted set — PARTIAL is reachable via truncated item bodies.
        let sc = source_capabilities(&h, &h.store.lock().unwrap().task(task).unwrap());
        let limits = &sc["actions"]["childMessages.read"]["limits"];
        assert_eq!(
            limits["kinds"],
            json!(["ASSIGNMENT","USER_INPUT","ASSISTANT_MESSAGE","TOOL_CALL","TOOL_RESULT","RETURN","STATUS"])
        );
        assert_eq!(
            limits["coverage"],
            json!(["FULL_SINCE_CAPTURE","SUMMARY_ONLY","PARTIAL","UNAVAILABLE"])
        );
        // SUMMARY_ONLY: the owning row predates stream capture — the
        // cumulative text rides both the Pi-shaped `summaryText` carrier and
        // the Codex `legacy` provenance object.
        let (status, page) =
            child_messages_page(&h, task, None, "aux-legacy", &HashMap::new()).unwrap();
        assert_eq!(status, 200);
        assert_eq!(page["schemaVersion"], "child-messages/1");
        assert_eq!(page["coverage"], "SUMMARY_ONLY");
        assert_eq!(
            page["legacy"],
            json!({"text":"recovered cumulative answer","source":"CUMULATIVE_OUTPUT","truncated":false})
        );
        assert_eq!(page["summaryText"], "recovered cumulative answer");
        assert!(page["items"].as_array().unwrap().is_empty());
        // UNAVAILABLE: no captured stream and no cumulative output — an
        // honest absence, never a fabricated timeline.
        let (_, page) =
            child_messages_page(&h, task, None, "aux-empty", &HashMap::new()).unwrap();
        assert_eq!(page["coverage"], "UNAVAILABLE");
        assert!(page.get("legacy").is_none());
        // FULL_SINCE_CAPTURE: the durable stream meta exists; emitted kinds
        // include TOOL_CALL (design kind list updated GAP-07).
        let (_, page) =
            child_messages_page(&h, task, None, "aux-captured", &HashMap::new()).unwrap();
        assert_eq!(page["coverage"], "FULL_SINCE_CAPTURE");
        assert!(page["captureStartedAt"].is_string());
        let kinds: Vec<&str> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|i| i["kind"].as_str())
            .collect();
        assert_eq!(kinds, ["TOOL_CALL", "ASSISTANT_MESSAGE"]);
        // PARTIAL is genuinely reachable: a page whose captured items carry
        // truncated bodies degrades honestly instead of claiming FULL.
        {
            let mut s = h.store.lock().unwrap();
            s.child_message(
                task,
                None,
                "aux-truncated",
                &store::ChildMsg {
                    kind: "ASSISTANT_MESSAGE",
                    text: &"x".repeat(store::MESSAGE_TEXT_MAX + 64),
                    status: "SUCCEEDED",
                    event_ref: None,
                },
            )
            .unwrap();
        }
        let (_, page) =
            child_messages_page(&h, task, None, "aux-truncated", &HashMap::new()).unwrap();
        assert_eq!(page["coverage"], "PARTIAL");
        assert!(page["items"].as_array().unwrap()[0]["textTruncated"] == true);
    }
    // L2-CDX-03 (bounded inbound frame, f3bb7c4) consumer-side pin: the
    // read step's bound is covered in adapter.rs (exact-ceiling accept,
    // ceiling+1 overrun). This test drives the exact frame sequence the
    // pump emits on an overrun — adapter/failure{code} followed by
    // adapter/exited — through the real watch/native_event path. The
    // durable record keeps only the mapped code: no frame bytes reach the
    // task row or the event journal, the kill reason is not clobbered by
    // the trailing exited frame, and the watch loop ends cleanly instead
    // of hanging on the dead transport.
    #[tokio::test]
    async fn adapter_failure_event_kills_task_with_code_only_no_frame_body() {
        let h = test_host();
        let task = "native-overrun";
        {
            let s = h.store.lock().unwrap();
            s.save(&json!({"taskId":task,"requestKey":"k-create","state":"RUNNING","launchState":"DISPATCH_STARTED","quiescent":false,"ownerBootId":"boot-test","generation":"g1","segmentId":"seg-1","currentCommandId":"cmd-1","nativeSessionId":"thread-1","nativeTurnId":"turn-1","inputRequests":{},"stateVersion":"1"})).unwrap();
        }
        // Dispatch pin: the failure frame resolves to its own reason code
        // and leaves the task row untouched (the settlement is watch's).
        let err = native_event(
            h.clone(),
            task,
            json!({"method":"adapter/failure","params":{"code":"PROTOCOL_FRAME_LIMIT"}}),
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "PROTOCOL_FRAME_LIMIT");
        assert_eq!(
            h.store.lock().unwrap().task(task).unwrap()["state"],
            "RUNNING"
        );
        // The pump's overrun sequence — with a hostile extra param trying
        // to smuggle frame bytes into the durable record.
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        watch(&h, task, rx);
        let marker = "OVERRUN_BODY_MUST_NOT_LEAK_9f27";
        tx.send(json!({"method":"adapter/failure","params":{"code":"PROTOCOL_FRAME_LIMIT","rawLine":marker}}))
            .await
            .unwrap();
        tx.send(json!({"method":"adapter/exited","params":{}}))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if h.store.lock().unwrap().task(task).unwrap()["state"] == "UNKNOWN" {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "ADAPTER_FAILURE_NOT_SETTLED"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        {
            let s = h.store.lock().unwrap();
            let t = s.task(task).unwrap();
            assert_eq!(t["state"], "UNKNOWN");
            // The trailing adapter/exited is never dispatched: the recorded
            // kill cause stays the real frame-limit code, not a clobbered
            // NATIVE_PROCESS_EXITED.
            assert_eq!(t["reasonCode"], "PROTOCOL_FRAME_LIMIT");
            let ev = s.events(task, 0, 100).unwrap();
            let failure = ev["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|e| e["type"] == "adapter.failure")
                .expect("adapter.failure event missing");
            assert_eq!(failure["payload"], json!({"code":"PROTOCOL_FRAME_LIMIT"}));
            assert!(
                !ev.to_string().contains(marker),
                "frame body leaked into the durable journal"
            );
            assert!(!t.to_string().contains(marker));
        }
        // The watch loop ended: the events receiver is dropped (the host
        // keeps serving — the store still reads — instead of hanging).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !tx.is_closed() {
            assert!(
                std::time::Instant::now() < deadline,
                "WATCH_LOOP_STILL_ALIVE"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
    // GAP-06 converged: GET /tasks/{t}/collaborations/{cid}/adoptions now
    // routes the flat {childId, ...record} item list the Pi host serves
    // (workers/pi/src/host/server.ts) — the PAC adoption read path depends
    // on it. The items come from the durable per-child ledger, never
    // recomputed.
    #[tokio::test]
    async fn get_collaboration_adoptions_lists_durable_records() {
        let h = test_host();
        let task = "native-gap06";
        {
            let mut s = h.store.lock().unwrap();
            s.save(&json!({"taskId":task,"requestKey":"k-create","state":"SUCCEEDED","launchState":"DISPATCH_STARTED","quiescent":true,"generation":"g1","segmentId":"seg-1","inputRequests":{},"stateVersion":"1"})).unwrap();
            s.accept_collaboration(
                task,
                &json!({"collaborationId":"collab-1","name":Value::Null,"createdAt":store::now()}),
                &[json!({"childId":"writer","goal":"g","goalSha256":"sha-w","dependsOn":[],"contextPackageIds":[],"writeScope":Value::Null,"budget":Value::Null,"workspace":"D:/ws/w","createdAt":store::now(),"deadlineEpochMillis":"9999999999999"})],
                "k-collab",
                &json!({"requestKey":"k-collab","children":[]}),
                &json!({"kind":"WORKER_LOCAL","requestKey":"k-collab","state":"ACCEPTED","collaborationId":"collab-1"}),
                &json!({"collaborationId":"collab-1","childCount":"1"}),
            )
            .unwrap();
            s.record_adoption(
                task,
                "collab-1",
                "writer",
                &json!({"requestKey":"k-ad-1","mode":"ALL","outcome":"ADOPTED","paths":[],"candidateDigest":"cd-1","at":store::now()}),
                "k-ad-1",
                &json!({"requestKey":"k-ad-1","childId":"writer","mode":"ALL"}),
                &json!({"kind":"WORKER_LOCAL","requestKey":"k-ad-1","state":"APPLIED"}),
                &json!({"type":"collaboration.adopted","payload":{"collaborationId":"collab-1","childId":"writer"}}),
            )
            .unwrap();
        }
        let t = h.store.lock().unwrap().task(task).unwrap();
        let no_query = HashMap::new();
        let declared = capabilities(&h)
            .into_iter()
            .find(|i| i["id"] == "execution.adoptions")
            .expect("execution.adoptions capability row missing");
        assert_eq!(declared["implementation"], "IMPLEMENTED");
        let (status, _) = collab_route(
            h.clone(),
            task,
            &t,
            &["collab-1"],
            "GET",
            Value::Null,
            &no_query,
        )
        .await
        .unwrap();
        assert_eq!(status, 200);
        // The read arm serves every child's recorded adoptions, flattened
        // with its childId exactly like the Pi list view.
        let (status, view) = collab_route(
            h.clone(),
            task,
            &t,
            &["collab-1", "adoptions"],
            "GET",
            Value::Null,
            &no_query,
        )
        .await
        .unwrap();
        assert_eq!(status, 200);
        let items = view["items"].as_array().expect("items array");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["childId"], "writer");
        assert_eq!(items[0]["requestKey"], "k-ad-1");
        assert_eq!(items[0]["outcome"], "ADOPTED");
        // The POST arm on the same path still routes into collab_adopt and
        // fails honestly on the unknown child.
        let err = collab_route(
            h.clone(),
            task,
            &t,
            &["collab-1", "adoptions"],
            "POST",
            json!({"requestKey":"k-gap06","childId":"ghost","mode":"ALL"}),
            &no_query,
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "COLLABORATION_CHILD_NOT_FOUND");
        let (status, _) = collab_route(
            h.clone(),
            task,
            &t,
            &[],
            "GET",
            Value::Null,
            &no_query,
        )
        .await
        .unwrap();
        assert_eq!(status, 200);
    }

    // GAP-04: the baseline producer reports layered patch facts, a bundle for
    // local-only commits and the untracked manifest — all digest-stamped and
    // byte-identical through git_export_part.
    #[test]
    fn git_baseline_reports_layers_and_serves_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().to_path_buf();
        let run = |args: &[&str]| {
            let out = Command::new("git").arg("-C").arg(&ws).args(args).output().unwrap();
            assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
            out.stdout
        };
        run(&["init"]);
        fs::write(ws.join("README.md"), "baseline").unwrap();
        run(&["add", "."]);
        run(&["-c", "user.name=T", "-c", "user.email=t@x", "commit", "-m", "baseline"]);
        // stash first (it restores tracked files to HEAD), then build layers
        fs::write(ws.join("stashme.txt"), "stash body").unwrap();
        run(&["add", "stashme.txt"]);
        run(&["-c", "user.name=T", "-c", "user.email=t@x", "stash", "push", "-m", "wip-stash"]);
        // local-only commit
        fs::write(ws.join("committed.txt"), "local commit body").unwrap();
        run(&["add", "committed.txt"]);
        run(&["-c", "user.name=T", "-c", "user.email=t@x", "commit", "-m", "local work"]);
        // staged + unstaged + untracked
        fs::write(ws.join("staged.txt"), "staged body").unwrap();
        fs::write(ws.join("staged.bin"), &[0u8, 1, 2, 250, 0, 128]).unwrap();
        run(&["add", "staged.txt", "staged.bin"]);
        fs::write(ws.join("README.md"), "baseline\nunstaged drift").unwrap();
        fs::write(ws.join("untracked.txt"), "untracked body").unwrap();
        // in-flight markers a real merge/rebase leaves behind
        let git_dir = String::from_utf8(run(&["rev-parse", "--absolute-git-dir"])).unwrap().trim().to_string();
        fs::write(Path::new(&git_dir).join("MERGE_HEAD"), "x\n").unwrap();
        fs::create_dir_all(Path::new(&git_dir).join("rebase-merge")).unwrap();
        let head = String::from_utf8(run(&["rev-parse", "HEAD"])).unwrap().trim().to_string();
        let t = json!({"workspace": ws.to_str().unwrap(), "baseCommit": "deadbeef"});
        let (entries, payloads) = git_baseline(&t);
        let by_kind = |k: &str| entries.iter().find(|e| e["kind"] == k).unwrap().clone();
        let repo = by_kind("REPOSITORY");
        assert_eq!(repo["present"], true);
        assert_eq!(repo["head"], head);
        assert_eq!(repo["baseCommit"], "deadbeef");
        let in_flight = repo["inFlight"].as_array().unwrap();
        assert!(in_flight.iter().any(|k| k == "MERGE") && in_flight.iter().any(|k| k == "REBASE"), "{in_flight:?}");
        for kind in ["PATCH_STAGED", "PATCH_UNSTAGED"] {
            let e = by_kind(kind);
            assert_eq!(e["state"], "AVAILABLE", "{kind}");
            let part = e["part"].as_str().unwrap();
            let bytes = git_export_part(&ws, part).unwrap();
            assert_eq!(e["sha256"], hash(&bytes), "{kind} digest");
            assert!(payloads.iter().any(|(n, b)| n == part && *b == bytes));
        }
        assert!(by_kind("PATCH_STAGED")["files"].as_array().unwrap().iter().any(|f| f["path"] == "staged.txt" && f["status"] == "A"));
        assert!(by_kind("PATCH_UNSTAGED")["files"].as_array().unwrap().iter().any(|f| f["path"] == "README.md" && f["status"] == "M"));
        let stash = by_kind("STASHES");
        assert_eq!(stash["items"][0]["ref"], "stash@{0}");
        assert!(stash["items"][0]["commit"].as_str().unwrap().len() == 40);
        let bundle = by_kind("BUNDLE");
        assert_eq!(bundle["state"], "AVAILABLE");
        assert_eq!(bundle["localCommits"], "2");
        let bundle_bytes = git_export_part(&ws, "git-bundle").unwrap();
        assert_eq!(bundle["sha256"], hash(&bundle_bytes));
        assert!(bundle_bytes.starts_with(b"# v2 git bundle"));
        let manifest = by_kind("UNTRACKED_MANIFEST");
        assert_eq!(manifest["total"], "1");
        assert_eq!(manifest["files"][0]["path"], "untracked.txt");
        assert_eq!(manifest["files"][0]["sha256"], hash(b"untracked body".as_slice()));
        // clean repo: honest EMPTY states and no payloads
        let clean = tempfile::tempdir().unwrap();
        let w2 = clean.path().to_path_buf();
        for args in [
            vec!["init"],
            vec!["-c", "user.name=T", "-c", "user.email=t@x", "commit", "--allow-empty", "-m", "i"],
        ] {
            let out = Command::new("git").arg("-C").arg(&w2).args(&args).output().unwrap();
            assert!(out.status.success());
        }
        let (entries2, payloads2) = git_baseline(&json!({"workspace": w2.to_str().unwrap()}));
        let by2 = |k: &str| entries2.iter().find(|e| e["kind"] == k).unwrap().clone();
        assert_eq!(by2("PATCH_STAGED")["state"], "EMPTY");
        assert_eq!(by2("PATCH_UNSTAGED")["state"], "EMPTY");
        // no remotes => every commit is local-only => a real bundle
        assert_eq!(by2("BUNDLE")["state"], "AVAILABLE");
        assert_eq!(by2("UNTRACKED_MANIFEST")["files"].as_array().unwrap().len(), 0);
        assert!(!payloads2.is_empty());
        // non-git workspace reports NON_GIT instead of fabricating coverage
        let nongit = tempfile::tempdir().unwrap();
        let (entries3, payloads3) = git_baseline(&json!({"workspace": nongit.path().to_str().unwrap()}));
        assert_eq!(entries3[0]["kind"], "NON_GIT");
        assert!(payloads3.is_empty());
        assert!(git_export_part(nongit.path(), "git-staged").is_err());
    }
}
