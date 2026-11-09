//! Explicit host-mediated dynamic tools on the unchanged pinned app-server.
use serde_json::{Value, json};
pub const EXPERIMENTAL_SCHEMA_SHA256: &str =
    "af491b61b83ff04943f2207268d2539d09b6e4bea7aef9972adb9eda2026d1be";

pub fn node_qualified(version: &str) -> bool {
    let mut parts=version.trim().trim_start_matches('v').split('.');
    parts.next()==Some("24") && parts.next().and_then(|n|n.parse::<u32>().ok()).is_some_and(|n|n>=16)
}

pub fn tools() -> Value {
    json!(["run_command", "run_build", "run_tests"].map(|name| json!({
        "type":"function", "name":name,
        "description": match name {
            "run_build" => "Run a real build in the owned workspace through the host command runner. Inspect output and exitCode.",
            "run_tests" => "Run real tests in the owned workspace through the host command runner. Inspect output and exitCode; failures are returned.",
            _ => "Run a shell command in the owned workspace through the host command runner. Returns actual stdout, stderr and exitCode."
        },
        "inputSchema":{"type":"object","properties":{
            "command":{"type":"string","description":"Shell command to execute."},
            "cwd":{"type":"string","description":"Optional workspace-relative working directory; defaults to the workspace root."},
            "timeoutMs":{"type":"integer","description":"Optional command timeout in milliseconds."}
        },"required":["command"],"additionalProperties":false}
    })))
}

use anyhow::{Result, anyhow, bail};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
    sync::Mutex,
    time::{Duration, timeout},
};

/// The child's stdin is an owner lease. Dropping the host closes this pipe even
/// after SIGKILL; the independent Runner owns group cleanup and its own ledger.
pub struct Runner {
    address: String,
    token: String,
    _child: Mutex<Child>,
}
impl Runner {
    pub async fn start(
        main: &Path,
        node: &Path,
        state: &Path,
        allowlist: &[String],
    ) -> Result<Arc<Self>> {
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let mut cmd = Command::new(node);
        cmd.arg(main)
            .args(["serve", "--state"])
            .arg(state)
            .args(["--port", "0"])
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in std::env::vars().filter(|(k, _)| runner_env_allowed(k, allowlist)) {
            cmd.env(k, v);
        }
        cmd.env("WORKER_COMMAND_RUNNER_TOKEN", &token);
        let mut child = cmd.spawn().map_err(|_| anyhow!("RUNNER_START_FAILED"))?;
        let mut output = BufReader::new(
            child
                .stdout
                .take()
                .ok_or_else(|| anyhow!("RUNNER_START_FAILED"))?,
        );
        let mut line = String::new();
        let ready = timeout(Duration::from_secs(15), output.read_line(&mut line)).await;
        if !matches!(ready, Ok(Ok(1..))) {
            drop(child.stdin.take());
            bail!("RUNNER_START_UNCONFIRMED")
        }
        let data: Value = serde_json::from_str(
            line.trim()
                .strip_prefix("COMMAND_RUNNER_READY ")
                .ok_or_else(|| anyhow!("RUNNER_START_FAILED"))?,
        )?;
        let url = data["url"]
            .as_str()
            .ok_or_else(|| anyhow!("RUNNER_START_FAILED"))?;
        let port = url
            .strip_prefix("http://127.0.0.1:")
            .and_then(|p| p.parse::<u16>().ok())
            .filter(|p| *p > 0)
            .ok_or_else(|| anyhow!("RUNNER_NON_LOOPBACK"))?;
        let runner = Arc::new(Self {
            address: format!("127.0.0.1:{port}"),
            token,
            _child: Mutex::new(child),
        });
        // Drain readiness stdout without persisting any raw diagnostics.
        tokio::spawn(async move {
            let mut sink = [0u8; 4096];
            while output.read(&mut sink).await.is_ok_and(|n| n > 0) {}
        });
        Ok(runner)
    }
    pub async fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        timeout(Duration::from_secs(20), self.exchange(method, path, body))
            .await
            .map_err(|_| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?
    }
    async fn exchange(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        if path.bytes().any(|b| b == b'\r' || b == b'\n') {
            bail!("INVALID_REQUEST")
        }
        let payload = body
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default();
        let idempotency = body
            .and_then(|b| b["requestKey"].as_str())
            .map(|key| format!("Idempotency-Key: {key}\r\n"))
            .unwrap_or_default();
        let mut stream = TcpStream::connect(&self.address)
            .await
            .map_err(|_| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?;
        let headers = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{idempotency}\r\n",
            self.address,
            self.token,
            payload.len()
        );
        stream
            .write_all(headers.as_bytes())
            .await
            .map_err(|_| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?;
        stream
            .write_all(&payload)
            .await
            .map_err(|_| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?;
        let mut bytes = Vec::new();
        stream
            .take(5 * 1024 * 1024)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?;
        let split = bytes
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?;
        let header = std::str::from_utf8(&bytes[..split])?;
        let status = header
            .split_whitespace()
            .nth(1)
            .and_then(|n| n.parse::<u16>().ok())
            .ok_or_else(|| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?;
        let result: Value = serde_json::from_slice(&bytes[split + 4..])
            .map_err(|_| anyhow!("RUNNER_RESPONSE_UNCONFIRMED"))?;
        if status == 404 {
            bail!("RUNNER_REQUEST_NOT_FOUND")
        }
        if !(200..300).contains(&status) {
            bail!("RUNNER_REQUEST_REJECTED")
        }
        Ok(result)
    }
}
pub fn runner_env_allowed(name: &str, allowlist: &[String]) -> bool {
    let name_upper = name.to_uppercase();
    if ["KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "AUTH"]
        .iter()
        .any(|n| name_upper.contains(n))
        || [
            "MODEL_",
            "LLM_",
            "WORKER_",
            "ACS_",
            "PAC_",
            "PERSONAL_",
            "DYLD_",
            "LD_",
            "GIT_CONFIG",
        ]
        .iter()
        .any(|n| name_upper.starts_with(n))
    {
        return false;
    }
    if matches!(name_upper.as_str(), "NODE_OPTIONS" | "BASH_ENV" | "ENV") {
        return false;
    }
    allowlist.iter().any(|n| n == name)
        || matches!(
            name_upper.as_str(),
            "PATH"
                | "HOME"
                | "TMP"
                | "TEMP"
                | "TMPDIR"
                | "LANG"
                | "LC_ALL"
                | "DEVELOPER_DIR"
                | "SDKROOT"
                | "JAVA_HOME"
                | "RUSTUP_HOME"
                | "CARGO_HOME"
        )
}

pub fn command(params: &Value, context_id: &str, request_key: &str) -> Result<Value> {
    let tool = params["tool"]
        .as_str()
        .ok_or_else(|| anyhow!("RUNNER_TOOL_UNSUPPORTED"))?;
    let kind = match tool {
        "run_command" => "command",
        "run_build" => "build",
        "run_tests" => "test",
        _ => bail!("RUNNER_TOOL_UNSUPPORTED"),
    };
    if params["namespace"].as_str().is_some_and(|s| !s.is_empty()) {
        bail!("RUNNER_TOOL_UNSUPPORTED")
    }
    let arguments = params["arguments"]
        .as_object()
        .ok_or_else(|| anyhow!("RUNNER_ARGUMENTS_INVALID"))?;
    if arguments
        .keys()
        .any(|k| !matches!(k.as_str(), "command" | "cwd" | "timeoutMs"))
    {
        bail!("RUNNER_ARGUMENTS_INVALID")
    }
    let script = arguments
        .get("command")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty() && s.len() <= 16384)
        .ok_or_else(|| anyhow!("RUNNER_ARGUMENTS_INVALID"))?;
    let cwd = arguments
        .get("cwd")
        .map(|v| {
            v.as_str()
                .filter(|s| !s.is_empty() && s.len() <= 512)
                .ok_or_else(|| anyhow!("RUNNER_ARGUMENTS_INVALID"))
        })
        .transpose()?
        .unwrap_or(".");
    // Runner canonicalizes too. Reject absolute/parent paths before dispatch.
    if PathBuf::from(cwd).is_absolute()
        || Path::new(cwd)
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        bail!("RUNNER_ARGUMENTS_INVALID")
    }
    let timeout_ms = arguments
        .get("timeoutMs")
        .map(|v| {
            v.as_u64()
                .filter(|n| (100..=600000).contains(n))
                .ok_or_else(|| anyhow!("RUNNER_ARGUMENTS_INVALID"))
        })
        .transpose()?
        .unwrap_or(120000);
    Ok(
        json!({"requestKey":request_key,"contextId":context_id,"kind":kind,"shell":{"script":script,"executable":"/bin/bash"},"cwd":cwd,"timeoutMs":timeout_ms}),
    )
}
pub fn terminal(v: &Value) -> bool {
    matches!(
        v["status"].as_str(),
        Some("SUCCEEDED" | "FAILED" | "CANCELLED" | "TIMED_OUT" | "UNKNOWN")
    )
}
pub fn settled(v: &Value) -> bool {
    terminal(v)
        && v["status"] != "UNKNOWN"
        && v["stopProof"]["scope"] == "REGISTERED_PROCESS_GROUP"
        && v["stopProof"]["groupExited"] == true
}
pub fn context_proven(v: &Value) -> bool {
    let empty = |v: &Value| v == 0 || v.as_array().is_some_and(Vec::is_empty);
    v["closed"] == true
        && empty(&v["activeCommands"])
        && empty(&v["unknownCommands"])
        && v["stopProof"]["scope"] == "REGISTERED_PROCESS_GROUP"
        && v["stopProof"]["groupExited"] == true
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runner_node_matches_shared_minimum() {
        assert!(node_qualified("v24.22.1"));assert!(node_qualified("v24.16.0"));
        for version in ["v24.15.0","v25.0.0","v22.20.0","invalid"] {assert!(!node_qualified(version));}
    }
    #[test]
    fn explicit_tools_and_untrusted_arguments() {
        assert_eq!(tools().as_array().unwrap().len(), 3);
        let p = json!({"tool":"run_tests","arguments":{"command":"node --test","cwd":".","timeoutMs":1000}});
        assert_eq!(command(&p, "ctx", "key").unwrap()["kind"], "test");
        for p in [
            json!({"tool":"exec","arguments":{"command":"x"}}),
            json!({"tool":"run_tests","arguments":{"command":"x","env":{"MODEL_API_KEY":"x"}}}),
            json!({"tool":"run_tests","arguments":{"command":"x","cwd":"../"}}),
            json!({"tool":"run_tests","arguments":{"command":"x","timeoutMs":0}}),
        ] {
            assert!(command(&p, "ctx", "key").is_err());
        }
    }
    #[test]
    fn runner_excludes_secrets_even_from_allowlist() {
        for key in [
            "MODEL_API_KEY",
            "LLM_HEADER_X",
            "WORKER_LOCAL_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "PERSONAL_X",
            "MY_PASSWORD",
        ] {
            assert!(!runner_env_allowed(key, &[key.into()]));
        }
        assert!(runner_env_allowed("PATH", &[]));
        assert!(runner_env_allowed("BUILD_MODE", &["BUILD_MODE".into()]));
    }
    #[test]
    fn group_scope_is_required_for_settlement() {
        assert!(!settled(&json!({"status":"SUCCEEDED","exitCode":0})));
        assert!(settled(
            &json!({"status":"FAILED","exitCode":1,"stopProof":{"scope":"REGISTERED_PROCESS_GROUP","groupExited":true}})
        ));
        assert!(!context_proven(
            &json!({"closed":true,"activeCommands":0,"unknownCommands":1,"stopProof":{"scope":"REGISTERED_PROCESS_GROUP","groupExited":true}})
        ));
    }
}

/// Freeze all sibling runtime .mjs sources, including the independent guardian.
pub fn bundle_digest(main: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let root = main
        .parent()
        .ok_or_else(|| anyhow!("RUNNER_CONFIGURATION_REQUIRED"))?;
    let mut files = std::fs::read_dir(root)?
        .map(|r| r.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.retain(|p| p.extension().is_some_and(|e| e == "mjs"));
    files.sort();
    if !files
        .iter()
        .any(|p| p.file_name().is_some_and(|n| n == "main.mjs"))
    {
        bail!("RUNNER_CONFIGURATION_REQUIRED")
    }
    let mut hasher = Sha256::new();
    for file in files {
        let name = file
            .file_name()
            .unwrap()
            .to_str()
            .ok_or_else(|| anyhow!("RUNNER_CONFIGURATION_REQUIRED"))?;
        let bytes = std::fs::read(&file)?;
        hasher.update((name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
    }
    Ok(hex::encode(hasher.finalize()))
}
