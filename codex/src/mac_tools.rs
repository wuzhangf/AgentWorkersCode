//! Host-owned native Mac task tools. Dynamic schemas come from the same Mac catalog;
//! task credentials never enter app-server configuration or a forked MCP process.
use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};
fn id(value: &Value) -> bool {
    value.as_str().is_some_and(|s| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    })
}
pub fn binding(key: &str) -> Result<Option<Value>> {
    let Ok(raw) = std::env::var("NWF_MAC_TASK_BINDINGS") else {
        return Ok(None);
    };
    let rows: Value =
        serde_json::from_str(&raw).map_err(|_| anyhow!("MAC_TOOL_BINDING_INVALID"))?;
    let rows = rows
        .as_array()
        .filter(|r| r.len() <= 16)
        .ok_or_else(|| anyhow!("MAC_TOOL_BINDING_INVALID"))?;
    let mut keys = std::collections::HashSet::new();
    let mut tasks = std::collections::HashSet::new();
    for row in rows {
        address(row)?;
        let credential = row["credentialEnvironment"].as_str().unwrap_or("");
        if !id(&row["taskID"])
            || !id(&row["storeID"])
            || !id(&row["workerRequestKey"])
            || row["ownsTask"] != true
            || credential.is_empty()
            || credential.len() > 128
            || !credential
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            || !row["ancestorReferences"].as_array().is_some_and(|r| {
                let id = row["taskID"].as_str().unwrap_or("");
                let unique: std::collections::HashSet<String> = r.iter().map(Value::to_string).collect();
                r.len() <= 4 && unique.len() == r.len() && r.iter().all(|v| v.as_str().is_some_and(|s| !s.is_empty() && s.len() <= 512 && s != id && !s.ends_with(&format!(":{id}"))))
            })
            || !keys.insert(row["workerRequestKey"].clone().to_string())
            || !tasks.insert(format!("{}/{}", row["origin"], row["taskID"]))
        {
            bail!("MAC_TOOL_BINDING_INVALID")
        }
    }
    Ok(rows.iter().find(|r| r["workerRequestKey"] == key).cloned())
}
fn address(binding: &Value) -> Result<String> {
    let origin = binding["origin"]
        .as_str()
        .unwrap_or("")
        .trim_end_matches('/');
    let address = origin
        .strip_prefix("http://127.0.0.1:")
        .ok_or_else(|| anyhow!("MAC_TOOL_BINDING_INVALID"))?;
    let port: u16 = address
        .parse()
        .map_err(|_| anyhow!("MAC_TOOL_BINDING_INVALID"))?;
    if port == 0 {
        bail!("MAC_TOOL_BINDING_INVALID")
    }
    Ok(format!("127.0.0.1:{port}"))
}
pub async fn call(
    binding_value: &Value,
    method: &str,
    suffix: &str,
    body: Option<&Value>,
) -> Result<Value> {
    if binding(binding_value["workerRequestKey"].as_str().unwrap_or(""))?.as_ref()
        != Some(binding_value)
    {
        bail!("MAC_TOOL_BINDING_CHANGED")
    }
    timeout(
        Duration::from_secs(20),
        exchange(binding_value, method, suffix, body),
    )
    .await
    .map_err(|_| anyhow!("MAC_OPERATION_UNCONFIRMED"))?
}
async fn exchange(b: &Value, method: &str, suffix: &str, body: Option<&Value>) -> Result<Value> {
    let address = address(b)?;
    let token = std::env::var(b["credentialEnvironment"].as_str().unwrap_or(""))
        .map_err(|_| anyhow!("MAC_TASK_CREDENTIAL_UNAVAILABLE"))?;
    if token.is_empty() || token.contains(['\r', '\n']) || suffix.contains(['\r', '\n']) {
        bail!("MAC_TOOL_BINDING_INVALID")
    }
    let payload = body
        .map(serde_json::to_vec)
        .transpose()?
        .unwrap_or_default();
    let idempotency = body
        .and_then(|b| b["requestKey"].as_str())
        .map(|key| format!("Idempotency-Key: {key}\r\n"))
        .unwrap_or_default();
    let mut stream = TcpStream::connect(&address)
        .await
        .map_err(|_| anyhow!("MAC_OPERATION_UNCONFIRMED"))?;
    let header = format!(
        "{method} /native/v1/tasks/{}{suffix} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nX-Worker-Local-Store: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{idempotency}\r\n",
        b["taskID"].as_str().unwrap(),
        b["storeID"].as_str().unwrap(),
        payload.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&payload).await?;
    let mut bytes = Vec::new();
    stream.take(1048577).read_to_end(&mut bytes).await?;
    if bytes.len() > 1048576 {
        bail!("MAC_RESPONSE_TOO_LARGE")
    }
    let split = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow!("MAC_OPERATION_UNCONFIRMED"))?;
    let header = std::str::from_utf8(&bytes[..split])?;
    let status = header
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        bail!("MAC_OPERATION_UNCONFIRMED")
    }
    serde_json::from_slice(&bytes[split + 4..]).map_err(|_| anyhow!("MAC_OPERATION_UNCONFIRMED"))
}
pub fn tool_name(id: &str) -> String {
    format!("mac__{}", id.replace('.', "_"))
}
pub async fn tools(b: &Value) -> Result<Vec<Value>> {
    let context = call(b, "GET", "/context", None).await?;
    if context["taskId"] != b["taskID"] || context["schemaVersion"] != "mac-native-context/1" {
        bail!("MAC_CONTEXT_CHANGED")
    }
    let capabilities = context["capabilities"]
        .as_array()
        .ok_or_else(|| anyhow!("MAC_CONTEXT_CHANGED"))?;
    Ok(capabilities.iter().map(|c|json!({"type":"function","name":tool_name(c["capabilityId"].as_str().unwrap_or("")),"description":c["description"],"inputSchema":c["inputSchema"]})).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn remote_and_header_injected_origins_are_rejected() {
        for origin in [
            "https://remote.test",
            "http://localhost:8080",
            "http://127.0.0.1:0",
            "http://127.0.0.1:80/path",
            "http://127.0.0.1:80\r\nInjected: true",
        ] {
            assert!(address(&json!({"origin":origin})).is_err());
        }
        assert_eq!(
            address(&json!({"origin":"http://127.0.0.1:12345"})).unwrap(),
            "127.0.0.1:12345"
        );
    }
    #[tokio::test]
    async fn native_http_uses_task_scoped_auth_store_and_original_request_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credential = "CODEX_MAC_SYNTHETIC_TEST_SCOPE_TOKEN";
        unsafe {
            std::env::set_var(credential, "synthetic-task-scope-only");
        }
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(bytes).unwrap();
            assert!(
                request.starts_with("POST /native/v1/tasks/owned-task/operations HTTP/1.1\r\n")
            );
            assert!(request.contains("Authorization: Bearer synthetic-task-scope-only\r\n"));
            assert!(request.contains("X-Worker-Local-Store: store\r\n"));
            assert!(request.contains("Idempotency-Key: original-key\r\n"));
            let payload =
                br#"{"taskId":"owned-task","requestKey":"original-key","state":"APPLIED","result":{"verification":"PASSED"}}"#;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(payload).await.unwrap();
        });
        let b = json!({"origin":format!("http://{address}"),"taskID":"owned-task","storeID":"store","credentialEnvironment":credential});
        let result = exchange(
            &b,
            "POST",
            "/operations",
            Some(&json!({"requestKey":"original-key"})),
        )
        .await
        .unwrap();
        assert_eq!(result["state"], "APPLIED");
        peer.await.unwrap();
        unsafe {
            std::env::remove_var(credential);
        }
    }
}
