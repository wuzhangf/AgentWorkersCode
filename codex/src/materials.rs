// CA-ANDROID-V2 C6 — staged material-upload profile. Upload metadata lives in
// the material_uploads row; bytes live at state/materials/<uploadKey>. Chunks
// are sequential-offset, digest-verified writes; seal proves total bytes and
// the whole-object sha256 before a message may bind the reference.
use crate::{
    host::{Shared, exact, string, valid_id},
    store,
};
use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

pub(crate) const CHUNK_MAX: usize = 256 * 1024;
pub(crate) const IMAGE_MAX: u64 = 5 * 1024 * 1024;
pub(crate) const TEXT_MAX: u64 = 1024 * 1024;
pub(crate) const MAX_PER_MESSAGE: usize = 4;
pub(crate) const TOTAL_MAX: u64 = 12 * 1024 * 1024;
const TTL_MS: u64 = 24 * 60 * 60 * 1000;
// Contract's first profile: PNG/JPEG images plus UTF-8 text/markdown/log/json
// reference text. LOG uploads carry the text/plain media type.
pub(crate) const MEDIA_TYPES: [&str; 5] = [
    "image/png",
    "image/jpeg",
    "text/plain",
    "text/markdown",
    "application/json",
];
pub(crate) fn material_path(h: &Shared, upload_key: &str) -> PathBuf {
    h.root.join("materials").join(upload_key)
}
pub(crate) fn is_image(media_type: &str) -> bool {
    media_type.starts_with("image/")
}
pub(crate) fn media_max(media_type: &str) -> u64 {
    if is_image(media_type) {
        IMAGE_MAX
    } else {
        TEXT_MAX
    }
}
fn sha_ok(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
// Lazily settle EXPIRED: an open upload past its durable expiry epoch is marked
// honestly, never silently extended. Terminal states are never rewritten.
pub(crate) fn material_fresh(s: &mut store::Store, key: &str) -> Result<Value> {
    let mut m = s.material(key)?;
    let open = matches!(
        m["state"].as_str(),
        Some("SELECTED" | "VALIDATING" | "UPLOADING")
    );
    if open {
        let expires = m["expiresEpochMillis"]
            .as_str()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(u64::MAX);
        if store::epoch_ms() > expires {
            m["state"] = json!("EXPIRED");
            m["expiredAt"] = json!(store::now());
            s.material_put(&m)?;
        }
    }
    Ok(m)
}
// Route family:
//   POST   /projects/{p}/material-uploads
//   GET    /projects/{p}/material-uploads/{uploadKey}
//   PUT    /projects/{p}/material-uploads/{uploadKey}/chunks/{offset}
//   POST   /projects/{p}/material-uploads/{uploadKey}/seal
//   DELETE /projects/{p}/material-uploads/{uploadKey}
// raw carries the bounded PUT body bytes; query carries ?digest= for chunks.
pub(crate) async fn material_route(
    h: Shared,
    project: &str,
    rest: &[&str],
    method: &str,
    b: &Value,
    raw: Option<Vec<u8>>,
    query: &std::collections::HashMap<String, String>,
) -> Result<(u16, Value)> {
    if project != h.registry.project_id {
        bail!("PROJECT_NOT_REGISTERED")
    }
    match (method, rest.len()) {
        ("POST", 0) => material_create(h, b).await,
        ("GET", 0) => Ok((
            200,
            json!({"items":h.store.lock().unwrap().materials(project)?}),
        )),
        ("GET", 1) => {
            let key = valid_id(rest[0])?;
            let m = material_fresh(&mut h.store.lock().unwrap(), key)?;
            Ok((200, m))
        }
        ("PUT", 3) if rest[1] == "chunks" => {
            let key = valid_id(rest[0])?;
            let offset: u64 = rest[2]
                .parse()
                .map_err(|_| anyhow!("MATERIAL_CHUNK_OFFSET"))?;
            material_chunk(h, key, offset, raw.unwrap_or_default(), query).await
        }
        ("POST", 2) if rest[1] == "seal" => {
            let key = valid_id(rest[0])?;
            material_seal(h, key, b).await
        }
        ("DELETE", 1) => {
            let key = valid_id(rest[0])?;
            material_delete(h, key).await
        }
        _ => bail!("FIXED_LOCAL_ROUTE_REQUIRED"),
    }
}
async fn material_create(h: Shared, b: &Value) -> Result<(u16, Value)> {
    exact(
        b,
        &[
            "requestKey",
            "target",
            "displayName",
            "mediaType",
            "sizeBytes",
            "sha256",
        ],
    )?;
    let key = valid_id(string(b, "requestKey")?)?.to_owned();
    // Target scope: an existing task or a not-yet-created create request key.
    let target = &b["target"];
    if !target.is_object() {
        bail!("INVALID_MATERIAL_TARGET")
    }
    exact(target, &["taskId", "createKey"])?;
    let (task_id, create_key) = (target["taskId"].as_str(), target["createKey"].as_str());
    if task_id.is_some() == create_key.is_some() {
        bail!("INVALID_MATERIAL_TARGET")
    }
    if let Some(t) = task_id {
        valid_id(t)?;
        h.store.lock().unwrap().task(t)?; // TASK_NOT_FOUND
    } else {
        valid_id(create_key.unwrap())?;
    }
    let display = b["displayName"]
        .as_str()
        .filter(|s| !s.trim().is_empty() && s.len() <= 128 && !s.chars().any(|c| (c as u32) < 0x20))
        .ok_or_else(|| anyhow!("INVALID_MATERIAL_NAME"))?
        .to_owned();
    let media = b["mediaType"]
        .as_str()
        .filter(|m| MEDIA_TYPES.contains(m))
        .ok_or_else(|| anyhow!("MATERIAL_MEDIA_UNSUPPORTED"))?
        .to_owned();
    let size: u64 = b["sizeBytes"]
        .as_str()
        .and_then(|v| v.parse().ok())
        .or_else(|| b["sizeBytes"].as_u64())
        .ok_or_else(|| anyhow!("INVALID_MATERIAL_SIZE"))?;
    if size == 0 || size > media_max(&media) {
        bail!("MATERIAL_TOO_LARGE")
    }
    let declared = string(b, "sha256")?;
    if !sha_ok(declared) {
        bail!("INVALID_MATERIAL_DIGEST")
    }
    {
        let s = h.store.lock().unwrap();
        if let Some(v) = s.existing("materials", &key, b)? {
            return Ok((202, v));
        }
    }
    let upload_key = format!("mat-{}", store::id());
    let row = json!({
        "uploadKey": upload_key,
        "projectId": h.registry.project_id,
        "target": target,
        "displayName": display,
        "mediaType": media,
        "sizeBytes": size.to_string(),
        "sha256": declared,
        "state": "SELECTED",
        "nextOffset": "0",
        "chunkSize": CHUNK_MAX.to_string(),
        "expiresAt": store::now_plus_ms(TTL_MS),
        "expiresEpochMillis": (store::epoch_ms() + TTL_MS).to_string(),
        "createdAt": store::now(),
        "requestKey": key,
    });
    {
        let mut s = h.store.lock().unwrap();
        s.accept_material("materials", &key, b, &row)?;
    }
    Ok((202, row))
}
// Sequential digest-verified chunk write. A replay carrying the identical
// offset+bytes confirms; an offset that rewrites different bytes conflicts.
async fn material_chunk(
    h: Shared,
    key: &str,
    offset: u64,
    bytes: Vec<u8>,
    query: &std::collections::HashMap<String, String>,
) -> Result<(u16, Value)> {
    if bytes.is_empty() || bytes.len() > CHUNK_MAX {
        bail!("MATERIAL_CHUNK_TOO_LARGE")
    }
    let digest = query
        .get("digest")
        .filter(|d| sha_ok(d))
        .ok_or_else(|| anyhow!("MATERIAL_CHUNK_DIGEST_REQUIRED"))?;
    if hash(&bytes) != *digest {
        bail!("MATERIAL_CHUNK_DIGEST_MISMATCH")
    }
    let path = material_path(&h, key);
    let mut m = {
        let mut s = h.store.lock().unwrap();
        material_fresh(&mut s, key)?
    };
    match m["state"].as_str() {
        Some("SELECTED" | "VALIDATING" | "UPLOADING") => {}
        Some("EXPIRED") => bail!("MATERIAL_EXPIRED"),
        _ => bail!("MATERIAL_NOT_OPEN"),
    }
    let next: u64 = m["nextOffset"]
        .as_str()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let declared: u64 = m["sizeBytes"]
        .as_str()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if offset > next {
        bail!("MATERIAL_CHUNK_OFFSET")
    }
    if offset + bytes.len() as u64 > declared {
        bail!("MATERIAL_TOO_LARGE")
    }
    if offset < next {
        // Idempotent replay: only byte-identical content already on disk is
        // confirmed; anything else is a chunk conflict, not a silent rewrite.
        let existing = fs::read(&path).unwrap_or_default();
        let end = (offset + bytes.len() as u64) as usize;
        if existing.len() >= end && existing[offset as usize..end] == bytes[..] {
            return Ok((200, m));
        }
        bail!("MATERIAL_CHUNK_CONFLICT")
    }
    fs::create_dir_all(path.parent().unwrap())?;
    let mut f = OpenOptions::new().create(true).write(true).open(&path)?;
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(&bytes)?;
    m["state"] = json!("UPLOADING");
    m["nextOffset"] = json!((offset + bytes.len() as u64).to_string());
    m["lastChunkDigest"] = json!(digest);
    h.store.lock().unwrap().material_put(&m)?;
    Ok((200, m))
}
// Seal: every declared byte on disk and the whole-object sha256 matches the
// declared digest — else the seal refuses and the upload stays open/failed.
async fn material_seal(h: Shared, key: &str, b: &Value) -> Result<(u16, Value)> {
    exact(b, &["requestKey", "sha256"])?;
    valid_id(string(b, "requestKey")?)?;
    let declared = string(b, "sha256")?;
    if !sha_ok(declared) {
        bail!("INVALID_MATERIAL_DIGEST")
    }
    let path = material_path(&h, key);
    let mut m = {
        let mut s = h.store.lock().unwrap();
        material_fresh(&mut s, key)?
    };
    match m["state"].as_str() {
        Some("SEALED") => {
            if m["sha256"].as_str() == Some(declared) {
                return Ok((200, m));
            }
            bail!("MATERIAL_DIGEST_MISMATCH")
        }
        Some("UPLOADING" | "SELECTED" | "VALIDATING") => {}
        Some("EXPIRED") => bail!("MATERIAL_EXPIRED"),
        _ => bail!("MATERIAL_NOT_OPEN"),
    }
    if m["sha256"].as_str() != Some(declared) {
        bail!("MATERIAL_DIGEST_MISMATCH")
    }
    let bytes = fs::read(&path).map_err(|_| anyhow!("MATERIAL_INCOMPLETE"))?;
    let declared_size: u64 = m["sizeBytes"]
        .as_str()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if bytes.len() as u64 != declared_size {
        bail!("MATERIAL_INCOMPLETE")
    }
    if hash(&bytes) != declared {
        bail!("MATERIAL_DIGEST_MISMATCH")
    }
    m["state"] = json!("SEALED");
    m["sealedAt"] = json!(store::now());
    h.store.lock().unwrap().material_put(&m)?;
    Ok((200, m))
}
// Only an unbound upload may be cancelled; bound/consumed material is durable
// evidence of a dispatched message and refuses deletion.
async fn material_delete(h: Shared, key: &str) -> Result<(u16, Value)> {
    let mut m = {
        let mut s = h.store.lock().unwrap();
        material_fresh(&mut s, key)?
    };
    match m["state"].as_str() {
        Some("BOUND" | "CONSUMED") => bail!("MATERIAL_BOUND"),
        Some("CANCELLED") => return Ok((200, m)),
        _ => {}
    }
    m["state"] = json!("CANCELLED");
    m["cancelledAt"] = json!(store::now());
    h.store.lock().unwrap().material_put(&m)?;
    let _ = fs::remove_file(material_path(&h, key));
    Ok((200, m))
}
// Message-side materials[] validation: {materialRef,sha256,role} only; the
// durable row supplies mediaType/size (the client never re-declares them).
// scope is the create requestKey for task creation or the task id for
// message-level binding. Returns (uploadKey, sha256, role) triples.
pub(crate) fn validate_materials(
    s: &store::Store,
    v: &Value,
    scope: &str,
) -> Result<Vec<(String, String, String)>> {
    if v.is_null() {
        return Ok(vec![]);
    }
    let a = v.as_array().ok_or_else(|| anyhow!("INVALID_MATERIALS"))?;
    if a.is_empty() || a.len() > MAX_PER_MESSAGE {
        bail!("INVALID_MATERIALS")
    }
    let mut binds = vec![];
    let mut seen = std::collections::HashSet::new();
    let mut total = 0u64;
    for item in a {
        exact(item, &["materialRef", "sha256", "role"])?;
        let material_ref = valid_id(string(item, "materialRef")?)?.to_owned();
        let sha = string(item, "sha256")?;
        if !sha_ok(sha) {
            bail!("INVALID_MATERIAL_DIGEST")
        }
        let role = item["role"]
            .as_str()
            .filter(|r| matches!(*r, "IMAGE" | "REFERENCE_TEXT"))
            .ok_or_else(|| anyhow!("INVALID_MATERIAL_ROLE"))?
            .to_owned();
        if !seen.insert(material_ref.clone()) {
            bail!("INVALID_MATERIALS")
        }
        let m = s.material(&material_ref)?;
        // Target binding must name this intake: a task-scoped upload binds to
        // its task, a create-scoped upload binds to the create requestKey.
        let target = &m["target"];
        if target["taskId"].as_str() != Some(scope) && target["createKey"].as_str() != Some(scope) {
            bail!("INVALID_MATERIAL_TARGET")
        }
        total += m["sizeBytes"]
            .as_str()
            .and_then(|x| x.parse::<u64>().ok())
            .unwrap_or(TOTAL_MAX);
        binds.push((material_ref, sha.to_owned(), role));
    }
    if total > TOTAL_MAX {
        bail!("MATERIAL_TOO_LARGE")
    }
    Ok(binds)
}
// Build the extra turn input for the bound uploads of one request: IMAGE refs
// become localImage items over the staged bytes; REFERENCE_TEXT refs append a
// bounded <<ATTACHED_MATERIAL>> section to the text item. Returns the extra
// input items and the appended text.
pub(crate) fn material_input(h: &Shared, task: &str, key: &str) -> Result<(Vec<Value>, String)> {
    let rows = {
        let s = h.store.lock().unwrap();
        s.request_materials(task, key)?
    };
    let mut items = vec![];
    let mut text = String::new();
    for m in rows {
        let path = material_path(h, m["uploadKey"].as_str().unwrap_or(""));
        if m["boundRole"].as_str() == Some("IMAGE") {
            items.push(json!({"type":"localImage","path":path}));
        } else {
            let bytes = fs::read(&path).unwrap_or_default();
            let body = String::from_utf8_lossy(&bytes);
            text.push_str(&format!(
                "\n\n<<ATTACHED_MATERIAL name={} mediaType={} sha256={}>>\n{}\n<</ATTACHED_MATERIAL>>",
                m["displayName"].as_str().unwrap_or(""),
                m["mediaType"].as_str().unwrap_or(""),
                m["sha256"].as_str().unwrap_or(""),
                body
            ));
        }
    }
    Ok((items, text))
}
