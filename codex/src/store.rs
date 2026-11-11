use anyhow::{Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::Path;
use uuid::Uuid;

pub fn now() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}
pub fn id() -> String {
    Uuid::new_v4().to_string()
}
// RFC3339 timestamps plus their millisecond-shifted forms for durable expiry
// facts; numeric epoch-millis for comparisons (RFC3339 strings do not sort
// correctly across the fractional-second boundary).
pub fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
pub fn now_plus_ms(ms: u64) -> String {
    (time::OffsetDateTime::now_utc() + time::Duration::milliseconds(ms as i64))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}
// Compare a persisted RFC3339 deadline against now; an unparseable stamp is
// treated as expired so a tampered row can never extend its own validity.
pub fn rfc3339_expired(s: &str) -> bool {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map(|t| t < time::OffsetDateTime::now_utc())
        .unwrap_or(true)
}
pub fn canonical(v: &Value) -> Result<Vec<u8>> {
    fn check(v: &Value) -> Result<()> {
        match v {
            Value::Number(n) if !n.is_i64() && !n.is_u64() => bail!("INTEGER_REQUIRED"),
            Value::Array(a) => {
                for v in a {
                    check(v)?
                }
            }
            Value::Object(o) => {
                for v in o.values() {
                    check(v)?
                }
            }
            _ => {}
        }
        Ok(())
    }
    check(v)?;
    Ok(serde_json::to_vec(v)?)
}
pub struct Store {
    pub db: Connection,
    pub identity: Value,
    // Deterministic commit-fault flag for tests: `fail_next_commit` arms it and
    // the next commit_tx bails BEFORE commit so callers can prove no partial
    // durable write survives. Production paths never set it.
    fail_commit_next: std::cell::Cell<bool>,
}
const SCHEMA_VERSION: i64 = 8;
// native-messages/1 bounds: a single stored public text is at most 64 KiB of
// UTF-8 and its JSON-escaped form at most 128 KiB (further char-boundary
// shrink when escaping would exceed that); one page's items serialize within
// 256 KiB and the whole read response within the existing 1 MiB bound.
pub(crate) const MESSAGE_TEXT_MAX: usize = 64 * 1024;
const MESSAGE_ESCAPED_MAX: usize = 128 * 1024;
const MESSAGE_PAGE_MAX: usize = 256 * 1024;
const MESSAGE_RESPONSE_MAX: usize = 1024 * 1024;
const HISTORY_STRUCTURED: &str = "STRUCTURED_FROM_START";
const HISTORY_LEGACY: &str = "LEGACY_PREFIX";
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let mut db = Connection::open(path)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")?;
        let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > SCHEMA_VERSION {
            bail!("SCHEMA_VERSION_UNSUPPORTED")
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS source_identity (id INTEGER PRIMARY KEY CHECK(id=1), json TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, json TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS requests (scope TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, phase TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope,key));
          CREATE TABLE IF NOT EXISTS events (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(task,seq));
          CREATE TABLE IF NOT EXISTS artifacts (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, json TEXT NOT NULL, bytes BLOB, PRIMARY KEY(task,id));
          CREATE TABLE IF NOT EXISTS native_effects (task TEXT NOT NULL REFERENCES tasks(id), effect_id TEXT NOT NULL, native_item_id TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, native_turn_id TEXT, started_at TEXT NOT NULL, settled_at TEXT, summary_json TEXT, PRIMARY KEY(task,native_item_id));
          CREATE TABLE IF NOT EXISTS turn_seals (task TEXT NOT NULL REFERENCES tasks(id), native_turn_id TEXT NOT NULL, command_id TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, status TEXT NOT NULL, reason TEXT, sealed_at TEXT NOT NULL, PRIMARY KEY(task,native_turn_id));
          CREATE TABLE IF NOT EXISTS usage_ledger (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, native_thread_id TEXT, native_turn_id TEXT, input_tokens INTEGER NOT NULL, cached_input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, reasoning_output_tokens INTEGER NOT NULL, total_tokens INTEGER NOT NULL, observed_at TEXT NOT NULL, PRIMARY KEY(task,seq));
          CREATE TABLE IF NOT EXISTS budgets (task TEXT PRIMARY KEY REFERENCES tasks(id), max_total_tokens INTEGER, max_input_tokens INTEGER, max_output_tokens INTEGER, max_cost_micro_usd INTEGER, enforcement TEXT NOT NULL, exceeded INTEGER NOT NULL DEFAULT 0, exceeded_at TEXT, exceeded_turn_id TEXT, exceed_usage_json TEXT, extensions INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS terminals (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, process_id TEXT NOT NULL, argv TEXT NOT NULL, pty INTEGER NOT NULL, state TEXT NOT NULL, exit_code INTEGER, bytes_out INTEGER NOT NULL DEFAULT 0, bytes_in INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL, exited TEXT, reason TEXT, PRIMARY KEY(task,id));
          CREATE TABLE IF NOT EXISTS context_packages (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, name TEXT, digest TEXT NOT NULL, size INTEGER NOT NULL, created TEXT NOT NULL, PRIMARY KEY(task,id));
          CREATE TABLE IF NOT EXISTS context_package_sources (task TEXT NOT NULL REFERENCES tasks(id), package_id TEXT NOT NULL, ordinal INTEGER NOT NULL, json TEXT NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(task,package_id,ordinal));
          CREATE TABLE IF NOT EXISTS aux_sessions (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, question TEXT NOT NULL, answer TEXT NOT NULL, state TEXT NOT NULL, request_key TEXT NOT NULL, native_thread_id TEXT, native_turn_id TEXT, budget_max_total INTEGER, usage_json TEXT, workspace TEXT, deliver_key TEXT, delivered_at TEXT, stop_reason TEXT, created TEXT NOT NULL, finished TEXT, packages_json TEXT NOT NULL DEFAULT '[]', deadline TEXT, PRIMARY KEY(task,id));
          CREATE TABLE IF NOT EXISTS transitions (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, from_epoch TEXT NOT NULL, to_epoch TEXT NOT NULL, from_segment TEXT NOT NULL, to_segment TEXT NOT NULL, compatibility TEXT NOT NULL, state TEXT NOT NULL, reason TEXT, created TEXT NOT NULL, PRIMARY KEY(task,id));
          CREATE TABLE IF NOT EXISTS collaborations (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, name TEXT, request_key TEXT NOT NULL, state TEXT NOT NULL, reason TEXT, result_json TEXT, created TEXT NOT NULL, finished TEXT, PRIMARY KEY(task,id));
          CREATE TABLE IF NOT EXISTS collab_children (task TEXT NOT NULL REFERENCES tasks(id), collab_id TEXT NOT NULL, id TEXT NOT NULL, goal TEXT NOT NULL, goal_sha256 TEXT NOT NULL, depends_on TEXT NOT NULL DEFAULT '[]', packages_json TEXT NOT NULL DEFAULT '[]', write_scope_json TEXT, budget_json TEXT, state TEXT NOT NULL, reason TEXT, failed_deps TEXT, workspace TEXT, baseline_json TEXT, candidate_json TEXT, adoptions_json TEXT NOT NULL DEFAULT '[]', native_thread_id TEXT, native_turn_id TEXT, usage_json TEXT, tool_calls INTEGER NOT NULL DEFAULT 0, output TEXT NOT NULL DEFAULT '', stop_proof TEXT, created TEXT NOT NULL, started TEXT, finished TEXT, deadline TEXT, PRIMARY KEY(task,collab_id,id));")?;
        // v1 keyed artifact_manifests by task alone; v2 keys by (task, segment_id) so a
        // later generation cannot overwrite a sealed snapshot. The old single row is
        // rebound to the segment its task record still names.
        let shape: Option<String> = db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='artifact_manifests'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        match shape {
            None => db.execute_batch("CREATE TABLE artifact_manifests (task TEXT NOT NULL REFERENCES tasks(id), segment_id TEXT NOT NULL, json TEXT NOT NULL, sealed_at TEXT NOT NULL, PRIMARY KEY(task,segment_id));")?,
            Some(sql) if !sql.contains("segment_id") => {
                let tx = db.transaction()?;
                tx.execute_batch("ALTER TABLE artifact_manifests RENAME TO artifact_manifests_v1;
                  CREATE TABLE artifact_manifests (task TEXT NOT NULL REFERENCES tasks(id), segment_id TEXT NOT NULL, json TEXT NOT NULL, sealed_at TEXT NOT NULL, PRIMARY KEY(task,segment_id));")?;
                let rows: Vec<(String, String)> = {
                    let mut q = tx.prepare("SELECT task,json FROM artifact_manifests_v1")?;
                    q.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                        .collect::<std::result::Result<_, _>>()?
                };
                for (task_id, manifest) in rows {
                    let segment: String = tx
                        .query_row("SELECT json FROM tasks WHERE id=?1", [&task_id], |r| {
                            r.get::<_, String>(0)
                        })
                        .optional()?
                        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                        .and_then(|t| t["segmentId"].as_str().map(str::to_owned))
                        .unwrap_or_else(|| "segment-legacy".into());
                    tx.execute(
                        "INSERT INTO artifact_manifests VALUES(?1,?2,?3,?4)",
                        params![task_id, segment, manifest, now()],
                    )?;
                }
                tx.execute_batch("DROP TABLE artifact_manifests_v1")?;
                tx.commit()?;
            }
            Some(_) => {}
        }
        // v2 seals carried no enforcement reason; v3 adds a nullable one so an
        // interrupted BUDGET_EXCEEDED seal stays distinguishable from a normal
        // completion. Existing rows keep NULL.
        let seal_shape: Option<String> = db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='turn_seals'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(sql) = seal_shape {
            if !sql.contains("reason") {
                db.execute_batch("ALTER TABLE turn_seals ADD COLUMN reason TEXT")?;
            }
        }
        if version < 7 {
            // v7: durable per-item public conversation index (native-messages/1).
            // public_messages projects only already-public text (the accepted
            // user instruction and agentMessage.text); private reasoning, raw
            // protocol frames, tool arguments and approval bodies never land
            // here. public_transcript_meta owns the per-task sequence and the
            // independent transcript revision counter plus the one-shot legacy
            // freeze. Table/index creation and the version bump share one
            // migration transaction; every pre-existing table stays untouched.
            let tx = db.transaction()?;
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS public_messages (
                  task_id TEXT NOT NULL REFERENCES tasks(id),
                  message_id TEXT NOT NULL,
                  sequence INTEGER NOT NULL,
                  revision INTEGER NOT NULL,
                  role TEXT NOT NULL,
                  text TEXT NOT NULL,
                  text_truncated INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL,
                  source_request_key TEXT,
                  native_thread_id TEXT,
                  native_turn_id TEXT,
                  native_item_id TEXT,
                  command_id TEXT,
                  generation TEXT,
                  segment_id TEXT,
                  occurred_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY(task_id,message_id),
                  UNIQUE(task_id,sequence)
                );
                CREATE TABLE IF NOT EXISTS public_transcript_meta (
                  task_id TEXT PRIMARY KEY REFERENCES tasks(id),
                  next_sequence INTEGER NOT NULL,
                  transcript_revision INTEGER NOT NULL,
                  history_mode TEXT NOT NULL,
                  legacy_text TEXT,
                  legacy_text_truncated INTEGER NOT NULL DEFAULT 0,
                  initialized_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS public_messages_user_key
                  ON public_messages(task_id,source_request_key)
                  WHERE role='user' AND source_request_key IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS public_messages_native_item
                  ON public_messages(task_id,native_thread_id,native_item_id)
                  WHERE native_thread_id IS NOT NULL AND native_item_id IS NOT NULL;",
            )?;
            tx.execute_batch("PRAGMA user_version=7;")?;
            tx.commit()?;
        }
        if version < SCHEMA_VERSION {
            // v8: mobile-control additive surfaces (CA-ANDROID-V2). All tables
            // are new; existing v1-v7 rows are never rewritten.
            //   child_messages/child_stream_meta: durable per-child public
            //     stream (collaboration children and auxiliary sessions). Only
            //     already-public facts land here (assignment text the caller
            //     submitted, published assistant text, tool kind+status facts,
            //     terminal return/status) — never tool arguments, raw output
            //     beyond the bounded public text, or private reasoning. The
            //     per-stream meta row owns the monotonic sequence and the
            //     captureStartedAt proof; allocation happens inside the same
            //     transaction as the fact it records so a restart can never
            //     renumber captured history.
            //   collab_preflights/collab_previews: frozen plan resources that
            //     bind a validated collaboration or adoption plan to a digest
            //     and an expiry without starting anything.
            //   source_seals: one-way handoff seal fact per (task,sealId).
            //   material_uploads: project-scoped staged material metadata;
            //     the bytes themselves live under state/materials/<key>.
            let tx = db.transaction()?;
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS child_messages (
                  task_id TEXT NOT NULL REFERENCES tasks(id),
                  collab_id TEXT,
                  child_id TEXT NOT NULL,
                  stream_id TEXT NOT NULL,
                  seq INTEGER NOT NULL,
                  kind TEXT NOT NULL,
                  text TEXT NOT NULL,
                  text_truncated INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  event_ref TEXT,
                  message_id TEXT NOT NULL,
                  PRIMARY KEY(task_id,stream_id,seq)
                );
                CREATE TABLE IF NOT EXISTS child_stream_meta (
                  task_id TEXT NOT NULL REFERENCES tasks(id),
                  stream_id TEXT NOT NULL,
                  owner_kind TEXT NOT NULL,
                  collab_id TEXT,
                  child_id TEXT NOT NULL,
                  next_seq INTEGER NOT NULL,
                  capture_started_at TEXT NOT NULL,
                  PRIMARY KEY(task_id,stream_id)
                );
                CREATE INDEX IF NOT EXISTS child_messages_lookup
                  ON child_messages(task_id,collab_id,child_id,seq);
                CREATE TABLE IF NOT EXISTS collab_preflights (
                  task TEXT NOT NULL REFERENCES tasks(id),
                  id TEXT NOT NULL,
                  plan_digest TEXT NOT NULL,
                  plan_json TEXT NOT NULL,
                  source_bindings TEXT NOT NULL,
                  allowed_scopes TEXT NOT NULL,
                  budget_terms TEXT NOT NULL,
                  expires_at TEXT NOT NULL,
                  created TEXT NOT NULL,
                  PRIMARY KEY(task,id)
                );
                CREATE TABLE IF NOT EXISTS collab_previews (
                  task TEXT NOT NULL REFERENCES tasks(id),
                  id TEXT NOT NULL,
                  collab_id TEXT NOT NULL,
                  child_id TEXT NOT NULL,
                  plan_digest TEXT NOT NULL,
                  files_json TEXT NOT NULL,
                  paths_json TEXT,
                  candidate_digest TEXT NOT NULL,
                  expires_at TEXT NOT NULL,
                  created TEXT NOT NULL,
                  PRIMARY KEY(task,id)
                );
                CREATE TABLE IF NOT EXISTS source_seals (
                  task TEXT NOT NULL REFERENCES tasks(id),
                  id TEXT NOT NULL,
                  request_key TEXT NOT NULL,
                  handoff_id TEXT NOT NULL,
                  target_ref TEXT NOT NULL,
                  proof_json TEXT NOT NULL,
                  affected_children TEXT NOT NULL,
                  sealed_at TEXT NOT NULL,
                  PRIMARY KEY(task,id)
                );
                CREATE TABLE IF NOT EXISTS material_uploads (
                  upload_key TEXT PRIMARY KEY,
                  json TEXT NOT NULL
                );",
            )?;
            tx.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION};"))?;
            tx.commit()?;
        }
        let prior: Option<String> = db
            .query_row("SELECT json FROM source_identity WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let identity = match prior {
            Some(s) => serde_json::from_str(&s)?,
            None => {
                let v = json!({"authorityId":format!("authority-{}",id()),"storeId":id(),"workerId":format!("worker-{}",id())});
                db.execute("INSERT INTO source_identity VALUES(1,?1)", [v.to_string()])?;
                v
            }
        };
        Ok(Self {
            db,
            identity,
            fail_commit_next: std::cell::Cell::new(false),
        })
    }
    // Single commit point for message-touching transactions so tests can
    // inject one deterministic pre-commit failure and prove no half write
    // survives. Static so the flag borrow never conflicts with the open
    // transaction's &mut borrow on `db`.
    fn commit_tx(_flag: &std::cell::Cell<bool>, tx: rusqlite::Transaction) -> Result<()> {
        #[cfg(test)]
        if _flag.replace(false) {
            drop(tx);
            bail!("INJECTED_COMMIT_FAILURE")
        }
        tx.commit()?;
        Ok(())
    }
    #[cfg(test)]
    pub fn fail_next_commit(&self) {
        self.fail_commit_next.set(true)
    }
    pub fn recover(&mut self) -> Result<()> {
        // Never infer a native result or replay a dispatch window after a restart.
        let tasks = self.tasks()?;
        for mut t in tasks {
            if t["quiescent"] != true {
                let phase: Option<String> = self
                    .db
                    .query_row(
                        "SELECT phase FROM requests WHERE scope='create' AND key=?1",
                        [t["requestKey"].as_str().unwrap_or("")],
                        |r| r.get(0),
                    )
                    .optional()?;
                // A durable QUEUED intent whose create receipt is still ACCEPTED was
                // provably never dispatched; it may be evaluated for admission exactly
                // once. Anything further along falls through to the normal window.
                if t["launchState"] == "QUEUED" && phase.as_deref() == Some("ACCEPTED") {
                    t["capabilities"] = json!({"stop":false,"steer":false,"followUp":false,"pause":false,"resume":false,"observeAfterRestart":true,"continueAfterRestart":false});
                    self.save(&t)?;
                    continue;
                }
                if phase.as_deref() == Some("ACCEPTED") {
                    t["state"] = json!("REJECTED");
                    t["launchState"] = json!("REJECTED");
                    t["reasonCode"] = json!("LOCAL_LAUNCH_INTERRUPTED");
                    t["quiescent"] = json!(true);
                } else {
                    t["state"] = json!("UNKNOWN");
                    t["reasonCode"] = json!("OUTCOME_UNCONFIRMED");
                    t["quiescent"] = json!(false);
                }
                t["capabilities"] = json!({"stop":false,"steer":false,"followUp":false,"pause":false,"resume":false,"observeAfterRestart":true,"continueAfterRestart":false});
                self.save(&t)?;
            }
        }
        // Each bulk receipt and the matching user-message projection commit
        // together; an already-proven CONSUMED is never downgraded.
        {
            let at = now();
            let tx = self.db.transaction()?;
            tx.execute("UPDATE requests SET phase='UNKNOWN',result=json_set(result,'$.state','UNKNOWN','$.reasonCode','OUTCOME_UNCONFIRMED') WHERE phase IN ('DISPATCHING','REQUESTED')",[])?;
            Self::tx_recover_message_phase(&tx, "UNKNOWN", &at)?;
            Self::commit_tx(&self.fail_commit_next, tx)?;
        }
        {
            let at = now();
            let tx = self.db.transaction()?;
            tx.execute("UPDATE requests SET phase='REJECTED',result=json_set(result,'$.state','REJECTED','$.reasonCode','LOCAL_LAUNCH_INTERRUPTED') WHERE phase='ACCEPTED'",[])?;
            Self::tx_recover_message_phase(&tx, "REJECTED", &at)?;
            Self::commit_tx(&self.fail_commit_next, tx)?;
        }
        // An unsettled effect can never acquire a proven outcome after a crash.
        self.db.execute(
            "UPDATE native_effects SET status='UNKNOWN',settled_at=?1 WHERE status='STARTED'",
            [now()],
        )?;
        // Every exec session lived inside a managed peer that died with this
        // host; their outcome is unproven, not a clean exit.
        self.db.execute(
            "UPDATE terminals SET state='LOST',exited=?1,reason='HOST_RESTART' WHERE state='OPEN'",
            [now()],
        )?;
        // In-flight auxiliary sessions lived in a peer that died with the host;
        // no cross-restart continuation is claimed for them.
        let open_aux: Vec<(String, String)> = {
            let mut q = self
                .db
                .prepare("SELECT task,id FROM aux_sessions WHERE state IN ('STARTING','RUNNING','STOPPING')")?;
            q.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<std::result::Result<_, _>>()?
        };
        self.db.execute(
            "UPDATE aux_sessions SET state='CANCELLED',finished=?1,stop_reason='HOST_RESTART_JOB_CLOSED' WHERE state IN ('STARTING','RUNNING','STOPPING')",
            [now()],
        )?;
        for (task, aux) in open_aux {
            // The stream keeps its captured prefix; the terminal fact lands on
            // it honestly instead of being silently lost with the peer.
            let _ = self.child_message(
                &task,
                None,
                &aux,
                &ChildMsg {
                    kind: "STATUS",
                    text: "CANCELLED HOST_RESTART_JOB_CLOSED",
                    status: "CANCELLED",
                    event_ref: None,
                },
            );
        }
        // W5b: collaboration children are fenced by the same job-close proof.
        // A RUNNING child reconciles CANCELLED, a never-launched dependent
        // reconciles BLOCKED; SUCCEEDED/FAILED child rows (and their sealed
        // candidate facts) stay untouched so adoption can still read them.
        let reconciled: Vec<(String, String, String, String)> = {
            let mut q = self.db.prepare(
                "SELECT task,collab_id,id,state FROM collab_children WHERE state IN ('RUNNING','PENDING') ORDER BY task,collab_id,created,rowid",
            )?;
            q.query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<std::result::Result<_, _>>()?
        };
        let stopped_collabs: Vec<(String, String)> = {
            let mut q = self
                .db
                .prepare("SELECT task,id FROM collaborations WHERE state='RUNNING'")?;
            q.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<std::result::Result<_, _>>()?
        };
        let at = now();
        self.db.execute(
            "UPDATE collab_children SET state='CANCELLED',finished=?1,reason='HOST_RESTART_JOB_CLOSED' WHERE state='RUNNING'",
            [at.clone()],
        )?;
        self.db.execute(
            "UPDATE collab_children SET state='BLOCKED',finished=?1,reason='HOST_RESTART_JOB_CLOSED' WHERE state='PENDING'",
            [at.clone()],
        )?;
        self.db.execute(
            "UPDATE collaborations SET state='CANCELLED',finished=?1,reason='HOST_RESTART_JOB_CLOSED' WHERE state='RUNNING'",
            [at],
        )?;
        for (task, cid, ch, old) in reconciled {
            if old == "PENDING" {
                let _ = self.event(
                    &task,
                    "collaboration.child.blocked",
                    json!({"collaborationId":cid,"childId":ch,"reasonCode":"HOST_RESTART_JOB_CLOSED"}),
                );
            } else {
                let _ = self.event(
                    &task,
                    "collaboration.child.completed",
                    json!({"collaborationId":cid,"childId":ch,"state":"CANCELLED","reasonCode":"HOST_RESTART_JOB_CLOSED"}),
                );
            }
            let status = if old == "PENDING" { "BLOCKED" } else { "CANCELLED" };
            let text = format!("{status} HOST_RESTART_JOB_CLOSED");
            let _ = self.child_message(
                &task,
                Some(&cid),
                &ch,
                &ChildMsg {
                    kind: "STATUS",
                    text: &text,
                    status,
                    event_ref: None,
                },
            );
        }
        for (task, cid) in stopped_collabs {
            let _ = self.event(
                &task,
                "collaboration.stopped",
                json!({"collaborationId":cid,"reasonCode":"HOST_RESTART_JOB_CLOSED"}),
            );
        }
        Ok(())
    }
    // Durable terminal session record. argv is stored for the audit projection;
    // diagnostics strip it through the body-key set like every other command
    // carrier.
    pub fn terminal_put(&self, task: &str, id: &str, process_id: &str, argv: &Value, pty: bool) -> Result<()> {
        self.db.execute(
            "INSERT INTO terminals VALUES(?1,?2,?3,?4,?5,'OPEN',NULL,0,0,?6,NULL,NULL)",
            params![task, id, process_id, argv.to_string(), pty as i64, now()],
        )?;
        Ok(())
    }
    fn terminal_row(row: &rusqlite::Row) -> rusqlite::Result<Value> {
        Ok(json!({
            "terminalId": row.get::<_, String>(0)?,
            "processId": row.get::<_, String>(1)?,
            "argv": serde_json::from_str::<Value>(&row.get::<_, String>(2)?).unwrap_or(json!([])),
            "pty": row.get::<_, i64>(3)? != 0,
            "state": row.get::<_, String>(4)?,
            "exitCode": row.get::<_, Option<i64>>(5)?,
            "bytesOut": row.get::<_, i64>(6)?.to_string(),
            "bytesIn": row.get::<_, i64>(7)?.to_string(),
            "created": row.get::<_, String>(8)?,
            "exited": row.get::<_, Option<String>>(9)?,
            "reason": row.get::<_, Option<String>>(10)?,
        }))
    }
    pub fn terminal(&self, task: &str, id: &str) -> Result<Value> {
        self.db
            .query_row(
                "SELECT id,process_id,argv,pty,state,exit_code,bytes_out,bytes_in,created,exited,reason FROM terminals WHERE task=?1 AND id=?2",
                params![task, id],
                |r| Self::terminal_row(r),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("TERMINAL_NOT_FOUND"))
    }
    pub fn terminals(&self, task: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(
            "SELECT id,process_id,argv,pty,state,exit_code,bytes_out,bytes_in,created,exited,reason FROM terminals WHERE task=?1 ORDER BY created",
        )?;
        let rows = q.query_map([task], |r| Self::terminal_row(r))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    pub fn open_terminal_count(&self, task: &str) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM terminals WHERE task=?1 AND state='OPEN'",
            [task],
            |r| r.get(0),
        )?)
    }
    // Single guarded transition: only an OPEN session can settle, so a delete
    // racing the deferred native exit resolves exactly once.
    pub fn terminal_finish(
        &self,
        task: &str,
        id: &str,
        state: &str,
        exit_code: Option<i64>,
        reason: Option<&str>,
        bytes_out: Option<i64>,
    ) -> Result<bool> {
        let changed = self.db.execute(
            "UPDATE terminals SET state=?3,exit_code=coalesce(?4,exit_code),exited=?5,reason=coalesce(?6,reason),bytes_out=coalesce(?7,bytes_out) WHERE task=?1 AND id=?2 AND state='OPEN'",
            params![task, id, state, exit_code, now(), reason, bytes_out],
        )?;
        Ok(changed > 0)
    }
    pub fn terminal_input(&self, task: &str, id: &str, bytes: i64) -> Result<bool> {
        Ok(self.db.execute(
            "UPDATE terminals SET bytes_in=bytes_in+?3 WHERE task=?1 AND id=?2 AND state='OPEN'",
            params![task, id, bytes],
        )? > 0)
    }
    // Bulk close for every session still OPEN on this task: STOP proved the
    // owning job emptied, so these are terminations with native proof.
    pub fn terminals_close_open(&self, task: &str, state: &str, reason: &str) -> Result<Vec<String>> {
        let mut q = self
            .db
            .prepare("SELECT id FROM terminals WHERE task=?1 AND state='OPEN'")?;
        let ids: Vec<String> = q
            .query_map([task], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        drop(q);
        self.db.execute(
            "UPDATE terminals SET state=?2,exited=?3,reason=?4 WHERE task=?1 AND state='OPEN'",
            params![task, state, now(), reason],
        )?;
        Ok(ids)
    }
    // Durable inventory of native items with external effects. STARTED is recorded at
    // item/started, SETTLED at item/completed; UNKNOWN means the outcome is unproven.
    pub fn effect_started(
        &self,
        task: &str,
        item_id: &str,
        kind: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        let t = self.task(task)?;
        self.db.execute(
            "INSERT INTO native_effects VALUES(?1,?2,?3,?4,'STARTED',?5,?6,?7,?8,NULL,?9) ON CONFLICT(task,native_item_id) DO NOTHING",
            params![
                task,
                format!("effect-{}", id()),
                item_id,
                kind,
                t["generation"].as_str().unwrap_or(""),
                t["segmentId"].as_str().unwrap_or(""),
                native_turn_id,
                now(),
                json!({"type":kind}).to_string()
            ],
        )?;
        Ok(())
    }
    pub fn effect_settled(&self, task: &str, item_id: &str, native_status: &str) -> Result<()> {
        self.db.execute(
            "UPDATE native_effects SET status='SETTLED',settled_at=?3,summary_json=json_set(coalesce(summary_json,'{}'),'$.nativeStatus',?4) WHERE task=?1 AND native_item_id=?2 AND status='STARTED'",
            params![task, item_id, now(), native_status],
        )?;
        Ok(())
    }
    pub fn effects_unknown(&self, task: &str) -> Result<()> {
        self.db.execute(
            "UPDATE native_effects SET status='UNKNOWN',settled_at=?2 WHERE task=?1 AND status='STARTED'",
            params![task, now()],
        )?;
        Ok(())
    }
    pub fn active_native_commands(&self, task: &str) -> Result<i64> {
        let t = self.task(task)?;
        Ok(self.db.query_row(
            "SELECT count(*) FROM native_effects WHERE task=?1 AND kind='commandExecution' AND status='STARTED' AND generation=?2 AND segment_id=?3 AND native_turn_id=?4",
            params![task,t["generation"].as_str(),t["segmentId"].as_str(),t["nativeTurnId"].as_str()],
            |r| r.get(0),
        )?)
    }
    pub fn open_effects(&self, task: &str) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM native_effects WHERE task=?1 AND status IN ('STARTED','UNKNOWN')",
            [task],
            |r| r.get(0),
        )?)
    }
    pub fn runner_requests(&self, task: &str) -> Result<Vec<(String, Value, Value)>> {
        let mut q=self.db.prepare("SELECT key,body,result FROM requests WHERE scope=?1 AND key GLOB 'runner-*' AND json_type(body,'$.native.callId')='text' AND json_type(body,'$.command.contextId')='text' ORDER BY rowid")?;
        let rows = q.query_map([task], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (key, body, result) = row?;
            Ok((
                key,
                serde_json::from_str(&body)?,
                serde_json::from_str(&result)?,
            ))
        })
        .collect()
    }
    // Runner proof and the effect transition commit together; a native item
    // completion alone cannot settle a host-dispatched command.
    pub fn runner_result(
        &mut self,
        task: &str,
        key: &str,
        call_id: &str,
        result: &Value,
        proven: bool,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "UPDATE requests SET phase=?3,result=?4 WHERE scope=?1 AND key=?2 AND phase!='APPLIED'",
            params![
                task,
                key,
                if proven { "APPLIED" } else { "UNKNOWN" },
                result.to_string()
            ],
        )?;
        if !proven {
            // An unproven runner outcome is a durable UNKNOWN receipt; its
            // user-message projection lands in the same commit (§6.2).
            Self::tx_project_receipt(&tx, task, key, "UNKNOWN")?;
        }
        tx.execute("UPDATE native_effects SET status=?3,settled_at=?4,summary_json=json_set(coalesce(summary_json,'{}'),'$.runnerRequestKey',?5,'$.runnerStatus',?6) WHERE task=?1 AND native_item_id=?2 AND status='STARTED'",params![task,call_id,if proven {"SETTLED"}else{"UNKNOWN"},now(),key,result["runner"]["status"].as_str().unwrap_or("UNKNOWN")])?;
        tx.commit()?;
        Ok(())
    }
    // Frozen completion fact for one bound native turn; never overwritten once sealed.
    pub fn seal_turn(
        &self,
        task: &str,
        native_turn_id: &str,
        command_id: &str,
        generation: &str,
        segment_id: &str,
        status: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        self.db.execute(
            "INSERT INTO turn_seals (task,native_turn_id,command_id,generation,segment_id,status,reason,sealed_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(task,native_turn_id) DO NOTHING",
            params![task, native_turn_id, command_id, generation, segment_id, status, reason, now()],
        )?;
        Ok(())
    }
    pub fn turn_seal(&self, task: &str, native_turn_id: &str) -> Result<Option<Value>> {
        let row: Option<String> = self
            .db
            .query_row(
                "SELECT json_object('commandId',command_id,'nativeTurnId',native_turn_id,'generation',generation,'segmentId',segment_id,'status',status,'reason',reason,'sealedAt',sealed_at) FROM turn_seals WHERE task=?1 AND native_turn_id=?2",
                params![task, native_turn_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(row.and_then(|s| serde_json::from_str(&s).ok()))
    }
    pub fn segment_manifest(&self, task: &str, segment: &str) -> Result<Option<Value>> {
        let row: Option<String> = self
            .db
            .query_row(
                "SELECT json FROM artifact_manifests WHERE task=?1 AND segment_id=?2",
                params![task, segment],
                |r| r.get(0),
            )
            .optional()?;
        Ok(row.and_then(|s| serde_json::from_str(&s).ok()))
    }
    pub fn segment_manifests(&self, task: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(
            "SELECT segment_id,sealed_at,json FROM artifact_manifests WHERE task=?1 ORDER BY rowid",
        )?;
        let rows = q.query_map([task], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        let mut out = vec![];
        for row in rows {
            let (segment_id, sealed_at, s) = row?;
            let mut m: Value = serde_json::from_str(&s)?;
            m["segmentId"] = json!(segment_id);
            m["sealedAt"] = json!(sealed_at);
            out.push(m);
        }
        Ok(out)
    }
    // Atomic stopped-session admission: the request, its intake event, the new
    // generation/segment/epoch task state and — for a user follow-up — its
    // durable ACCEPTED user message commit or fail together.
    pub fn admit_reopen(
        &mut self,
        task: &str,
        key: &str,
        body: &Value,
        result: &Value,
        budget: Option<&Value>,
        user_text: Option<&str>,
        bind: &[(String, String, String)],
        change: impl FnOnce(&mut Value),
    ) -> Result<Value> {
        let mut t = self.task(task)?;
        change(&mut t);
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        let tx = self.db.transaction()?;
        // Reopen admissions bind their materials inside the same transaction as
        // the generation rotation — a bad reference refuses the whole reopen.
        for (material_ref, sha256, role) in bind {
            Self::tx_material_bind(&tx, material_ref, sha256, role, task, key)?;
        }
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![t["taskId"].as_str(), t.to_string()],
        )?;
        // EXTEND_BUDGET replaces the cap row inside the same atomic admission so a
        // crash can never leave the task rotated but the budget un-extended.
        if let Some(caps) = budget {
            tx.execute(
                "INSERT INTO budgets VALUES(?1,?2,?3,?4,?5,'SOFT',0,NULL,NULL,NULL,1,?6) ON CONFLICT(task) DO UPDATE SET max_total_tokens=excluded.max_total_tokens,max_input_tokens=excluded.max_input_tokens,max_output_tokens=excluded.max_output_tokens,max_cost_micro_usd=excluded.max_cost_micro_usd,exceeded=0,exceeded_at=NULL,exceeded_turn_id=NULL,extensions=extensions+1,updated_at=excluded.updated_at",
                params![
                    task,
                    budget_cap(caps, "maxTotalTokens"),
                    budget_cap(caps, "maxInputTokens"),
                    budget_cap(caps, "maxOutputTokens"),
                    budget_cap(caps, "maxCostMicroUsd"),
                    now()
                ],
            )?;
        }
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![
                task,
                key,
                String::from_utf8(canonical(body)?)?,
                result.to_string()
            ],
        )?;
        // A user follow-up binds its message to the NEW generation/segment the
        // admission just rotated; control admissions (EXTEND_BUDGET) carry no
        // user text and produce no message row.
        if let Some(text) = user_text {
            Self::tx_insert_message(
                &tx,
                task,
                &NewMessage {
                    role: "user",
                    text,
                    status: "ACCEPTED",
                    source_request_key: Some(key),
                    command_id: Some(key),
                    generation: t["generation"].as_str(),
                    segment_id: t["segmentId"].as_str(),
                    ..NewMessage::default()
                },
            )?;
        }
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"admission":"STOPPED_SESSION_REOPEN"}).to_string()])?;
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(t)
    }
    pub fn tasks(&self) -> Result<Vec<Value>> {
        let mut q = self.db.prepare("SELECT json FROM tasks ORDER BY rowid")?;
        let rows = q.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    pub fn task(&self, key: &str) -> Result<Value> {
        let s: Option<String> = self
            .db
            .query_row("SELECT json FROM tasks WHERE id=?1", [key], |r| r.get(0))
            .optional()?;
        Ok(serde_json::from_str(
            &s.ok_or_else(|| anyhow::anyhow!("TASK_NOT_FOUND"))?,
        )?)
    }
    pub fn save(&self, v: &Value) -> Result<()> {
        self.db.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![v["taskId"].as_str(), v.to_string()],
        )?;
        Ok(())
    }
    pub fn update(&mut self, task: &str, change: impl FnOnce(&mut Value)) -> Result<Value> {
        let mut v = self.task(task)?;
        change(&mut v);
        let n = v["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        v["stateVersion"] = json!(n.to_string());
        self.save(&v)?;
        Ok(v)
    }
    pub fn event(&self, task: &str, kind: &str, payload: Value) -> Result<Value> {
        let t = self.task(task)?;
        let seq: i64 = self.db.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let local_kind = match kind {
            "turn.started" => "turn/started",
            "turn.completed" => "turn/completed",
            "message.completed" => "ASSISTANT_MESSAGE",
            _ => kind,
        };
        let at = now();
        let ev = json!({"eventId":format!("event-{}",id()),"sequence":seq.to_string(),"seq":seq.to_string(),"cursor":seq.to_string(),"kind":local_kind,"occurredAt":at,"source":self.identity,"taskId":task,"runId":t["runId"],"segmentId":t["segmentId"],"nativeTurnId":t["nativeTurnId"],"type":kind,"observedAt":at,"payload":payload});
        self.db.execute(
            "INSERT INTO events VALUES(?1,?2,?3)",
            params![task, seq, ev.to_string()],
        )?;
        Ok(ev)
    }
    pub fn events(&self, task: &str, cursor: i64, limit: i64) -> Result<Value> {
        let mut q = self.db.prepare(
            "SELECT seq,json FROM events WHERE task=?1 AND seq>?2 ORDER BY seq LIMIT ?3",
        )?;
        let mut end = cursor;
        let rows = q.query_map(params![task, cursor, limit], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut items = vec![];
        for row in rows {
            let (seq, s) = row?;
            end = seq;
            let mut event: Value = serde_json::from_str(&s)?;
            if event.get("cursor").is_none() {
                event["cursor"] = json!(seq.to_string());
            }
            if event.get("kind").is_none() {
                event["kind"] = json!(match event["type"].as_str().unwrap_or("UNKNOWN") {
                    "turn.started" => "turn/started",
                    "turn.completed" => "turn/completed",
                    "message.completed" => "ASSISTANT_MESSAGE",
                    kind => kind,
                });
            }
            if event.get("occurredAt").is_none() {
                event["occurredAt"] = event["observedAt"].clone();
            }
            items.push(event)
        }
        Ok(json!({"items":items,"cursor":end.to_string(),"cursorExpired":false}))
    }
    // occurredAt copied verbatim from the earliest durable journal event whose
    // `type` is one of `types`; timing projections never invent a time of
    // their own.
    pub fn earliest_event_at(&self, task: &str, types: &[&str]) -> Result<Option<String>> {
        self.event_at(task, types)
    }
    fn event_at(&self, task: &str, types: &[&str]) -> Result<Option<String>> {
        if types.is_empty() {
            return Ok(None);
        }
        let placeholders = types.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT json_extract(json,'$.occurredAt') FROM events WHERE task=? AND json_extract(json,'$.type') IN ({placeholders}) ORDER BY seq ASC LIMIT 1"
        );
        let params_iter = std::iter::once(&task as &dyn rusqlite::ToSql)
            .chain(types.iter().map(|t| t as &dyn rusqlite::ToSql));
        let row = self
            .db
            .query_row(&sql, rusqlite::params_from_iter(params_iter), |r| {
                r.get::<_, Option<String>>(0)
            })
            .optional()?;
        Ok(row.flatten())
    }
    // The most recent applied-observation fact: the durable request.observed
    // event that recorded a WORKER_LOCAL op's APPLIED settle.
    pub fn latest_applied_observation_at(&self, task: &str) -> Result<Option<String>> {
        let row = self
            .db
            .query_row(
                "SELECT json_extract(json,'$.occurredAt') FROM events WHERE task=?1 AND json_extract(json,'$.type')='request.observed' AND json_extract(json,'$.payload.state')='APPLIED' ORDER BY seq DESC LIMIT 1",
                [task],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?;
        Ok(row.flatten())
    }
    pub fn existing(&self, scope: &str, key: &str, body: &Value) -> Result<Option<Value>> {
        let row: Option<(String, String)> = self
            .db
            .query_row(
                "SELECT body,result FROM requests WHERE scope=?1 AND key=?2",
                params![scope, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match row {
            None => Ok(None),
            Some((s, result)) => {
                if s.as_bytes() != canonical(body)? {
                    bail!("IDEMPOTENCY_CONFLICT")
                }
                Ok(Some(serde_json::from_str(&result)?))
            }
        }
    }
    pub fn accept(
        &mut self,
        scope: &str,
        key: &str,
        body: &Value,
        result: &Value,
        task: &str,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![
                scope,
                key,
                String::from_utf8(canonical(body)?)?,
                result.to_string()
            ],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        tx.commit()?;
        Ok(())
    }
    // Atomic user admission (follow-up / steer): the original request row,
    // its intake event, the task mutations the admission requires and exactly
    // one ACCEPTED user message bound to the original request key — one
    // transaction, one commit. A replayed key is answered by the earlier
    // durable request lookup (existing()) and never reaches this insert.
    pub fn accept_user(
        &mut self,
        task: &str,
        key: &str,
        body: &Value,
        result: &Value,
        text: &str,
        mark_engaged: bool,
        bind: &[(String, String, String)],
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        // Sealed material references bind to this request inside the same
        // admission: an unsealed/mismatched/mistyped reference refuses the
        // whole write. The tuple is (uploadKey, sha256, role).
        for (material_ref, sha256, role) in bind {
            Self::tx_material_bind(&tx, material_ref, sha256, role, task, key)?;
        }
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![
                task,
                key,
                String::from_utf8(canonical(body)?)?,
                result.to_string()
            ],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let mut t: Value = serde_json::from_str(&s)?;
        if mark_engaged {
            // Follow-up re-engagement: the new command owns the next turn and
            // the session is busy until its completion fact arrives.
            t["state"] = json!("STARTING");
            t["currentCommandId"] = json!(key);
            t["quiescent"] = json!(false);
            let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
            t["stateVersion"] = json!(n.to_string());
            tx.execute(
                "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
                params![task, t.to_string()],
            )?;
        }
        Self::tx_insert_message(
            &tx,
            task,
            &NewMessage {
                role: "user",
                text,
                status: "ACCEPTED",
                source_request_key: Some(key),
                command_id: Some(key),
                generation: t["generation"].as_str(),
                segment_id: t["segmentId"].as_str(),
                ..NewMessage::default()
            },
        )?;
        Self::commit_tx(&self.fail_commit_next, tx)
    }
    pub fn accept_task(
        &mut self,
        task: &Value,
        key: &str,
        body: &Value,
        bind: &[(String, String, String)],
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        // createKey-scoped sealed uploads become task-bound inside the same
        // atomic task admission. The tuple is (uploadKey, sha256, role).
        for (material_ref, sha256, role) in bind {
            Self::tx_material_bind(&tx, material_ref, sha256, role, task["taskId"].as_str().unwrap_or(""), key)?;
        }
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2)",
            params![task["taskId"].as_str(), task.to_string()],
        )?;
        tx.execute(
            "INSERT INTO requests VALUES('create',?1,?2,'ACCEPTED',?3)",
            params![key, String::from_utf8(canonical(body)?)?, task.to_string()],
        )?;
        // A new task is structured from the start: its transcript meta is
        // created here (never later re-frozen as a legacy task), and the first
        // requirement lands as one durable ACCEPTED user message bound to the
        // original create request key.
        tx.execute(
            "INSERT INTO public_transcript_meta VALUES(?1,1,0,?2,NULL,0,?3)",
            params![task["taskId"].as_str(), HISTORY_STRUCTURED, now()],
        )?;
        Self::tx_insert_message(
            &tx,
            task["taskId"].as_str().unwrap_or(""),
            &NewMessage {
                role: "user",
                text: body["instruction"].as_str().unwrap_or(""),
                status: "ACCEPTED",
                source_request_key: Some(key),
                command_id: Some(key),
                generation: task["generation"].as_str(),
                segment_id: task["segmentId"].as_str(),
                ..NewMessage::default()
            },
        )?;
        let at = now();
        // A soft budget binds at acceptance so later usage observations enforce the
        // same caps the client admitted, even across a restart.
        if let Some(caps) = task["budget"].get("caps") {
            tx.execute(
                "INSERT INTO budgets VALUES(?1,?2,?3,?4,?5,?6,0,NULL,NULL,NULL,0,?7)",
                params![
                    task["taskId"].as_str(),
                    budget_cap(caps, "maxTotalTokens"),
                    budget_cap(caps, "maxInputTokens"),
                    budget_cap(caps, "maxOutputTokens"),
                    budget_cap(caps, "maxCostMicroUsd"),
                    task["budget"]["enforcement"].as_str().unwrap_or("SOFT"),
                    at
                ],
            )?;
        }
        tx.execute("INSERT INTO events VALUES(?1,1,?2)",params![task["taskId"].as_str(),json!({"cursor":"1","sequence":"1","seq":"1","kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task["taskId"],"requestKey":key}).to_string()])?;
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(())
    }
    // The durable receipt and its message projection commit together: a
    // terminal REJECTED/UNKNOWN receipt moves the bound user row to the same
    // honest state, while a proven CONSUMED is never downgraded by a late
    // REQUESTED/UNKNOWN answer. APPLIED/REQUESTED receipts are ledger facts
    // only — they do not assert model consumption.
    pub fn receipt(&mut self, scope: &str, key: &str, phase: &str, result: &Value) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "UPDATE requests SET phase=?3,result=?4 WHERE scope=?1 AND key=?2 AND NOT (phase='APPLIED' AND ?3 IN ('ACCEPTED','DISPATCHING','REQUESTED'))",
            params![scope, key, phase, result.to_string()],
        )?;
        if matches!(phase, "REJECTED" | "UNKNOWN") {
            // Task-scope requests resolve to their scope; a create-scope key
            // resolves through the owning task's durable requestKey.
            let task: Option<String> = if scope == "create" {
                tx.query_row(
                    "SELECT id FROM tasks WHERE json_extract(json,'$.requestKey')=?1",
                    [key],
                    |r| r.get(0),
                )
                .optional()?
            } else {
                Some(scope.to_string())
            };
            if let Some(task) = task {
                Self::tx_project_receipt(&tx, &task, key, phase)?;
            }
        }
        Self::commit_tx(&self.fail_commit_next, tx)
    }
    // native-messages/1 — durable per-item public conversation index.
    //
    // Bounded public text: 64 KiB UTF-8, shrunk further on character
    // boundaries whenever its JSON-escaped form would exceed 128 KiB. The
    // flag records that the stored text is a real prefix, not the full body.
    fn message_text(raw: &str) -> (String, bool) {
        let mut text = raw.to_string();
        let mut truncated = false;
        // String::truncate panics off a char boundary; walk down to one first.
        let boundary = |s: &str, mut n: usize| {
            while !s.is_char_boundary(n) {
                n -= 1;
            }
            n
        };
        if text.len() > MESSAGE_TEXT_MAX {
            let n = boundary(&text, MESSAGE_TEXT_MAX);
            text.truncate(n);
            truncated = true;
        }
        while serde_json::to_string(&text)
            .map(|s| s.len() > MESSAGE_ESCAPED_MAX)
            .unwrap_or(false)
        {
            let n = boundary(&text, (text.len() / 2).max(1));
            text.truncate(n);
            truncated = true;
        }
        (text, truncated)
    }
    // One-shot legacy freeze (design §7): the first read or first message
    // write on a task that predates this schema initializes its meta with the
    // bounded public output captured AT THAT MOMENT. It runs before any new
    // message insert inside the same transaction and never runs twice; new
    // tasks are created STRUCTURED_FROM_START in accept_task instead.
    fn tx_ensure_meta(tx: &rusqlite::Transaction, task: &str) -> Result<()> {
        let exists = tx
            .query_row(
                "SELECT 1 FROM public_transcript_meta WHERE task_id=?1",
                [task],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        if exists {
            return Ok(());
        }
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let t: Value = serde_json::from_str(&s)?;
        tx.execute(
            "INSERT INTO public_transcript_meta VALUES(?1,1,0,?2,?3,0,?4)",
            params![
                task,
                HISTORY_LEGACY,
                t["output"].as_str().unwrap_or(""),
                now()
            ],
        )?;
        Ok(())
    }
    // One message insert: sequence comes from the task's meta counter and the
    // independent transcript revision moves once per write. Callers supply
    // the binding observed at write time; no field is refreshed on update.
    fn tx_insert_message(tx: &rusqlite::Transaction, task: &str, m: &NewMessage) -> Result<i64> {
        Self::tx_ensure_meta(tx, task)?;
        let seq: i64 = tx.query_row(
            "SELECT next_sequence FROM public_transcript_meta WHERE task_id=?1",
            [task],
            |r| r.get(0),
        )?;
        let (text, truncated) = Self::message_text(m.text);
        let at = now();
        tx.execute(
            "INSERT INTO public_messages VALUES(?1,?2,?3,1,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                task,
                format!("msg-{}", id()),
                seq,
                m.role,
                text,
                truncated as i64,
                m.status,
                m.source_request_key,
                m.native_thread_id,
                m.native_turn_id,
                m.native_item_id,
                m.command_id,
                m.generation,
                m.segment_id,
                at,
                at
            ],
        )?;
        tx.execute(
            "UPDATE public_transcript_meta SET next_sequence=next_sequence+1,transcript_revision=transcript_revision+1 WHERE task_id=?1",
            [task],
        )?;
        Ok(seq)
    }
    // A durable REJECTED/UNKNOWN receipt projects onto the bound user row in
    // the same transaction. CONSUMED (a native consumption proof) and REJECTED
    // (a determined negative outcome) are terminal: later answers never
    // downgrade them, and replaying the same outcome is a no-op.
    fn tx_project_receipt(
        tx: &rusqlite::Transaction,
        task: &str,
        key: &str,
        phase: &str,
    ) -> Result<()> {
        let changed = tx.execute(
            "UPDATE public_messages SET status=?3,revision=revision+1,updated_at=?4 WHERE task_id=?1 AND source_request_key=?2 AND role='user' AND status IN ('ACCEPTED','UNKNOWN') AND status!=?3",
            params![task, key, phase, now()],
        )?;
        if changed > 0 {
            tx.execute(
                "UPDATE public_transcript_meta SET transcript_revision=transcript_revision+1 WHERE task_id=?1",
                [task],
            )?;
        }
        Ok(())
    }
    // Crash-recovery projection paired with the bulk request receipts in
    // recover(): every still-ACCEPTED user row whose request just settled to
    // `phase` moves to it. Task-scope requests join by scope; create-scope
    // requests join through the owning task's durable requestKey.
    fn tx_recover_message_phase(tx: &rusqlite::Transaction, phase: &str, at: &str) -> Result<()> {
        const PREDICATE: &str = "role='user' AND status='ACCEPTED' AND EXISTS(
            SELECT 1 FROM requests r
            WHERE r.key=public_messages.source_request_key AND r.phase=?1
              AND (r.scope=public_messages.task_id
                   OR (r.scope='create' AND EXISTS(
                         SELECT 1 FROM tasks t WHERE t.id=public_messages.task_id
                           AND json_extract(t.json,'$.requestKey')=r.key))))";
        let affected: Vec<String> = {
            let mut q = tx.prepare(&format!(
                "SELECT DISTINCT task_id FROM public_messages WHERE {PREDICATE}"
            ))?;
            q.query_map([phase], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        tx.execute(
            &format!("UPDATE public_messages SET status=?1,revision=revision+1,updated_at=?2 WHERE {PREDICATE}"),
            params![phase, at],
        )?;
        for task in affected {
            tx.execute(
                "UPDATE public_transcript_meta SET transcript_revision=transcript_revision+1 WHERE task_id=?1",
                [task],
            )?;
        }
        Ok(())
    }
    // Atomic assistant completion: the durable (task,thread,item) row, the
    // bounded accumulated output + legacy publicItemIds dedup mark, the public
    // message.completed event and the transcript revision commit together.
    // An item already in publicItemIds (a replayed or pre-migration
    // completion) is never projected a second time.
    pub fn record_agent_message(
        &mut self,
        task: &str,
        thread: &str,
        turn: &str,
        item_id: &str,
        text: &str,
    ) -> Result<bool> {
        let tx = self.db.transaction()?;
        Self::tx_ensure_meta(&tx, task)?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let mut t: Value = serde_json::from_str(&s)?;
        // Re-verify the binding on the durable row inside the transaction: the
        // caller's snapshot can predate a generation/turn rotation, and a
        // stale-turn item must never project as current history.
        if t["nativeSessionId"].as_str() != Some(thread) || t["nativeTurnId"].as_str() != Some(turn) {
            Self::commit_tx(&self.fail_commit_next, tx)?;
            return Ok(false);
        }
        let legacy_seen = t["publicItemIds"]
            .as_array()
            .is_some_and(|v| v.iter().any(|v| v == item_id));
        let indexed_seen = tx
            .query_row(
                "SELECT 1 FROM public_messages WHERE task_id=?1 AND native_thread_id=?2 AND native_item_id=?3",
                params![task, thread, item_id],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        if legacy_seen || indexed_seen {
            Self::commit_tx(&self.fail_commit_next, tx)?;
            return Ok(false);
        }
        Self::tx_insert_message(
            &tx,
            task,
            &NewMessage {
                role: "assistant",
                text,
                status: "COMPLETED",
                native_thread_id: Some(thread),
                native_turn_id: Some(turn),
                native_item_id: Some(item_id),
                command_id: t["currentCommandId"].as_str(),
                generation: t["generation"].as_str(),
                segment_id: t["segmentId"].as_str(),
                ..NewMessage::default()
            },
        )?;
        let mut output = t["output"].as_str().unwrap_or("").to_owned();
        if !output.is_empty() {
            output.push('\n')
        }
        output.push_str(text);
        if output.len() > 65536 {
            let mut start = output.len() - 65536;
            while !output.is_char_boundary(start) {
                start += 1;
            }
            output = output[start..].into();
        }
        t["output"] = json!(output);
        match t["publicItemIds"].as_array_mut() {
            Some(ids) => ids.push(json!(item_id)),
            None => t["publicItemIds"] = json!([item_id]),
        }
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![task, t.to_string()],
        )?;
        Self::event_tx(
            &self.identity,
            &tx,
            &t,
            task,
            "message.completed",
            &json!({"nativeItemId":item_id,"nativeTurnId":turn,"text":text}),
        )?;
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(true)
    }
    // Atomic user-input consumption proof: a native userMessage item binds to
    // exactly one durable request key of this task (its own ledger scope or
    // the original create request), marks that user row CONSUMED with its
    // observed native ids, and writes the APPLIED receipt + input.consumed
    // fact — all in one transaction. Nothing is inferred from text, timing or
    // "latest pending" guesses; anything without a deterministic binding is
    // reported for diagnostics instead of creating a row.
    pub fn record_user_consumption(
        &mut self,
        task: &str,
        key: &str,
        thread: &str,
        turn: &str,
        item_id: &str,
    ) -> Result<Consumption> {
        let tx = self.db.transaction()?;
        Self::tx_ensure_meta(&tx, task)?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let t: Value = serde_json::from_str(&s)?;
        // A user item naming another thread can never confirm this task's
        // input; auxiliary/collaboration sessions never reach this path.
        if t["nativeSessionId"].as_str() != Some(thread) {
            Self::commit_tx(&self.fail_commit_next, tx)?;
            return Ok(Consumption::Unbound("THREAD_MISMATCH"));
        }
        let scope = if tx
            .query_row(
                "SELECT 1 FROM requests WHERE scope=?1 AND key=?2",
                params![task, key],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some()
        {
            Some(task.to_string())
        } else if t["requestKey"].as_str() == Some(key)
            && tx
                .query_row(
                    "SELECT 1 FROM requests WHERE scope='create' AND key=?1",
                    [key],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
                .is_some()
        {
            Some("create".to_string())
        } else {
            None
        };
        let Some(scope) = scope else {
            Self::commit_tx(&self.fail_commit_next, tx)?;
            return Ok(Consumption::Unbound("REQUEST_NOT_BOUND"));
        };
        // When the request's native turn is already durably bound, an item
        // naming a different turn is contradictory evidence, not consumption.
        // Before the RPC ACK fills turns[key], turn/started has already
        // established the live turn. A missing ACK is not permission for an
        // arbitrary stale turn to consume an accepted request on this thread.
        let bound_turn = t["turns"][key]["nativeTurnId"]
            .as_str()
            .or_else(|| t["nativeTurnId"].as_str());
        if bound_turn != Some(turn) {
            Self::commit_tx(&self.fail_commit_next, tx)?;
            return Ok(Consumption::Unbound("TURN_MISMATCH"));
        }
        let changed = tx.execute(
            "UPDATE public_messages SET status='CONSUMED',native_thread_id=?3,native_turn_id=?4,native_item_id=?5,revision=revision+1,updated_at=?6 WHERE task_id=?1 AND source_request_key=?2 AND role='user' AND status IN ('ACCEPTED','UNKNOWN')",
            params![task, key, thread, turn, item_id, now()],
        )?;
        if changed > 0 {
            tx.execute(
                "UPDATE public_transcript_meta SET transcript_revision=transcript_revision+1 WHERE task_id=?1",
                [task],
            )?;
        } else {
            let prior: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT status,native_item_id FROM public_messages WHERE task_id=?1 AND source_request_key=?2 AND role='user'",
                    params![task, key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            match prior {
                // Same evidence replayed: the durable fact already stands, so
                // the receipt and event are not written a second time.
                Some((status, prior_item))
                    if status == "CONSUMED" && prior_item.as_deref() == Some(item_id) =>
                {
                    Self::commit_tx(&self.fail_commit_next, tx)?;
                    return Ok(Consumption::Bound);
                }
                // A different item claiming an already-settled key is
                // contradictory evidence; report it, never rebind the row.
                Some((status, _)) if status == "CONSUMED" || status == "REJECTED" => {
                    Self::commit_tx(&self.fail_commit_next, tx)?;
                    return Ok(Consumption::Unbound("ALREADY_SETTLED"));
                }
                // A request accepted before this schema has no row yet; the
                // bound item backfills it at CONSUMED with the originally
                // accepted public text from the durable request body.
                None => {
                    let body: Option<String> = tx
                        .query_row(
                            "SELECT body FROM requests WHERE scope=?1 AND key=?2",
                            params![scope, key],
                            |r| r.get(0),
                        )
                        .optional()?;
                    let text = body
                        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                        .and_then(|b| {
                            b["instruction"]
                                .as_str()
                                .or_else(|| b["content"].as_str())
                                .map(str::to_owned)
                        })
                        .unwrap_or_default();
                    Self::tx_insert_message(
                        &tx,
                        task,
                        &NewMessage {
                            role: "user",
                            text: &text,
                            status: "CONSUMED",
                            source_request_key: Some(key),
                            native_thread_id: Some(thread),
                            native_turn_id: Some(turn),
                            native_item_id: Some(item_id),
                            command_id: Some(key),
                            generation: t["generation"].as_str(),
                            segment_id: t["segmentId"].as_str(),
                            ..NewMessage::default()
                        },
                    )?;
                }
                Some(_) => {}
            }
        }
        // The ledger keeps its own guard: a proven APPLIED is never rewritten
        // by an earlier phase, and replaying the same evidence is idempotent.
        let v = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","detail":"Native user-message item confirms input consumption","consumptionEvidence":{"nativeItemId":item_id,"nativeTurnId":turn,"clientId":key}});
        tx.execute(
            "UPDATE requests SET phase='APPLIED',result=?3 WHERE scope=?1 AND key=?2 AND NOT (phase='APPLIED' AND 'APPLIED' IN ('ACCEPTED','DISPATCHING','REQUESTED'))",
            params![scope, key, v.to_string()],
        )?;
        Self::event_tx(&self.identity, &tx, &t, task, "input.consumed", &v)?;
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(Consumption::Bound)
    }
    // Bounded one-page read over the durable message index. Direction follows
    // the query: `cursor` reads strictly newer sequences, `before`/the default
    // read the newest page (collecting from the largest candidate backwards,
    // then re-ordering ascending). Rows are i64-ordered and never compared as
    // floats. The items array stays within 256 KiB of JSON; the whole
    // serialized response within 1 MiB or the read fails honestly.
    pub fn messages(
        &mut self,
        task: &str,
        cursor: Option<i64>,
        before: Option<i64>,
        limit: i64,
    ) -> Result<Value> {
        let tx = self.db.transaction()?;
        Self::tx_ensure_meta(&tx, task)?;
        let (next_sequence, revision, mode, legacy_text, legacy_truncated) = tx.query_row(
            "SELECT next_sequence,transcript_revision,history_mode,legacy_text,legacy_text_truncated FROM public_transcript_meta WHERE task_id=?1",
            [task],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            },
        )?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let t: Value = serde_json::from_str(&s)?;
        const COLS: &str = "message_id,sequence,revision,role,text,text_truncated,status,source_request_key,native_thread_id,native_turn_id,native_item_id,command_id,generation,segment_id,occurred_at,updated_at";
        let forward = cursor.is_some();
        let mut rows: Vec<Value> = vec![];
        {
            let mut q = if forward {
                tx.prepare(&format!(
                    "SELECT {COLS} FROM public_messages WHERE task_id=?1 AND sequence>?2 ORDER BY sequence ASC LIMIT ?3"
                ))?
            } else {
                tx.prepare(&format!(
                    "SELECT {COLS} FROM public_messages WHERE task_id=?1 AND sequence<?2 ORDER BY sequence DESC LIMIT ?3"
                ))?
            };
            let bound = if forward {
                cursor.unwrap_or(0)
            } else {
                before.unwrap_or(i64::MAX)
            };
            let fetched = q.query_map(params![task, bound, limit + 1], |r| {
                Self::message_row_json(r)
            })?;
            for row in fetched {
                rows.push(row?);
            }
        }
        let mut items: Vec<Value> = vec![];
        let mut bytes = 2usize; // the serialized [] frame
        for item in rows.iter() {
            if items.len() as i64 >= limit {
                break;
            }
            let need = serde_json::to_vec(item)?.len() + if items.is_empty() { 0 } else { 1 };
            if !items.is_empty() && bytes + need > MESSAGE_PAGE_MAX {
                break;
            }
            bytes += need;
            items.push(item.clone());
        }
        // hasMore reports whether the current direction still has candidates —
        // both the limit and the byte-budget stop keep it honest.
        let has_more = rows.len() > items.len();
        if !forward {
            items.reverse();
        }
        let seq_of =
            |v: Option<&Value>| v.map(|i| i["sequence"].as_str().unwrap_or("0").to_owned());
        let first = seq_of(items.first());
        let last = seq_of(items.last());
        // Forward pages continue from the last emitted sequence and an empty
        // forward page keeps the caller's cursor; backward/latest pages hand
        // back the smallest emitted sequence (null when the page is empty).
        let out_cursor = if forward {
            json!(last.or_else(|| cursor.map(|c| c.to_string())))
        } else {
            json!(first)
        };
        let legacy = if mode == HISTORY_LEGACY {
            json!({"text":legacy_text.unwrap_or_default(),"textTruncated":legacy_truncated != 0,"kind":"BOUNDED_ACCUMULATED_PUBLIC_OUTPUT"})
        } else {
            Value::Null
        };
        let v = json!({
            "messageSchemaVersion": "native-messages/1",
            "taskId": task,
            "storeId": self.identity["storeId"],
            "transcriptRevision": revision.to_string(),
            "items": items,
            "firstSequence": first,
            "cursor": out_cursor,
            "latestSequence": (next_sequence - 1).to_string(),
            "hasMore": has_more,
            "cursorExpired": false,
            "historyMode": mode,
            "legacy": legacy,
            "text": t["output"],
            "turns": t["turns"],
            "coverage": "BOUNDED_PUBLIC_MESSAGES"
        });
        if serde_json::to_vec(&v)?.len() > MESSAGE_RESPONSE_MAX {
            bail!("MESSAGE_PAGE_TOO_LARGE")
        }
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(v)
    }
    fn message_row_json(r: &rusqlite::Row) -> rusqlite::Result<Value> {
        let mut v = json!({
            "messageId": r.get::<_, String>(0)?,
            "sequence": r.get::<_, i64>(1)?.to_string(),
            "revision": r.get::<_, i64>(2)?.to_string(),
            "role": r.get::<_, String>(3)?,
            "text": r.get::<_, String>(4)?,
            "textTruncated": r.get::<_, i64>(5)? != 0,
            "status": r.get::<_, String>(6)?,
            "occurredAt": r.get::<_, String>(14)?,
            "updatedAt": r.get::<_, String>(15)?,
        });
        for (i, k) in [
            (7, "sourceRequestKey"),
            (8, "nativeThreadId"),
            (9, "nativeTurnId"),
            (10, "nativeItemId"),
            (11, "commandId"),
            (12, "generation"),
            (13, "segmentId"),
        ] {
            if let Some(s) = r.get::<_, Option<String>>(i)? {
                v[k] = json!(s);
            }
        }
        Ok(v)
    }
    pub fn lookup(&self, scope: &str, key: &str) -> Result<Value> {
        let s: Option<String> = self
            .db
            .query_row(
                "SELECT result FROM requests WHERE scope=?1 AND key=?2",
                params![scope, key],
                |r| r.get(0),
            )
            .optional()?;
        let mut value: Value = serde_json::from_str(
            &s.ok_or_else(|| anyhow::anyhow!("REQUEST_NOT_FOUND"))?,
        )?;
        // Consumption evidence may replace the phase projection. The durable
        // ledger still owns the task binding, including receipts written by
        // older builds; expose it on lookup rather than making clients guess.
        if scope == "create" && value.get("taskId").is_none() {
            let task: Option<String> = self.db.query_row(
                "SELECT id FROM tasks WHERE json_extract(json,'$.requestKey')=?1", [key], |r| r.get(0)).optional()?;
            if let (Some(task), Some(fields)) = (task, value.as_object_mut()) {
                fields.insert("taskId".into(), Value::String(task));
            }
        }
        Ok(value)
    }
    pub fn request_body(&self, scope: &str, key: &str) -> Result<Option<Value>> {
        let s: Option<String> = self
            .db
            .query_row(
                "SELECT body FROM requests WHERE scope=?1 AND key=?2",
                params![scope, key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(s.and_then(|b| serde_json::from_str(&b).ok()))
    }
    // FIFO order is a durable ordinal written at acceptance; queued intents are
    // admitted lowest-ordinal-first as slots free.
    pub fn queued(&self) -> Result<Vec<(String, i64)>> {
        let mut out = vec![];
        for t in self.tasks()? {
            if t["launchState"] == "QUEUED" {
                out.push((
                    t["taskId"].as_str().unwrap_or("").to_string(),
                    t["queueOrdinal"].as_u64().unwrap_or(0) as i64,
                ));
            }
        }
        out.sort_by_key(|(_, o)| *o);
        Ok(out)
    }
    // Slot occupancy: a queued intent waits for a slot, it does not hold one.
    // Terminal states hold none either: a crashed task whose owned writers died
    // with the host cannot block new work forever - its unproven remote effects
    // stay honestly UNKNOWN in the durable inventory and still gate reopen.
    pub fn active_count(&self, exclude: Option<&str>) -> Result<usize> {
        const TERMINAL: &[&str] = &[
            "FAILED",
            "REJECTED",
            "CANCELLED",
            "BINDING_UNAVAILABLE",
            "BUDGET_EXCEEDED",
            "UNKNOWN",
        ];
        Ok(self
            .tasks()?
            .iter()
            .filter(|t| {
                Some(t["taskId"].as_str().unwrap_or("")) != exclude
                    && t["quiescent"] != true
                    && t["launchState"] != "QUEUED"
                    && !TERMINAL.contains(&t["state"].as_str().unwrap_or(""))
            })
            .count())
    }
    // Atomic queued-intent admission: the task, its dispatching receipt and the
    // admission mark commit or fail together. A task that stopped being queued
    // (STOP won the window) is refused rather than launched.
    pub fn admit_queued(&mut self, task: &str) -> Result<Value> {
        let mut t = self.task(task)?;
        if t["launchState"] != "QUEUED" {
            bail!("QUEUE_ADMISSION_STALE")
        }
        let key = t["requestKey"].as_str().unwrap_or("").to_string();
        let ordinal = t["queueOrdinal"].clone();
        t["state"] = json!("PREPARING");
        t["launchState"] = json!("PREPARING");
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![task, t.to_string()],
        )?;
        tx.execute(
            "UPDATE requests SET phase='DISPATCHING',result=?3 WHERE scope='create' AND key=?2",
            params![task, key, t.to_string()],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"queue.admitted","type":"queue.admitted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"queueOrdinal":ordinal}).to_string()])?;
        tx.commit()?;
        Ok(t)
    }
    pub fn budget(&self, task: &str) -> Result<Option<Value>> {
        let row: Option<(
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            String,
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            String,
        )> = self
            .db
            .query_row(
                "SELECT max_total_tokens,max_input_tokens,max_output_tokens,max_cost_micro_usd,enforcement,exceeded,exceeded_at,exceeded_turn_id,exceed_usage_json,extensions,updated_at FROM budgets WHERE task=?1",
                [task],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                        r.get(10)?,
                    ))
                },
            )
            .optional()?;
        let Some((tt, it, ot, ct, enforcement, exceeded, at, turn, usage, extensions, updated)) = row
        else {
            return Ok(None);
        };
        let mut caps = serde_json::Map::new();
        if let Some(v) = tt {
            caps.insert("maxTotalTokens".into(), json!(v.to_string()));
        }
        if let Some(v) = it {
            caps.insert("maxInputTokens".into(), json!(v.to_string()));
        }
        if let Some(v) = ot {
            caps.insert("maxOutputTokens".into(), json!(v.to_string()));
        }
        if let Some(v) = ct {
            caps.insert("maxCostMicroUsd".into(), json!(v.to_string()));
        }
        Ok(Some(json!({
            "caps": caps,
            "enforcement": enforcement,
            "exceeded": exceeded != 0,
            "exceededAt": at,
            "exceededTurnId": turn,
            "exceededUsage": usage.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "extensions": extensions.to_string(),
            "updatedAt": updated,
            "costCoverage": if ct.is_some() { "UNOBSERVED" } else { "ABSENT" }
        })))
    }
    // Cumulative task-level usage: a thread total may reset across thread/resume
    // generations, so each observed snapshot contributes only its positive delta.
    const USAGE_DIMS: [&str; 5] = [
        "inputTokens",
        "cachedInputTokens",
        "outputTokens",
        "reasoningOutputTokens",
        "totalTokens",
    ];
    fn counter(u: &Value, dim: &str) -> u64 {
        u[dim]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| u[dim].as_u64())
            .unwrap_or(0)
    }
    fn budget_dims(caps: &[(Option<i64>, &str)], counters: &Value) -> Vec<String> {
        caps.iter()
            .filter(|(cap, dim)| cap.map_or(false, |c| Self::counter(counters, *dim) >= c as u64))
            .map(|(_, dim)| (*dim).to_string())
            .collect()
    }
    // Caps that are currently tripped by the observed counters; a soft budget
    // only knows what native usage notifications delivered.
    pub fn budget_check(&self, task: &str) -> Result<Vec<String>> {
        let Some(b) = self.budget(task)? else {
            return Ok(vec![]);
        };
        let t = self.task(task)?;
        let caps = &b["caps"];
        Ok(Self::budget_dims(
            &[
                (budget_cap(caps, "maxTotalTokens"), "totalTokens"),
                (budget_cap(caps, "maxInputTokens"), "inputTokens"),
                (budget_cap(caps, "maxOutputTokens"), "outputTokens"),
            ],
            &t["usageTotals"],
        ))
    }
    // Append a native token-usage observation, update cumulative counters and
    // atomically mark BUDGET_EXCEEDED + emit the durable fact when a cap trips.
    // Returns the exceed payload when this observation crossed a cap.
    pub fn usage_observed(
        &mut self,
        task: &str,
        thread_id: &str,
        turn_id: &str,
        totals: &Value,
    ) -> Result<Option<Value>> {
        let mut t = self.task(task)?;
        let prev: Vec<u64> = self
            .db
            .query_row(
                "SELECT input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens FROM usage_ledger WHERE task=?1 ORDER BY seq DESC LIMIT 1",
                [task],
                |r| Ok(vec![r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64, r.get::<_, i64>(3)? as u64, r.get::<_, i64>(4)? as u64]),
            )
            .optional()?
            .unwrap_or_else(|| vec![0; 5]);
        let mut counters = [0u64; 5];
        for (i, d) in Self::USAGE_DIMS.iter().enumerate() {
            counters[i] = Self::counter(&t["usageTotals"], d);
        }
        let cur = [
            Self::counter(totals, "inputTokens"),
            Self::counter(totals, "cachedInputTokens"),
            Self::counter(totals, "outputTokens"),
            Self::counter(totals, "reasoningOutputTokens"),
            Self::counter(totals, "totalTokens"),
        ];
        let mut usage = serde_json::Map::new();
        let mut totals_map = serde_json::Map::new();
        for (i, d) in Self::USAGE_DIMS.iter().enumerate() {
            counters[i] += cur[i].saturating_sub(prev[i]);
            usage.insert(d.to_string(), json!(cur[i].to_string()));
            totals_map.insert(d.to_string(), json!(counters[i].to_string()));
        }
        t["usage"] = Value::from(usage);
        t["usageTotals"] = Value::from(totals_map);
        let b = self.budget(task)?;
        let dims = match &b {
            Some(b) if b["exceeded"] == false => Self::budget_dims(
                &[
                    (budget_cap(&b["caps"], "maxTotalTokens"), "totalTokens"),
                    (budget_cap(&b["caps"], "maxInputTokens"), "inputTokens"),
                    (budget_cap(&b["caps"], "maxOutputTokens"), "outputTokens"),
                ],
                &t["usageTotals"],
            ),
            _ => vec![],
        };
        let hit = !dims.is_empty();
        if hit
            && !matches!(
                t["state"].as_str(),
                Some("SUCCEEDED" | "FAILED" | "CANCELLED" | "REJECTED" | "BINDING_UNAVAILABLE" | "BUDGET_EXCEEDED" | "STOP_REQUESTED")
            )
        {
            t["state"] = json!("BUDGET_EXCEEDED");
            t["reasonCode"] = json!("BUDGET_EXCEEDED");
        }
        let tx = self.db.transaction()?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM usage_ledger WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO usage_ledger VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![task, seq, thread_id, turn_id, cur[0] as i64, cur[1] as i64, cur[2] as i64, cur[3] as i64, cur[4] as i64, now()],
        )?;
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![task, t.to_string()],
        )?;
        let mut exceeded_payload = None;
        if hit {
            let at = now();
            let caps = b.as_ref().map(|b| b["caps"].clone()).unwrap_or(json!({}));
            exceeded_payload = Some(json!({
                "enforcement": "SOFT",
                "caps": caps,
                "usage": t["usageTotals"],
                "exceeded": dims,
                "nativeTurnId": turn_id,
                "costCoverage": "UNOBSERVED_IF_CAPPED"
            }));
            tx.execute(
                "UPDATE budgets SET exceeded=1,exceeded_at=?2,exceeded_turn_id=?3,exceed_usage_json=?4,updated_at=?2 WHERE task=?1 AND exceeded=0",
                params![task, at, turn_id, t["usageTotals"].to_string()],
            )?;
            let eseq: i64 = tx.query_row(
                "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
                [task],
                |r| r.get(0),
            )?;
            tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,eseq,json!({"cursor":eseq.to_string(),"sequence":eseq.to_string(),"seq":eseq.to_string(),"kind":"budget.exceeded","type":"budget.exceeded","occurredAt":at,"observedAt":at,"taskId":task,"payload":exceeded_payload}).to_string()])?;
        }
        tx.commit()?;
        Ok(exceeded_payload)
    }
    // Idempotent durable exceed mark for boundary checks (before turn/start and at
    // turn/completed); a no-op when the budget row is already exceeded.
    pub fn budget_mark_exceeded(&mut self, task: &str, turn_id: &str) -> Result<bool> {
        let Some(b) = self.budget(task)? else {
            return Ok(false);
        };
        if b["exceeded"] == true {
            return Ok(false);
        }
        let t = self.task(task)?;
        let dims = self.budget_check(task)?;
        if dims.is_empty() {
            return Ok(false);
        }
        let mut t = t;
        if !matches!(
            t["state"].as_str(),
            Some("SUCCEEDED" | "FAILED" | "CANCELLED" | "REJECTED" | "BINDING_UNAVAILABLE" | "BUDGET_EXCEEDED" | "STOP_REQUESTED")
        ) {
            t["state"] = json!("BUDGET_EXCEEDED");
            t["reasonCode"] = json!("BUDGET_EXCEEDED");
        }
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![task, t.to_string()],
        )?;
        let at = now();
        tx.execute(
            "UPDATE budgets SET exceeded=1,exceeded_at=?2,exceeded_turn_id=?3,exceed_usage_json=?4,updated_at=?2 WHERE task=?1 AND exceeded=0",
            params![task, at, turn_id, t["usageTotals"].to_string()],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let payload = json!({"enforcement":"SOFT","caps":b["caps"],"usage":t["usageTotals"],"exceeded":dims,"nativeTurnId":turn_id,"costCoverage":"UNOBSERVED_IF_CAPPED","boundary":true});
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"budget.exceeded","type":"budget.exceeded","occurredAt":at,"observedAt":at,"taskId":task,"payload":payload}).to_string()])?;
        tx.commit()?;
        Ok(true)
    }
    // W5a — context packages (P11/N16). Frozen source bytes live in
    // context_package_sources so post-creation workspace writes can never
    // mutate a package; the package digest recomputes over the recorded
    // per-source sha256 list and stays re-verifiable after restart.
    pub fn accept_context_package(
        &mut self,
        task: &str,
        key: &str,
        body: &Value,
        package: &Value,
        sources: &[Value],
        source_bytes: &[Vec<u8>],
        result: &Value,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'APPLIED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        tx.execute(
            "INSERT INTO context_packages VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                task,
                package["packageId"].as_str(),
                package["name"].as_str(),
                package["digest"].as_str(),
                package["size"].as_i64().or_else(|| package["size"].as_str().and_then(|s| s.parse::<i64>().ok())),
                package["createdAt"].as_str()
            ],
        )?;
        for (i, (meta, bytes)) in sources.iter().zip(source_bytes.iter()).enumerate() {
            tx.execute(
                "INSERT INTO context_package_sources VALUES(?1,?2,?3,?4,?5)",
                params![task, package["packageId"].as_str(), i as i64, meta.to_string(), bytes],
            )?;
        }
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq+1,json!({"cursor":(seq+1).to_string(),"sequence":(seq+1).to_string(),"seq":(seq+1).to_string(),"kind":"context-package.created","type":"context-package.created","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"payload":{"packageId":package["packageId"],"name":package["name"],"digest":package["digest"],"size":package["size"],"sourceCount":sources.len()}}).to_string()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn context_packages(&self, task: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(
            "SELECT id,name,digest,size,created FROM context_packages WHERE task=?1 ORDER BY created,rowid",
        )?;
        let rows = q.query_map([task], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        let mut out = vec![];
        for row in rows {
            let (id, name, digest, size, created) = row?;
            let metas = self.context_package_metas(task, &id)?;
            let file_count = metas
                .iter()
                .filter(|m| m["kind"].as_str() == Some("file"))
                .count();
            out.push(json!({
                "packageId": id,
                "name": name,
                "digest": digest,
                "size": size,
                "sourceCount": metas.len(),
                "fileCount": file_count,
                "sources": metas,
                "createdAt": created,
            }));
        }
        Ok(out)
    }
    fn context_package_metas(&self, task: &str, package_id: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(
            "SELECT json FROM context_package_sources WHERE task=?1 AND package_id=?2 ORDER BY ordinal",
        )?;
        let rows = q.query_map(params![task, package_id], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    // Package detail with frozen source bytes; the recorded sha256 of every
    // source is re-verified on read so a corrupted blob cannot masquerade.
    pub fn context_package(&self, task: &str, package_id: &str) -> Result<Value> {
        let head: Option<(String, Option<String>, String, i64, String)> = self
            .db
            .query_row(
                "SELECT id,name,digest,size,created FROM context_packages WHERE task=?1 AND id=?2",
                params![task, package_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let Some((id, name, digest, size, created)) = head else {
            bail!("PACKAGE_NOT_FOUND")
        };
        let mut q = self.db.prepare(
            "SELECT json,bytes FROM context_package_sources WHERE task=?1 AND package_id=?2 ORDER BY ordinal",
        )?;
        let rows = q.query_map(params![task, package_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        let mut sources = vec![];
        let mut metas = vec![];
        let mut file_count = 0usize;
        for row in rows {
            let (meta, bytes) = row?;
            let mut meta: Value = serde_json::from_str(&meta)?;
            let actual = hex::encode(Sha256::digest(&bytes));
            if meta["sha256"].as_str() != Some(actual.as_str()) {
                bail!("PACKAGE_SOURCE_CHANGED")
            }
            metas.push(meta.clone());
            if meta["kind"].as_str() == Some("file") {
                file_count += 1;
                meta["content"] = json!(STANDARD.encode(&bytes));
            } else {
                meta["text"] = json!(String::from_utf8_lossy(&bytes).into_owned());
            }
            sources.push(meta);
        }
        // Recompute the canonical digest over the persisted metadata — the same
        // recipe as creation — so the detail reports whether the durable bytes
        // and identity still line up (digestVerified + coverage mirror the Pi
        // host's packageMeta projection).
        let digest_verified = crate::host::hash(&String::from_utf8(canonical(&json!({"sources": metas}))?)?) == digest;
        Ok(json!({"packageId":id,"name":name,"digest":digest,"digestVerified":digest_verified,"size":size,"sourceCount":sources.len(),"fileCount":file_count,"coverage":"FROZEN_AT_CREATION","sources":sources,"createdAt":created}))
    }
    pub fn context_package_count(&self, task: &str) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM context_packages WHERE task=?1",
            [task],
            |r| r.get(0),
        )?)
    }
    // W5a — auxiliary sessions (P12/N17). Durable task-scoped rows; the live
    // peer and isolated workspace copy are never durable claims, only the row is.
    // Public aux projection mirrors the Pi host's auxPublic shape; codex adds
    // the owned snapshot `workspace` for inspection and keeps `requestKey`.
    fn aux_row(row: &rusqlite::Row) -> rusqlite::Result<Value> {
        Ok(json!({
            "auxSessionId": row.get::<_, String>(0)?,
            "state": row.get::<_, String>(1)?,
            "question": row.get::<_, String>(2)?,
            "answer": row.get::<_, String>(3)?,
            "requestKey": row.get::<_, String>(4)?,
            "nativeSessionId": row.get::<_, Option<String>>(5)?,
            "nativeTurnId": row.get::<_, Option<String>>(6)?,
            "budget": row.get::<_, Option<i64>>(7)?.map(|b| json!({"maxTotalTokens":b.to_string(),"enforcement":"SOFT","basis":"ENGINE_REPORTED_POST_HOC","costCoverage":"UNKNOWN"})),
            "usage": row.get::<_, Option<String>>(8)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "workspace": row.get::<_, Option<String>>(9)?,
            "deliverRequestKey": row.get::<_, Option<String>>(10)?,
            "deliveredAt": row.get::<_, Option<String>>(11)?,
            "reasonCode": row.get::<_, Option<String>>(12)?,
            "createdAt": row.get::<_, String>(13)?,
            "finishedAt": row.get::<_, Option<String>>(14)?,
            "contextPackageIds": row.get::<_, Option<String>>(15)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()).filter(|v| v.is_array()).unwrap_or(json!([])),
            "deadlineEpochMillis": row.get::<_, Option<String>>(16)?,
        }))
    }
    const AUX_COLS: &str = "id,state,question,answer,request_key,native_thread_id,native_turn_id,budget_max_total,usage_json,workspace,deliver_key,delivered_at,stop_reason,created,finished,packages_json,deadline";
    pub fn aux_session(&self, task: &str, id: &str) -> Result<Value> {
        self.db
            .query_row(
                &format!("SELECT {} FROM aux_sessions WHERE task=?1 AND id=?2", Self::AUX_COLS),
                params![task, id],
                |r| Self::aux_row(r),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("AUX_SESSION_NOT_FOUND"))
    }
    pub fn aux_sessions(&self, task: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(&format!(
            "SELECT {} FROM aux_sessions WHERE task=?1 ORDER BY created,rowid",
            Self::AUX_COLS
        ))?;
        let rows = q.query_map([task], |r| Self::aux_row(r))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    pub fn aux_count(&self, task: &str) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM aux_sessions WHERE task=?1",
            [task],
            |r| r.get(0),
        )?)
    }
    // Durable acceptance of an auxiliary-session intent: request row + session
    // row + intake event commit or fail together.
    pub fn accept_aux(
        &mut self,
        task: &str,
        aux: &Value,
        key: &str,
        body: &Value,
        result: &Value,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        tx.execute(
            "INSERT INTO aux_sessions VALUES(?1,?2,?3,'','RUNNING',?4,NULL,NULL,?5,'{}',?6,NULL,NULL,NULL,?7,NULL,?8,?9)",
            params![
                task,
                aux["auxSessionId"].as_str(),
                aux["question"].as_str(),
                key,
                aux["budget"]["caps"]["maxTotalTokens"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| aux["budget"]["caps"]["maxTotalTokens"].as_i64())
                    .or_else(|| aux["budget"]["maxTotalTokens"].as_str().and_then(|s| s.parse().ok())),
                aux["workspace"].as_str(),
                aux["createdAt"].as_str(),
                aux["contextPackageIds"].to_string(),
                aux["deadlineEpochMillis"].as_str()
            ],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        tx.commit()?;
        Ok(())
    }
    // Cumulative native usage for an aux session. A usage report can outlive
    // its own turn's completion notification on the wire, so the counters are
    // recorded even onto terminal rows — no state or settlement timestamp
    // changes here, just the accounting fact.
    pub fn aux_record_usage(&self, task: &str, id: &str, usage: &Value) -> Result<bool> {
        Ok(self.db.execute(
            "UPDATE aux_sessions SET usage_json=?3 WHERE task=?1 AND id=?2",
            params![task, id, usage.to_string()],
        )? > 0)
    }
    // Guarded aux state update: only rows still in one of `from` states change,
    // so a terminal fact (SUCCEEDED/FAILED/CANCELLED/BUDGET_EXCEEDED) is never
    // overwritten by a racing native notification or a late dispatch step.
    pub fn aux_transition(
        &self,
        task: &str,
        id: &str,
        from: &[&str],
        state: &str,
        reason: Option<&str>,
        change: impl FnOnce(&mut Value),
    ) -> Result<bool> {
        let mut row = self.aux_session(task, id)?;
        if !from.contains(&row["state"].as_str().unwrap_or("")) {
            return Ok(false);
        }
        row["state"] = json!(state);
        if let Some(r) = reason {
            row["reasonCode"] = json!(r);
        }
        change(&mut row);
        if matches!(state, "SUCCEEDED" | "FAILED" | "CANCELLED" | "BUDGET_EXCEEDED") {
            row["finishedAt"] = json!(now());
        }
        let usage = if row["usage"].is_null() {
            None
        } else {
            Some(row["usage"].to_string())
        };
        let states: Vec<String> = from.iter().map(|s| format!("'{}'", s)).collect();
        let changed = self.db.execute(
            &format!(
                "UPDATE aux_sessions SET state=?3,answer=?4,native_thread_id=?5,native_turn_id=?6,usage_json=?7,deliver_key=?8,delivered_at=?9,stop_reason=?10,finished=?11 WHERE task=?1 AND id=?2 AND state IN ({})",
                states.join(",")
            ),
            params![
                task,
                id,
                row["state"].as_str(),
                row["answer"].as_str().unwrap_or(""),
                row["nativeSessionId"].as_str(),
                row["nativeTurnId"].as_str(),
                usage,
                row["deliverRequestKey"].as_str(),
                row["deliveredAt"].as_str(),
                row["reasonCode"].as_str(),
                row["finishedAt"].as_str()
            ],
        )?;
        Ok(changed > 0)
    }
    // Host-wide open aux count — the NWF_CODEX_MAX_ACTIVE_AUX bound (Pi's
    // maxAux / AUX_SESSION_BUSY intake refusal).
    pub fn aux_active_count(&self) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM aux_sessions WHERE state='RUNNING'",
            [],
            |r| r.get(0),
        )?)
    }
    // W5a — auxiliary delivery (N17): the injected follow-up request, the task
    // admission change and the aux row's deliveredAt mark commit in one
    // transaction; the guarded UPDATE makes double-delivery impossible even
    // under a racing second request. The row keeps its SUCCEEDED state — the
    // recorded answer stays readable afterwards (Pi parity).
    pub fn admit_aux_deliver(
        &mut self,
        task: &str,
        aux_id: &str,
        key: &str,
        body: &Value,
        result: &Value,
        change: impl FnOnce(&mut Value),
    ) -> Result<Value> {
        let mut t = self.task(task)?;
        change(&mut t);
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![t["taskId"].as_str(), t.to_string()],
        )?;
        let marked = tx.execute(
            "UPDATE aux_sessions SET delivered_at=?3,deliver_key=?4 WHERE task=?1 AND id=?2 AND state='SUCCEEDED' AND delivered_at IS NULL",
            params![task, aux_id, now(), key],
        )?;
        if marked == 0 {
            // Precise refusal: a re-read inside the rolled-back transaction still
            // sees the durable row, so the code names the real blocker.
            let (state, delivered): (String, Option<String>) = tx.query_row(
                "SELECT state,delivered_at FROM aux_sessions WHERE task=?1 AND id=?2",
                params![task, aux_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if delivered.is_some() {
                bail!("AUX_ALREADY_DELIVERED")
            }
            let _ = state;
            bail!("AUX_NOT_FINISHED")
        }
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"admission":"AUX_DELIVERY"}).to_string()])?;
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq+1,json!({"cursor":(seq+1).to_string(),"sequence":(seq+1).to_string(),"seq":(seq+1).to_string(),"kind":"aux-session.delivered","type":"aux-session.delivered","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"payload":{"auxSessionId":aux_id,"requestKey":key,"targetCommandId":key}}).to_string()])?;
        tx.commit()?;
        Ok(t)
    }
    // W5a — assignment transitions (P13/N18): the new owner epoch/segment
    // admission, its durable transition row, the request and both facts commit
    // atomically. The old epoch is fenced afterwards by the ordinary
    // generation/session/turn/command guards.
    pub fn admit_transition(
        &mut self,
        task: &str,
        key: &str,
        body: &Value,
        result: &Value,
        row: &Value,
        change: impl FnOnce(&mut Value),
    ) -> Result<Value> {
        let mut t = self.task(task)?;
        change(&mut t);
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![t["taskId"].as_str(), t.to_string()],
        )?;
        tx.execute(
            "INSERT INTO transitions VALUES(?1,?2,?3,?4,?5,?6,?7,'ADMITTED',NULL,?8)",
            params![
                task,
                row["transitionId"].as_str(),
                row["fromEpoch"].as_str(),
                row["toEpoch"].as_str(),
                row["fromSegment"].as_str(),
                row["toSegment"].as_str(),
                Some(row["compatibility"].to_string()),
                now()
            ],
        )?;
        // Bounded history: keep the newest 32 durable records per task.
        tx.execute(
            "DELETE FROM transitions WHERE task=?1 AND id NOT IN (SELECT id FROM transitions WHERE task=?1 ORDER BY created DESC,rowid DESC LIMIT 32)",
            [task],
        )?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"admission":"TRANSITION","transitionId":row["transitionId"]}).to_string()])?;
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq+1,json!({"cursor":(seq+1).to_string(),"sequence":(seq+1).to_string(),"seq":(seq+1).to_string(),"kind":"transition.admitted","type":"transition.admitted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"payload":{"transitionId":row["transitionId"],"fromEpoch":row["fromEpoch"],"toEpoch":row["toEpoch"],"fromSegment":row["fromSegment"],"toSegment":row["toSegment"],"compatibility":row["compatibility"],"via":"SESSION_REOPEN","method":"THREAD_RESUME"}}).to_string()])?;
        tx.commit()?;
        Ok(t)
    }
    pub fn transitions(&self, task: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(
            "SELECT id,from_epoch,to_epoch,from_segment,to_segment,compatibility,state,reason,created FROM transitions WHERE task=?1 ORDER BY created,rowid",
        )?;
        let rows = q.query_map([task], |r| {
            Ok(json!({
                "transitionId": r.get::<_, String>(0)?,
                "fromEpoch": r.get::<_, String>(1)?,
                "toEpoch": r.get::<_, String>(2)?,
                "fromSegment": r.get::<_, String>(3)?,
                "toSegment": r.get::<_, String>(4)?,
                "compatibility": r.get::<_, String>(5).ok().and_then(|s| serde_json::from_str::<Value>(&s).ok()).unwrap_or(Value::Null),
                "state": r.get::<_, String>(6)?,
                "reason": r.get::<_, Option<String>>(7)?,
                "createdAt": r.get::<_, String>(8)?,
            }))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    // W5b — writable collaboration DAGs (P14) and guarded adoptions (P15).
    // Durable rows are the collaboration fact; the live child peer and the
    // isolated checkout are never durable claims. Child ids are unique inside
    // a collaboration; goal bodies stay off event/audit projections (hash only).
    const COLLAB_COLS: &str = "id,name,request_key,state,reason,result_json,created,finished";
    fn collab_row(row: &rusqlite::Row) -> rusqlite::Result<Value> {
        Ok(json!({
            "collaborationId": row.get::<_, String>(0)?,
            "name": row.get::<_, Option<String>>(1)?,
            "requestKey": row.get::<_, String>(2)?,
            "state": row.get::<_, String>(3)?,
            "reasonCode": row.get::<_, Option<String>>(4)?,
            "result": row.get::<_, Option<String>>(5)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "createdAt": row.get::<_, String>(6)?,
            "finishedAt": row.get::<_, Option<String>>(7)?,
        }))
    }
    const COLLAB_CHILD_COLS: &str = "id,goal,goal_sha256,depends_on,packages_json,write_scope_json,budget_json,state,reason,failed_deps,workspace,baseline_json,candidate_json,adoptions_json,native_thread_id,native_turn_id,usage_json,tool_calls,output,stop_proof,created,started,finished,deadline";
    fn collab_child_row(row: &rusqlite::Row) -> rusqlite::Result<Value> {
        Ok(json!({
            "childId": row.get::<_, String>(0)?,
            "goal": row.get::<_, String>(1)?,
            "goalSha256": row.get::<_, String>(2)?,
            "dependsOn": serde_json::from_str::<Value>(&row.get::<_, String>(3)?).unwrap_or(json!([])),
            "contextPackageIds": serde_json::from_str::<Value>(&row.get::<_, String>(4)?).unwrap_or(json!([])),
            "writeScope": row.get::<_, Option<String>>(5)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "budget": row.get::<_, Option<String>>(6)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "state": row.get::<_, String>(7)?,
            "reasonCode": row.get::<_, Option<String>>(8)?,
            "failedDependencies": row.get::<_, Option<String>>(9)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "workspace": row.get::<_, Option<String>>(10)?,
            "baseline": row.get::<_, Option<String>>(11)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "candidate": row.get::<_, Option<String>>(12)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "adoptions": serde_json::from_str::<Value>(&row.get::<_, String>(13)?).unwrap_or(json!([])),
            "nativeSessionId": row.get::<_, Option<String>>(14)?,
            "nativeTurnId": row.get::<_, Option<String>>(15)?,
            "usage": row.get::<_, Option<String>>(16)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "toolCalls": row.get::<_, i64>(17)?.to_string(),
            "output": row.get::<_, String>(18)?,
            "stopProof": row.get::<_, Option<String>>(19)?.and_then(|s| serde_json::from_str::<Value>(&s).ok()),
            "createdAt": row.get::<_, String>(20)?,
            "startedAt": row.get::<_, Option<String>>(21)?,
            "finishedAt": row.get::<_, Option<String>>(22)?,
            "deadlineEpochMillis": row.get::<_, Option<String>>(23)?,
        }))
    }
    pub fn collab_children(&self, task: &str, collab_id: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(&format!(
            "SELECT {} FROM collab_children WHERE task=?1 AND collab_id=?2 ORDER BY rowid",
            Self::COLLAB_CHILD_COLS
        ))?;
        let rows = q.query_map(params![task, collab_id], |r| Self::collab_child_row(r))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    pub fn collab_child(&self, task: &str, collab_id: &str, id: &str) -> Result<Value> {
        self.db
            .query_row(
                &format!(
                    "SELECT {} FROM collab_children WHERE task=?1 AND collab_id=?2 AND id=?3",
                    Self::COLLAB_CHILD_COLS
                ),
                params![task, collab_id, id],
                |r| Self::collab_child_row(r),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("COLLABORATION_CHILD_NOT_FOUND"))
    }
    // Collaboration projection with the full ordered child list (Pi's
    // collabPublic + codex's owned checkout `workspace` on each child).
    pub fn collaboration(&self, task: &str, id: &str) -> Result<Value> {
        let mut c = self
            .db
            .query_row(
                &format!(
                    "SELECT {} FROM collaborations WHERE task=?1 AND id=?2",
                    Self::COLLAB_COLS
                ),
                params![task, id],
                |r| Self::collab_row(r),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("COLLABORATION_NOT_FOUND"))?;
        c["children"] = json!(self.collab_children(task, id)?);
        c["taskId"] = json!(task);
        Ok(c)
    }
    pub fn collaborations(&self, task: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(&format!(
            "SELECT {} FROM collaborations WHERE task=?1 ORDER BY created,rowid",
            Self::COLLAB_COLS
        ))?;
        let rows = q.query_map([task], |r| Self::collab_row(r))?;
        let mut out = vec![];
        for row in rows {
            let mut c = row?;
            let cid = c["collaborationId"].as_str().unwrap_or("").to_string();
            c["children"] = json!(self.collab_children(task, &cid)?);
            c["taskId"] = json!(task);
            out.push(c);
        }
        Ok(out)
    }
    // Active (RUNNING) collaboration count on one task — the per-task intake
    // bound (COLLABORATION_LIMIT, Pi maxPerTask).
    pub fn collab_running_on_task(&self, task: &str) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM collaborations WHERE task=?1 AND state='RUNNING'",
            [task],
            |r| r.get(0),
        )?)
    }
    // Durable acceptance of a collaboration intent: request row + collaboration
    // row + all child rows + intake/created events commit or fail together.
    pub fn accept_collaboration(
        &mut self,
        task: &str,
        collab: &Value,
        children: &[Value],
        key: &str,
        body: &Value,
        result: &Value,
        created_event: &Value,
    ) -> Result<()> {
        let t = self.task(task)?;
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        tx.execute(
            "INSERT INTO collaborations VALUES(?1,?2,?3,?4,'RUNNING',NULL,NULL,?5,NULL)",
            params![
                task,
                collab["collaborationId"].as_str(),
                collab["name"].as_str(),
                key,
                collab["createdAt"].as_str()
            ],
        )?;
        for ch in children {
            tx.execute(
                "INSERT INTO collab_children VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'PENDING',NULL,NULL,?10,NULL,NULL,'[]',NULL,NULL,NULL,0,'',NULL,?11,NULL,NULL,?12)",
                params![
                    task,
                    collab["collaborationId"].as_str(),
                    ch["childId"].as_str(),
                    ch["goal"].as_str(),
                    ch["goalSha256"].as_str(),
                    ch["dependsOn"].to_string(),
                    ch["contextPackageIds"].to_string(),
                    ch["writeScope"].as_object().map(|_| ch["writeScope"].to_string()),
                    ch["budget"].as_object().map(|_| ch["budget"].to_string()),
                    ch["workspace"].as_str(),
                    ch["createdAt"].as_str(),
                    ch["deadlineEpochMillis"].as_str()
                ],
            )?;
        }
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        Self::event_tx(&self.identity, &tx, &t, task, "collaboration.created", created_event)?;
        tx.commit()?;
        Ok(())
    }
    // Guarded child transition: only rows still in one of `from` states change.
    // The change closure mutates the public projection (usage, nativeSessionId,
    // candidate, adoptions, stopProof, output, toolCalls, failedDependencies,
    // workspace, baseline); all mutable columns write back in one UPDATE.
    pub fn collab_child_transition(
        &self,
        task: &str,
        collab_id: &str,
        child_id: &str,
        from: &[&str],
        state: &str,
        reason: Option<&str>,
        change: impl FnOnce(&mut Value),
    ) -> Result<bool> {
        let mut row = self.collab_child(task, collab_id, child_id)?;
        if !from.contains(&row["state"].as_str().unwrap_or("")) {
            return Ok(false);
        }
        row["state"] = json!(state);
        if let Some(r) = reason {
            row["reasonCode"] = json!(r);
        }
        if state == "RUNNING" && row["startedAt"].is_null() {
            row["startedAt"] = json!(now());
        }
        change(&mut row);
        if matches!(state, "SUCCEEDED" | "FAILED" | "CANCELLED" | "BLOCKED") {
            row["finishedAt"] = json!(now());
        }
        let states: Vec<String> = from.iter().map(|s| format!("'{}'", s)).collect();
        let changed = self.db.execute(
            &format!(
                "UPDATE collab_children SET state=?4,reason=?5,failed_deps=?6,baseline_json=?7,candidate_json=?8,adoptions_json=?9,native_thread_id=?10,native_turn_id=?11,usage_json=?12,tool_calls=?13,output=?14,stop_proof=?15,started=?16,finished=?17,workspace=?18 WHERE task=?1 AND collab_id=?2 AND id=?3 AND state IN ({})",
                states.join(",")
            ),
            params![
                task,
                collab_id,
                child_id,
                row["state"].as_str(),
                row["reasonCode"].as_str(),
                row["failedDependencies"].as_array().map(|_| row["failedDependencies"].to_string()),
                row["baseline"].as_object().map(|_| row["baseline"].to_string()),
                row["candidate"].as_object().map(|_| row["candidate"].to_string()),
                row["adoptions"].to_string(),
                row["nativeSessionId"].as_str(),
                row["nativeTurnId"].as_str(),
                row["usage"].as_object().map(|_| row["usage"].to_string()),
                row["toolCalls"].as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0),
                row["output"].as_str().unwrap_or(""),
                row["stopProof"].as_object().map(|_| row["stopProof"].to_string()),
                row["startedAt"].as_str(),
                row["finishedAt"].as_str(),
                row["workspace"].as_str()
            ],
        )?;
        Ok(changed > 0)
    }
    pub fn collab_record_usage(&self, task: &str, collab_id: &str, child_id: &str, usage: &Value) -> Result<bool> {
        Ok(self.db.execute(
            "UPDATE collab_children SET usage_json=?4 WHERE task=?1 AND collab_id=?2 AND id=?3",
            params![task, collab_id, child_id, usage.to_string()],
        )? > 0)
    }
    // One event row appended inside an open transaction; shares the durable
    // envelope with `event()` (kind/type identity except native-turn aliasing).
    fn event_tx(
        identity: &Value,
        tx: &rusqlite::Transaction,
        t: &Value,
        task: &str,
        kind: &str,
        payload: &Value,
    ) -> Result<i64> {
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        // Keep the same kind projection as event(): readers key on the mapped
        // value (ASSISTANT_MESSAGE etc.), not the raw type.
        let local_kind = match kind {
            "turn.started" => "turn/started",
            "turn.completed" => "turn/completed",
            "message.completed" => "ASSISTANT_MESSAGE",
            _ => kind,
        };
        let at = now();
        let ev = json!({"eventId":format!("event-{}",id()),"sequence":seq.to_string(),"seq":seq.to_string(),"cursor":seq.to_string(),"kind":local_kind,"occurredAt":at,"source":identity,"taskId":task,"runId":t["runId"],"segmentId":t["segmentId"],"nativeTurnId":t["nativeTurnId"],"type":kind,"observedAt":at,"payload":payload});
        tx.execute(
            "INSERT INTO events VALUES(?1,?2,?3)",
            params![task, seq, ev.to_string()],
        )?;
        Ok(seq)
    }
    // Atomic DAG pump (Pi pumpCollab): inside one transaction, PENDING children
    // with failed/cancelled/blocked dependencies become BLOCKED, PENDING
    // children whose dependencies all SUCCEEDED become RUNNING up to the
    // host-wide pool bound, and an all-terminal collaboration seals COMPLETED.
    // Returns the child ids to launch and the completion result to receipt.
    pub fn collab_pump(
        &mut self,
        task: &str,
        collab_id: &str,
        max_children: usize,
    ) -> Result<(Vec<String>, Option<(String, Value)>)> {
        let t = self.task(task)?;
        let tx = self.db.transaction()?;
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT state,request_key FROM collaborations WHERE task=?1 AND id=?2",
                params![task, collab_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let mut launch: Vec<String> = vec![];
        let mut completed: Option<(String, Value)> = None;
        if let Some((state, request_key)) = row {
            if state == "RUNNING" {
                let mut q = tx.prepare(
                    "SELECT id,state,depends_on FROM collab_children WHERE task=?1 AND collab_id=?2 ORDER BY rowid",
                )?;
                let mut children: Vec<(String, String, Vec<String>)> = q
                    .query_map(params![task, collab_id], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            serde_json::from_str::<Vec<String>>(&r.get::<_, String>(2)?)
                                .unwrap_or_default(),
                        ))
                    })?
                    .collect::<std::result::Result<_, _>>()?;
                drop(q);
                let state_of = |children: &[(String, String, Vec<String>)], id: &str| {
                    children
                        .iter()
                        .find(|c| c.0 == id)
                        .map(|c| c.1.clone())
                        .unwrap_or_else(|| "MISSING".to_string())
                };
                let at = now();
                // Failed dependencies settle the dependent honestly: BLOCKED, never
                // launched, with the durable reason and the failed dep list.
                let pending: Vec<String> = children
                    .iter()
                    .filter(|c| c.1 == "PENDING")
                    .map(|c| c.0.clone())
                    .collect();
                // Dependency order is not guaranteed to match durable row order.
                // Propagate BLOCKED to a fixed point before deciding completion.
                for _ in 0..children.len() {
                    let mut changed = false;
                for ch in pending.iter().cloned() {
                    if state_of(&children, &ch) != "PENDING" { continue; }
                    let deps = children
                        .iter()
                        .find(|c| c.0 == ch)
                        .map(|c| c.2.clone())
                        .unwrap_or_default();
                    let failed: Vec<String> = deps
                        .iter()
                        .filter(|d| {
                            matches!(
                                state_of(&children, d).as_str(),
                                "FAILED" | "CANCELLED" | "BLOCKED"
                            )
                        })
                        .cloned()
                        .collect();
                    if !failed.is_empty() {
                        changed = true;
                        tx.execute(
                            "UPDATE collab_children SET state='BLOCKED',reason='DEPENDENCY_UNSATISFIED',failed_deps=?4,finished=?5 WHERE task=?1 AND collab_id=?2 AND id=?3 AND state='PENDING'",
                            params![task, collab_id, ch, json!(failed).to_string(), at],
                        )?;
                        if let Some(c) = children.iter_mut().find(|c| c.0 == ch) {
                            c.1 = "BLOCKED".into();
                        }
                        Self::tx_child_message(
                            &tx,
                            task,
                            &collab_stream_id(collab_id, &ch),
                            "COLLAB",
                            Some(collab_id),
                            &ch,
                            &ChildMsg {
                                kind: "STATUS",
                                text: "BLOCKED DEPENDENCY_UNSATISFIED",
                                status: "BLOCKED",
                                event_ref: None,
                            },
                        )?;
                        Self::event_tx(&self.identity, 
                            &tx,
                            &t,
                            task,
                            "collaboration.child.blocked",
                            &json!({"collaborationId":collab_id,"childId":ch,"reasonCode":"DEPENDENCY_UNSATISFIED","failedDependencies":failed}),
                        )?;
                    }
                }
                    if !changed { break; }
                }
                // Launch-ready dependents fill the shared pool up to its bound.
                let mut running: i64 = tx.query_row(
                    "SELECT count(*) FROM collab_children WHERE state='RUNNING'",
                    [],
                    |r| r.get(0),
                )?;
                for ch in pending {
                    if running >= max_children as i64 {
                        break;
                    }
                    let deps = children
                        .iter()
                        .find(|c| c.0 == ch)
                        .map(|c| c.2.clone())
                        .unwrap_or_default();
                    let ready = deps.is_empty()
                        || deps.iter().all(|d| state_of(&children, d) == "SUCCEEDED");
                    let still_pending = children
                        .iter()
                        .find(|c| c.0 == ch)
                        .map(|c| c.1 == "PENDING")
                        .unwrap_or(false);
                    if still_pending && ready {
                        tx.execute(
                            "UPDATE collab_children SET state='RUNNING',started=?4 WHERE task=?1 AND collab_id=?2 AND id=?3 AND state='PENDING'",
                            params![task, collab_id, ch, at],
                        )?;
                        if let Some(c) = children.iter_mut().find(|c| c.0 == ch) {
                            c.1 = "RUNNING".into();
                        }
                        running += 1;
                        launch.push(ch.clone());
                        Self::tx_child_message(
                            &tx,
                            task,
                            &collab_stream_id(collab_id, &ch),
                            "COLLAB",
                            Some(collab_id),
                            &ch,
                            &ChildMsg {
                                kind: "STATUS",
                                text: "RUNNING",
                                status: "RUNNING",
                                event_ref: None,
                            },
                        )?;
                        Self::event_tx(&self.identity, 
                            &tx,
                            &t,
                            task,
                            "collaboration.child.started",
                            &json!({"collaborationId":collab_id,"childId":ch}),
                        )?;
                    }
                }
                // All terminal → the collaboration itself seals COMPLETED with its
                // child-outcome tally and the durable event.
                let all_terminal = children.iter().all(|c| {
                    matches!(
                        c.1.as_str(),
                        "SUCCEEDED" | "FAILED" | "CANCELLED" | "BLOCKED"
                    )
                });
                if all_terminal {
                    let mut result = serde_json::Map::new();
                    for dim in ["succeeded", "failed", "cancelled", "blocked"] {
                        let n = children
                            .iter()
                            .filter(|c| c.1.to_lowercase() == dim)
                            .count();
                        result.insert(dim.to_string(), json!(n.to_string()));
                    }
                    tx.execute(
                        "UPDATE collaborations SET state='COMPLETED',result_json=?3,finished=?4 WHERE task=?1 AND id=?2 AND state='RUNNING'",
                        params![task, collab_id, Value::from(result.clone()).to_string(), at],
                    )?;
                    let mut payload = serde_json::Map::new();
                    payload.insert("collaborationId".into(), json!(collab_id));
                    payload.extend(result.clone());
                    Self::event_tx(&self.identity, &tx, &t, task, "collaboration.completed", &Value::from(payload))?;
                    completed = Some((request_key, Value::from(result)));
                }
            }
        }
        tx.commit()?;
        Ok((launch, completed))
    }
    // Atomic collaboration stop mark: PENDING children become BLOCKED and the
    // collaboration seals CANCELLED with its durable facts; RUNNING children
    // are returned for the caller to drain (kill + honest finish) afterwards.
    pub fn collab_stop_mark(
        &mut self,
        task: &str,
        collab_id: &str,
        reason: &str,
    ) -> Result<Vec<String>> {
        let t = self.task(task)?;
        let tx = self.db.transaction()?;
        let pending: Vec<String> = {
            let mut q = tx.prepare(
                "SELECT id FROM collab_children WHERE task=?1 AND collab_id=?2 AND state='PENDING'",
            )?;
            q.query_map(params![task, collab_id], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let running: Vec<String> = {
            let mut q = tx.prepare(
                "SELECT id FROM collab_children WHERE task=?1 AND collab_id=?2 AND state='RUNNING'",
            )?;
            q.query_map(params![task, collab_id], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let at = now();
        for ch in &pending {
            tx.execute(
                "UPDATE collab_children SET state='BLOCKED',reason=?4,finished=?5 WHERE task=?1 AND collab_id=?2 AND id=?3 AND state='PENDING'",
                params![task, collab_id, ch, reason, at],
            )?;
            let text = format!("BLOCKED {reason}");
            Self::tx_child_message(
                &tx,
                task,
                &collab_stream_id(collab_id, ch),
                "COLLAB",
                Some(collab_id),
                ch,
                &ChildMsg {
                    kind: "STATUS",
                    text: &text,
                    status: "BLOCKED",
                    event_ref: None,
                },
            )?;
            Self::event_tx(&self.identity, 
                &tx,
                &t,
                task,
                "collaboration.child.blocked",
                &json!({"collaborationId":collab_id,"childId":ch,"reasonCode":reason}),
            )?;
        }
        tx.execute(
            "UPDATE collaborations SET state='CANCELLED',reason=?3,finished=?4 WHERE task=?1 AND id=?2 AND state='RUNNING'",
            params![task, collab_id, reason, at],
        )?;
        Self::event_tx(&self.identity, 
            &tx,
            &t,
            task,
            "collaboration.stopped",
            &json!({"collaborationId":collab_id,"reasonCode":reason}),
        )?;
        tx.commit()?;
        Ok(running)
    }
    // Atomic adoption record: the child row's bounded adoptions projection, the
    // APPLIED request row and both events commit or fail together. A replay of
    // the same key reads this stored result; it is never recomputed.
    pub fn record_adoption(
        &mut self,
        task: &str,
        collab_id: &str,
        child_id: &str,
        adoption: &Value,
        key: &str,
        body: &Value,
        result: &Value,
        event: &Value,
    ) -> Result<()> {
        let t = self.task(task)?;
        let tx = self.db.transaction()?;
        let mut adoptions: Vec<Value> = {
            let s: String = tx.query_row(
                "SELECT adoptions_json FROM collab_children WHERE task=?1 AND collab_id=?2 AND id=?3",
                params![task, collab_id, child_id],
                |r| r.get(0),
            )?;
            serde_json::from_str(&s).unwrap_or_default()
        };
        adoptions.push(adoption.clone());
        // Bounded projection: keep the newest 32 records (Pi parity).
        if adoptions.len() > 32 {
            adoptions = adoptions.split_off(adoptions.len() - 32);
        }
        tx.execute(
            "UPDATE collab_children SET adoptions_json=?4 WHERE task=?1 AND collab_id=?2 AND id=?3",
            params![task, collab_id, child_id, json!(adoptions).to_string()],
        )?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'APPLIED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        Self::event_tx(&self.identity, &tx, &t, task, event["type"].as_str().unwrap_or("collaboration.adopted"), &event["payload"].clone())?;
        tx.commit()?;
        Ok(())
    }
    // child-messages/1 — durable per-child public message stream (auxiliary
    // sessions and collaboration children). The per-stream meta row owns the
    // monotonic sequence and the captureStartedAt proof; sequence allocation
    // happens inside the same transaction as the fact it records, so a
    // restart never renumbers or reuses captured history. Texts are bounded
    // with the same 64 KiB/128 KiB rules as the parent transcript, and only
    // already-public facts are eligible for insertion.
    fn tx_child_message(
        tx: &rusqlite::Transaction,
        task: &str,
        stream: &str,
        owner: &str,
        collab: Option<&str>,
        child: &str,
        m: &ChildMsg,
    ) -> Result<i64> {
        let at = now();
        tx.execute(
            "INSERT INTO child_stream_meta(task_id,stream_id,owner_kind,collab_id,child_id,next_seq,capture_started_at) VALUES(?1,?2,?3,?4,?5,1,?6) ON CONFLICT(task_id,stream_id) DO NOTHING",
            params![task, stream, owner, collab, child, at],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT next_seq FROM child_stream_meta WHERE task_id=?1 AND stream_id=?2",
            params![task, stream],
            |r| r.get(0),
        )?;
        let (text, truncated) = Self::message_text(m.text);
        tx.execute(
            "INSERT INTO child_messages(task_id,collab_id,child_id,stream_id,seq,kind,text,text_truncated,status,created_at,event_ref,message_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                task,
                collab,
                child,
                stream,
                seq,
                m.kind,
                text,
                truncated as i64,
                m.status,
                at,
                m.event_ref,
                format!("cm-{}", id())
            ],
        )?;
        tx.execute(
            "UPDATE child_stream_meta SET next_seq=next_seq+1 WHERE task_id=?1 AND stream_id=?2",
            params![task, stream],
        )?;
        Ok(seq)
    }
    // Standalone stream append for facts that are not tied to a state
    // transition (tool-call/result facts mid-turn, recover-time status rows).
    // The stream identity derives from the owning row: an auxiliary stream
    // ids itself, a collaboration child stream is collab:{collabId}:{childId}.
    // `&self` so event handlers holding a shared store lock can append public
    // facts; unchecked_transaction still opens a real BEGIN IMMEDIATE, so the
    // stream-meta init + seq allocation + insert stay one commit.
    pub fn child_message(
        &self,
        task: &str,
        collab: Option<&str>,
        child: &str,
        m: &ChildMsg,
    ) -> Result<i64> {
        let (stream, owner) = match collab {
            Some(c) => (collab_stream_id(c, child), "COLLAB"),
            None => (aux_stream_id(child), "AUX"),
        };
        let tx = self.db.unchecked_transaction()?;
        let seq = Self::tx_child_message(&tx, task, &stream, owner, collab, child, m)?;
        tx.commit()?;
        Ok(seq)
    }
    // Guarded aux transition + stream messages in ONE transaction: the state
    // row and the child-message facts it publishes commit or fail together.
    // Mirrors aux_transition; the guarded UPDATE keeps terminal facts immune
    // to racing notifications.
    pub fn aux_transition_msgs(
        &mut self,
        task: &str,
        id: &str,
        from: &[&str],
        state: &str,
        reason: Option<&str>,
        change: impl FnOnce(&mut Value),
        msgs: &[ChildMsg<'_>],
    ) -> Result<bool> {
        let tx = self.db.transaction()?;
        let mut row: Value = tx
            .query_row(
                &format!(
                    "SELECT {} FROM aux_sessions WHERE task=?1 AND id=?2",
                    Self::AUX_COLS
                ),
                params![task, id],
                |r| Self::aux_row(r),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("AUX_SESSION_NOT_FOUND"))?;
        if !from.contains(&row["state"].as_str().unwrap_or("")) {
            tx.commit()?;
            return Ok(false);
        }
        row["state"] = json!(state);
        if let Some(r) = reason {
            row["reasonCode"] = json!(r);
        }
        change(&mut row);
        if matches!(state, "SUCCEEDED" | "FAILED" | "CANCELLED" | "BUDGET_EXCEEDED") {
            row["finishedAt"] = json!(now());
        }
        let usage = if row["usage"].is_null() {
            None
        } else {
            Some(row["usage"].to_string())
        };
        let states: Vec<String> = from.iter().map(|s| format!("'{}'", s)).collect();
        let changed = tx.execute(
            &format!(
                "UPDATE aux_sessions SET state=?3,answer=?4,native_thread_id=?5,native_turn_id=?6,usage_json=?7,deliver_key=?8,delivered_at=?9,stop_reason=?10,finished=?11 WHERE task=?1 AND id=?2 AND state IN ({})",
                states.join(",")
            ),
            params![
                task,
                id,
                row["state"].as_str(),
                row["answer"].as_str().unwrap_or(""),
                row["nativeSessionId"].as_str(),
                row["nativeTurnId"].as_str(),
                usage,
                row["deliverRequestKey"].as_str(),
                row["deliveredAt"].as_str(),
                row["reasonCode"].as_str(),
                row["finishedAt"].as_str()
            ],
        )?;
        if changed > 0 {
            for m in msgs {
                Self::tx_child_message(&tx, task, &aux_stream_id(id), "AUX", None, id, m)?;
            }
        }
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(changed > 0)
    }
    // Guarded collab-child transition + stream messages in ONE transaction —
    // same semantics as collab_child_transition with the public stream facts
    // appended atomically.
    pub fn collab_child_transition_msgs(
        &mut self,
        task: &str,
        collab_id: &str,
        child_id: &str,
        from: &[&str],
        state: &str,
        reason: Option<&str>,
        change: impl FnOnce(&mut Value),
        msgs: &[ChildMsg<'_>],
    ) -> Result<bool> {
        let tx = self.db.transaction()?;
        let mut row: Value = tx
            .query_row(
                &format!(
                    "SELECT {} FROM collab_children WHERE task=?1 AND collab_id=?2 AND id=?3",
                    Self::COLLAB_CHILD_COLS
                ),
                params![task, collab_id, child_id],
                |r| Self::collab_child_row(r),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("COLLABORATION_CHILD_NOT_FOUND"))?;
        if !from.contains(&row["state"].as_str().unwrap_or("")) {
            tx.commit()?;
            return Ok(false);
        }
        row["state"] = json!(state);
        if let Some(r) = reason {
            row["reasonCode"] = json!(r);
        }
        if state == "RUNNING" && row["startedAt"].is_null() {
            row["startedAt"] = json!(now());
        }
        change(&mut row);
        if matches!(state, "SUCCEEDED" | "FAILED" | "CANCELLED" | "BLOCKED") {
            row["finishedAt"] = json!(now());
        }
        let states: Vec<String> = from.iter().map(|s| format!("'{}'", s)).collect();
        let changed = tx.execute(
            &format!(
                "UPDATE collab_children SET state=?4,reason=?5,failed_deps=?6,baseline_json=?7,candidate_json=?8,adoptions_json=?9,native_thread_id=?10,native_turn_id=?11,usage_json=?12,tool_calls=?13,output=?14,stop_proof=?15,started=?16,finished=?17,workspace=?18 WHERE task=?1 AND collab_id=?2 AND id=?3 AND state IN ({})",
                states.join(",")
            ),
            params![
                task,
                collab_id,
                child_id,
                row["state"].as_str(),
                row["reasonCode"].as_str(),
                row["failedDependencies"].as_array().map(|_| row["failedDependencies"].to_string()),
                row["baseline"].as_object().map(|_| row["baseline"].to_string()),
                row["candidate"].as_object().map(|_| row["candidate"].to_string()),
                row["adoptions"].to_string(),
                row["nativeSessionId"].as_str(),
                row["nativeTurnId"].as_str(),
                row["usage"].as_object().map(|_| row["usage"].to_string()),
                row["toolCalls"].as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0),
                row["output"].as_str().unwrap_or(""),
                row["stopProof"].as_object().map(|_| row["stopProof"].to_string()),
                row["startedAt"].as_str(),
                row["finishedAt"].as_str(),
                row["workspace"].as_str()
            ],
        )?;
        if changed > 0 {
            for m in msgs {
                Self::tx_child_message(
                    &tx,
                    task,
                    &collab_stream_id(collab_id, child_id),
                    "COLLAB",
                    Some(collab_id),
                    child_id,
                    m,
                )?;
            }
        }
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(changed > 0)
    }
    fn child_message_row_json(r: &rusqlite::Row) -> rusqlite::Result<Value> {
        Ok(json!({
            "id": r.get::<_, String>(0)?,
            "seq": r.get::<_, i64>(1)?.to_string(),
            "kind": r.get::<_, String>(2)?,
            "text": r.get::<_, String>(3)?,
            "textTruncated": r.get::<_, i64>(4)? != 0,
            "status": r.get::<_, String>(5)?,
            "createdAt": r.get::<_, String>(6)?,
            "eventRef": r.get::<_, Option<String>>(7)?,
        }))
    }
    // Stream identity resolution shared by every reader/writer: the returned
    // meta proves who owns this stream on this task. No meta row means capture
    // never started for a stream with this identity.
    pub fn child_stream(&self, task: &str, collab: Option<&str>, child: &str) -> Result<Option<Value>> {
        let (stream, owner) = match collab {
            Some(c) => (collab_stream_id(c, child), "COLLAB"),
            None => (aux_stream_id(child), "AUX"),
        };
        let row = self
            .db
            .query_row(
                "SELECT next_seq,capture_started_at,owner_kind FROM child_stream_meta WHERE task_id=?1 AND stream_id=?2 AND owner_kind=?3 AND child_id=?4 AND ((collab_id IS NULL AND ?5 IS NULL) OR collab_id=?5)",
                params![task, stream, owner, child, collab],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )
            .optional()?;
        Ok(row.map(|(next, started, kind)| {
            json!({"streamId":stream,"ownerKind":kind,"captureStartedAt":started,"latestSeq":(next-1).to_string()})
        }))
    }
    // Bounded one-page read over a durable child stream. Same paging contract
    // as the parent transcript: `cursor` reads strictly newer seqs forward,
    // `before`/the default read the newest page re-ordered ascending. The
    // caller resolves coverage (FULL_SINCE_CAPTURE vs SUMMARY_ONLY vs
    // UNAVAILABLE) — this returns the honest page plus the meta when one
    // exists.
    pub fn child_messages(
        &self,
        task: &str,
        collab: Option<&str>,
        child: &str,
        cursor: Option<i64>,
        before: Option<i64>,
        limit: i64,
    ) -> Result<Value> {
        let (stream, _) = match collab {
            Some(c) => (collab_stream_id(c, child), "COLLAB"),
            None => (aux_stream_id(child), "AUX"),
        };
        let meta = self.child_stream(task, collab, child)?;
        const COLS: &str = "message_id,seq,kind,text,text_truncated,status,created_at,event_ref";
        let forward = cursor.is_some();
        let mut rows: Vec<Value> = vec![];
        {
            let mut q = if forward {
                self.db.prepare(&format!(
                    "SELECT {COLS} FROM child_messages WHERE task_id=?1 AND stream_id=?2 AND seq>?3 ORDER BY seq ASC LIMIT ?4"
                ))?
            } else {
                self.db.prepare(&format!(
                    "SELECT {COLS} FROM child_messages WHERE task_id=?1 AND stream_id=?2 AND seq<?3 ORDER BY seq DESC LIMIT ?4"
                ))?
            };
            let bound = if forward {
                cursor.unwrap_or(0)
            } else {
                before.unwrap_or(i64::MAX)
            };
            let fetched = q.query_map(params![task, stream, bound, limit + 1], |r| {
                Self::child_message_row_json(r)
            })?;
            for row in fetched {
                rows.push(row?);
            }
        }
        let mut items: Vec<Value> = vec![];
        let mut bytes = 2usize;
        let mut cut = false;
        for item in rows.iter() {
            if items.len() as i64 >= limit {
                cut = true;
                break;
            }
            let need = serde_json::to_vec(item)?.len() + if items.is_empty() { 0 } else { 1 };
            if !items.is_empty() && bytes + need > MESSAGE_PAGE_MAX {
                cut = true;
                break;
            }
            bytes += need;
            items.push(item.clone());
        }
        if !forward {
            items.reverse();
        }
        let first = items
            .first()
            .and_then(|i| i["seq"].as_str().and_then(|s| s.parse::<i64>().ok()));
        let last = items
            .last()
            .and_then(|i| i["seq"].as_str().and_then(|s| s.parse::<i64>().ok()));
        let has_more_before = match first {
            Some(first) => self.db.query_row(
                "SELECT count(*) FROM child_messages WHERE task_id=?1 AND stream_id=?2 AND seq<?3",
                params![task, stream, first],
                |r| r.get::<_, i64>(0),
            )? > 0,
            None => false,
        };
        let has_more_after = match last {
            Some(last) => self.db.query_row(
                "SELECT count(*) FROM child_messages WHERE task_id=?1 AND stream_id=?2 AND seq>?3",
                params![task, stream, last],
                |r| r.get::<_, i64>(0),
            )? > 0,
            None => false,
        };
        let next_cursor = match (forward, last) {
            (true, Some(last)) => json!(last.to_string()),
            (true, None) => json!(cursor.map(|c| c.to_string())),
            (false, Some(last)) => json!(last.to_string()),
            (false, None) => Value::Null,
        };
        let v = json!({
            "streamId": stream,
            "captureStartedAt": meta.as_ref().map(|m| m["captureStartedAt"].clone()),
            "latestSeq": meta.as_ref().map(|m| m["latestSeq"].clone()),
            "hasMeta": meta.is_some(),
            "items": items,
            "nextCursor": next_cursor,
            "hasMoreBefore": has_more_before,
            "hasMoreAfter": has_more_after,
            "truncated": cut,
        });
        if serde_json::to_vec(&v)?.len() > MESSAGE_RESPONSE_MAX {
            bail!("MESSAGE_PAGE_TOO_LARGE")
        }
        Ok(v)
    }
    // C3 — collaboration preflight: a frozen validated plan bound to a digest
    // and an expiry. No collaboration row, no child spawn; ≤32 per task.
    pub fn accept_preflight(
        &mut self,
        task: &str,
        row: &Value,
        key: &str,
        body: &Value,
        result: &Value,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'APPLIED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        tx.execute(
            "INSERT INTO collab_preflights VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                task,
                row["preflightId"].as_str(),
                row["planDigest"].as_str(),
                row["plan"].to_string(),
                row["sourceBindings"].to_string(),
                row["allowedScopes"].to_string(),
                row["budgetTerms"].to_string(),
                row["expiresAt"].as_str(),
                now()
            ],
        )?;
        tx.execute(
            "DELETE FROM collab_preflights WHERE task=?1 AND id NOT IN (SELECT id FROM collab_preflights WHERE task=?1 ORDER BY created DESC,rowid DESC LIMIT 32)",
            [task],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq+1,json!({"cursor":(seq+1).to_string(),"sequence":(seq+1).to_string(),"seq":(seq+1).to_string(),"kind":"collaboration.preflight","type":"collaboration.preflight","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"payload":{"preflightId":row["preflightId"],"planDigest":row["planDigest"],"expiresAt":row["expiresAt"]}}).to_string()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn collab_preflight(&self, task: &str, id: &str) -> Result<Option<Value>> {
        let row = self
            .db
            .query_row(
                "SELECT id,plan_digest,plan_json,source_bindings,allowed_scopes,budget_terms,expires_at,created FROM collab_preflights WHERE task=?1 AND id=?2",
                params![task, id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(|(id, digest, plan, bindings, scopes, budget, expires, created)| {
            json!({"preflightId":id,"planDigest":digest,"plan":serde_json::from_str::<Value>(&plan).unwrap_or(Value::Null),"sourceBindings":serde_json::from_str::<Value>(&bindings).unwrap_or(Value::Null),"allowedScopes":serde_json::from_str::<Value>(&scopes).unwrap_or(Value::Null),"budgetTerms":serde_json::from_str::<Value>(&budget).unwrap_or(Value::Null),"expiresAt":expires,"expired":rfc3339_expired(&expires),"createdAt":created})
        }))
    }
    // C4 — adoption preview: the plan the apply path would execute, locked to
    // the candidate digest and the parent/baseline shas observed at preview
    // time. A preview is a durable row but performs no parent write.
    pub fn accept_preview(
        &mut self,
        task: &str,
        row: &Value,
        key: &str,
        body: &Value,
        result: &Value,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'APPLIED',?4)",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        tx.execute(
            "INSERT INTO collab_previews VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                task,
                row["previewId"].as_str(),
                row["collaborationId"].as_str(),
                row["childId"].as_str(),
                row["planDigest"].as_str(),
                row["files"].to_string(),
                row["paths"].to_string(),
                row["candidateDigest"].as_str(),
                row["expiresAt"].as_str(),
                now()
            ],
        )?;
        tx.execute(
            "DELETE FROM collab_previews WHERE task=?1 AND id NOT IN (SELECT id FROM collab_previews WHERE task=?1 ORDER BY created DESC,rowid DESC LIMIT 32)",
            [task],
        )?;
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = now();
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq,json!({"cursor":seq.to_string(),"sequence":seq.to_string(),"seq":seq.to_string(),"kind":"request.accepted","type":"request.accepted","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key}).to_string()])?;
        tx.execute("INSERT INTO events VALUES(?1,?2,?3)",params![task,seq+1,json!({"cursor":(seq+1).to_string(),"sequence":(seq+1).to_string(),"seq":(seq+1).to_string(),"kind":"collaboration.preview","type":"collaboration.preview","occurredAt":at,"observedAt":at,"taskId":task,"requestKey":key,"payload":{"previewId":row["previewId"],"collaborationId":row["collaborationId"],"childId":row["childId"],"planDigest":row["planDigest"],"fileCount":row["files"].as_array().map(|a|a.len()).unwrap_or(0)}}).to_string()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn collab_preview(&self, task: &str, id: &str) -> Result<Value> {
        let row = self
            .db
            .query_row(
                "SELECT id,collab_id,child_id,plan_digest,files_json,paths_json,candidate_digest,expires_at,created FROM collab_previews WHERE task=?1 AND id=?2",
                params![task, id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                        r.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("PREVIEW_NOT_FOUND"))?;
        let (id, cid, ch, digest, files, paths, candidate, expires, created) = row;
        Ok(json!({"previewId":id,"collaborationId":cid,"childId":ch,"planDigest":digest,"files":serde_json::from_str::<Value>(&files).unwrap_or(json!([])),"paths":paths.and_then(|s|serde_json::from_str::<Value>(&s).ok()),"candidateDigest":candidate,"expiresAt":expires,"expired":rfc3339_expired(&expires),"createdAt":created,"taskId":task}))
    }
    // C5 — one-way source seal: the task's sealedFor fact, the durable seal
    // row, the APPLIED receipt and the source.sealed event commit together.
    // A second seal admission on an already-sealed task cannot happen here —
    // the route refuses before dispatch — and the request-key idempotency
    // still replays the original result.
    pub fn seal_source(
        &mut self,
        task: &str,
        seal: &Value,
        key: &str,
        body: &Value,
        result: &Value,
    ) -> Result<()> {
        let mut t = self.task(task)?;
        // The check rides the same store lock that commits the seal: two
        // concurrent different-key seals must not both win (route-level
        // sealedFor read is a stale snapshot).
        if t["sealedFor"].is_object() {
            bail!("SOURCE_ALREADY_SEALED")
        }
        // F33: the quiescence gate is re-checked on the fresh row inside the
        // commit — an owner that came back to life (reopen, queue admission,
        // follow-up dispatch) between the route check and now refuses before
        // the fence fact exists, so it can never land over live writes.
        if !crate::host::seal_quiescent(&t) {
            bail!("TASK_NOT_QUIESCENT")
        }
        t["sealedFor"] = seal.clone();
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![task, t.to_string()],
        )?;
        tx.execute(
            "INSERT INTO source_seals VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                task,
                seal["sealId"].as_str(),
                key,
                seal["handoffId"].as_str(),
                seal["targetRef"].to_string(),
                seal["proof"].to_string(),
                seal["affectedChildren"].to_string(),
                seal["sealedAt"].as_str()
            ],
        )?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'APPLIED',?4) ON CONFLICT(scope,key) DO UPDATE SET phase='APPLIED',result=excluded.result",
            params![task, key, String::from_utf8(canonical(body)?)?, result.to_string()],
        )?;
        Self::event_tx(
            &self.identity,
            &tx,
            &t,
            task,
            "source.sealed",
            &json!({"sealId":seal["sealId"],"handoffId":seal["handoffId"],"targetRef":seal["targetRef"],"sealedAt":seal["sealedAt"],"affectedChildren":seal["affectedChildren"],"proof":seal["proof"]}),
        )?;
        Self::commit_tx(&self.fail_commit_next, tx)?;
        Ok(())
    }
    pub fn source_seal(&self, task: &str, id: &str) -> Result<Value> {
        let row = self
            .db
            .query_row(
                "SELECT id,request_key,handoff_id,target_ref,proof_json,affected_children,sealed_at FROM source_seals WHERE task=?1 AND id=?2",
                params![task, id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("SEAL_NOT_FOUND"))?;
        let (id, key, handoff, target, proof, affected, at) = row;
        Ok(json!({"sealId":id,"requestKey":key,"handoffId":handoff,"targetRef":serde_json::from_str::<Value>(&target).unwrap_or(Value::Null),"sealedAt":at,"proof":serde_json::from_str::<Value>(&proof).unwrap_or(Value::Null),"affectedChildren":serde_json::from_str::<Value>(&affected).unwrap_or(json!([])),"taskId":task}))
    }
    // Latest seal for a task, if any — detail/handoff projection reads it
    // without needing the seal id.
    pub fn task_seal(&self, task: &str) -> Result<Option<Value>> {
        let row = self
            .db
            .query_row(
                "SELECT id,request_key,handoff_id,target_ref,proof_json,affected_children,sealed_at FROM source_seals WHERE task=?1 ORDER BY rowid DESC LIMIT 1",
                params![task],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(|(id, key, handoff, target, proof, affected, at)| {
            json!({"sealId":id,"requestKey":key,"handoffId":handoff,"targetRef":serde_json::from_str::<Value>(&target).unwrap_or(Value::Null),"sealedAt":at,"proof":serde_json::from_str::<Value>(&proof).unwrap_or(Value::Null),"affectedChildren":serde_json::from_str::<Value>(&affected).unwrap_or(json!([])),"taskId":task})
        }))
    }
    // C6 — staged material uploads. Metadata lives in this row; bytes live
    // under state/materials/<upload_key>. The row is the lifecycle fact.
    // Atomic intake: the idempotent request record and the upload row commit
    // together under the project-scoped "materials" request scope.
    pub fn accept_material(
        &mut self,
        scope: &str,
        key: &str,
        body: &Value,
        row: &Value,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'APPLIED',?4)",
            params![scope, key, String::from_utf8(canonical(body)?)?, row.to_string()],
        )?;
        tx.execute(
            "INSERT INTO material_uploads VALUES(?1,?2)",
            params![row["uploadKey"].as_str(), row.to_string()],
        )?;
        Self::commit_tx(&self.fail_commit_next, tx)
    }
    pub fn material_put(&mut self, row: &Value) -> Result<()> {
        self.db.execute(
            "INSERT INTO material_uploads VALUES(?1,?2) ON CONFLICT(upload_key) DO UPDATE SET json=excluded.json",
            params![row["uploadKey"].as_str(), row.to_string()],
        )?;
        Ok(())
    }
    pub fn material(&self, key: &str) -> Result<Value> {
        let s: String = self
            .db
            .query_row(
                "SELECT json FROM material_uploads WHERE upload_key=?1",
                [key],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("MATERIAL_NOT_FOUND"))?;
        Ok(serde_json::from_str(&s)?)
    }
    pub fn materials(&self, project: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(
            "SELECT json FROM material_uploads WHERE json_extract(json,'$.projectId')=?1 ORDER BY json_extract(json,'$.createdAt')",
        )?;
        let rows = q.query_map([project], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    // Bind a SEALED upload to (task,request) inside an open admission tx.
    // Only a SEALED row whose declared sha matches the request's reference may
    // bind, and the declared role must fit the stored media type: the server
    // reads MIME from the durable row, never from the request. Anything else
    // refuses the whole admission.
    fn tx_material_bind(
        tx: &rusqlite::Transaction,
        material_ref: &str,
        sha256: &str,
        role: &str,
        task: &str,
        request_key: &str,
    ) -> Result<()> {
        let s: String = tx
            .query_row(
                "SELECT json FROM material_uploads WHERE upload_key=?1",
                [material_ref],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("MATERIAL_NOT_FOUND"))?;
        let mut m: Value = serde_json::from_str(&s)?;
        if m["state"].as_str() != Some("SEALED") {
            bail!("MATERIAL_NOT_SEALED")
        }
        if m["sha256"].as_str() != Some(sha256) {
            bail!("MATERIAL_DIGEST_MISMATCH")
        }
        let image = m["mediaType"].as_str().unwrap_or("").starts_with("image/");
        match role {
            "IMAGE" if !image => bail!("INVALID_MATERIAL_ROLE"),
            "REFERENCE_TEXT" if image => bail!("INVALID_MATERIAL_ROLE"),
            "IMAGE" | "REFERENCE_TEXT" => {}
            _ => bail!("INVALID_MATERIAL_ROLE"),
        }
        m["state"] = json!("BOUND");
        m["boundTaskId"] = json!(task);
        m["boundRequestKey"] = json!(request_key);
        m["boundRole"] = json!(role);
        m["boundAt"] = json!(now());
        tx.execute(
            "INSERT INTO material_uploads VALUES(?1,?2) ON CONFLICT(upload_key) DO UPDATE SET json=excluded.json",
            params![material_ref, m.to_string()],
        )?;
        Ok(())
    }
    // Uploads bound to one request — used at dispatch time to assemble the
    // material text sections that accompany the turn input.
    pub fn request_materials(&self, task: &str, key: &str) -> Result<Vec<Value>> {
        let mut q = self.db.prepare(
            "SELECT json FROM material_uploads WHERE json_extract(json,'$.boundTaskId')=?1 AND json_extract(json,'$.boundRequestKey')=?2 ORDER BY json_extract(json,'$.createdAt')",
        )?;
        let rows = q.query_map(params![task, key], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    // Once the bound turn dispatched and the request earned its APPLIED
    // receipt, the material is CONSUMED — a durable consumption fact, never
    // assumed earlier.
    pub fn materials_consume(&mut self, task: &str, key: &str) -> Result<()> {
        let tx = self.db.transaction()?;
        let mut q = tx.prepare(
            "SELECT upload_key,json FROM material_uploads WHERE json_extract(json,'$.boundTaskId')=?1 AND json_extract(json,'$.boundRequestKey')=?2 AND json_extract(json,'$.state')='BOUND'",
        )?;
        let rows: Vec<(String, String)> = q
            .query_map(params![task, key], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        drop(q);
        for (k, s) in rows {
            let mut m: Value = serde_json::from_str(&s)?;
            m["state"] = json!("CONSUMED");
            m["consumedAt"] = json!(now());
            tx.execute(
                "UPDATE material_uploads SET json=?2 WHERE upload_key=?1",
                params![k, m.to_string()],
            )?;
        }
        Self::commit_tx(&self.fail_commit_next, tx)
    }
    // Seal lookup by handoff id for the public work-handoff reads: handoff ids
    // are minted at seal time and are unique, so the owning task resolves here.
    pub fn seal_by_handoff(&self, handoff: &str) -> Result<Option<(String, Value)>> {
        let row = self
            .db
            .query_row(
                "SELECT task,id,request_key,handoff_id,target_ref,proof_json,affected_children,sealed_at FROM source_seals WHERE handoff_id=?1",
                [handoff],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(|(task, id, key, hid, target, proof, affected, at)| {
            (
                task.clone(),
                json!({"sealId":id,"requestKey":key,"handoffId":hid,"targetRef":serde_json::from_str::<Value>(&target).unwrap_or(Value::Null),"sealedAt":at,"proof":serde_json::from_str::<Value>(&proof).unwrap_or(Value::Null),"affectedChildren":serde_json::from_str::<Value>(&affected).unwrap_or(json!([])),"taskId":task}),
            )
        }))
    }
    // C5 — read-only handoff export parts. Every part serializes only durable
    // rows already exposed by the public projections; bytes are canonical and
    // deterministic for a fixed durable state so the reported digest stays
    // stable across reads. Parts are byte-bounded: an oversized section is
    // truncated deterministically at an item boundary and marked, never
    // silently cut mid-row.
    const EXPORT_PART_MAX: usize = 8 * 1024 * 1024;
    fn export_bounded(items: Vec<Value>, extra: Value) -> Result<Vec<u8>> {
        let total = items.len();
        let mut kept = vec![];
        let mut bytes = 64usize;
        let mut truncated = false;
        for item in items {
            let need = serde_json::to_vec(&item)?.len() + 1;
            if !kept.is_empty() && bytes + need > Self::EXPORT_PART_MAX {
                truncated = true;
                break;
            }
            bytes += need;
            kept.push(item);
        }
        let mut v = json!({"items":kept});
        if truncated {
            v["truncated"] = json!(true);
            v["keptItems"] = json!(v["items"].as_array().map(|a| a.len()).unwrap_or(0).to_string());
            v["totalItems"] = json!(total.to_string());
        }
        if let Some(o) = extra.as_object() {
            for (k, x) in o {
                v[k.clone()] = x.clone();
            }
        }
        canonical(&v)
    }
    pub fn export_part(&mut self, task: &str, part: &str) -> Result<Vec<u8>> {
        match part {
            "manifest" => {
                let t = self.task(task)?;
                let mut m = json!({
                    "taskId": t["taskId"],
                    "storeId": self.identity["storeId"],
                    "authorityId": self.identity["authorityId"],
                    "workerId": self.identity["workerId"],
                    "engineId": t["engineId"],
                    "engineVersion": t["engineVersion"],
                    "projectId": t["projectId"],
                    "state": t["state"],
                    "reasonCode": t["reasonCode"],
                    "generation": t["generation"],
                    "segmentId": t["segmentId"],
                    "ownerEpoch": t["ownerEpoch"],
                    "baseCommit": t["baseCommit"],
                    "requestKey": t["requestKey"],
                    "createdAt": t["createdAt"],
                    "quiescent": t["quiescent"],
                    "configuredModel": t["configuredModel"],
                    "usageTotals": t["usageTotals"],
                    "writeScope": t["writeScope"],
                    "executionPolicy": t["executionPolicy"],
                    "policyPreset": t["policyPreset"],
                    "sealedFor": t["sealedFor"],
                });
                m["transcriptRevision"] = json!(self
                    .db
                    .query_row(
                        "SELECT transcript_revision FROM public_transcript_meta WHERE task_id=?1",
                        [task],
                        |r| r.get::<_, i64>(0),
                    )
                    .optional()?
                    .unwrap_or(0)
                    .to_string());
                m["eventHead"] = json!(self
                    .db
                    .query_row(
                        "SELECT coalesce(max(seq),0) FROM events WHERE task=?1",
                        [task],
                        |r| r.get::<_, i64>(0),
                    )?
                    .to_string());
                canonical(&json!({"manifest":m}))
            }
            "messages" => {
                let mut q = self.db.prepare(
                    "SELECT message_id,sequence,revision,role,text,text_truncated,status,source_request_key,native_thread_id,native_turn_id,native_item_id,command_id,generation,segment_id,occurred_at,updated_at FROM public_messages WHERE task_id=?1 ORDER BY sequence",
                )?;
                let rows = q.query_map([task], |r| Self::message_row_json(r))?;
                let mut items = vec![];
                for r in rows {
                    items.push(r?);
                }
                drop(q);
                let meta: Option<(i64, String, Option<String>, i64)> = self
                    .db
                    .query_row(
                        "SELECT transcript_revision,history_mode,legacy_text,legacy_text_truncated FROM public_transcript_meta WHERE task_id=?1",
                        [task],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                    )
                    .optional()?;
                let extra = json!({"transcript":meta.map(|(rev,mode,text,tr)| json!({"transcriptRevision":rev.to_string(),"historyMode":mode,"legacy":{"text":text.unwrap_or_default(),"textTruncated":tr!=0}}))});
                Self::export_bounded(items, extra)
            }
            "events" => {
                let mut q = self
                    .db
                    .prepare("SELECT json FROM events WHERE task=?1 ORDER BY seq")?;
                let rows = q.query_map([task], |r| r.get::<_, String>(0))?;
                let mut items = vec![];
                for r in rows {
                    items.push(serde_json::from_str(&r?)?);
                }
                drop(q);
                Self::export_bounded(items, Value::Null)
            }
            "auxiliary" => {
                let items = self.aux_sessions(task)?.into_iter().collect();
                Self::export_bounded(items, Value::Null)
            }
            "collaborations" => {
                let items = self.collaborations(task)?.into_iter().collect();
                Self::export_bounded(items, Value::Null)
            }
            "child-messages" => {
                let mut q = self.db.prepare(
                    "SELECT m.stream_id,m.collab_id,m.child_id,m.message_id,m.seq,m.kind,m.text,m.text_truncated,m.status,m.created_at,m.event_ref FROM child_messages m WHERE m.task_id=?1 ORDER BY m.stream_id,m.seq",
                )?;
                let rows = q.query_map([task], |r| {
                    Ok(json!({
                        "streamId": r.get::<_, String>(0)?,
                        "collaborationId": r.get::<_, Option<String>>(1)?,
                        "childId": r.get::<_, String>(2)?,
                        "id": r.get::<_, String>(3)?,
                        "seq": r.get::<_, i64>(4)?.to_string(),
                        "kind": r.get::<_, String>(5)?,
                        "text": r.get::<_, String>(6)?,
                        "textTruncated": r.get::<_, i64>(7)? != 0,
                        "status": r.get::<_, String>(8)?,
                        "createdAt": r.get::<_, String>(9)?,
                        "eventRef": r.get::<_, Option<String>>(10)?,
                    }))
                })?;
                let mut items = vec![];
                for r in rows {
                    items.push(r?);
                }
                drop(q);
                let mut q2 = self.db.prepare(
                    "SELECT stream_id,owner_kind,collab_id,child_id,capture_started_at,next_seq FROM child_stream_meta WHERE task_id=?1 ORDER BY stream_id",
                )?;
                let metas: Vec<Value> = q2
                    .query_map([task], |r| {
                        Ok(json!({
                            "streamId": r.get::<_, String>(0)?,
                            "ownerKind": r.get::<_, String>(1)?,
                            "collaborationId": r.get::<_, Option<String>>(2)?,
                            "childId": r.get::<_, String>(3)?,
                            "captureStartedAt": r.get::<_, String>(4)?,
                            "latestSeq": (r.get::<_, i64>(5)? - 1).to_string(),
                        }))
                    })?
                    .collect::<std::result::Result<_, _>>()?;
                drop(q2);
                Self::export_bounded(items, json!({"streams":metas}))
            }
            "artifacts" => {
                let manifests = self.segment_manifests(task)?;
                Self::export_bounded(manifests, Value::Null)
            }
            "requests" => {
                let t = self.task(task)?;
                let mut q = self.db.prepare(
                    "SELECT scope,key,body,phase,result FROM requests WHERE scope=?1 OR (scope='create' AND key=?2)",
                )?;
                let rows = q.query_map(
                    params![task, t["requestKey"].as_str().unwrap_or("")],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, String>(4)?,
                        ))
                    },
                )?;
                let mut items = vec![];
                for r in rows {
                    let (scope, key, phase, result) = r?;
                    items.push(json!({"scope":scope,"requestKey":key,"phase":phase,"result":audit_project(&serde_json::from_str::<Value>(&result).unwrap_or(json!({})))}));
                }
                drop(q);
                Self::export_bounded(items, Value::Null)
            }
            _ => bail!("EXPORT_PART_UNKNOWN"),
        }
    }
    // Ordered durable audit chain: intake intents, receipts, dispatch marks,
    // native turn/effect facts, seals and manifests. Prompt bodies are never
    // stored in these projections.
    pub fn audit(&self, task: &str) -> Result<Vec<Value>> {
        let t = self.task(task)?;
        let mut items = vec![];
        let mut q = self.db.prepare(
            "SELECT scope,key,body,phase,result FROM requests WHERE scope=?1 OR (scope='create' AND key=?2)",
        )?;
        let rows = q.query_map(
            params![task, t["requestKey"].as_str().unwrap_or("")],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            },
        )?;
        for row in rows {
            let (scope, key, body, phase, result) = row?;
            let b: Value = serde_json::from_str(&body).unwrap_or(json!({}));
            items.push(json!({
                "at": Value::Null,
                "seq": Value::Null,
                "kind": "request",
                "scope": scope,
                "requestKey": key,
                "intent": b["intent"].as_str().or(b["kind"].as_str()).unwrap_or("CREATE"),
                "phase": phase,
                "result": audit_project(&serde_json::from_str::<Value>(&result).unwrap_or(json!({})))
            }));
        }
        if let Some(events) = self.events(task, 0, i64::MAX)?["items"].as_array() {
            for e in events {
                items.push(json!({
                    "at": e["occurredAt"],
                    "seq": e["seq"],
                    "kind": "event",
                    "type": e["type"],
                    "detail": audit_project(&e["payload"])
                }));
            }
        }
        let mut q = self.db.prepare(
            "SELECT native_item_id,kind,status,generation,segment_id,native_turn_id,started_at,settled_at FROM native_effects WHERE task=?1 ORDER BY rowid",
        )?;
        let rows = q.query_map([task], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, Option<String>>(7)?,
            ))
        })?;
        for row in rows {
            let (item, kind, status, generation, segment, turn, started, settled) = row?;
            items.push(json!({
                "at": started,
                "seq": Value::Null,
                "kind": "effect",
                "nativeItemId": item,
                "effectKind": kind,
                "status": status,
                "generation": generation,
                "segmentId": segment,
                "nativeTurnId": turn,
                "settledAt": settled
            }));
        }
        let mut q = self.db.prepare(
            "SELECT native_turn_id,command_id,generation,segment_id,status,reason,sealed_at FROM turn_seals WHERE task=?1 ORDER BY sealed_at",
        )?;
        let rows = q.query_map([task], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, String>(6)?,
            ))
        })?;
        for row in rows {
            let (turn, command, generation, segment, status, reason, at) = row?;
            items.push(json!({
                "at": at,
                "seq": Value::Null,
                "kind": "turn.seal",
                "nativeTurnId": turn,
                "commandId": command,
                "generation": generation,
                "segmentId": segment,
                "status": status,
                "reason": reason
            }));
        }
        for m in self.segment_manifests(task)? {
            items.push(json!({
                "at": m["sealedAt"],
                "seq": Value::Null,
                "kind": "manifest.seal",
                "segmentId": m["segmentId"],
                "coverage": m["coverage"],
                "items": m["items"].as_array().map(|i| i.len()).unwrap_or(0)
            }));
        }
        for term in self.terminals(task)? {
            items.push(json!({
                "at": term["created"],
                "seq": Value::Null,
                "kind": "terminal",
                "terminalId": term["terminalId"],
                "processId": term["processId"],
                "pty": term["pty"],
                "state": term["state"],
                "exitCode": term["exitCode"],
                "bytesOut": term["bytesOut"],
                "bytesIn": term["bytesIn"],
                "exited": term["exited"],
                "reason": term["reason"]
            }));
        }
        for p in self.context_packages(task)? {
            items.push(json!({
                "at": p["createdAt"],
                "seq": Value::Null,
                "kind": "context.package",
                "packageId": p["packageId"],
                "name": p["name"],
                "digest": p["digest"],
                "size": p["size"],
                "sourceCount": p["sources"].as_array().map(|a| a.len()).unwrap_or(0)
            }));
        }
        for a in self.aux_sessions(task)? {
            items.push(json!({
                "at": a["createdAt"],
                "seq": Value::Null,
                "kind": "aux.session",
                "auxSessionId": a["auxSessionId"],
                "state": a["state"],
                "nativeSessionId": a["nativeSessionId"],
                "budget": a["budget"],
                "deliverRequestKey": a["deliverRequestKey"],
                "deliveredAt": a["deliveredAt"],
                "finishedAt": a["finishedAt"],
                "reasonCode": a["reasonCode"]
            }));
        }
        for tr in self.transitions(task)? {
            items.push(json!({
                "at": tr["createdAt"],
                "seq": Value::Null,
                "kind": "transition",
                "transitionId": tr["transitionId"],
                "fromEpoch": tr["fromEpoch"],
                "toEpoch": tr["toEpoch"],
                "fromSegment": tr["fromSegment"],
                "toSegment": tr["toSegment"],
                "compatibility": tr["compatibility"],
                "state": tr["state"]
            }));
        }
        // Collaboration rows and children audit only metadata: goal bodies and
        // candidate bytes never enter this projection (hashes and counts only).
        for c in self.collaborations(task)? {
            items.push(json!({
                "at": c["createdAt"],
                "seq": Value::Null,
                "kind": "collaboration",
                "collaborationId": c["collaborationId"],
                "name": c["name"],
                "state": c["state"],
                "reasonCode": c["reasonCode"],
                "result": c["result"],
                "childCount": c["children"].as_array().map(|a| a.len()).unwrap_or(0),
                "finishedAt": c["finishedAt"]
            }));
            for ch in c["children"].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
                items.push(json!({
                    "at": ch["createdAt"],
                    "seq": Value::Null,
                    "kind": "collaboration.child",
                    "collaborationId": c["collaborationId"],
                    "childId": ch["childId"],
                    "state": ch["state"],
                    "reasonCode": ch["reasonCode"],
                    "goalSha256": ch["goalSha256"],
                    "dependsOn": ch["dependsOn"],
                    "nativeSessionId": ch["nativeSessionId"],
                    "nativeTurnId": ch["nativeTurnId"],
                    "usage": ch["usage"],
                    "toolCalls": ch["toolCalls"],
                    "stopProof": ch["stopProof"],
                    "startedAt": ch["startedAt"],
                    "finishedAt": ch["finishedAt"]
                }));
            }
        }
        if let Some(b) = self.budget(task)? {
            items.push(json!({"at": b["updatedAt"], "seq": Value::Null, "kind": "budget", "caps": b["caps"], "enforcement": b["enforcement"], "exceeded": b["exceeded"], "extensions": b["extensions"], "costCoverage": b["costCoverage"]}));
        }
        let mut q = self.db.prepare(
            "SELECT seq,native_thread_id,native_turn_id,input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,observed_at FROM usage_ledger WHERE task=?1 ORDER BY seq",
        )?;
        let rows = q.query_map([task], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, i64>(7)?,
                r.get::<_, String>(8)?,
            ))
        })?;
        for row in rows {
            let (seq, thread, turn, input, cached, output, reasoning, total, at) = row?;
            items.push(json!({
                "at": at,
                "seq": seq.to_string(),
                "kind": "usage",
                "nativeThreadId": thread,
                "nativeTurnId": turn,
                "inputTokens": input.to_string(),
                "cachedInputTokens": cached.to_string(),
                "outputTokens": output.to_string(),
                "reasoningOutputTokens": reasoning.to_string(),
                "totalTokens": total.to_string()
            }));
        }
        let audit_seq = |v: &Value| -> i64 {
            v["seq"].as_str().and_then(|s| s.parse().ok()).or_else(|| v["seq"].as_i64()).unwrap_or(0)
        };
        items.sort_by(|a, b| {
            (a["at"].as_str().unwrap_or(""), audit_seq(a), a["kind"].as_str().unwrap_or(""))
                .cmp(&(b["at"].as_str().unwrap_or(""), audit_seq(b), b["kind"].as_str().unwrap_or("")))
        });
        Ok(items)
    }
}
// One durable child-stream fact. `event_ref` carries the native item/turn
// identity the fact came from when one exists (never a payload carrier).
pub struct ChildMsg<'a> {
    pub kind: &'a str,
    pub text: &'a str,
    pub status: &'a str,
    pub event_ref: Option<&'a str>,
}
// Immutable stream references: an auxiliary session's stream ids itself; a
// collaboration child's stream ids by (collaboration, child). Both are
// unambiguous under the id alphabets those namespaces admit.
pub fn aux_stream_id(aux: &str) -> String {
    aux.to_string()
}
pub fn collab_stream_id(collab: &str, child: &str) -> String {
    format!("collab:{collab}:{child}")
}
// Field set for one durable public-message insert. Only already-public text
// is carried; bindings are fixed at write time and never refreshed later.
#[derive(Default)]
struct NewMessage<'a> {
    role: &'a str,
    text: &'a str,
    status: &'a str,
    source_request_key: Option<&'a str>,
    native_thread_id: Option<&'a str>,
    native_turn_id: Option<&'a str>,
    native_item_id: Option<&'a str>,
    command_id: Option<&'a str>,
    generation: Option<&'a str>,
    segment_id: Option<&'a str>,
}
// Whether a native userMessage item could be deterministically bound to one
// durable request of this task; unbound evidence is diagnostic, never a row.
pub enum Consumption {
    Bound,
    Unbound(&'static str),
}
// Budget cap values travel as decimal strings per contract; accept plain
// integers too and normalize to i64 for storage and comparison.
fn budget_cap(caps: &Value, name: &str) -> Option<i64> {
    caps[name]
        .as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| caps[name].as_i64())
        .filter(|v| *v >= 0)
}
// Audit projections keep intents and facts, never prompt/command bodies.
// `params` is the raw native request body blob; it may carry the proposed
// command under keys the host does not enumerate (parsedCmd, argv, ...), so the
// whole carrier is stripped rather than relying on a key allowlist.
fn audit_project(v: &Value) -> Value {
    const STRIP: &[&str] = &[
        "instruction",
        "content",
        "text",
        "response",
        "output",
        "aggregatedOutput",
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
        "message",
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
        "changes",
        "params",
        "diff",
        "stdout",
        "stderr",
    ];
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .filter(|(k, _)| !STRIP.contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), audit_project(v)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(audit_project).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runner_outcome_and_inventory_commit_together_and_crash_never_settles_dispatch() {
        let mut s=Store::open(Path::new(":memory:")).unwrap();
        let t=json!({"taskId":"runner-task","requestKey":"create-runner","generation":"g","segmentId":"seg","nativeSessionId":"thread","nativeTurnId":"turn","state":"RUNNING","stateVersion":"1","quiescent":false});
        s.save(&t).unwrap();
        let p=json!({"native":{"callId":"call"},"command":{"requestKey":"runner-key","contextId":"ctx"}});
        s.effect_started("runner-task","call","dynamicToolCall","turn").unwrap();
        s.accept("runner-task","runner-key",&p,&json!({"state":"DISPATCHING"}),"runner-task").unwrap();
        s.receipt("runner-task","runner-key","DISPATCHING",&json!({"state":"DISPATCHING"})).unwrap();
        let result=json!({"state":"APPLIED","runner":{"status":"FAILED","exitCode":7,"stdout":"actual"}});
        s.runner_result("runner-task","runner-key","call",&result,true).unwrap();
        assert_eq!(s.open_effects("runner-task").unwrap(),0);
        assert_eq!(s.lookup("runner-task","runner-key").unwrap(),result);
        // A later duplicate/error cannot erase the already-proven original receipt.
        s.runner_result("runner-task","runner-key","call",&json!({"state":"UNKNOWN"}),false).unwrap();
        assert_eq!(s.lookup("runner-task","runner-key").unwrap(),result);
        s.effect_started("runner-task","call-2","dynamicToolCall","turn").unwrap();
        s.accept("runner-task","runner-key-2",&p,&json!({"state":"DISPATCHING"}),"runner-task").unwrap();
        s.receipt("runner-task","runner-key-2","DISPATCHING",&json!({"state":"DISPATCHING"})).unwrap();
        s.recover().unwrap();
        assert_eq!(s.lookup("runner-task","runner-key-2").unwrap()["state"],"UNKNOWN");
        assert_eq!(s.open_effects("runner-task").unwrap(),1);
        assert_eq!(s.runner_requests("runner-task").unwrap().len(),2);
    }
    #[test]
    fn identity_is_durable_and_future_schema_refused() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s.db");
        let a = Store::open(&p).unwrap().identity;
        let b = Store::open(&p).unwrap();
        assert_eq!(a, b.identity);
        b.db.execute_batch(&format!("PRAGMA user_version={}", SCHEMA_VERSION + 1))
            .unwrap();
        assert!(Store::open(&p).is_err())
    }
    #[test]
    fn v1_store_migrates_manifest_to_segment_and_new_tables() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s.db");
        {
            let raw = Connection::open(&p).unwrap();
            raw.execute_batch("CREATE TABLE source_identity (id INTEGER PRIMARY KEY CHECK(id=1), json TEXT NOT NULL);
              CREATE TABLE tasks (id TEXT PRIMARY KEY, json TEXT NOT NULL);
              CREATE TABLE requests (scope TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, phase TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope,key));
              CREATE TABLE events (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(task,seq));
              CREATE TABLE artifacts (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, json TEXT NOT NULL, bytes BLOB, PRIMARY KEY(task,id));
              CREATE TABLE artifact_manifests (task TEXT PRIMARY KEY REFERENCES tasks(id), json TEXT NOT NULL);
              PRAGMA user_version=1;")
                .unwrap();
            raw.execute(
                "INSERT INTO source_identity VALUES(1,?1)",
                [json!({"authorityId":"a","storeId":"store-1","workerId":"w"}).to_string()],
            )
            .unwrap();
            raw.execute(
                "INSERT INTO tasks VALUES('native-t',?1)",
                [json!({"taskId":"native-t","segmentId":"segment-old","state":"CANCELLED","quiescent":true}).to_string()],
            )
            .unwrap();
            raw.execute(
                "INSERT INTO artifact_manifests VALUES('native-t',?1)",
                [json!({"items":[{"artifactId":"file-aa","path":"x","status":"AVAILABLE","sha256":"h"}],"coverage":"CHANGED_OWNED_FILES"}).to_string()],
            )
            .unwrap();
        }
        let s = Store::open(&p).unwrap();
        assert_eq!(s.identity["storeId"], "store-1");
        let v: i64 =
            s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
        assert_eq!(v, SCHEMA_VERSION as i64);
        let m = s
            .segment_manifest("native-t", "segment-old")
            .unwrap()
            .unwrap();
        assert_eq!(m["items"][0]["artifactId"], "file-aa");
        assert!(
            s.segment_manifest("native-t", "other-segment")
                .unwrap()
                .is_none()
        );
        let segments = s.segment_manifests("native-t").unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0]["segmentId"], "segment-old");
        // Migrated stores keep working and accept the v3 inventory tables.
        s.seal_turn("native-t", "turn-1", "cmd", "g", "segment-old", "completed", None)
            .unwrap();
        s.effect_started("native-t", "item-1", "commandExecution", "turn-1")
            .unwrap();
        let reopened = Store::open(&p).unwrap();
        assert_eq!(reopened.open_effects("native-t").unwrap(), 1);
    }
    #[test]
    fn effects_settle_once_and_started_becomes_unknown_on_recover() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-fx","generation":"g1","segmentId":"seg-1","state":"RUNNING","quiescent":false}))
            .unwrap();
        s.effect_started("native-fx", "item-a", "commandExecution", "turn-1")
            .unwrap();
        s.effect_started("native-fx", "item-b", "fileChange", "turn-1")
            .unwrap();
        // Duplicate start notification is deduplicated on the durable native id.
        s.effect_started("native-fx", "item-b", "fileChange", "turn-1")
            .unwrap();
        assert_eq!(s.open_effects("native-fx").unwrap(), 2);
        s.effect_settled("native-fx", "item-a", "completed").unwrap();
        // An already-settled effect cannot be re-settled or resurrected.
        s.effect_settled("native-fx", "item-a", "failed").unwrap();
        assert_eq!(s.open_effects("native-fx").unwrap(), 1);
        s.recover().unwrap();
        assert_eq!(s.open_effects("native-fx").unwrap(), 1);
        let status: String = s
            .db
            .query_row(
                "SELECT status FROM native_effects WHERE task='native-fx' AND native_item_id='item-b'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "UNKNOWN");
        let settled: String = s
            .db
            .query_row(
                "SELECT status FROM native_effects WHERE task='native-fx' AND native_item_id='item-a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(settled, "SETTLED");
    }
    #[test]
    fn active_command_detection_is_bound_and_never_reconciles_unknown() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"stopping","generation":"g1","segmentId":"s1","nativeTurnId":"turn1"});
        s.save(&t).unwrap();
        s.effect_started("stopping","cmd","commandExecution","turn1").unwrap();
        s.effect_started("stopping","other-turn","commandExecution","turn0").unwrap();
        s.effect_started("stopping","opaque","mcpToolCall","turn1").unwrap();
        assert_eq!(s.active_native_commands("stopping").unwrap(),1);
        for field in ["generation","segmentId","nativeTurnId"] {
            let mut other=t.clone();other[field]=json!("different");s.save(&other).unwrap();
            assert_eq!(s.active_native_commands("stopping").unwrap(),0,"stale {field}");
        }
        s.save(&t).unwrap();s.effects_unknown("stopping").unwrap();
        assert_eq!(s.active_native_commands("stopping").unwrap(),0);
        assert_eq!(s.open_effects("stopping").unwrap(),3);
    }
    #[test]
    fn turn_seals_are_immutable_once_written() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-seal","state":"RUNNING"}))
            .unwrap();
        s.seal_turn("native-seal", "turn-1", "cmd-1", "g1", "seg-1", "completed", None)
            .unwrap();
        s.seal_turn("native-seal", "turn-1", "cmd-2", "g9", "seg-9", "failed", None)
            .unwrap();
        let seal = s.turn_seal("native-seal", "turn-1").unwrap().unwrap();
        assert_eq!(seal["commandId"], "cmd-1");
        assert_eq!(seal["generation"], "g1");
        assert_eq!(seal["segmentId"], "seg-1");
        assert_eq!(seal["status"], "completed");
        assert!(seal["reason"].is_null());
        s.seal_turn("native-seal", "turn-2", "cmd-2", "g1", "seg-1", "interrupted", Some("BUDGET_EXCEEDED"))
            .unwrap();
        assert_eq!(
            s.turn_seal("native-seal", "turn-2").unwrap().unwrap()["reason"],
            "BUDGET_EXCEEDED"
        );
        assert_eq!(
            s.turn_seal("native-seal", "turn-2").unwrap().unwrap()["status"],
            "interrupted"
        );
        assert!(s.turn_seal("native-seal", "turn-3").unwrap().is_none());
    }
    #[test]
    fn reopen_admission_commits_request_and_new_generation_atomically() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-admit","state":"CANCELLED","quiescent":true,"stateVersion":"7","generation":"g1","segmentId":"seg-1","nativeSessionId":"th-1","nativeTurnId":"tu-1","currentCommandId":"c0"}))
            .unwrap();
        let body = json!({"requestKey":"k2","kind":"NATIVE_FOLLOW_UP","content":"next","generation":"g1"});
        let admitted = s
            .admit_reopen(
                "native-admit",
                "k2",
                &body,
                &json!({"state":"ACCEPTED"}),
                None,
                None,&[], 
                |t: &mut Value| {
                    t["generation"] = json!("g2");
                    t["segmentId"] = json!("seg-2");
                    t["ownerEpoch"] = json!("2");
                    t["state"] = json!("UNKNOWN");
                    t["launchState"] = json!("RESTORING");
                    t["currentCommandId"] = json!("k2");
                    t["quiescent"] = json!(false);
                },
            )
            .unwrap();
        assert_eq!(admitted["generation"], "g2");
        assert_eq!(admitted["segmentId"], "seg-2");
        assert_eq!(admitted["launchState"], "RESTORING");
        assert_eq!(admitted["stateVersion"], "8");
        assert_eq!(s.lookup("native-admit", "k2").unwrap()["state"], "ACCEPTED");
        // Same key cannot be admitted twice.
        assert!(
            s.admit_reopen(
                "native-admit",
                "k2",
                &body,
                &json!({}),
                None,
                None,&[], 
                |_: &mut Value| {}
            )
            .is_err()
        );
        assert_eq!(
            s.events("native-admit", 0, 20).unwrap()["items"][0]["kind"],
            "request.accepted"
        );
    }
    #[test]
    fn original_key_conflicts_and_dispatch_is_never_replayed() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-a","state":"RUNNING"}))
            .unwrap();
        let b = json!({"guard":"1"});
        s.accept("task", "k", &b, &json!({"state":"ACCEPTED"}), "native-a")
            .unwrap();
        s.receipt("task", "k", "DISPATCHING", &json!({"state":"DISPATCHING"}))
            .unwrap();
        s.recover().unwrap();
        assert_eq!(s.lookup("task", "k").unwrap()["state"], "UNKNOWN");
        assert_eq!(
            s.existing("task", "k", &b).unwrap().unwrap()["state"],
            "UNKNOWN"
        );
        assert!(s.existing("task", "k", &json!({"guard":"2"})).is_err());
        assert_eq!(
            s.events("native-a", 0, 20).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        )
    }
    #[test]
    fn canonical_has_stable_unicode_order_and_rejects_floats() {
        assert_eq!(
            canonical(&json!({"z":1,"a":"中"})).unwrap(),
            "{\"a\":\"中\",\"z\":1}".as_bytes()
        );
        assert!(canonical(&json!({"x":0.5})).is_err())
    }
    #[test]
    fn task_acceptance_is_atomic_and_undispatched_intent_is_rejected_safely() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t =
            json!({"taskId":"native-first","requestKey":"k","state":"PREPARING","quiescent":false});
        s.accept_task(&t, "k", &json!({"requestKey":"k"}), &[]).unwrap();
        let mut other = t.clone();
        other["taskId"] = json!("native-second");
        assert!(
            s.accept_task(&other, "k", &json!({"requestKey":"k"}), &[])
                .is_err()
        );
        assert!(s.task("native-second").is_err());
        s.recover().unwrap();
        let t = s.task("native-first").unwrap();
        assert_eq!(t["state"], "REJECTED");
        assert_eq!(t["quiescent"], true);
    }
    #[test]
    fn shared_canonical_vectors() {
        let path = std::env::var_os("NWF_GOLDEN_VECTORS")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../contracts/worker-host/v1/golden-vectors.json")
            });
        let vectors: Value = serde_json::from_slice(
            &std::fs::read(path).expect("shared worker-host golden vectors"),
        )
        .unwrap();
        use sha2::{Digest, Sha256};
        for vector in vectors["items"].as_array().unwrap() {
            let bytes = canonical(&vector["input"]).unwrap();
            assert_eq!(
                String::from_utf8(bytes.clone()).unwrap(),
                vector["canonical"]
            );
            assert_eq!(hex::encode(Sha256::digest(bytes)), vector["sha256"]);
        }
    }
    #[test]
    fn legacy_event_reads_keep_consumer_projection() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-history"})).unwrap();
        s.db.execute(
            "INSERT INTO events VALUES('native-history',1,?1)",
            [
                json!({"type":"turn.started","observedAt":"2026-10-24T00:00:00Z","sequence":"1"})
                    .to_string(),
            ],
        )
        .unwrap();
        let page = s.events("native-history", 0, 100).unwrap();
        let e = &page["items"][0];
        assert_eq!(e["cursor"], "1");
        assert_eq!(e["kind"], "turn/started");
        assert_eq!(e["occurredAt"], "2026-10-24T00:00:00Z");
    }
    #[test]
    fn consumption_proof_is_not_overwritten_by_late_rpc_ack() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-race"})).unwrap();
        let b = json!({"requestKey":"k"});
        s.accept(
            "native-race",
            "k",
            &b,
            &json!({"state":"ACCEPTED"}),
            "native-race",
        )
        .unwrap();
        s.receipt(
            "native-race",
            "k",
            "APPLIED",
            &json!({"state":"APPLIED","consumptionEvidence":{"nativeItemId":"actual-native-item"}}),
        )
        .unwrap();
        s.receipt(
            "native-race",
            "k",
            "REQUESTED",
            &json!({"state":"REQUESTED"}),
        )
        .unwrap();
        assert_eq!(s.lookup("native-race", "k").unwrap()["state"], "APPLIED");
    }
    #[test]
    fn v2_store_migrates_to_v3_budget_seal_reason() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s.db");
        {
            let raw = Connection::open(&p).unwrap();
            raw.execute_batch("CREATE TABLE source_identity (id INTEGER PRIMARY KEY CHECK(id=1), json TEXT NOT NULL);
              CREATE TABLE tasks (id TEXT PRIMARY KEY, json TEXT NOT NULL);
              CREATE TABLE requests (scope TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, phase TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope,key));
              CREATE TABLE events (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(task,seq));
              CREATE TABLE artifacts (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, json TEXT NOT NULL, bytes BLOB, PRIMARY KEY(task,id));
              CREATE TABLE artifact_manifests (task TEXT NOT NULL REFERENCES tasks(id), segment_id TEXT NOT NULL, json TEXT NOT NULL, sealed_at TEXT NOT NULL, PRIMARY KEY(task,segment_id));
              CREATE TABLE native_effects (task TEXT NOT NULL REFERENCES tasks(id), effect_id TEXT NOT NULL, native_item_id TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, native_turn_id TEXT, started_at TEXT NOT NULL, settled_at TEXT, summary_json TEXT, PRIMARY KEY(task,native_item_id));
              CREATE TABLE turn_seals (task TEXT NOT NULL REFERENCES tasks(id), native_turn_id TEXT NOT NULL, command_id TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, status TEXT NOT NULL, sealed_at TEXT NOT NULL, PRIMARY KEY(task,native_turn_id));
              INSERT INTO tasks VALUES('native-t','{}');
              INSERT INTO turn_seals VALUES('native-t','turn-1','cmd','g','seg','completed','2026-10-24T00:00:00Z');
              PRAGMA user_version=2;")
                .unwrap();
        }
        let s = Store::open(&p).unwrap();
        let v: i64 =
            s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
        assert_eq!(v, SCHEMA_VERSION as i64);
        // The v2 seal survives unmoved and gains a NULL reason.
        let seal = s.turn_seal("native-t", "turn-1").unwrap().unwrap();
        assert_eq!(seal["status"], "completed");
        assert!(seal["reason"].is_null());
        // New v3 tables exist and accept rows.
        s.seal_turn("native-t", "turn-2", "cmd", "g", "seg", "interrupted", Some("BUDGET_EXCEEDED"))
            .unwrap();
        assert_eq!(
            s.turn_seal("native-t", "turn-2").unwrap().unwrap()["reason"],
            "BUDGET_EXCEEDED"
        );
        s.db.execute(
            "INSERT INTO budgets VALUES('native-t','100',NULL,NULL,NULL,'SOFT',0,NULL,NULL,NULL,0,'2026-10-24T00:00:00Z')",
            [],
        )
        .unwrap();
        assert_eq!(
            s.budget("native-t").unwrap().unwrap()["caps"]["maxTotalTokens"],
            "100"
        );
    }
    #[test]
    fn budget_exceed_marks_task_and_emits_fact_once() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"native-b","requestKey":"k","state":"RUNNING","quiescent":false,"budget":{"caps":{"maxTotalTokens":"100"},"enforcement":"SOFT"}});
        s.accept_task(&t, "k", &json!({"requestKey":"k"}), &[]).unwrap();
        // Observation below the cap: ledger row, counters, no exceed.
        assert!(s
            .usage_observed(
                "native-b",
                "th",
                "tu-1",
                &json!({"inputTokens":40,"outputTokens":30,"totalTokens":70,"cachedInputTokens":0,"reasoningOutputTokens":0})
            )
            .unwrap()
            .is_none());
        assert_eq!(s.budget_check("native-b").unwrap(), Vec::<String>::new());
        let b = s.budget("native-b").unwrap().unwrap();
        assert_eq!(b["exceeded"], false);
        // Crossing the cap: the same atomic write marks the row, the task and the event.
        let hit = s
            .usage_observed(
                "native-b",
                "th",
                "tu-1",
                &json!({"inputTokens":60,"outputTokens":50,"totalTokens":110,"cachedInputTokens":0,"reasoningOutputTokens":0})
            )
            .unwrap()
            .expect("cap trip");
        assert_eq!(hit["exceeded"], json!(["totalTokens"]));
        assert_eq!(hit["enforcement"], "SOFT");
        let t = s.task("native-b").unwrap();
        assert_eq!(t["state"], "BUDGET_EXCEEDED");
        // Native totals are cumulative per thread: 70 then 110 means 110 used.
        assert_eq!(t["usageTotals"]["totalTokens"], "110");
        let b = s.budget("native-b").unwrap().unwrap();
        assert_eq!(b["exceeded"], true);
        assert_eq!(b["exceededTurnId"], "tu-1");
        let page = s.events("native-b", 0, 20).unwrap();
        let kinds: Vec<&str> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["type"].as_str())
            .collect();
        assert_eq!(
            kinds.iter().filter(|k| **k == "budget.exceeded").count(),
            1
        );
        // A later observation cannot duplicate the exceed fact.
        assert!(s
            .usage_observed(
                "native-b",
                "th",
                "tu-1",
                &json!({"inputTokens":80,"outputTokens":70,"totalTokens":150,"cachedInputTokens":0,"reasoningOutputTokens":0})
            )
            .unwrap()
            .is_none());
        let page = s.events("native-b", 0, 20).unwrap();
        let kinds: Vec<&str> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["type"].as_str())
            .collect();
        assert_eq!(
            kinds.iter().filter(|k| **k == "budget.exceeded").count(),
            1
        );
        assert_eq!(s.budget_check("native-b").unwrap(), vec!["totalTokens".to_string()]);
    }
    #[test]
    fn usage_counters_survive_native_total_resets() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"native-u","requestKey":"k","state":"RUNNING","budget":{"caps":{"maxTotalTokens":"1000"},"enforcement":"SOFT"}});
        s.accept_task(&t, "k", &json!({"requestKey":"k"}), &[]).unwrap();
        s.usage_observed(
            "native-u",
            "th",
            "tu-1",
            &json!({"inputTokens":10,"outputTokens":5,"totalTokens":15,"cachedInputTokens":0,"reasoningOutputTokens":0}),
        )
        .unwrap();
        // A resumed thread restarts its cumulative total; the task counter must
        // keep the earlier share instead of subtracting backwards.
        s.usage_observed(
            "native-u",
            "th",
            "tu-2",
            &json!({"inputTokens":4,"outputTokens":2,"totalTokens":6,"cachedInputTokens":0,"reasoningOutputTokens":0}),
        )
        .unwrap();
        let t = s.task("native-u").unwrap();
        assert_eq!(t["usageTotals"]["totalTokens"], "15");
        assert_eq!(t["usage"]["totalTokens"], "6");
    }
    #[test]
    fn budget_extend_is_atomic_with_the_reopen_admission() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"native-e","requestKey":"k","state":"BUDGET_EXCEEDED","quiescent":true,"stateVersion":"2","generation":"g1","segmentId":"seg-1","nativeSessionId":"th","nativeTurnId":"tu","budget":{"caps":{"maxTotalTokens":"100"},"enforcement":"SOFT"}});
        s.accept_task(&t, "k", &json!({"requestKey":"k"}), &[]).unwrap();
        s.db.execute("UPDATE budgets SET exceeded=1,exceeded_at='2026-10-24T00:00:00Z',exceeded_turn_id='tu' WHERE task='native-e'", []).unwrap();
        let body = json!({"requestKey":"k2","intent":"EXTEND_BUDGET","budget":{"caps":{"maxTotalTokens":"9000"},"enforcement":"SOFT"},"content":"go on"});
        let admitted = s
            .admit_reopen(
                "native-e",
                "k2",
                &body,
                &json!({"state":"ACCEPTED"}),
                Some(&json!({"maxTotalTokens":"9000"})),
                None,&[], 
                |t: &mut Value| {
                    t["generation"] = json!("g2");
                    t["segmentId"] = json!("seg-2");
                    t["ownerEpoch"] = json!("2");
                    t["state"] = json!("UNKNOWN");
                    t["launchState"] = json!("RESTORING");
                    t["currentCommandId"] = json!("k2");
                    t["quiescent"] = json!(false);
                },
            )
            .unwrap();
        assert_eq!(admitted["generation"], "g2");
        let b = s.budget("native-e").unwrap().unwrap();
        assert_eq!(b["caps"]["maxTotalTokens"], "9000");
        assert_eq!(b["exceeded"], false);
        assert_eq!(b["extensions"], "1");
        // A failed admission must not leak a partial budget replacement.
        assert!(
            s.admit_reopen(
                "native-e",
                "k2",
                &body,
                &json!({}),
                Some(&json!({"maxTotalTokens":"5"})),
                None,&[], 
                |_: &mut Value| {}
            )
            .is_err()
        );
        assert_eq!(
            s.budget("native-e").unwrap().unwrap()["caps"]["maxTotalTokens"],
            "9000"
        );
    }
    #[test]
    fn queued_intent_admits_atomically_and_only_once() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"native-q","requestKey":"kq","state":"QUEUED","launchState":"QUEUED","queueOrdinal":1,"quiescent":false});
        s.accept_task(&t, "kq", &json!({"requestKey":"kq","instruction":"build"}), &[])
            .unwrap();
        let admitted = s.admit_queued("native-q").unwrap();
        assert_eq!(admitted["state"], "PREPARING");
        assert_eq!(admitted["launchState"], "PREPARING");
        assert_eq!(
            s.lookup("create", "kq").unwrap()["state"],
            "PREPARING"
        );
        let page = s.events("native-q", 0, 20).unwrap();
        let kinds: Vec<&str> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["type"].as_str())
            .collect();
        assert!(kinds.contains(&"queue.admitted"));
        // The intent is spent: a second admission is refused, not replayed.
        assert!(s.admit_queued("native-q").is_err());
        // A concurrent STOP wins: queued -> cancelled intent cannot be admitted.
        s.update("native-q", |t| {
            t["launchState"] = json!("QUEUED");
            t["state"] = json!("CANCELLED");
        })
        .unwrap();
    }
    #[test]
    fn recover_rearms_queued_intent_exactly_once() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"native-rq","requestKey":"k","state":"QUEUED","launchState":"QUEUED","queueOrdinal":2,"quiescent":false});
        s.accept_task(&t, "k", &json!({"requestKey":"k"}), &[]).unwrap();
        s.recover().unwrap();
        let t = s.task("native-rq").unwrap();
        // Still queued, never rejected: the ACCEPTED receipt proves it was
        // never dispatched, so it may be evaluated once at startup.
        assert_eq!(t["state"], "QUEUED");
        assert_eq!(t["launchState"], "QUEUED");
        // A queued intent whose receipt moved into the dispatch window is not
        // re-dispatched; it keeps the standard UNKNOWN outcome instead.
        let mut s2 = Store::open(Path::new(":memory:")).unwrap();
        let t2 = json!({"taskId":"native-dq","requestKey":"k2","state":"QUEUED","launchState":"QUEUED","queueOrdinal":1,"quiescent":false});
        s2.accept_task(&t2, "k2", &json!({"requestKey":"k2"}), &[]).unwrap();
        s2.receipt("create", "k2", "DISPATCHING", &json!({"state":"DISPATCHING"}))
            .unwrap();
        s2.recover().unwrap();
        let t2 = s2.task("native-dq").unwrap();
        assert_eq!(t2["state"], "UNKNOWN");
        assert_eq!(t2["reasonCode"], "OUTCOME_UNCONFIRMED");
    }
    #[test]
    fn audit_chain_orders_records_and_strips_prompt_bodies() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let t = json!({"taskId":"native-au","requestKey":"k","state":"RUNNING","budget":{"caps":{"maxTotalTokens":"50"},"enforcement":"SOFT"}});
        s.accept_task(
            &t,
            "k",
            &json!({"requestKey":"k","instruction":"secret prompt text"}),
            &[])
        .unwrap();
        s.event(
            "native-au",
            "turn.bound",
            json!({"requestKey":"k","nativeTurnId":"tu","text":"prompt body"}),
        )
        .unwrap();
        // Native approval params may carry the proposed command under keys the
        // host does not enumerate; the whole `params` carrier must be stripped.
        s.event(
            "native-au",
            "input.requested",
            json!({"inputRequestId":"q1","nativeTurnId":"tu","scope":{"cwd":"w"},"params":{"parsedCmd":[{"command":"secret prompt text"}],"argv":["prompt body"]}}),
        )
        .unwrap();
        s.usage_observed(
            "native-au",
            "th",
            "tu",
            &json!({"inputTokens":20,"outputTokens":30,"totalTokens":50,"cachedInputTokens":0,"reasoningOutputTokens":0}),
        )
        .unwrap();
        s.seal_turn("native-au", "tu", "k", "g", "seg", "interrupted", Some("BUDGET_EXCEEDED"))
            .unwrap();
        let items = s.audit("native-au").unwrap();
        assert!(items.len() >= 4);
        let kinds: Vec<&str> = items.iter().filter_map(|i| i["kind"].as_str()).collect();
        assert!(kinds.contains(&"request"));
        assert!(kinds.contains(&"event"));
        assert!(kinds.contains(&"budget"));
        assert!(kinds.contains(&"usage"));
        assert!(kinds.contains(&"turn.seal"));
        // No prompt body may appear anywhere in the durable projection.
        let serialized = serde_json::to_string(&items).unwrap();
        assert!(!serialized.contains("secret prompt text"));
        assert!(!serialized.contains("prompt body"));
        // Ordering is by observed time then sequence.
        let ats: Vec<&str> = items.iter().filter_map(|i| i["at"].as_str()).collect();
        let mut sorted = ats.clone();
        sorted.sort();
        assert_eq!(ats, sorted);
        // The create request row projects intent and phase, not the body.
        let request = items.iter().find(|i| i["kind"] == "request").unwrap();
        assert_eq!(request["requestKey"], "k");
        assert!(request["result"].get("instruction").is_none());
        // Request identity survives stripping of the raw params carrier.
        let input = items
            .iter()
            .find(|i| i["type"] == "input.requested")
            .unwrap();
        assert_eq!(input["detail"]["inputRequestId"], "q1");
        assert_eq!(input["detail"]["scope"]["cwd"], "w");
        assert!(input["detail"].get("params").is_none());
    }
    #[test]
    fn terminal_states_do_not_hold_concurrency_slots() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        for (id, state, quiescent, launch) in [
            ("native-run", "RUNNING", false, "DISPATCHED"),
            ("native-done", "SUCCEEDED", false, "DISPATCHED"),
            ("native-dead", "UNKNOWN", false, "DISPATCHED"),
            ("native-budget", "BUDGET_EXCEEDED", true, "DISPATCHED"),
            ("native-bind", "BINDING_UNAVAILABLE", true, "REJECTED"),
            ("native-stop", "CANCELLED", true, "DISPATCHED"),
            ("native-q", "QUEUED", false, "QUEUED"),
        ] {
            let t = json!({"taskId":id,"requestKey":format!("k-{id}"),"state":state,"launchState":launch,"quiescent":quiescent});
            s.accept_task(&t, &format!("k-{id}"), &json!({"requestKey":format!("k-{id}")}), &[])
                .unwrap();
        }
        // Only live/follow-up-capable work holds slots: RUNNING + SUCCEEDED = 2.
        assert_eq!(s.active_count(None).unwrap(), 2);
        // Excluding one slot holder leaves the other.
        assert_eq!(s.active_count(Some("native-run")).unwrap(), 1);
    }
    // v4 stores gain the W5a task-scoped tables in place; existing rows are
    // untouched and the bump is recorded honestly.
    #[test]
    fn v4_store_migrates_to_v5_w5a_tables() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s.db");
        {
            let raw = Connection::open(&p).unwrap();
            raw.execute_batch("CREATE TABLE source_identity (id INTEGER PRIMARY KEY CHECK(id=1), json TEXT NOT NULL);
              CREATE TABLE tasks (id TEXT PRIMARY KEY, json TEXT NOT NULL);
              CREATE TABLE requests (scope TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, phase TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope,key));
              CREATE TABLE events (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(task,seq));
              CREATE TABLE artifacts (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, json TEXT NOT NULL, bytes BLOB, PRIMARY KEY(task,id));
              CREATE TABLE artifact_manifests (task TEXT NOT NULL REFERENCES tasks(id), segment_id TEXT NOT NULL, json TEXT NOT NULL, sealed_at TEXT NOT NULL, PRIMARY KEY(task,segment_id));
              CREATE TABLE native_effects (task TEXT NOT NULL REFERENCES tasks(id), effect_id TEXT NOT NULL, native_item_id TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, native_turn_id TEXT, started_at TEXT NOT NULL, settled_at TEXT, summary_json TEXT, PRIMARY KEY(task,native_item_id));
              CREATE TABLE turn_seals (task TEXT NOT NULL REFERENCES tasks(id), native_turn_id TEXT NOT NULL, command_id TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, status TEXT NOT NULL, reason TEXT, sealed_at TEXT NOT NULL, PRIMARY KEY(task,native_turn_id));
              CREATE TABLE usage_ledger (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, native_thread_id TEXT, native_turn_id TEXT, input_tokens INTEGER NOT NULL, cached_input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, reasoning_output_tokens INTEGER NOT NULL, total_tokens INTEGER NOT NULL, observed_at TEXT NOT NULL, PRIMARY KEY(task,seq));
              CREATE TABLE budgets (task TEXT PRIMARY KEY REFERENCES tasks(id), max_total_tokens INTEGER, max_input_tokens INTEGER, max_output_tokens INTEGER, max_cost_micro_usd INTEGER, enforcement TEXT NOT NULL, exceeded INTEGER NOT NULL DEFAULT 0, exceeded_at TEXT, exceeded_turn_id TEXT, exceed_usage_json TEXT, extensions INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
              CREATE TABLE terminals (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, process_id TEXT NOT NULL, argv TEXT NOT NULL, pty INTEGER NOT NULL, state TEXT NOT NULL, exit_code INTEGER, bytes_out INTEGER NOT NULL DEFAULT 0, bytes_in INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL, exited TEXT, reason TEXT, PRIMARY KEY(task,id));
              INSERT INTO tasks VALUES('native-t','{\"taskId\":\"native-t\",\"state\":\"SUCCEEDED\"}');
              INSERT INTO terminals VALUES('native-t','term-1','proc-1','[]',0,'LOST',NULL,0,0,'2026-10-24T00:00:00Z','2026-10-24T00:01:00Z','HOST_RESTART');
              PRAGMA user_version=4;")
                .unwrap();
        }
        let mut s = Store::open(&p).unwrap();
        let v: i64 =
            s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
        assert_eq!(v, SCHEMA_VERSION as i64);
        // Pre-existing rows survive untouched.
        assert_eq!(s.terminal("native-t", "term-1").unwrap()["state"], "LOST");
        // The new W5a tables accept durable rows.
        let package = json!({"packageId":"pkg-1","name":"p","digest":"d","size":"3","createdAt":now()});
        let sources = vec![json!({"kind":"text","sha256":hex::encode(Sha256::digest(b"abc")),"size":"3"})];
        s.accept_context_package(
            "native-t",
            "k-pkg",
            &json!({"requestKey":"k-pkg"}),
            &package,
            &sources,
            &[b"abc".to_vec()],
            &json!({"packageId":"pkg-1"}),
        )
        .unwrap();
        let detail = s.context_package("native-t", "pkg-1").unwrap();
        assert_eq!(detail["sources"][0]["sha256"], hex::encode(Sha256::digest(b"abc")));
        assert_eq!(s.transitions("native-t").unwrap().len(), 0);
        assert_eq!(s.aux_sessions("native-t").unwrap().len(), 0);
    }
    #[test]
    fn context_package_detail_reverifies_source_hashes() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-pkg"})).unwrap();
        let package = json!({"packageId":"pkg-1","name":"ctx","digest":"dg","size":"4","createdAt":now()});
        let meta = vec![
            json!({"kind":"file","path":"a.txt","sha256":hex::encode(Sha256::digest(b"aaaa")),"size":"4"}),
            json!({"kind":"summary","sha256":hex::encode(Sha256::digest(b"summ")),"size":"4"}),
        ];
        s.accept_context_package(
            "native-pkg",
            "k",
            &json!({"requestKey":"k"}),
            &package,
            &meta,
            &[b"aaaa".to_vec(), b"summ".to_vec()],
            &json!({"packageId":"pkg-1"}),
        )
        .unwrap();
        assert_eq!(s.context_package_count("native-pkg").unwrap(), 1);
        let list = s.context_packages("native-pkg").unwrap();
        assert_eq!(list[0]["packageId"], "pkg-1");
        assert_eq!(list[0]["sources"].as_array().unwrap().len(), 2);
        // List rows carry metadata only; content resolves on the detail read.
        assert!(list[0]["sources"][0].get("content").is_none());
        let detail = s.context_package("native-pkg", "pkg-1").unwrap();
        assert_eq!(
            STANDARD.decode(detail["sources"][0]["content"].as_str().unwrap()).unwrap(),
            b"aaaa"
        );
        // A corrupted blob is detected by re-verification, not served.
        s.db.execute(
            "UPDATE context_package_sources SET bytes=x'ff' WHERE task='native-pkg' AND package_id='pkg-1' AND ordinal=0",
            [],
        )
        .unwrap();
        assert_eq!(
            s.context_package("native-pkg", "pkg-1")
                .unwrap_err()
                .to_string(),
            "PACKAGE_SOURCE_CHANGED"
        );
        assert_eq!(
            s.context_package("native-pkg", "other")
                .unwrap_err()
                .to_string(),
            "PACKAGE_NOT_FOUND"
        );
    }
    #[test]
    fn aux_delivery_marks_atomically_and_only_once() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-aux","state":"SUCCEEDED","stateVersion":"3"}))
            .unwrap();
        let aux = json!({"auxSessionId":"aux-1","question":"q","budget":{"maxTotalTokens":"900"},"workspace":"D:/aux","createdAt":now()});
        s.accept_aux(
            "native-aux",
            &aux,
            "k-aux",
            &json!({"requestKey":"k-aux","question":"q"}),
            &json!({"auxSessionId":"aux-1","state":"DISPATCHING"}),
        )
        .unwrap();
        assert_eq!(s.aux_count("native-aux").unwrap(), 1);
        let row = s.aux_session("native-aux", "aux-1").unwrap();
        assert_eq!(row["state"], "RUNNING");
        assert_eq!(row["budget"]["maxTotalTokens"], "900");
        // A RUNNING aux is not deliverable: the whole admission rolls back.
        assert!(
            s.admit_aux_deliver(
                "native-aux",
                "aux-1",
                "k-del",
                &json!({"requestKey":"k-del"}),
                &json!({}),
                |t| { t["state"] = json!("STARTING") }
            )
            .is_err()
        );
        assert_eq!(s.task("native-aux").unwrap()["state"], "SUCCEEDED");
        assert_eq!(s.task("native-aux").unwrap()["stateVersion"], "3");
        // Complete the session, then deliver: request, task and aux mark commit together.
        assert!(
            s.aux_transition(
                "native-aux",
                "aux-1",
                &["RUNNING"],
                "SUCCEEDED",
                None,
                |r| { r["answer"] = json!("the answer") }
            )
            .unwrap()
        );
        let admitted = s
            .admit_aux_deliver(
                "native-aux",
                "aux-1",
                "k-del",
                &json!({"requestKey":"k-del","kind":"NATIVE_FOLLOW_UP"}),
                &json!({"state":"ACCEPTED"}),
                |t| {
                    t["state"] = json!("STARTING");
                    t["currentCommandId"] = json!("k-del");
                },
            )
            .unwrap();
        assert_eq!(admitted["state"], "STARTING");
        assert_eq!(admitted["stateVersion"], "4");
        let row = s.aux_session("native-aux", "aux-1").unwrap();
        assert_eq!(row["state"], "SUCCEEDED");
        assert_eq!(row["deliverRequestKey"], "k-del");
        assert!(row["deliveredAt"].is_string());
        // A second delivery is refused atomically — nothing half-applied.
        assert!(
            s.admit_aux_deliver(
                "native-aux",
                "aux-1",
                "k-del2",
                &json!({"requestKey":"k-del2"}),
                &json!({}),
                |_: &mut Value| {}
            )
            .is_err()
        );
        let page = s.events("native-aux", 0, 20).unwrap();
        let kinds: Vec<&str> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["type"].as_str())
            .collect();
        assert_eq!(
            kinds.iter().filter(|k| **k == "aux-session.delivered").count(),
            1
        );
    }
    #[test]
    fn aux_recover_marks_in_flight_lost_and_keeps_terminal() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-ar"})).unwrap();
        for (id, question) in [("aux-run", "q1"), ("aux-done", "q2")] {
            let aux = json!({"auxSessionId":id,"question":question,"workspace":"D:/a","createdAt":now()});
            s.accept_aux(
                "native-ar",
                &aux,
                &format!("k-{id}"),
                &json!({"requestKey":format!("k-{id}")}),
                &json!({}),
            )
            .unwrap();
        }
        s.aux_transition("native-ar", "aux-done", &["RUNNING"], "SUCCEEDED", None, |r| {
            r["answer"] = json!("a")
        })
        .unwrap();
        s.recover().unwrap();
        assert_eq!(s.aux_session("native-ar", "aux-run").unwrap()["state"], "CANCELLED");
        assert_eq!(
            s.aux_session("native-ar", "aux-run").unwrap()["reasonCode"],
            "HOST_RESTART_JOB_CLOSED"
        );
        // A completed answer survives restart and stays deliverable.
        assert_eq!(s.aux_session("native-ar", "aux-done").unwrap()["state"], "SUCCEEDED");
        assert_eq!(s.aux_session("native-ar", "aux-done").unwrap()["answer"], "a");
    }
    #[test]
    fn transition_admission_is_atomic_with_row_and_events() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-tr","state":"CANCELLED","quiescent":true,"stateVersion":"5","generation":"g1","segmentId":"seg-1","ownerEpoch":"1","currentCommandId":"c0"}))
            .unwrap();
        let body = json!({"requestKey":"k-tr","targetProfile":{"model":"m"}});
        let row = json!({"transitionId":"tr-1","fromEpoch":"1","toEpoch":"2","fromSegment":"seg-1","toSegment":"seg-2","compatibility":{"engineId":"codex","sameEngine":true,"method":"COMPLETED_SESSION"}});
        let admitted = s
            .admit_transition(
                "native-tr",
                "k-tr",
                &body,
                &json!({"state":"ACCEPTED"}),
                &row,
                |t| {
                    t["generation"] = json!("g2");
                    t["segmentId"] = json!("seg-2");
                    t["ownerEpoch"] = json!("2");
                    t["state"] = json!("UNKNOWN");
                    t["launchState"] = json!("RESTORING");
                    t["currentCommandId"] = json!("k-tr");
                    t["quiescent"] = json!(false);
                },
            )
            .unwrap();
        assert_eq!(admitted["ownerEpoch"], "2");
        assert_eq!(admitted["stateVersion"], "6");
        let list = s.transitions("native-tr").unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["transitionId"], "tr-1");
        assert_eq!(list[0]["fromEpoch"], "1");
        assert_eq!(list[0]["toEpoch"], "2");
        assert_eq!(list[0]["state"], "ADMITTED");
        assert_eq!(list[0]["compatibility"]["engineId"], "codex");
        assert_eq!(list[0]["compatibility"]["sameEngine"], true);
        assert_eq!(s.lookup("native-tr", "k-tr").unwrap()["state"], "ACCEPTED");
        let page = s.events("native-tr", 0, 20).unwrap();
        let kinds: Vec<&str> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["type"].as_str())
            .collect();
        assert!(kinds.contains(&"transition.admitted"));
        assert!(kinds.contains(&"request.accepted"));
        // Same key cannot admit a second transition.
        assert!(
            s.admit_transition(
                "native-tr",
                "k-tr",
                &body,
                &json!({}),
                &row,
                |_: &mut Value| {}
            )
            .is_err()
        );
        assert_eq!(s.transitions("native-tr").unwrap().len(), 1);
    }
    #[test]
    fn audit_includes_w5a_rows_without_bodies() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-a5"})).unwrap();
        let aux = json!({"auxSessionId":"aux-9","question":"secret question body","workspace":"D:/a","createdAt":now()});
        s.accept_aux(
            "native-a5",
            &aux,
            "k-aux",
            &json!({"requestKey":"k-aux"}),
            &json!({}),
        )
        .unwrap();
        let package = json!({"packageId":"pkg-9","digest":"dg","size":"1","createdAt":now()});
        s.accept_context_package(
            "native-a5",
            "k-pkg",
            &json!({"requestKey":"k-pkg"}),
            &package,
            &[json!({"kind":"text","sha256":"h","size":"1"})],
            &[b"x".to_vec()],
            &json!({}),
        )
        .unwrap();
        let items = s.audit("native-a5").unwrap();
        let kinds: Vec<&str> = items.iter().filter_map(|i| i["kind"].as_str()).collect();
        assert!(kinds.contains(&"aux.session"));
        assert!(kinds.contains(&"context.package"));
        // Question and answer bodies never enter the audit projection.
        assert!(
            !serde_json::to_string(&items)
                .unwrap()
                .contains("secret question body")
        );
    }
    // ==================== W5b — collaboration DAG + guarded adoption ====================
    fn collab_child_spec(id: &str, deps: &[&str]) -> Value {
        json!({
            "childId": id,
            "goal": format!("goal-body-{id}"),
            "goalSha256": format!("sha-{id}"),
            "dependsOn": deps,
            "contextPackageIds": [],
            "writeScope": Value::Null,
            "budget": Value::Null,
            "workspace": format!("D:/ws/{id}"),
            "createdAt": now(),
            "deadlineEpochMillis": "9999999999999"
        })
    }
    fn collab_seed(s: &mut Store, task: &str, collab_id: &str, key: &str, children: Vec<Value>) {
        let collab = json!({"collaborationId":collab_id,"name":Value::Null,"createdAt":now()});
        let body = json!({"requestKey":key,"children":[]});
        let result = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"ACCEPTED","collaborationId":collab_id});
        let created = json!({"collaborationId":collab_id,"childCount":children.len().to_string()});
        s.accept_collaboration(task, &collab, &children, key, &body, &result, &created)
            .unwrap();
    }
    #[test]
    fn collab_accept_commits_rows_events_and_request_atomically() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-c1","state":"RUNNING"})).unwrap();
        collab_seed(
            &mut s,
            "native-c1",
            "collab-1",
            "k-c1",
            vec![collab_child_spec("writer", &[]), collab_child_spec("after", &["writer"])],
        );
        let c = s.collaboration("native-c1", "collab-1").unwrap();
        assert_eq!(c["state"], "RUNNING");
        assert_eq!(c["taskId"], "native-c1");
        let children = c["children"].as_array().unwrap();
        assert_eq!(children.len(), 2);
        assert!(children.iter().all(|ch| ch["state"] == "PENDING"));
        assert_eq!(children[1]["dependsOn"], json!(["writer"]));
        assert_eq!(s.collab_running_on_task("native-c1").unwrap(), 1);
        // The intake request row is durable for the idempotent replay path.
        let stored = s
            .existing("native-c1", "k-c1", &json!({"requestKey":"k-c1","children":[]}))
            .unwrap()
            .unwrap();
        assert_eq!(stored["collaborationId"], "collab-1");
        // Same key with a different body conflicts honestly.
        assert!(
            s.existing(
                "native-c1",
                "k-c1",
                &json!({"requestKey":"k-c1","children":[1]})
            )
            .is_err()
        );
        // The created event is durable; goal bodies stay off the event log.
        let ev = s.events("native-c1", 0, 50).unwrap();
        let kinds: Vec<&str> = ev["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["type"].as_str())
            .collect();
        assert!(kinds.contains(&"collaboration.created"));
        assert!(kinds.contains(&"request.accepted"));
        assert!(
            !serde_json::to_string(&ev)
                .unwrap()
                .contains("goal-body-writer")
        );
        // A second accept under the same request key cannot insert a second row.
        let collab = json!({"collaborationId":"collab-2","name":Value::Null,"createdAt":now()});
        assert!(
            s.accept_collaboration(
                "native-c1",
                &collab,
                &[collab_child_spec("solo", &[])],
                "k-c1",
                &json!({"requestKey":"k-c1","children":[]}),
                &json!({}),
                &json!({}),
            )
            .is_err()
        );
        assert!(s.collaboration("native-c1", "collab-2").is_err());
    }
    #[test]
    fn collab_child_transition_moves_only_from_open_states() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-c2","state":"RUNNING"})).unwrap();
        collab_seed(&mut s, "native-c2", "collab-1", "k-c2", vec![collab_child_spec("solo", &[])]);
        // A PENDING row cannot jump to SUCCEEDED through a RUNNING-only guard.
        assert!(
            !s.collab_child_transition(
                "native-c2",
                "collab-1",
                "solo",
                &["RUNNING"],
                "SUCCEEDED",
                None,
                |_: &mut Value| {}
            )
            .unwrap()
        );
        // A guarded open-source transition commits and stamps startedAt.
        assert!(
            s.collab_child_transition(
                "native-c2",
                "collab-1",
                "solo",
                &["PENDING", "RUNNING"],
                "RUNNING",
                None,
                |r| {
                    r["nativeSessionId"] = json!("thread-1");
                    r["nativeTurnId"] = json!("turn-1");
                }
            )
            .unwrap()
        );
        let ch = s.collab_child("native-c2", "collab-1", "solo").unwrap();
        assert_eq!(ch["state"], "RUNNING");
        assert!(ch["startedAt"].is_string());
        assert_eq!(ch["nativeSessionId"], "thread-1");
        // A terminal transition stamps finishedAt and freezes the row.
        assert!(
            s.collab_child_transition(
                "native-c2",
                "collab-1",
                "solo",
                &["PENDING", "RUNNING"],
                "SUCCEEDED",
                None,
                |r| {
                    r["candidate"] = json!({"digest":"d9"});
                }
            )
            .unwrap()
        );
        let ch = s.collab_child("native-c2", "collab-1", "solo").unwrap();
        assert_eq!(ch["state"], "SUCCEEDED");
        assert!(ch["finishedAt"].is_string());
        assert_eq!(ch["candidate"]["digest"], "d9");
        assert!(
            !s.collab_child_transition(
                "native-c2",
                "collab-1",
                "solo",
                &["PENDING", "RUNNING"],
                "FAILED",
                None,
                |_: &mut Value| {}
            )
            .unwrap()
        );
        // A missing child names its own error.
        assert_eq!(
            s.collab_child_transition(
                "native-c2",
                "collab-1",
                "ghost",
                &["PENDING"],
                "RUNNING",
                None,
                |_: &mut Value| {}
            )
            .unwrap_err()
            .to_string(),
            "COLLABORATION_CHILD_NOT_FOUND"
        );
    }
    #[test]
    fn collab_pump_enforces_dependency_order_pool_bound_and_seal() {
        // Admission row order is stable even when RFC3339 fractional precision differs.
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-c3","state":"RUNNING"})).unwrap();
        collab_seed(
            &mut s,
            "native-c3",
            "collab-1",
            "k-c3",
            vec![
                collab_child_spec("writer", &[]),
                collab_child_spec("after", &["writer"]),
                collab_child_spec("solo", &[]),
            ],
        );
        // Pool bound 1: only the first ready child leaves PENDING.
        let (launch, completed) = s.collab_pump("native-c3", "collab-1", 1).unwrap();
        assert_eq!(launch, vec!["writer".to_string()]);
        assert!(completed.is_none());
        assert_eq!(s.collab_child("native-c3", "collab-1", "writer").unwrap()["state"], "RUNNING");
        assert_eq!(s.collab_child("native-c3", "collab-1", "after").unwrap()["state"], "PENDING");
        assert_eq!(s.collab_child("native-c3", "collab-1", "solo").unwrap()["state"], "PENDING");
        // writer finishes: `after` becomes ready and wins the single slot.
        s.collab_child_transition(
            "native-c3",
            "collab-1",
            "writer",
            &["PENDING", "RUNNING"],
            "SUCCEEDED",
            None,
            |_: &mut Value| {},
        )
        .unwrap();
        let (launch, _) = s.collab_pump("native-c3", "collab-1", 1).unwrap();
        assert_eq!(launch, vec!["after".to_string()]);
        assert_eq!(s.collab_child("native-c3", "collab-1", "solo").unwrap()["state"], "PENDING");
        // after finishes: solo launches; all-terminal seals the collaboration.
        s.collab_child_transition(
            "native-c3",
            "collab-1",
            "after",
            &["PENDING", "RUNNING"],
            "SUCCEEDED",
            None,
            |_: &mut Value| {},
        )
        .unwrap();
        let (launch, _) = s.collab_pump("native-c3", "collab-1", 1).unwrap();
        assert_eq!(launch, vec!["solo".to_string()]);
        s.collab_child_transition(
            "native-c3",
            "collab-1",
            "solo",
            &["PENDING", "RUNNING"],
            "SUCCEEDED",
            None,
            |_: &mut Value| {},
        )
        .unwrap();
        let (launch, completed) = s.collab_pump("native-c3", "collab-1", 1).unwrap();
        assert!(launch.is_empty());
        let (key, result) = completed.unwrap();
        assert_eq!(key, "k-c3");
        assert_eq!(result["succeeded"], "3");
        let c = s.collaboration("native-c3", "collab-1").unwrap();
        assert_eq!(c["state"], "COMPLETED");
        assert!(c["finishedAt"].is_string());
        let ev = s.events("native-c3", 0, 50).unwrap();
        let kinds: Vec<&str> = ev["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["type"].as_str())
            .collect();
        assert!(kinds.contains(&"collaboration.child.started"));
        assert!(kinds.contains(&"collaboration.completed"));
        // A sealed collaboration pumps nothing further.
        let (launch, completed) = s.collab_pump("native-c3", "collab-1", 4).unwrap();
        assert!(launch.is_empty() && completed.is_none());
    }
    #[test]
    fn collab_pump_blocks_dependents_of_failed_children() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-c4","state":"RUNNING"})).unwrap();
        collab_seed(
            &mut s,
            "native-c4",
            "collab-1",
            "k-c4",
            vec![collab_child_spec("tail", &["after"]), collab_child_spec("after", &["writer"]), collab_child_spec("writer", &[])],
        );
        let (launch, _) = s.collab_pump("native-c4", "collab-1", 4).unwrap();
        assert_eq!(launch, vec!["writer".to_string()]);
        s.collab_child_transition(
            "native-c4",
            "collab-1",
            "writer",
            &["PENDING", "RUNNING"],
            "FAILED",
            Some("TURN_FAILED"),
            |_: &mut Value| {},
        )
        .unwrap();
        // Failure propagates honestly: dependents are BLOCKED with the failed
        // dep list, and a further dependent blocks transitively.
        let (launch, completed) = s.collab_pump("native-c4", "collab-1", 4).unwrap();
        assert!(launch.is_empty());
        assert!(completed.is_some());
        let after = s.collab_child("native-c4", "collab-1", "after").unwrap();
        assert_eq!(after["state"], "BLOCKED");
        assert_eq!(after["reasonCode"], "DEPENDENCY_UNSATISFIED");
        assert_eq!(after["failedDependencies"], json!(["writer"]));
        let tail = s.collab_child("native-c4", "collab-1", "tail").unwrap();
        assert_eq!(tail["state"], "BLOCKED");
        let c = s.collaboration("native-c4", "collab-1").unwrap();
        assert_eq!(c["state"], "COMPLETED");
        assert_eq!(c["result"]["blocked"], "2");
        assert_eq!(c["result"]["failed"], "1");
    }
    #[test]
    fn collab_stop_mark_blocks_pending_and_lists_running() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-c5","state":"RUNNING"})).unwrap();
        collab_seed(
            &mut s,
            "native-c5",
            "collab-1",
            "k-c5",
            vec![collab_child_spec("writer", &[]), collab_child_spec("after", &["writer"])],
        );
        let (launch, _) = s.collab_pump("native-c5", "collab-1", 4).unwrap();
        assert_eq!(launch.len(), 1);
        let running = s.collab_stop_mark("native-c5", "collab-1", "PARENT_STOP").unwrap();
        assert_eq!(running, vec!["writer".to_string()]);
        let after = s.collab_child("native-c5", "collab-1", "after").unwrap();
        assert_eq!(after["state"], "BLOCKED");
        assert_eq!(after["reasonCode"], "PARENT_STOP");
        let c = s.collaboration("native-c5", "collab-1").unwrap();
        assert_eq!(c["state"], "CANCELLED");
        assert_eq!(c["reasonCode"], "PARENT_STOP");
        let ev = s.events("native-c5", 0, 50).unwrap();
        let kinds: Vec<&str> = ev["items"].as_array().unwrap().iter().filter_map(|e| e["type"].as_str()).collect();
        assert!(kinds.contains(&"collaboration.stopped"));
        // A re-stop returns the still-RUNNING child again: the drain is
        // idempotent and the caller's finish path must settle the row.
        let again = s.collab_stop_mark("native-c5", "collab-1", "PARENT_STOP").unwrap();
        assert_eq!(again, vec!["writer".to_string()]);
        s.collab_child_transition(
            "native-c5",
            "collab-1",
            "writer",
            &["RUNNING"],
            "CANCELLED",
            Some("PARENT_STOP"),
            |_: &mut Value| {},
        )
        .unwrap();
        assert!(
            s.collab_stop_mark("native-c5", "collab-1", "PARENT_STOP")
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn collab_adoption_records_are_bounded_and_idempotent() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-c6","state":"RUNNING"})).unwrap();
        collab_seed(&mut s, "native-c6", "collab-1", "k-c6", vec![collab_child_spec("writer", &[])]);
        let body = json!({"requestKey":"k-ad","childId":"writer","mode":"ALL"});
        let result = json!({"kind":"WORKER_LOCAL","requestKey":"k-ad","state":"APPLIED","outcome":"ADOPTED"});
        let event = json!({"type":"collaboration.adopted","payload":{"collaborationId":"collab-1","childId":"writer","outcome":"ADOPTED"}});
        s.record_adoption("native-c6", "collab-1", "writer", &json!({"requestKey":"k-ad","mode":"ALL","outcome":"ADOPTED","paths":[]}), "k-ad", &body, &result, &event)
            .unwrap();
        // The APPLIED receipt is what the replay path serves — never recomputed.
        let replayed = s.existing("native-c6", "k-ad", &body).unwrap().unwrap();
        assert_eq!(replayed["outcome"], "ADOPTED");
        assert!(
            s.existing(
                "native-c6",
                "k-ad",
                &json!({"requestKey":"k-ad","childId":"writer","mode":"FILES"})
            )
            .is_err()
        );
        let ch = s.collab_child("native-c6", "collab-1", "writer").unwrap();
        assert_eq!(ch["adoptions"].as_array().unwrap().len(), 1);
        // The durable projection stays bounded at 32 records.
        for i in 0..40 {
            s.record_adoption(
                "native-c6",
                "collab-1",
                "writer",
                &json!({"requestKey":format!("k-x{i}"),"mode":"ALL","outcome":"NO_CHANGES","paths":[]}),
                &format!("k-x{i}"),
                &json!({"requestKey":format!("k-x{i}")}),
                &json!({"state":"APPLIED","outcome":"NO_CHANGES"}),
                &json!({"type":"collaboration.adopted","payload":{}}),
            )
            .unwrap();
        }
        let ch = s.collab_child("native-c6", "collab-1", "writer").unwrap();
        assert_eq!(ch["adoptions"].as_array().unwrap().len(), 32);
    }
    #[test]
    fn recover_cancels_running_blocks_pending_and_keeps_terminal_children() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        // quiescent so task-level recovery does not touch the fixture task.
        s.save(&json!({"taskId":"native-c7","state":"SUCCEEDED","quiescent":true})).unwrap();
        collab_seed(
            &mut s,
            "native-c7",
            "collab-1",
            "k-c7",
            vec![collab_child_spec("done", &[]), collab_child_spec("writer", &["done"]), collab_child_spec("after", &["writer"])],
        );
        s.collab_child_transition(
            "native-c7",
            "collab-1",
            "done",
            &["PENDING"],
            "SUCCEEDED",
            None,
            |r| {
                r["candidate"] = json!({"digest":"sealed"});
            },
        )
        .unwrap();
        s.collab_child_transition(
            "native-c7",
            "collab-1",
            "writer",
            &["PENDING"],
            "RUNNING",
            None,
            |_: &mut Value| {},
        )
        .unwrap();
        s.collab_child_transition("native-c7", "collab-1", "writer", &["PENDING"], "RUNNING", None, |_| {})
            .unwrap();
        s.recover().unwrap();
        // Terminal rows and their sealed candidates survive untouched.
        let done = s.collab_child("native-c7", "collab-1", "done").unwrap();
        assert_eq!(done["state"], "SUCCEEDED");
        assert_eq!(done["candidate"]["digest"], "sealed");
        // The in-flight child is honestly CANCELLED, never reported complete.
        let writer = s.collab_child("native-c7", "collab-1", "writer").unwrap();
        assert_eq!(writer["state"], "CANCELLED");
        assert_eq!(writer["reasonCode"], "HOST_RESTART_JOB_CLOSED");
        // A never-launched dependent reconciles BLOCKED with the same reason.
        let after = s.collab_child("native-c7", "collab-1", "after").unwrap();
        assert_eq!(after["state"], "BLOCKED");
        assert_eq!(after["reasonCode"], "HOST_RESTART_JOB_CLOSED");
        let c = s.collaboration("native-c7", "collab-1").unwrap();
        assert_eq!(c["state"], "CANCELLED");
        let ev = s.events("native-c7", 0, 50).unwrap();
        let kinds: Vec<&str> = ev["items"].as_array().unwrap().iter().filter_map(|e| e["type"].as_str()).collect();
        assert!(kinds.contains(&"collaboration.stopped"));
        assert!(kinds.contains(&"collaboration.child.blocked"));
        assert!(kinds.contains(&"collaboration.child.completed"));
    }
    #[test]
    fn audit_includes_w5b_rows_without_goal_or_candidate_bodies() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"native-c8","state":"RUNNING"})).unwrap();
        collab_seed(
            &mut s,
            "native-c8",
            "collab-1",
            "k-c8",
            vec![collab_child_spec("writer", &[])],
        );
        s.collab_child_transition("native-c8", "collab-1", "writer", &["PENDING"], "SUCCEEDED", None, |r| {
            r["candidate"] = json!({"files":{"secret.txt":{"sha256":"aa","content":"SECRET-BYTES"}}});
        })
        .unwrap();
        let items = s.audit("native-c8").unwrap();
        let kinds: Vec<&str> = items.iter().filter_map(|i| i["kind"].as_str()).collect();
        assert!(kinds.contains(&"collaboration"));
        assert!(kinds.contains(&"collaboration.child"));
        let blob = serde_json::to_string(&items).unwrap();
        assert!(!blob.contains("goal-body-writer"));
        assert!(!blob.contains("SECRET-BYTES"));
    }
    #[test]
    fn v5_store_migrates_to_v6_collab_tables() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s.db");
        {
            let raw = Connection::open(&p).unwrap();
            raw.execute_batch("CREATE TABLE source_identity (id INTEGER PRIMARY KEY CHECK(id=1), json TEXT NOT NULL);
              CREATE TABLE tasks (id TEXT PRIMARY KEY, json TEXT NOT NULL);
              CREATE TABLE requests (scope TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, phase TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope,key));
              CREATE TABLE events (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(task,seq));
              CREATE TABLE artifacts (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, json TEXT NOT NULL, bytes BLOB, PRIMARY KEY(task,id));
              CREATE TABLE artifact_manifests (task TEXT NOT NULL REFERENCES tasks(id), segment_id TEXT NOT NULL, json TEXT NOT NULL, sealed_at TEXT NOT NULL, PRIMARY KEY(task,segment_id));
              CREATE TABLE native_effects (task TEXT NOT NULL REFERENCES tasks(id), effect_id TEXT NOT NULL, native_item_id TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, native_turn_id TEXT, started_at TEXT NOT NULL, settled_at TEXT, summary_json TEXT, PRIMARY KEY(task,native_item_id));
              CREATE TABLE turn_seals (task TEXT NOT NULL REFERENCES tasks(id), native_turn_id TEXT NOT NULL, command_id TEXT NOT NULL, generation TEXT NOT NULL, segment_id TEXT NOT NULL, status TEXT NOT NULL, reason TEXT, sealed_at TEXT NOT NULL, PRIMARY KEY(task,native_turn_id));
              CREATE TABLE usage_ledger (task TEXT NOT NULL REFERENCES tasks(id), seq INTEGER NOT NULL, native_thread_id TEXT, native_turn_id TEXT, input_tokens INTEGER NOT NULL, cached_input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, reasoning_output_tokens INTEGER NOT NULL, total_tokens INTEGER NOT NULL, observed_at TEXT NOT NULL, PRIMARY KEY(task,seq));
              CREATE TABLE budgets (task TEXT NOT NULL REFERENCES tasks(id), max_total_tokens INTEGER, max_input_tokens INTEGER, max_output_tokens INTEGER, max_cost_micro_usd INTEGER, enforcement TEXT NOT NULL, exceeded INTEGER NOT NULL DEFAULT 0, exceeded_at TEXT, exceeded_turn_id TEXT, exceed_usage_json TEXT, extensions INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
              CREATE TABLE terminals (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, process_id TEXT NOT NULL, argv TEXT NOT NULL, pty INTEGER NOT NULL, state TEXT NOT NULL, exit_code INTEGER, bytes_out INTEGER NOT NULL DEFAULT 0, bytes_in INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL, exited TEXT, reason TEXT, PRIMARY KEY(task,id));
              CREATE TABLE context_packages (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, name TEXT, digest TEXT NOT NULL, size INTEGER NOT NULL, created TEXT NOT NULL, PRIMARY KEY(task,id));
              CREATE TABLE context_package_sources (task TEXT NOT NULL REFERENCES tasks(id), package_id TEXT NOT NULL, ordinal INTEGER NOT NULL, json TEXT NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(task,package_id,ordinal));
              CREATE TABLE aux_sessions (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, question TEXT NOT NULL, answer TEXT NOT NULL, state TEXT NOT NULL, request_key TEXT NOT NULL, native_thread_id TEXT, native_turn_id TEXT, budget_max_total INTEGER, usage_json TEXT, workspace TEXT, deliver_key TEXT, delivered_at TEXT, stop_reason TEXT, created TEXT NOT NULL, finished TEXT, packages_json TEXT NOT NULL DEFAULT '[]', deadline TEXT, PRIMARY KEY(task,id));
              CREATE TABLE transitions (task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL, from_epoch TEXT NOT NULL, to_epoch TEXT NOT NULL, from_segment TEXT NOT NULL, to_segment TEXT NOT NULL, compatibility TEXT NOT NULL, state TEXT NOT NULL, reason TEXT, created TEXT NOT NULL, PRIMARY KEY(task,id));
              INSERT INTO tasks VALUES('native-t','{\"taskId\":\"native-t\",\"state\":\"RUNNING\"}');
              PRAGMA user_version=5;")
                .unwrap();
        }
        let mut s = Store::open(&p).unwrap();
        let v: i64 =
            s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
        assert_eq!(v, SCHEMA_VERSION as i64);
        // The W5b tables exist and accept durable rows on a migrated store.
        collab_seed(&mut s, "native-t", "collab-1", "k-c9", vec![collab_child_spec("writer", &[])]);
        assert_eq!(s.collaboration("native-t", "collab-1").unwrap()["state"], "RUNNING");
        assert_eq!(s.collab_children("native-t", "collab-1").unwrap().len(), 1);
    }
    // ---- native-messages/1: structured public conversation (C01-C14) ----
    fn msg_task(id: &str, output: &str) -> Value {
        json!({"taskId":id,"requestKey":format!("create-{id}"),"generation":"g1","segmentId":"seg-1","nativeSessionId":"th-1","nativeTurnId":"tu-1","state":"RUNNING","stateVersion":"1","quiescent":false,"output":output,"turns":{}})
    }
    fn msg_create(s: &mut Store, id: &str) {
        s.accept_task(
            &msg_task(id, ""),
            &format!("create-{id}"),
            &json!({"requestKey":format!("create-{id}"),"instruction":format!("build {id}")}),
            &[])
        .unwrap();
    }
    #[test]
    fn creation_lookup_retains_ledger_task_binding_after_legacy_consumption_projection() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let mut task = msg_task("receipt-restart", "");
        task["requestKey"] = json!("create-restart");
        s.accept_task(&task, "create-restart", &json!({"requestKey":"create-restart","instruction":"test"}), &[]).unwrap();
        // Older workers wrote this proven consumption projection without taskId.
        s.receipt("create", "create-restart", "APPLIED", &json!({"kind":"WORKER_LOCAL","requestKey":"create-restart","state":"APPLIED","consumptionEvidence":{"nativeItemId":"original-item"}})).unwrap();
        let receipt = s.lookup("create", "create-restart").unwrap();
        assert_eq!(receipt["taskId"], "receipt-restart");
        assert_eq!(receipt["requestKey"], "create-restart");
        assert_eq!(receipt["state"], "APPLIED");
        assert_eq!(receipt["consumptionEvidence"]["nativeItemId"], "original-item");
        assert!(s.lookup("create", "foreign-key").is_err());
    }
    fn msg_follow_up(s: &mut Store, task: &str, key: &str, text: &str) {
        let b = json!({"requestKey":key,"kind":"NATIVE_FOLLOW_UP","content":text});
        s.accept_user(task, key, &b, &json!({"state":"ACCEPTED"}), text, true, &[])
            .unwrap();
    }
    // C01/C02: one user row per original request key; same-key replay adds
    // none, same-key different body conflicts, same text new key adds a row.
    #[test]
    fn user_admission_is_one_row_per_request_key() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m1");
        let page = s.messages("m1", None, None, 50).unwrap();
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        assert_eq!(page["items"][0]["role"], "user");
        assert_eq!(page["items"][0]["status"], "ACCEPTED");
        assert_eq!(page["items"][0]["text"], "build m1");
        assert_eq!(page["items"][0]["sourceRequestKey"], "create-m1");
        assert_eq!(page["historyMode"], "STRUCTURED_FROM_START");
        // Follow-up and steer admissions land as their own user rows.
        msg_follow_up(&mut s, "m1", "k2", "next step");
        let b3 = json!({"requestKey":"k3","kind":"NATIVE_STEER","content":"steer now"});
        s.accept_user(
            "m1",
            "k3",
            &b3,
            &json!({"state":"ACCEPTED"}),
            "steer now",
            false,
            &[])
        .unwrap();
        // Same-key same-body replay is answered by existing() without a row.
        let b2 = json!({"requestKey":"k2","kind":"NATIVE_FOLLOW_UP","content":"next step"});
        assert!(s.existing("m1", "k2", &b2).unwrap().is_some());
        // Same key different body is a conflict, never a silent second row.
        assert!(
            s.existing(
                "m1",
                "k2",
                &json!({"requestKey":"k2","kind":"NATIVE_FOLLOW_UP","content":"changed"})
            )
            .is_err()
        );
        // Same text under a different key is a distinct user row (C02).
        msg_follow_up(&mut s, "m1", "k4", "next step");
        let page = s.messages("m1", None, None, 50).unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 4);
        assert!(items.iter().all(|i| i["role"] == "user"));
        let keys: Vec<&str> = items
            .iter()
            .map(|i| i["sourceRequestKey"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["create-m1", "k2", "k3", "k4"]);
    }
    // C03/C04: assistant rows dedup by (task,thread,item), never by text;
    // replayed completions and pre-migration publicItemIds insert nothing;
    // several assistant messages in one turn are all kept.
    #[test]
    fn assistant_items_dedup_by_native_identity_not_text() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m2");
        assert!(
            s.record_agent_message("m2", "th-1", "tu-1", "i-1", "same words")
                .unwrap()
        );
        // A replayed item/completed changes nothing.
        assert!(
            !s.record_agent_message("m2", "th-1", "tu-1", "i-1", "same words")
                .unwrap()
        );
        // Identical text under a different native item is a second row.
        assert!(
            s.record_agent_message("m2", "th-1", "tu-1", "i-2", "same words")
                .unwrap()
        );
        // A third message in the same turn is preserved too (C04).
        assert!(
            s.record_agent_message("m2", "th-1", "tu-1", "i-3", "more")
                .unwrap()
        );
        let page = s.messages("m2", None, None, 50).unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 4); // 1 user + 3 assistant
        let assistants: Vec<&str> = items
            .iter()
            .filter(|i| i["role"] == "assistant")
            .map(|i| i["nativeItemId"].as_str().unwrap())
            .collect();
        assert_eq!(assistants, vec!["i-1", "i-2", "i-3"]);
        // A pre-migration publicItemIds mark suppresses the new-format row.
        s.save(&json!({"taskId":"m3","requestKey":"create-m3","generation":"g","segmentId":"seg","nativeSessionId":"th-9","nativeTurnId":"tu-9","state":"RUNNING","stateVersion":"1","output":"earlier","publicItemIds":["old-item"]})).unwrap();
        assert!(
            !s.record_agent_message("m3", "th-9", "tu-9", "old-item", "earlier")
                .unwrap()
        );
        let page = s.messages("m3", None, None, 50).unwrap();
        assert!(page["items"].as_array().unwrap().is_empty());
        assert_eq!(page["historyMode"], "LEGACY_PREFIX");
    }
    // C05/C14: a bound userMessage item moves its row to CONSUMED once; a
    // late REQUESTED/UNKNOWN receipt cannot downgrade it; a same-sequence
    // re-read exposes the newer transcript revision.
    #[test]
    fn consumption_marks_once_and_late_receipts_never_downgrade() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m4");
        msg_follow_up(&mut s, "m4", "k2", "next");
        let before = s.messages("m4", None, None, 50).unwrap();
        match s
            .record_user_consumption("m4", "k2", "th-1", "tu-1", "u-item-1")
            .unwrap()
        {
            Consumption::Bound => {}
            _ => panic!("expected bound"),
        }
        // Same evidence replayed: bound, no duplicate fact or revision churn.
        match s
            .record_user_consumption("m4", "k2", "th-1", "tu-1", "u-item-1")
            .unwrap()
        {
            Consumption::Bound => {}
            _ => panic!("expected bound"),
        }
        s.receipt("m4", "k2", "UNKNOWN", &json!({"state":"UNKNOWN"}))
            .unwrap();
        s.receipt("m4", "k2", "REQUESTED", &json!({"state":"REQUESTED"}))
            .unwrap();
        let after = s.messages("m4", None, None, 50).unwrap();
        let row = after["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["sourceRequestKey"] == "k2")
            .unwrap();
        assert_eq!(row["status"], "CONSUMED");
        assert_eq!(row["nativeItemId"], "u-item-1");
        assert_eq!(row["nativeThreadId"], "th-1");
        // Same sequence, newer revision: clients re-read by revision (C14).
        let first = before["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["sourceRequestKey"] == "k2")
            .unwrap();
        assert_eq!(row["sequence"], first["sequence"]);
        assert_ne!(row["revision"], first["revision"]);
        assert_ne!(after["transcriptRevision"], before["transcriptRevision"]);
        // A different item claiming the settled key is contradictory.
        match s
            .record_user_consumption("m4", "k2", "th-1", "tu-1", "u-item-2")
            .unwrap()
        {
            Consumption::Unbound("ALREADY_SETTLED") => {}
            _ => panic!("expected ALREADY_SETTLED"),
        }
    }
    // C06: no clientId / wrong thread / wrong turn / foreign keys never
    // falsely bind; the row keeps its honest accepted state.
    #[test]
    fn unbound_native_user_items_never_guess_association() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m5");
        msg_follow_up(&mut s, "m5", "k2", "next");
        // An item naming another thread can never confirm this task's input.
        match s
            .record_user_consumption("m5", "k2", "th-other", "tu-x", "i-x")
            .unwrap()
        {
            Consumption::Unbound("THREAD_MISMATCH") => {}
            _ => panic!("expected THREAD_MISMATCH"),
        }
        // A key that is not this task's request is never bound.
        match s
            .record_user_consumption("m5", "foreign-key", "th-1", "tu-1", "i-x")
            .unwrap()
        {
            Consumption::Unbound("REQUEST_NOT_BOUND") => {}
            _ => panic!("expected REQUEST_NOT_BOUND"),
        }
        // A turn contradicting the durably bound turn is not consumption.
        // The same fence must hold before the ACK has populated turns[key].
        assert!(matches!(
            s.record_user_consumption("m5", "k2", "th-1", "tu-9", "i-x").unwrap(),
            Consumption::Unbound("TURN_MISMATCH")
        ));
        s.update("m5", |t: &mut Value| {
            t["turns"]["k2"] = json!({"nativeTurnId":"tu-1"})
        })
        .unwrap();
        match s
            .record_user_consumption("m5", "k2", "th-1", "tu-9", "i-x")
            .unwrap()
        {
            Consumption::Unbound("TURN_MISMATCH") => {}
            _ => panic!("expected TURN_MISMATCH"),
        }
        // The same key on ANOTHER task's store does not bind here.
        msg_create(&mut s, "m6");
        match s
            .record_user_consumption("m6", "k2", "th-1", "tu-1", "i-x")
            .unwrap()
        {
            Consumption::Unbound("REQUEST_NOT_BOUND") => {}
            _ => panic!("expected REQUEST_NOT_BOUND"),
        }
        // None of the guesses created or modified a user row.
        let page = s.messages("m5", None, None, 50).unwrap();
        let row = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["sourceRequestKey"] == "k2")
            .unwrap();
        assert_eq!(row["status"], "ACCEPTED");
        assert!(row.get("nativeItemId").is_none() || row["nativeItemId"].is_null());
        let page = s.messages("m6", None, None, 50).unwrap();
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
    }
    // C07: pre-admission rejection leaves no row; post-admission rejection
    // and unknown outcomes project honestly onto the existing row.
    #[test]
    fn rejection_and_unknown_project_onto_the_bound_row() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m7");
        // Rejected before admission: no request, no message.
        s.receipt(
            "m7",
            "never-accepted",
            "REJECTED",
            &json!({"state":"REJECTED"}),
        )
        .unwrap();
        assert_eq!(
            s.messages("m7", None, None, 50).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        // Rejected after admission: the bound row reports REJECTED.
        msg_follow_up(&mut s, "m7", "k-rej", "try this");
        s.receipt("m7", "k-rej", "REJECTED", &json!({"state":"REJECTED"}))
            .unwrap();
        let page = s.messages("m7", None, None, 50).unwrap();
        let row = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["sourceRequestKey"] == "k-rej")
            .unwrap();
        assert_eq!(row["status"], "REJECTED");
        // An in-flight request whose outcome is unconfirmed reads UNKNOWN.
        msg_follow_up(&mut s, "m7", "k-unk", "lost");
        s.receipt(
            "m7",
            "k-unk",
            "DISPATCHING",
            &json!({"state":"DISPATCHING"}),
        )
        .unwrap();
        s.recover().unwrap();
        let page = s.messages("m7", None, None, 50).unwrap();
        let row = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["sourceRequestKey"] == "k-unk")
            .unwrap();
        assert_eq!(row["status"], "UNKNOWN");
        // The determined REJECTED row is untouched by recovery.
        let row = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["sourceRequestKey"] == "k-rej")
            .unwrap();
        assert_eq!(row["status"], "REJECTED");
    }
    // C08: an injected pre-commit failure rolls request, task mark, event
    // and message back together — nothing half written.
    #[test]
    fn commit_failure_rolls_back_every_message_projection() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m8");
        s.fail_next_commit();
        let b = json!({"requestKey":"k-f","kind":"NATIVE_FOLLOW_UP","content":"boom"});
        assert!(
            s.accept_user("m8", "k-f", &b, &json!({"state":"ACCEPTED"}), "boom", true, &[])
                .is_err()
        );
        assert!(s.existing("m8", "k-f", &b).unwrap().is_none());
        assert_eq!(
            s.messages("m8", None, None, 50).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            s.events("m8", 0, 50).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        // The next attempt commits cleanly.
        msg_follow_up(&mut s, "m8", "k-f", "boom");
        assert_eq!(
            s.messages("m8", None, None, 50).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
    // C09: a schema-6 store migrates to 7 inside one transaction, every
    // original row is preserved, and the legacy text freezes exactly once.
    #[test]
    fn schema6_to_7_migration_preserves_rows_and_freezes_legacy_once() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s.db");
        {
            // A genuine v6 store: open once at the current schema, drop the
            // v7 objects and pin the version back to 6 before seeding rows.
            let seeded = Store::open(&p).unwrap();
            seeded.db.execute_batch("DROP TABLE public_messages; DROP TABLE public_transcript_meta; PRAGMA user_version=6;").unwrap();
            seeded.save(&json!({"taskId":"old-task","requestKey":"create-old","generation":"g0","segmentId":"seg-0","nativeSessionId":"th-0","nativeTurnId":"tu-0","state":"SUCCEEDED","quiescent":true,"output":"legacy accumulated output","turns":{"create-old":{"state":"completed","nativeTurnId":"tu-0"}}})).unwrap();
            seeded.db.execute("INSERT INTO requests VALUES('create','create-old','{\"requestKey\":\"create-old\",\"instruction\":\"orig\"}','APPLIED','{\"state\":\"APPLIED\"}')", []).unwrap();
            seeded.db.execute("INSERT INTO events VALUES('old-task',1,'{\"kind\":\"request.accepted\",\"requestKey\":\"create-old\"}')", []).unwrap();
            seeded.db.execute("INSERT INTO artifacts VALUES('old-task','file-1','{\"kind\":\"file\"}',x'0102')", []).unwrap();
            seeded.db.execute("INSERT INTO artifact_manifests VALUES('old-task','seg-0','{\"items\":[]}','2026-10-29T00:00:00Z')", []).unwrap();
            seeded.db.execute("INSERT INTO turn_seals VALUES('old-task','tu-0','create-old','g0','seg-0','completed',NULL,'2026-10-29T00:00:00Z')", []).unwrap();
            seeded.db.execute("INSERT INTO native_effects VALUES('old-task','effect-1','item-1','commandExecution','SETTLED','g0','seg-0','tu-0','2026-10-29T00:00:00Z','2026-10-29T00:00:01Z','{}')", []).unwrap();
            seeded.db.execute("INSERT INTO usage_ledger VALUES('old-task',1,'th-0','tu-0',10,2,5,1,15,'2026-10-29T00:00:00Z')", []).unwrap();
        }
        let mut s = Store::open(&p).unwrap();
        let v: i64 =
            s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
        assert_eq!(v, SCHEMA_VERSION as i64);
        // Original tables and rows survive untouched.
        assert_eq!(
            s.task("old-task").unwrap()["output"],
            "legacy accumulated output"
        );
        assert_eq!(
            s.lookup("create", "create-old").unwrap()["state"],
            "APPLIED"
        );
        assert_eq!(
            s.turn_seal("old-task", "tu-0").unwrap().unwrap()["status"],
            "completed"
        );
        assert_eq!(
            s.events("old-task", 0, 10).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let bytes: i64 =
            s.db.query_row(
                "SELECT count(*) FROM artifacts WHERE task='old-task'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(bytes, 1);
        // The first read freezes the legacy accumulated text exactly once.
        let page = s.messages("old-task", None, None, 50).unwrap();
        assert_eq!(page["historyMode"], "LEGACY_PREFIX");
        assert_eq!(page["legacy"]["text"], "legacy accumulated output");
        assert_eq!(page["legacy"]["textTruncated"], false);
        assert!(page["items"].as_array().unwrap().is_empty());
        let rev = page["transcriptRevision"].as_str().unwrap().to_owned();
        // Output growth after the freeze never mutates the frozen block.
        s.update("old-task", |t: &mut Value| {
            t["output"] = json!("legacy accumulated output\nmore")
        })
        .unwrap();
        let again = s.messages("old-task", None, None, 50).unwrap();
        assert_eq!(again["legacy"]["text"], "legacy accumulated output");
        assert_eq!(again["transcriptRevision"], rev);
        // New structured messages land after the freeze and do not hide it.
        // (Bound to the task's current thread/turn, as the host requires.)
        assert!(
            s.record_agent_message("old-task", "th-0", "tu-0", "i-new", "fresh reply")
                .unwrap()
        );
        let page = s.messages("old-task", None, None, 50).unwrap();
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        assert_eq!(page["legacy"]["text"], "legacy accumulated output");
    }
    // C10: a task created at schema 7 is structured from the start and never
    // carries a synthesized legacy block.
    #[test]
    fn new_task_has_no_pseudo_legacy_text() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m10");
        let page = s.messages("m10", None, None, 50).unwrap();
        assert_eq!(page["historyMode"], "STRUCTURED_FROM_START");
        assert!(page["legacy"].is_null());
        assert_eq!(page["items"].as_array().unwrap()[0]["text"], "build m10");
        // Old response fields are preserved verbatim.
        assert_eq!(page["coverage"], "BOUNDED_PUBLIC_MESSAGES");
        assert_eq!(page["messageSchemaVersion"], "native-messages/1");
    }
    // C11: newest/backward/forward/empty pages keep sequence order and
    // directionally honest hasMore/cursor under a byte budget too.
    #[test]
    fn message_pages_paginate_in_both_directions() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m11");
        for i in 0..11 {
            assert!(
                s.record_agent_message(
                    "m11",
                    "th-1",
                    "tu-1",
                    &format!("i-{i}"),
                    &format!("reply {i}")
                )
                .unwrap()
            );
        }
        // Default recent page: newest 4, ascending.
        let page = s.messages("m11", None, None, 4).unwrap();
        let seqs: Vec<i64> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["sequence"].as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(seqs, vec![9, 10, 11, 12]);
        assert_eq!(page["hasMore"], true);
        assert_eq!(page["firstSequence"], "9");
        // Backward walk keeps descending windows, still ascending output.
        let page = s.messages("m11", None, Some(9), 4).unwrap();
        let seqs: Vec<i64> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["sequence"].as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(seqs, vec![5, 6, 7, 8]);
        assert_eq!(page["hasMore"], true);
        let page = s.messages("m11", None, Some(5), 4).unwrap();
        let seqs: Vec<i64> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["sequence"].as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        assert_eq!(page["hasMore"], false);
        // Forward walk from 0 returns the oldest first.
        let page = s.messages("m11", Some(0), None, 4).unwrap();
        let seqs: Vec<i64> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["sequence"].as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        assert_eq!(page["cursor"], "4");
        // Empty forward page keeps the caller's cursor.
        let page = s.messages("m11", Some(12), None, 4).unwrap();
        assert!(page["items"].as_array().unwrap().is_empty());
        assert_eq!(page["cursor"], "12");
        assert_eq!(page["hasMore"], false);
        // Empty before page answers cursor=null.
        let page = s.messages("m11", None, Some(1), 4).unwrap();
        assert!(page["items"].as_array().unwrap().is_empty());
        assert!(page["cursor"].is_null());
        assert_eq!(page["hasMore"], false);
    }
    // C11b: the 256 KiB items budget stops a page early with at least one
    // item and an honest hasMore in both directions.
    #[test]
    fn message_page_byte_budget_keeps_at_least_one_item() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m12");
        let fat = "x".repeat(60 * 1024);
        for i in 0..6 {
            assert!(
                s.record_agent_message("m12", "th-1", "tu-1", &format!("i-{i}"), &fat)
                    .unwrap()
            );
        }
        // 60 KiB texts: only a few fit the 256 KiB items budget.
        let page = s.messages("m12", None, None, 10).unwrap();
        let n = page["items"].as_array().unwrap().len();
        assert!((1..=4).contains(&n));
        assert_eq!(page["hasMore"], true);
        assert!(serde_json::to_vec(&page["items"]).unwrap().len() <= 256 * 1024);
        // Backward pages keep the LARGEST candidates under the budget.
        let seqs: Vec<i64> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["sequence"].as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(*seqs.last().unwrap(), 7);
        let page = s.messages("m12", Some(0), None, 10).unwrap();
        let seqs: Vec<i64> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["sequence"].as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(*seqs.first().unwrap(), 1);
    }
    // C13: unicode roundtrips, over-limit text records truncation honestly,
    // and i64-wide sequences page correctly.
    #[test]
    fn unicode_truncation_and_large_sequences_survive() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        msg_create(&mut s, "m13");
        assert!(
            s.record_agent_message("m13", "th-1", "tu-1", "i-u", "héllo wörld — 你好 🦀")
                .unwrap()
        );
        // 64 KiB+ text is stored truncated on a char boundary with the flag.
        let long: String = std::iter::repeat('漢').take(30_000).collect(); // ~90 KiB
        assert!(
            s.record_agent_message("m13", "th-1", "tu-1", "i-t", &long)
                .unwrap()
        );
        // Escape-heavy text shrinks until the JSON form fits 128 KiB.
        let esc = "\"".repeat(70_000);
        assert!(
            s.record_agent_message("m13", "th-1", "tu-1", "i-e", &esc)
                .unwrap()
        );
        // Move the sequence far out, then keep paging correctly.
        s.db.execute("UPDATE public_transcript_meta SET next_sequence=9223372036854775700 WHERE task_id='m13'", []).unwrap();
        assert!(
            s.record_agent_message("m13", "th-1", "tu-1", "i-big", "far out")
                .unwrap()
        );
        let page = s.messages("m13", None, None, 50).unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(
            items.iter().find(|i| i["nativeItemId"] == "i-u").unwrap()["text"],
            "héllo wörld — 你好 🦀"
        );
        let t = items.iter().find(|i| i["nativeItemId"] == "i-t").unwrap();
        assert_eq!(t["textTruncated"], true);
        assert!(t["text"].as_str().unwrap().len() <= 64 * 1024);
        let e = items.iter().find(|i| i["nativeItemId"] == "i-e").unwrap();
        assert_eq!(e["textTruncated"], true);
        assert!(serde_json::to_string(&e["text"]).unwrap().len() <= 128 * 1024);
        let big = items.iter().find(|i| i["nativeItemId"] == "i-big").unwrap();
        assert_eq!(big["sequence"], "9223372036854775700");
        let page = s
            .messages("m13", Some(9223372036854775699), None, 10)
            .unwrap();
        assert_eq!(
            page["items"].as_array().unwrap()[0]["nativeItemId"],
            "i-big"
        );
        assert_eq!(page["hasMore"], false);
        let page = s
            .messages("m13", None, Some(9223372036854775700), 3)
            .unwrap();
        assert_eq!(page["items"].as_array().unwrap().len(), 3);
        assert!(serde_json::to_vec(&page).unwrap().len() <= 1024 * 1024);
    }
    // ===== CA-ANDROID-V2 deterministic store tests =====
    // C2: stream meta + monotonic seqs + kind coverage + both paging modes.
    #[test]
    fn child_messages_capture_stream_and_paginate_both_ways() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"cm-task","state":"RUNNING"})).unwrap();
        collab_seed(
            &mut s,
            "cm-task",
            "collab-1",
            "k-cm",
            vec![collab_child_spec("writer", &[])],
        );
        let msg = |kind: &'static str, text: &'static str, status: &'static str| {
            ChildMsg {
                kind,
                text,
                status,
                event_ref: None,
            }
        };
        // Aux stream: ids by the aux id itself.
        s.child_message("cm-task", None, "aux-9", &msg("ASSIGNMENT", "q?", "PENDING"))
            .unwrap();
        s.child_message("cm-task", None, "aux-9", &msg("STATUS", "RUNNING", "RUNNING"))
            .unwrap();
        // Collab stream: collab:{collab}:{child}.
        for _ in 0..7 {
            s.child_message(
                "cm-task",
                Some("collab-1"),
                "writer",
                &msg("ASSISTANT_MESSAGE", "progress note", "RUNNING"),
            )
            .unwrap();
        }
        s.child_message(
            "cm-task",
            Some("collab-1"),
            "writer",
            &msg("RETURN", "done", "SUCCEEDED"),
        )
        .unwrap();
        let meta = s
            .child_stream("cm-task", Some("collab-1"), "writer")
            .unwrap()
            .unwrap();
        assert_eq!(meta["streamId"], "collab:collab-1:writer");
        assert_eq!(meta["latestSeq"], "8");
        assert_eq!(meta["ownerKind"], "COLLAB");
        // Newest page (no cursor): descending fetch re-ordered ascending,
        // hasMoreBefore marks the cut prefix.
        let page = s
            .child_messages("cm-task", Some("collab-1"), "writer", None, None, 5)
            .unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 5);
        assert_eq!(items[0]["seq"], "4");
        assert_eq!(items[4]["seq"], "8");
        assert_eq!(items[4]["kind"], "RETURN");
        assert_eq!(page["hasMoreBefore"], true);
        assert_eq!(page["hasMoreAfter"], false);
        // Forward cursor page continues strictly after seq 5.
        let page2 = s
            .child_messages("cm-task", Some("collab-1"), "writer", Some(5), None, 50)
            .unwrap();
        let seqs: Vec<&str> = page2["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["seq"].as_str().unwrap())
            .collect();
        assert_eq!(seqs, ["6", "7", "8"]);
        assert_eq!(page2["hasMoreAfter"], false);
        // `before` reads strictly older seqs.
        let page3 = s
            .child_messages("cm-task", Some("collab-1"), "writer", None, Some(5), 2)
            .unwrap();
        let seqs: Vec<&str> = page3["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["seq"].as_str().unwrap())
            .collect();
        assert_eq!(seqs, ["3", "4"]);
        assert_eq!(page3["hasMoreBefore"], true);
        // Aux stream wrote seqs 1,2.
        let apage = s
            .child_messages("cm-task", None, "aux-9", None, None, 50)
            .unwrap();
        assert_eq!(apage["items"].as_array().unwrap().len(), 2);
        assert_eq!(apage["items"][0]["kind"], "ASSIGNMENT");
        // No captured stream: hasMeta is false, items empty.
        let empty = s
            .child_messages("cm-task", None, "ghost", None, None, 50)
            .unwrap();
        assert_eq!(empty["hasMeta"], false);
        assert_eq!(empty["items"].as_array().unwrap().len(), 0);
    }
    // C2 atomicity: a commit failure rolls back the state row AND the stream
    // fact together — a half-captured terminal is impossible.
    #[test]
    fn transition_msgs_commit_atomically_or_not_at_all() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"cm-atomic"})).unwrap();
        let aux = json!({"auxSessionId":"aux-a","question":"q","workspace":"D:/a","createdAt":now()});
        s.accept_aux(
            "cm-atomic",
            &aux,
            "k-a",
            &json!({"requestKey":"k-a"}),
            &json!({}),
        )
        .unwrap();
        s.fail_next_commit();
        assert!(
            s.aux_transition_msgs(
                "cm-atomic",
                "aux-a",
                &["RUNNING"],
                "FAILED",
                Some("X"),
                |_| {},
                &[ChildMsg {
                    kind: "STATUS",
                    text: "FAILED",
                    status: "FAILED",
                    event_ref: None,
                }],
            )
            .is_err()
        );
        // Nothing landed: the row is still open, no stream was minted.
        assert_eq!(s.aux_session("cm-atomic", "aux-a").unwrap()["state"], "RUNNING");
        assert!(
            s.child_stream("cm-atomic", None, "aux-a")
                .unwrap()
                .is_none()
        );
        // The same call afterwards commits both halves.
        assert!(
            s.aux_transition_msgs(
                "cm-atomic",
                "aux-a",
                &["RUNNING"],
                "FAILED",
                Some("X"),
                |_| {},
                &[ChildMsg {
                    kind: "STATUS",
                    text: "FAILED",
                    status: "FAILED",
                    event_ref: None,
                }],
            )
            .unwrap()
        );
        let page = s
            .child_messages("cm-atomic", None, "aux-a", None, None, 10)
            .unwrap();
        assert_eq!(page["items"].as_array().unwrap()[0]["kind"], "STATUS");
        // A stale `from` guard refuses to move a terminal row and writes no msg.
        assert!(
            !s.aux_transition_msgs(
                "cm-atomic",
                "aux-a",
                &["RUNNING"],
                "SUCCEEDED",
                None,
                |_| {},
                &[],
            )
            .unwrap()
        );
    }
    // C3/C4: frozen preflight and preview rows persist digest+expiry facts and
    // read back with honest `expired` computation.
    #[test]
    fn collab_preflight_and_preview_rows_round_trip() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"cm-pf","generation":"g","segmentId":"seg"}))
            .unwrap();
        let row = json!({
            "preflightId":"pf-1","planDigest":"digest-1",
            "plan":{"children":[{"childId":"a","goalSha256":"g-a"}]},
            "sourceBindings":{"taskId":"cm-pf","generation":"g","segmentId":"seg"},
            "allowedScopes":[],"budgetTerms":[],
            "expiresAt":now_plus_ms(60_000),
        });
        s.accept_preflight(
            "cm-pf",
            &row,
            "k-pf",
            &json!({"requestKey":"k-pf","children":[]}),
            &json!({"kind":"WORKER_LOCAL","requestKey":"k-pf","state":"APPLIED","preflightId":"pf-1"}),
        )
        .unwrap();
        let got = s.collab_preflight("cm-pf", "pf-1").unwrap().unwrap();
        assert_eq!(got["planDigest"], "digest-1");
        assert_eq!(got["expired"], false);
        assert_eq!(got["sourceBindings"]["generation"], "g");
        // The idempotent request row replays.
        assert!(
            s.existing("cm-pf", "k-pf", &json!({"requestKey":"k-pf","children":[]}))
                .unwrap()
                .is_some()
        );
        // Unknown preflight resolves to None (PREFLIGHT_NOT_FOUND is a route
        // concern, the reader stays honest).
        assert!(s.collab_preflight("cm-pf", "pf-ghost").unwrap().is_none());
        let pv = json!({
            "previewId":"pv-1","collaborationId":"collab-1","childId":"a",
            "planDigest":"pd-1","files":[{"path":"x","kind":"CREATE"}],
            "paths":["x"],"candidateDigest":"cd-1",
            "expiresAt":now_plus_ms(60_000),
        });
        s.accept_preview(
            "cm-pf",
            &pv,
            "k-pv",
            &json!({"requestKey":"k-pv","childId":"a","mode":"FILES","paths":["x"]}),
            &json!({"kind":"WORKER_LOCAL","requestKey":"k-pv","state":"APPLIED","previewId":"pv-1"}),
        )
        .unwrap();
        let got = s.collab_preview("cm-pf", "pv-1").unwrap();
        assert_eq!(got["candidateDigest"], "cd-1");
        assert_eq!(got["expired"], false);
        assert!(s.collab_preview("cm-pf", "pv-ghost").is_err());
    }
    // C5: seal commits task.sealedFor + seal row + request + event atomically;
    // lookups resolve by seal id and by public handoff id.
    #[test]
    fn source_seal_marks_task_and_exports_deterministically() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"cm-seal","state":"SUCCEEDED","generation":"g","segmentId":"seg","requestKey":"create-k","stateVersion":"1"}))
            .unwrap();
        collab_seed(
            &mut s,
            "cm-seal",
            "collab-1",
            "k-cs",
            vec![collab_child_spec("writer", &[])],
        );
        s.child_message(
            "cm-seal",
            Some("collab-1"),
            "writer",
            &ChildMsg {
                kind: "RETURN",
                text: "done",
                status: "SUCCEEDED",
                event_ref: None,
            },
        )
        .unwrap();
        let seal = json!({
            "sealId":"seal-1","handoffId":"wh-1",
            "targetRef":{"kind":"SEGMENT","segmentId":"seg"},
            "proof":{"sealedBy":"codex"},
            "affectedChildren":[{"kind":"COLLABORATION_CHILD","collaborationId":"collab-1","childId":"writer","newState":"CANCELLED"}],
            "sealedAt":now(),
        });
        s.seal_source(
            "cm-seal",
            &seal,
            "k-seal",
            &json!({"requestKey":"k-seal"}),
            &json!({"kind":"WORKER_LOCAL","requestKey":"k-seal","state":"APPLIED","sealId":"seal-1"}),
        )
        .unwrap();
        let t = s.task("cm-seal").unwrap();
        assert_eq!(t["sealedFor"]["sealId"], "seal-1");
        let got = s.source_seal("cm-seal", "seal-1").unwrap();
        assert_eq!(got["handoffId"], "wh-1");
        assert_eq!(
            got["affectedChildren"][0]["childId"], "writer"
        );
        let (task, by_handoff) = s.seal_by_handoff("wh-1").unwrap().unwrap();
        assert_eq!(task, "cm-seal");
        assert_eq!(by_handoff["sealId"], "seal-1");
        assert!(s.seal_by_handoff("wh-ghost").unwrap().is_none());
        assert_eq!(s.task_seal("cm-seal").unwrap().unwrap()["sealId"], "seal-1");
        // Export parts are deterministic for a fixed durable state and carry
        // only the bounded canonical projection.
        let m1 = s.export_part("cm-seal", "manifest").unwrap();
        let m2 = s.export_part("cm-seal", "manifest").unwrap();
        assert_eq!(m1, m2);
        let manifest: Value = serde_json::from_slice(&m1).unwrap();
        assert_eq!(manifest["manifest"]["sealedFor"]["sealId"], "seal-1");
        let cm = s.export_part("cm-seal", "child-messages").unwrap();
        let cmp: Value = serde_json::from_slice(&cm).unwrap();
        assert_eq!(
            cmp["items"][0]["streamId"], "collab:collab-1:writer"
        );
        for part in ["messages", "events", "auxiliary", "collaborations", "artifacts", "requests"] {
            assert!(s.export_part("cm-seal", part).is_ok(), "{part}");
        }
        assert!(s.export_part("cm-seal", "nope").is_err());
    }
    // C6: material intake/binding refuses unsealed or mismatched refs, binds
    // atomically inside the admission tx and consumes only on demand.
    #[test]
    fn material_bind_is_atomic_with_admission_and_consumption_is_explicit() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"cm-mat","state":"RUNNING","stateVersion":"1"}))
            .unwrap();
        let row = |key: &str, state: &str, sha: &str| {
            json!({"uploadKey":key,"projectId":"p","target":{"taskId":"cm-mat"},
                   "displayName":"n","mediaType":"text/plain","sizeBytes":"5",
                   "sha256":sha,"state":state,"createdAt":now(),"requestKey":"k-create"})
        };
        let sha_a = "a".repeat(64);
        s.accept_material(
            "materials",
            "k-m1",
            &json!({"requestKey":"k-m1"}),
            &row("mat-1", "SEALED", &sha_a),
        )
        .unwrap();
        s.accept_material(
            "materials",
            "k-m2",
            &json!({"requestKey":"k-m2"}),
            &row("mat-2", "UPLOADING", &sha_a),
        )
        .unwrap();
        // UPLOADING material refuses the whole admission — no task row, no
        // request row, no message row.
        assert!(
            s.accept_user(
                "cm-mat",
                "k-msg",
                &json!({"requestKey":"k-msg"}),
                &json!({"state":"ACCEPTED"}),
                "hello",
                true,
                &[("mat-2".into(), sha_a.clone(), "REFERENCE_TEXT".into())],
            )
            .is_err()
        );
        // A wrong digest refuses likewise.
        let sha_b = "b".repeat(64);
        assert!(
            s.accept_user(
                "cm-mat",
                "k-msg",
                &json!({"requestKey":"k-msg"}),
                &json!({"state":"ACCEPTED"}),
                "hello",
                true,
                &[("mat-1".into(), sha_b, "REFERENCE_TEXT".into())],
            )
            .is_err()
        );
        // Role/media mismatch refuses: an IMAGE role against a text upload.
        assert!(
            s.accept_user(
                "cm-mat",
                "k-msg",
                &json!({"requestKey":"k-msg"}),
                &json!({"state":"ACCEPTED"}),
                "hello",
                true,
                &[("mat-1".into(), sha_a.clone(), "IMAGE".into())],
            )
            .is_err()
        );
        // Valid sealed ref binds inside the same admission commit.
        s.accept_user(
            "cm-mat",
            "k-msg",
            &json!({"requestKey":"k-msg"}),
            &json!({"state":"ACCEPTED"}),
            "hello",
            true,
            &[("mat-1".into(), sha_a.clone(), "REFERENCE_TEXT".into())],
        )
        .unwrap();
        let m = s.material("mat-1").unwrap();
        assert_eq!(m["state"], "BOUND");
        assert_eq!(m["boundTaskId"], "cm-mat");
        assert_eq!(m["boundRequestKey"], "k-msg");
        let bound = s.request_materials("cm-mat", "k-msg").unwrap();
        assert_eq!(bound.len(), 1);
        s.materials_consume("cm-mat", "k-msg").unwrap();
        assert_eq!(s.material("mat-1").unwrap()["state"], "CONSUMED");
        // Consuming again is a no-op; the durable fact is already settled.
        s.materials_consume("cm-mat", "k-msg").unwrap();
        // Project listing exposes the durable rows.
        assert_eq!(s.materials("p").unwrap().len(), 2);
    }
    // C5 stop-mark variant: collab_stop_mark writes the BLOCKED stream fact
    // inside the same transaction that settles the row.
    #[test]
    fn collab_stop_mark_writes_stream_status_atomically() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"cm-stop"})).unwrap();
        collab_seed(
            &mut s,
            "cm-stop",
            "collab-1",
            "k-stop",
            vec![
                collab_child_spec("runner", &[]),
                collab_child_spec("after", &["runner"]),
            ],
        );
        s.collab_child_transition("cm-stop", "collab-1", "runner", &["PENDING"], "RUNNING", None, |_| {})
            .unwrap();
        let running = s
            .collab_stop_mark("cm-stop", "collab-1", "PARENT_STOP")
            .unwrap();
        assert_eq!(running, vec!["runner"]);
        let children = s.collab_children("cm-stop", "collab-1").unwrap();
        assert_eq!(
            children
                .iter()
                .find(|c| c["childId"] == "after")
                .unwrap()["state"],
            "BLOCKED"
        );
        // The stream fact for the blocked dependent landed atomically.
        let page = s
            .child_messages("cm-stop", Some("collab-1"), "after", None, None, 10)
            .unwrap();
        assert_eq!(page["items"].as_array().unwrap()[0]["kind"], "STATUS");
        assert_eq!(page["items"].as_array().unwrap()[0]["status"], "BLOCKED");
    }
    // Recover writes a durable restart-reconciliation STATUS for children that
    // were open across the restart — coverage stays truthful.
    #[test]
    fn recover_writes_restart_status_messages() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        s.save(&json!({"taskId":"cm-rec","state":"RUNNING","quiescent":false}))
            .unwrap();
        let aux = json!({"auxSessionId":"aux-r","question":"q","workspace":"D:/a","createdAt":now()});
        s.accept_aux(
            "cm-rec",
            &aux,
            "k-r",
            &json!({"requestKey":"k-r"}),
            &json!({}),
        )
        .unwrap();
        s.recover().unwrap();
        let page = s
            .child_messages("cm-rec", None, "aux-r", None, None, 10)
            .unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["kind"], "STATUS");
        assert_eq!(items[0]["status"], "CANCELLED");
    }
}
