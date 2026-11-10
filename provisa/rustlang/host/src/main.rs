// Copyright (c) 2026 Kenneth Stott
// Canary: 62676b7c-29d7-4af6-a62c-141e77d846d1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

//! The host a Rust source's worker runs (REQ-2227): the protocol of every code source
//! (provisa/codesource/worker.py), in Rust.
//!
//!     host <socket path | fd:N[,M...]>
//!
//! Like a Go source's host, it is compiled with the code linked in (provisa/rustlang/build.py):
//! one static binary per code hash, whose generated exports.rs calls each tagged function. It
//! connects to the socket PROVISA_HOST_CONNECTIONS times (or takes the connections it inherited,
//! already connected), says on each which functions it has, and serves every connection's calls
//! until one closes, when it exits. Each call runs on a thread of its own; a cancelled call sends
//! nothing more, and stops at its next row.
//!
//! Rows go as Arrow IPC batches against the schema the call gives (REQ-2098,
//! provisa/codesource/arrow_rows.py); a table's in batches of 1000, a stream's each as it is
//! yielded. What the code prints goes to stderr, which Provisa keeps to name a crash.

mod exports;
mod glue;
mod rows;
mod wire;

use std::collections::HashMap;
use std::io::{self, BufReader, Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use base64::Engine;
use serde_json::{json, Value};

use glue::{Answer, Args, Export};

/// The protocol this host speaks (provisa/codesource/worker.py PROTOCOL).
const PROTOCOL: u64 = 4;
const CONNECTIONS_ENV: &str = "PROVISA_HOST_CONNECTIONS";
const CODE_HASH_ENV: &str = "PROVISA_CODE_HASH";
const JSON_FRAME: u8 = b'J';
const ARROW_FRAME: u8 = b'A';
/// A table's rows go in batches of this many.
const BATCH: usize = 1000;

fn registry() -> &'static HashMap<&'static str, Export> {
    static REGISTRY: OnceLock<HashMap<&'static str, Export>> = OnceLock::new();
    REGISTRY.get_or_init(|| exports::exports().into_iter().collect())
}

fn export_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = registry().keys().copied().collect();
    names.sort_unstable();
    names
}

/// One running call's state: whether Provisa cancelled it, and the frames it may still send.
struct Running {
    cancelled: AtomicBool,
    credits: Mutex<u64>,
    wake: Condvar,
}

impl Running {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.wake.notify_all();
    }

    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn credit(&self, n: u64) {
        *self.credits.lock().unwrap() += n;
        self.wake.notify_all();
    }

    /// One credit, once there is one: false when the call is cancelled meanwhile.
    fn take(&self) -> bool {
        let mut credits = self.credits.lock().unwrap();
        while *credits == 0 && !self.cancelled() {
            credits = self.wake.wait_timeout(credits, Duration::from_millis(200)).unwrap().0;
        }
        if self.cancelled() {
            return false;
        }
        *credits -= 1;
        true
    }
}

/// One of the host's connections to Provisa, and the calls running on it.
struct Connection {
    writer: Mutex<UnixStream>,
    calls: Mutex<HashMap<u64, Arc<Running>>>,
}

impl Connection {
    fn frame(&self, kind: u8, body: &[u8]) -> io::Result<()> {
        let length = u32::try_from(body.len() + 1).map_err(|_| io::Error::other("a frame too large"))?;
        let mut head = length.to_be_bytes().to_vec();
        head.push(kind);
        let mut writer = self.writer.lock().unwrap();
        writer.write_all(&head)?;
        writer.write_all(body)
    }

    fn send(&self, message: Value) -> io::Result<()> {
        self.frame(JSON_FRAME, message.to_string().as_bytes())
    }

    fn send_rows(&self, id: u64, ipc: &[u8]) -> io::Result<()> {
        let mut body = id.to_be_bytes().to_vec();
        body.extend_from_slice(ipc);
        self.frame(ARROW_FRAME, &body)
    }
}

fn read_message(reader: &mut impl Read) -> io::Result<Value> {
    let mut head = [0u8; 4];
    reader.read_exact(&mut head)?;
    let mut body = vec![0u8; u32::from_be_bytes(head) as usize];
    reader.read_exact(&mut body)?;
    if body.first() != Some(&JSON_FRAME) {
        return Err(io::Error::other("a frame of unknown kind"));
    }
    serde_json::from_slice(&body[1..]).map_err(io::Error::other)
}

fn texts(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().map(|i| i.as_str().unwrap_or_default().to_string()).collect())
        .unwrap_or_default()
}

/// One set of a call's arguments: their values and their parameters' names.
struct ArgumentSet {
    args: Vec<Value>,
    names: Vec<String>,
}

/// How a host ends: Provisa refused it, or a connection failed.
enum Ended {
    Refused(String),
    Failed(String),
}

fn serve(conn: Arc<Connection>, stream: UnixStream) -> Result<(), Ended> {
    let failed = |e: io::Error| Ended::Failed(e.to_string());
    conn.send(json!({
        "op": "hello",
        "protocol": PROTOCOL,
        "code_hash": std::env::var(CODE_HASH_ENV).unwrap_or_default(),
        "exports": export_names(),
    }))
    .map_err(failed)?;
    let mut reader = BufReader::with_capacity(1 << 16, stream);
    loop {
        let message = match read_message(&mut reader) {
            Ok(message) => message,
            Err(_) => {
                // The socket closed: Provisa stopped the worker.
                for running in conn.calls.lock().unwrap().values() {
                    running.cancel();
                }
                return Ok(());
            }
        };
        let id = message.get("id").and_then(Value::as_u64).unwrap_or(0);
        match message.get("op").and_then(Value::as_str).unwrap_or_default() {
            "refused" => {
                let said = message.get("message").and_then(Value::as_str).unwrap_or_default();
                return Err(Ended::Refused(said.to_string()));
            }
            "ping" => conn.send(json!({"op": "pong", "id": id})).map_err(failed)?,
            "cancel" => {
                if let Some(running) = conn.calls.lock().unwrap().get(&id) {
                    running.cancel();
                }
            }
            "credit" => {
                if let Some(running) = conn.calls.lock().unwrap().get(&id) {
                    running.credit(message.get("n").and_then(Value::as_u64).unwrap_or(0));
                }
            }
            "call" => {
                let window = message.get("window").and_then(Value::as_u64).unwrap_or(0);
                let running = Arc::new(Running {
                    cancelled: AtomicBool::new(false),
                    credits: Mutex::new(window),
                    wake: Condvar::new(),
                });
                conn.calls.lock().unwrap().insert(id, running.clone());
                let conn = conn.clone();
                thread::spawn(move || run(&conn, id, &message, &running));
            }
            _ => {}
        }
    }
}

fn cpu_ms() -> f64 {
    // SAFETY: getrusage fills the struct it is given and reads nothing else.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0.0;
    }
    let ms = |t: libc::timeval| t.tv_sec as f64 * 1000.0 + t.tv_usec as f64 / 1000.0;
    ms(usage.ru_utime) + ms(usage.ru_stime)
}

fn panic_text(panicked: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = panicked.downcast_ref::<&str>() {
        return format!("panic: {text}");
    }
    if let Some(text) = panicked.downcast_ref::<String>() {
        return format!("panic: {text}");
    }
    "panic".to_string()
}

fn run(conn: &Connection, id: u64, message: &Value, running: &Running) {
    let started = cpu_ms();
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| call(conn, id, message, running)));
    conn.calls.lock().unwrap().remove(&id);
    if running.cancelled() {
        return; // Provisa stopped the call and is no longer waiting for it
    }
    let failure = match outcome {
        Ok(Ok(())) => None,
        Ok(Err(message)) => Some(message),
        Err(panicked) => Some(panic_text(panicked.as_ref())),
    };
    let _ = match failure {
        Some(said) => conn.send(json!({"op": "error", "id": id, "message": said, "stack": ""})),
        None => conn.send(json!({"op": "end", "id": id, "cpu_ms": cpu_ms() - started})),
    };
}

fn call(conn: &Connection, id: u64, message: &Value, running: &Running) -> Result<(), String> {
    let export = message.get("export").and_then(Value::as_str).unwrap_or_default();
    let kind = message.get("kind").and_then(Value::as_str).unwrap_or_default();
    let schema = match message.get("schema").and_then(Value::as_str) {
        None => None,
        Some(text) => Some(
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .map_err(|e| e.to_string())
                .and_then(rows::read_schema)
                .map_err(|e| format!("the call's schema cannot be read: {e}"))?,
        ),
    };
    let function = registry().get(export).ok_or_else(|| format!("the code has no function {export}"))?;
    if message.get("keyed") == Some(&Value::Bool(true)) {
        return Err(format!("{export}: a Rust source has no @loader"));
    }
    if texts(message.get("takes")).iter().any(|t| t == "context") {
        return Err(format!("{export}: a Rust source has no read context"));
    }
    let sets: Vec<ArgumentSet> = match message.get("calls").and_then(Value::as_array) {
        // REQ-2099: a table called once per argument set, in order, under the one call id.
        Some(sets) => sets
            .iter()
            .map(|set| ArgumentSet {
                args: set.get("args").and_then(Value::as_array).cloned().unwrap_or_default(),
                names: texts(set.get("names")),
            })
            .collect(),
        None => vec![ArgumentSet {
            args: message.get("args").and_then(Value::as_array).cloned().unwrap_or_default(),
            names: texts(message.get("names")),
        }],
    };
    for set in &sets {
        let given = Args { export, args: &set.args, names: &set.names };
        let answer = function(&given)?;
        match &schema {
            None => answer_value(conn, id, answer)?,
            Some(schema) => answer_rows(conn, id, kind, export, schema, answer, running)?,
        }
        if running.cancelled() {
            return Ok(());
        }
    }
    Ok(())
}

/// A single value, as its value message: rows given where a value is asked for are its list.
fn answer_value(conn: &Connection, id: u64, answer: Answer) -> Result<(), String> {
    let value = match answer {
        Answer::Nothing => Value::Null,
        Answer::Value(value) => value,
        Answer::Rows(rows) => Value::Array(rows.collect::<Result<Vec<Value>, String>>()?),
    };
    conn.send(json!({"op": "value", "id": id, "value": wire::to_wire(value)})).map_err(|e| e.to_string())
}

/// Rows, as Arrow frames, each sent once the call has a credit for it: the function is pulled no
/// further than its reader's window ahead.
fn answer_rows(
    conn: &Connection,
    id: u64,
    kind: &str,
    export: &str,
    schema: &arrow_schema::SchemaRef,
    answer: Answer,
    running: &Running,
) -> Result<(), String> {
    let source: glue::Rows = match answer {
        Answer::Rows(rows) => rows,
        Answer::Value(Value::Array(items)) => Box::new(items.into_iter().map(Ok)),
        Answer::Value(_) => return Err(format!("{export} returned a value, not a collection of rows")),
        Answer::Nothing => return Err(format!("{export} returned no rows")),
    };
    let size = if kind == "stream" { 1 } else { BATCH };
    let send = |batch: &[Value]| -> Result<bool, String> {
        let ipc = rows::ipc(schema, batch)?;
        if !running.take() {
            return Ok(false);
        }
        conn.send_rows(id, &ipc).map_err(|e| e.to_string())?;
        Ok(true)
    };
    let mut batch = Vec::with_capacity(size);
    for row in source {
        if running.cancelled() {
            return Ok(());
        }
        batch.push(row?);
        if batch.len() >= size {
            if !send(&batch)? {
                return Ok(());
            }
            batch.clear();
        }
    }
    if !batch.is_empty() {
        send(&batch)?;
    }
    Ok(())
}

fn connections(address: &str) -> Result<Vec<UnixStream>, String> {
    if let Some(descriptors) = address.strip_prefix("fd:") {
        // The Linux sandbox refuses the host any new socket but TCP: Provisa hands it these.
        return descriptors
            .split(',')
            .map(|fd| {
                let n: i32 = fd.parse().map_err(|_| format!("fd:{fd} is not a descriptor"))?;
                // SAFETY: the descriptor is a connected socket Provisa handed this process.
                Ok(unsafe { UnixStream::from_raw_fd(n) })
            })
            .collect();
    }
    let count: usize = std::env::var(CONNECTIONS_ENV)
        .ok()
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| format!("{CONNECTIONS_ENV} is not a number of connections"))?;
    (0..count).map(|_| UnixStream::connect(address).map_err(|e| e.to_string())).collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: host <socket path | fd:N[,M...]>");
        std::process::exit(2);
    }
    // What the code prints goes to stderr; the protocol is the socket's alone.
    // SAFETY: dup2 of two descriptors this process holds.
    unsafe {
        libc::dup2(2, 1);
    }
    let streams = match connections(&args[1]) {
        Ok(streams) => streams,
        Err(failed) => {
            eprintln!("{failed}");
            std::process::exit(1);
        }
    };
    let (done, ended) = mpsc::channel();
    for stream in streams {
        let writer = match stream.try_clone() {
            Ok(writer) => writer,
            Err(failed) => {
                eprintln!("{failed}");
                std::process::exit(1);
            }
        };
        let conn = Arc::new(Connection { writer: Mutex::new(writer), calls: Mutex::new(HashMap::new()) });
        let done = done.clone();
        thread::spawn(move || {
            let _ = done.send(serve(conn, stream));
        });
    }
    // One connection closing is Provisa stopping the worker: the others' calls end with it.
    match ended.recv() {
        Ok(Err(Ended::Refused(said))) => {
            eprintln!("provisa refused this host: {said}");
            std::process::exit(2);
        }
        Ok(Err(Ended::Failed(failed))) => {
            eprintln!("{failed}");
            std::process::exit(1);
        }
        // Threads still running a cancelled call do not hold the process open.
        _ => std::process::exit(0),
    }
}
