//! Private macOS process guardian. No model/configuration/credential data crosses it.
use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    os::unix::process::CommandExt,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub fn internal_command() -> Option<std::io::Result<()>> {
    let mut args = std::env::args_os().skip(1);
    match args.next()?.to_str()? {
        mode @ ("--nwf-internal-exec" | "--nwf-internal-exec-no-fork") => Some((|| {
            let executable = args
                .next()
                .ok_or_else(|| std::io::Error::other("missing executable"))?;
            // Spawn returns after exec of this wrapper. The host then registers
            // its PID with kqueue before continuing it into the real engine.
            unsafe {
                libc::raise(libc::SIGSTOP);
            }
            if mode == "--nwf-internal-exec-no-fork" {
                Err(Command::new("/usr/bin/sandbox-exec")
                    .args(["-p", "(version 1) (allow default) (deny process-fork)"])
                    .arg(executable)
                    .args(args)
                    .exec())
            } else {
                Err(Command::new(executable).args(args).exec())
            }
        })()),
        "--nwf-internal-guard" => Some(guard(args.next().map(std::path::PathBuf::from))),
        _ => None,
    }
}
fn guard(marker: Option<std::path::PathBuf>) -> std::io::Result<()> {
    // Covers EOF before PID registration and every setup/monitor error. The
    // wrapper may already exist and be SIGSTOPped in this group.
    struct KillOwnedGroup;
    impl Drop for KillOwnedGroup {
        fn drop(&mut self) {
            unsafe {
                libc::kill(-libc::getpgrp(), libc::SIGKILL);
            }
        }
    }
    let _cleanup = KillOwnedGroup;
    let marker = marker.ok_or_else(|| std::io::Error::other("missing marker"))?;
    let mut input = BufReader::new(std::io::stdin());
    let mut line = String::new();
    input.read_line(&mut line)?;
    let pid: usize = line.trim().parse().map_err(std::io::Error::other)?;
    if pid > i32::MAX as usize || unsafe { libc::getpgid(pid as i32) } != unsafe { libc::getpgrp() }
    {
        return Err(std::io::Error::other("root outside owned group"));
    }
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let registration = libc::kevent {
        ident: pid,
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
        fflags: libc::NOTE_FORK | libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    if unsafe {
        libc::kevent(
            queue,
            &registration,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    println!("READY");
    std::io::stdout().flush()?;
    let seal = Arc::new(AtomicBool::new(false));
    let request = seal.clone();
    std::thread::spawn(move || {
        let mut line = String::new();
        loop {
            line.clear();
            match input.read_line(&mut line) {
                Ok(0) | Err(_) => {
                    unsafe {
                        libc::kill(-libc::getpgrp(), libc::SIGKILL);
                    }
                    return;
                }
                Ok(_) if line.trim() == "SEAL" => request.store(true, Ordering::SeqCst),
                _ => (),
            }
        }
    });
    let forked = AtomicBool::new(false);
    let taint = || -> std::io::Result<()> {
        forked.store(true, Ordering::SeqCst);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&marker)?;
        file.write_all(b"FORK_OR_MONITOR_UNPROVEN\n")?;
        file.sync_all()
    };
    loop {
        let mut event: libc::kevent = unsafe { std::mem::zeroed() };
        let wait = libc::timespec {
            tv_sec: 0,
            tv_nsec: 20_000_000,
        };
        let n = unsafe { libc::kevent(queue, std::ptr::null(), 0, &mut event, 1, &wait) };
        if n < 0 {
            taint()?;
            return Err(std::io::Error::last_os_error());
        }
        if n > 0 && (event.fflags & libc::NOTE_FORK != 0 || event.flags & libc::EV_ERROR != 0) {
            taint()?;
        }
        if seal.swap(false, Ordering::SeqCst) {
            // Drain queued fork events after the root was SIGSTOPped.
            loop {
                let zero = libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                };
                let n = unsafe { libc::kevent(queue, std::ptr::null(), 0, &mut event, 1, &zero) };
                if n == 0 {
                    break;
                }
                if n < 0 || event.fflags & libc::NOTE_FORK != 0 || event.flags & libc::EV_ERROR != 0
                {
                    taint()?;
                }
                if n < 0 {
                    break;
                }
            }
            println!(
                "{}",
                if forked.load(Ordering::SeqCst) {
                    "TAINTED"
                } else {
                    "SEALED"
                }
            );
            std::io::stdout().flush()?;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires built NWF_CODEX_HOST_EXECUTABLE: actual pre-arm host-pipe loss"]
    fn macos_guard_pre_arm_eof_kills_stopped_wrapper() {
        let host = std::env::var("NWF_CODEX_HOST_EXECUTABLE").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut guardian = Command::new(&host)
            .arg("--nwf-internal-guard")
            .arg(dir.path().join("fork-marker"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let group = guardian.id() as i32;
        let mut wrapper = Command::new(&host)
            .args(["--nwf-internal-exec", "/usr/bin/true"])
            .process_group(group)
            .spawn()
            .unwrap();
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(wrapper.id() as i32, &mut status, libc::WUNTRACED) },
            wrapper.id() as i32
        );
        assert!(libc::WIFSTOPPED(status));
        // Simulate the host dying before it writes the first PID.
        drop(guardian.stdin.take());
        guardian.wait().unwrap();
        wrapper.wait().unwrap();
        assert_eq!(unsafe { libc::kill(-group, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
}
