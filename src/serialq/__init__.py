#!/usr/bin/env python3
"""
serialq — one-at-a-time execution.

A cross-process serial gate plus a persistent FIFO job queue, for CLIs and
APIs that must never run concurrently: strictly-serial LLM backends,
license-limited tools, shared hardware, flaky rate limits.

Two primitives, one guarantee — whoever holds the gate is the only thing
running:

    serialq run --gate qwen -- my-llm-cli ask "summarize this"
    serialq enqueue --gate qwen -- my-llm-cli batch job-42.json
    serialq worker --gate qwen            # drains the queue, forever
    serialq worker --gate qwen --once     # drains the queue, then exits

Standard library only. POSIX only (needs fcntl).
"""

import argparse
import fcntl
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

VERSION = "0.1.0"
GATE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
DEFAULT_GATE = os.environ.get("SERIALQ_GATE", "default")
TERM_GRACE_SECS = 5


def die(msg, code=1):
    print(f"serialq: error: {msg}", file=sys.stderr)
    sys.exit(code)


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- paths/locks

def gate_paths(gate):
    if not GATE_RE.match(gate or ""):
        die(f"invalid gate name {gate!r}: use letters, digits, '-' and '_' (max 64 chars)")
    base = os.environ.get(
        "SERIALQ_DIR",
        os.path.join(os.path.expanduser("~"), ".local", "share", "serialq"),
    )
    d = os.path.join(base, "gates", gate)
    os.makedirs(os.path.join(d, "logs"), exist_ok=True)
    return {
        "dir": d,
        "gate_lock": os.path.join(d, "gate.lock"),
        "worker_lock": os.path.join(d, "worker.lock"),
        "jobs": os.path.join(d, "jobs.json"),
        "logs": os.path.join(d, "logs"),
    }


class LockFile:
    """Advisory fcntl lock. The kernel releases it when the holder dies,
    so a crashed process can never leave a stale lock behind."""

    def __init__(self, path):
        self.path = path
        self.fh = None

    def acquire(self, timeout=None):
        """blocking=True semantics; timeout=None waits forever.
        Returns True on success, False on timeout."""
        self.fh = open(self.path, "a+b")
        if timeout is None:
            fcntl.flock(self.fh, fcntl.LOCK_EX)
            return True
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    self.fh.close()
                    self.fh = None
                    return False
                time.sleep(0.05)

    def try_acquire(self):
        self.fh = open(self.path, "a+b")
        try:
            fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            self.fh.close()
            self.fh = None
            return False

    def release(self):
        if self.fh is not None:
            try:
                fcntl.flock(self.fh, fcntl.LOCK_UN)
            finally:
                self.fh.close()
                self.fh = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


# ---------------------------------------------------------------- job store

def _read_jobs(fh, jobs_path):
    fh.seek(0)
    raw = fh.read().strip()
    if not raw:
        return {}
    try:
        jobs = json.loads(raw)
    except json.JSONDecodeError:
        # Never silently drop the queue: quarantine the corrupt file.
        bad = jobs_path + ".corrupt-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        os.rename(jobs_path, bad)
        print(f"serialq: warning: quarantined corrupt job store to {bad}", file=sys.stderr)
        return {}
    return jobs if isinstance(jobs, dict) else {}


def with_jobs(paths, fn):
    """Run fn(jobs) with the queue lock held; persist when fn returns True."""
    fh = open(paths["jobs"], "a+b")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX)
        jobs = _read_jobs(fh, paths["jobs"])
        if fn(jobs):
            fh.seek(0)
            fh.truncate()
            fh.write(json.dumps(jobs, indent=1, sort_keys=True).encode())
            fh.flush()
            os.fsync(fh.fileno())
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


def _iter_jobs(jobs):
    """Job records only (skips the __meta__ bookkeeping entry)."""
    return [j for k, j in jobs.items() if k != "__meta__" and isinstance(j, dict)]


def new_job_id(jobs):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    while True:
        jid = f"{stamp}-" + "".join(
            "0123456789abcdef"[b % 16] for b in os.urandom(6)
        )
        if jid not in jobs:
            return jid


def next_seq(jobs):
    """Monotonic insertion counter, assigned under the queue lock so FIFO
    order is exact even when several jobs share a timestamp."""
    meta = jobs.setdefault("__meta__", {"seq": 0})
    meta["seq"] += 1
    return meta["seq"]


# ---------------------------------------------------------------- processes

def _kill_tree(proc):
    """SIGTERM the whole process group, escalate to SIGKILL after a grace period."""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        proc.wait(timeout=TERM_GRACE_SECS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        proc.wait()


def run_child(cmd, log_path=None, kill_after=None, forward_signals=False):
    """Run cmd in its own process group.

    log_path=None inherits stdio (foreground `run`); otherwise stdout+stderr
    are appended to the log file. Returns (exit_code, timed_out).
    With forward_signals, SIGINT/SIGTERM are relayed to the child group first
    so Ctrl-C behaves like a normal foreground command.
    """
    log = open(log_path, "a") if log_path else None
    if log:
        log.write(f"# started {now_iso()} :: {' '.join(shlex.quote(c) for c in cmd)}\n")
        log.flush()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=log if log else None,
            stderr=subprocess.STDOUT if log else None,
            start_new_session=True,
        )
    except FileNotFoundError:
        msg = f"failed to start: {cmd[0]}: command not found"
        if log:
            log.write(f"# {msg}\n")
            log.close()
        else:
            print(f"serialq: error: {msg}", file=sys.stderr)
        return 127, False
    except OSError as e:
        msg = f"failed to start: {e}"
        if log:
            log.write(f"# {msg}\n")
            log.close()
        else:
            print(f"serialq: error: {msg}", file=sys.stderr)
        return 126, False

    relay = {}

    def _relay(signum, _frame):
        try:
            os.killpg(proc.pid, signum)
        except (ProcessLookupError, PermissionError):
            pass
        relay["signum"] = signum

    if forward_signals:
        old_int = signal.signal(signal.SIGINT, _relay)
        old_term = signal.signal(signal.SIGTERM, _relay)

    timed_out = False
    deadline = time.monotonic() + kill_after if kill_after else None
    try:
        while True:
            try:
                rc = proc.wait(timeout=0.2)
                break
            except subprocess.TimeoutExpired:
                pass
            if "signum" in relay:
                rc = proc.wait()  # child got the signal; reap it
                break
            if deadline is not None and time.monotonic() >= deadline:
                _kill_tree(proc)
                rc = proc.wait()
                timed_out = True
                break
    finally:
        if forward_signals:
            signal.signal(signal.SIGINT, old_int)
            signal.signal(signal.SIGTERM, old_term)

    if log:
        log.write(f"# ended {now_iso()} :: exit={rc}" + (" (timed out)" if timed_out else "") + "\n")
        log.close()
    if "signum" in relay and relay["signum"] == signal.SIGINT:
        # Behave like a normal interrupted foreground command.
        raise KeyboardInterrupt
    return rc, timed_out


# ---------------------------------------------------------------- commands

def cmd_run(args):
    cmd = [c for c in args.cmd if c != "--"]
    if not cmd:
        die("no command given")
    paths = gate_paths(args.gate)
    gate = LockFile(paths["gate_lock"])
    if args.timeout is not None and args.timeout < 0:
        die("--timeout must be >= 0")
    if not gate.acquire(timeout=args.timeout):
        die(f"timed out after {args.timeout}s waiting for gate {args.gate!r}")
    try:
        rc, timed_out = run_child(cmd, kill_after=args.kill_after, forward_signals=True)
    finally:
        gate.release()
    if timed_out:
        print(f"serialq: command killed after {args.kill_after}s", file=sys.stderr)
    sys.exit(rc if rc >= 0 else 128 - rc)


def cmd_enqueue(args):
    cmd = [c for c in args.cmd if c != "--"]
    if not cmd:
        die("no command given")
    if args.retries < 0:
        die("--retries must be >= 0")
    paths = gate_paths(args.gate)
    job = {
        "id": None,
        "gate": args.gate,
        "name": args.name or " ".join(cmd)[:80],
        "cmd": cmd,
        "created": now_iso(),
        "seq": None,
        "status": "queued",
        "attempts": 0,
        "max_retries": args.retries,
        "kill_after": args.kill_after,
        "not_before": 0,
        "started": None,
        "ended": None,
        "exit_code": None,
        "error": None,
    }

    def _add(jobs):
        job["id"] = new_job_id(jobs)
        job["seq"] = next_seq(jobs)
        jobs[job["id"]] = job
        return True

    with_jobs(paths, _add)
    print(job["id"])


def _pick_job(jobs):
    """Oldest queued job whose backoff has elapsed (FIFO by insertion order)."""
    now = time.time()
    candidates = [
        j for j in _iter_jobs(jobs)
        if j["status"] == "queued" and j.get("not_before", 0) <= now
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda j: (j.get("seq", 0), j["id"]))


def _has_pending(jobs):
    """Any job not yet in a terminal state (matters for --once + backoff)."""
    return any(j["status"] in ("queued", "running") for j in _iter_jobs(jobs))


def cmd_worker(args):
    paths = gate_paths(args.gate)
    worker_lock = LockFile(paths["worker_lock"])
    if not worker_lock.try_acquire():
        die(f"another worker is already running for gate {args.gate!r}")
    gate = LockFile(paths["gate_lock"])
    stop = {"flag": False}

    def _on_signal(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    # Crash recovery: this worker owns the lifecycle now (it holds
    # worker_lock), so any job still marked running belongs to a dead worker.
    def _recover(jobs):
        changed = False
        for j in _iter_jobs(jobs):
            if j["status"] == "running":
                j["status"] = "queued"
                j["error"] = "requeued: previous worker died mid-job"
                changed = True
        return changed

    with_jobs(paths, _recover)

    idle_since = time.monotonic()
    try:
        while not stop["flag"]:
            job = {}

            def _claim(jobs):
                j = _pick_job(jobs)
                if j is None:
                    return False
                j["status"] = "running"
                j["attempts"] += 1
                j["started"] = now_iso()
                j["error"] = None
                job.update(j)
                return True

            with_jobs(paths, _claim)
            if not job:
                pending = []

                def _check(jobs):
                    pending.append(_has_pending(jobs))
                    return False

                with_jobs(paths, _check)
                if args.once and not pending[0]:
                    break
                if args.idle_timeout and time.monotonic() - idle_since >= args.idle_timeout:
                    break
                time.sleep(0.5)
                continue
            idle_since = time.monotonic()

            # Wait for the gate in slices so signals stay responsive.
            while not stop["flag"]:
                if gate.acquire(timeout=0.5):
                    break
            if stop["flag"]:
                # Hand the job back untouched; a later worker will run it.
                def _unclaim(jobs):
                    j = jobs.get(job["id"])
                    if j and j["status"] == "running":
                        j["status"] = "queued"
                        j["attempts"] -= 1
                        return True
                    return False

                with_jobs(paths, _unclaim)
                break

            try:
                log_path = os.path.join(paths["logs"], job["id"] + ".log")
                rc, timed_out = run_child(
                    job["cmd"], log_path=log_path, kill_after=job.get("kill_after")
                )
            finally:
                gate.release()

            def _finish(jobs):
                j = jobs.get(job["id"])
                if not j:
                    return False
                j["ended"] = now_iso()
                j["exit_code"] = rc
                if rc == 0:
                    j["status"] = "done"
                    return True
                if timed_out:
                    j["error"] = f"killed after {job.get('kill_after')}s (timeout)"
                else:
                    j["error"] = f"exit code {rc}"
                if j["attempts"] <= j["max_retries"]:
                    backoff = min(300, 5 * 2 ** (j["attempts"] - 1))
                    j["status"] = "queued"
                    j["not_before"] = time.time() + backoff
                    j["error"] += f"; retrying in {backoff:.0f}s (attempt {j['attempts']}/{j['max_retries'] + 1})"
                else:
                    j["status"] = "failed"
                return True

            with_jobs(paths, _finish)
    finally:
        worker_lock.release()


def cmd_list(args):
    paths = gate_paths(args.gate)
    rows = []

    def _collect(jobs):
        for j in sorted(_iter_jobs(jobs), key=lambda j: (j.get("seq", 0), j["id"])):
            if not args.all and j["status"] not in ("queued", "running"):
                continue
            rows.append(j)
        return False

    with_jobs(paths, _collect)
    if not rows:
        print("(no jobs)" if args.all else "(queue empty)")
        return
    print(f"{'ID':<20}{'NAME':<34}{'STATUS':<10}{'TRY':<5}{'EXIT':<6}CREATED")
    for j in rows:
        exit_code = "" if j["exit_code"] is None else str(j["exit_code"])
        print(f"{j['id']:<20}{j['name'][:33]:<34}{j['status']:<10}{j['attempts']:<5}{exit_code:<6}{j['created']}")


def cmd_log(args):
    paths = gate_paths(args.gate)
    log_path = os.path.join(paths["logs"], args.id + ".log")
    if not os.path.exists(log_path):
        die(f"no log for job {args.id!r}")
    with open(log_path) as f:
        sys.stdout.write(f.read())


def cmd_cancel(args):
    paths = gate_paths(args.gate)

    def _cancel(jobs):
        j = jobs.get(args.id)
        if j is None:
            die(f"no such job {args.id!r}")
        if j["status"] != "queued":
            die(f"job {args.id!r} is {j['status']}, only queued jobs can be cancelled")
        j["status"] = "cancelled"
        j["ended"] = now_iso()
        return True

    with_jobs(paths, _cancel)
    print(f"cancelled {args.id}")


def cmd_status(args):
    paths = gate_paths(args.gate)
    gate = LockFile(paths["gate_lock"])
    held = not gate.try_acquire()
    if not held:
        gate.release()
    counts = {"queued": 0, "running": 0, "done": 0, "failed": 0, "cancelled": 0}
    running_id = None

    def _count(jobs):
        for j in _iter_jobs(jobs):
            counts[j["status"]] = counts.get(j["status"], 0) + 1
            if j["status"] == "running":
                nonlocal_running[0] = j["id"]
        return False

    nonlocal_running = [None]
    with_jobs(paths, _count)
    running_id = nonlocal_running[0]
    print(f"gate {args.gate!r}: {'BUSY' if held else 'free'}")
    print(f"queued={counts['queued']} running={counts['running']} "
          f"done={counts['done']} failed={counts['failed']} cancelled={counts['cancelled']}")
    if running_id:
        print(f"running job: {running_id}")


# ---------------------------------------------------------------- cli

def build_parser():
    p = argparse.ArgumentParser(
        prog="serialq",
        description="One-at-a-time execution: a serial gate and FIFO job queue.",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("run", help="run a command under the gate (blocks until free)")
    r.add_argument("-g", "--gate", default=DEFAULT_GATE, help="gate name (default: %(default)s)")
    r.add_argument("--timeout", type=float, default=None,
                   help="max seconds to wait for the gate (default: wait forever)")
    r.add_argument("--kill-after", type=float, default=None,
                   help="kill the command if it runs longer than this many seconds")
    r.add_argument("cmd", nargs=argparse.REMAINDER, help="command to run (after --)")
    r.set_defaults(func=cmd_run)

    e = sub.add_parser("enqueue", help="queue a command to run later, prints the job id")
    e.add_argument("-g", "--gate", default=DEFAULT_GATE)
    e.add_argument("--name", default=None, help="human-readable job name")
    e.add_argument("--retries", type=int, default=0, help="retries on failure (default: 0)")
    e.add_argument("--kill-after", type=float, default=None)
    e.add_argument("cmd", nargs=argparse.REMAINDER, help="command to run (after --)")
    e.set_defaults(func=cmd_enqueue)

    w = sub.add_parser("worker", help="drain the queue serially")
    w.add_argument("-g", "--gate", default=DEFAULT_GATE)
    w.add_argument("--once", action="store_true", help="exit when the queue is empty")
    w.add_argument("--idle-timeout", type=float, default=None,
                   help="exit after this many idle seconds (default: run forever)")
    w.set_defaults(func=cmd_worker)

    l = sub.add_parser("list", help="list jobs")
    l.add_argument("-g", "--gate", default=DEFAULT_GATE)
    l.add_argument("--all", action="store_true", help="include finished jobs")
    l.set_defaults(func=cmd_list)

    g = sub.add_parser("log", help="print a job's log")
    g.add_argument("-g", "--gate", default=DEFAULT_GATE)
    g.add_argument("id", help="job id")
    g.set_defaults(func=cmd_log)

    c = sub.add_parser("cancel", help="cancel a queued job")
    c.add_argument("-g", "--gate", default=DEFAULT_GATE)
    c.add_argument("id", help="job id")
    c.set_defaults(func=cmd_cancel)

    s = sub.add_parser("status", help="show gate and queue status")
    s.add_argument("-g", "--gate", default=DEFAULT_GATE)
    s.set_defaults(func=cmd_status)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:
        sys.exit(0)


if __name__ == "__main__":
    main()
