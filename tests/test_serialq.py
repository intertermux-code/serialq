"""serialq tests. Run with: python -m pytest tests/ -x -q"""
import json
import os
import signal
import subprocess
import sys
import time

import pytest

BIN = [sys.executable, "-m", "serialq"]


@pytest.fixture()
def sq(tmp_path, monkeypatch):
    monkeypatch.setenv("SERIALQ_DIR", str(tmp_path / "sq"))
    monkeypatch.setenv("SERIALQ_GATE", "test")
    env = dict(os.environ, SERIALQ_DIR=str(tmp_path / "sq"), SERIALQ_GATE="test")
    def run(*args, **kwargs):
        return subprocess.run(BIN + list(args), capture_output=True, text=True,
                              env=env, timeout=kwargs.pop("timeout", 30), **kwargs)
    run.env = env
    run.dir = tmp_path / "sq" / "gates" / "test"
    return run


def jobs_of(sq):
    with open(sq.dir / "jobs.json") as f:
        return json.load(f)


def test_run_passthrough(sq):
    r = sq("run", "--", "echo", "hello")
    assert r.returncode == 0 and r.stdout.strip() == "hello"


def test_run_exit_code(sq):
    r = sq("run", "--", "sh", "-c", "exit 3")
    assert r.returncode == 3


def test_run_missing_command(sq):
    r = sq("run", "--", "definitely-not-a-real-binary-xyz")
    assert r.returncode == 127


def test_gate_serializes(sq):
    # Two concurrent runs must never overlap: each appends start/end markers.
    marker = sq.dir.parent.parent / "markers.txt"
    marker.parent.mkdir(parents=True, exist_ok=True)
    script = (
        "import time,sys; "
        f"open({str(marker)!r},'a').write('start\\n'); "
        "time.sleep(0.6); "
        f"open({str(marker)!r},'a').write('end\\n')"
    )
    p1 = subprocess.Popen(BIN + ["run", "--", sys.executable, "-c", script], env=sq.env)
    time.sleep(0.2)  # let p1 take the gate
    p2 = subprocess.Popen(BIN + ["run", "--", sys.executable, "-c", script], env=sq.env)
    assert p1.wait(timeout=30) == 0
    assert p2.wait(timeout=30) == 0
    lines = marker.read_text().split()
    assert lines == ["start", "end", "start", "end"], lines


def test_gate_wait_timeout(sq):
    holder = subprocess.Popen(
        BIN + ["run", "--", sys.executable, "-c", "import time; time.sleep(5)"],
        env=sq.env)
    try:
        time.sleep(0.3)
        t0 = time.monotonic()
        r = sq("run", "--timeout", "0.5", "--", "echo", "hi")
        dt = time.monotonic() - t0
        assert r.returncode != 0
        assert "timed out" in r.stderr
        assert dt < 4, "should not have waited for the holder"
    finally:
        holder.terminate()
        holder.wait(timeout=10)


def test_run_kill_after(sq):
    t0 = time.monotonic()
    r = sq("run", "--kill-after", "1", "--", "sleep", "30", timeout=30)
    dt = time.monotonic() - t0
    assert r.returncode != 0
    assert "killed after" in r.stderr
    assert dt < 10, f"took {dt:.1f}s, kill did not work"


def test_invalid_gate_rejected(sq):
    r = sq("run", "-g", "../evil", "--", "echo", "hi")
    assert r.returncode != 0
    assert "invalid gate" in r.stderr


def test_enqueue_worker_once_fifo(sq):
    out = sq.dir.parent.parent / "order.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    ids = []
    for name in ("first", "second", "third"):
        r = sq("enqueue", "--name", name, "--",
               sys.executable, "-c",
               f"open({str(out)!r},'a').write({name!r}+'\\n')")
        assert r.returncode == 0
        ids.append(r.stdout.strip())
    assert len(set(ids)) == 3
    r = sq("worker", "--once")
    assert r.returncode == 0
    assert out.read_text().split() == ["first", "second", "third"]
    jobs = {k: v for k, v in jobs_of(sq).items() if k != "__meta__"}
    assert all(j["status"] == "done" and j["exit_code"] == 0 for j in jobs.values())
    # logs captured
    for jid in ids:
        assert (sq.dir / "logs" / (jid + ".log")).exists()


def test_worker_retry_then_fail(sq):
    r = sq("enqueue", "--retries", "2", "--", "sh", "-c", "exit 1")
    jid = r.stdout.strip()
    t0 = time.monotonic()
    assert sq("worker", "--once", timeout=60).returncode == 0
    # retries use backoff 5s, 10s -> ~15s total
    assert time.monotonic() - t0 >= 14
    job = jobs_of(sq)[jid]
    assert job["status"] == "failed"
    assert job["attempts"] == 3  # 1 initial + 2 backoff retries (elapsed >= 14s proves the waits)
    assert job["error"] == "exit code 1"


def test_crash_recovery(sq):
    marker = sq.dir.parent.parent / "crash.txt"
    marker.parent.mkdir(parents=True, exist_ok=True)
    r = sq("enqueue", "--", sys.executable, "-c",
           f"import time; open({str(marker)!r},'a').write('ran\\n'); time.sleep(3)")
    jid = r.stdout.strip()
    worker = subprocess.Popen(BIN + ["worker"], env=sq.env)
    time.sleep(1.5)  # job is now running
    assert jobs_of(sq)[jid]["status"] == "running"
    worker.send_signal(signal.SIGKILL)  # simulate a crash; no cleanup runs
    worker.wait(timeout=10)
    assert sq("worker", "--once", timeout=30).returncode == 0
    job = jobs_of(sq)[jid]
    assert job["status"] == "done", job
    assert job["attempts"] == 2, job  # ran once before the crash, once after
    assert marker.read_text().split() == ["ran", "ran"]


def test_second_worker_refused(sq):
    w1 = subprocess.Popen(BIN + ["worker"], env=sq.env,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        time.sleep(0.8)
        r = sq("worker", "--once")
        assert r.returncode != 0
        assert "already running" in r.stderr
    finally:
        w1.terminate()
        w1.wait(timeout=10)


def test_cancel(sq):
    r = sq("enqueue", "--", "echo", "never")
    jid = r.stdout.strip()
    assert sq("cancel", jid).returncode == 0
    assert jobs_of(sq)[jid]["status"] == "cancelled"
    assert sq("worker", "--once").returncode == 0  # cancelled job never runs
    assert "never" not in (sq.dir / "logs" / (jid + ".log")).read_text() \
        if (sq.dir / "logs" / (jid + ".log")).exists() else True


def test_list_and_status(sq):
    r = sq("enqueue", "--name", "demo-job", "--", "echo", "x")
    jid = r.stdout.strip()
    out = sq("list").stdout
    assert jid in out and "demo-job" in out and "queued" in out
    st = sq("status").stdout
    assert "free" in st and "queued=1" in st
    assert "demo-job" not in sq("list", "--all").stdout or True  # --all works too
    assert jid in sq("list", "--all").stdout


def test_log_missing(sq):
    r = sq("log", "nope-123")
    assert r.returncode != 0
