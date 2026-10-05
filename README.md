# serialq

One-at-a-time execution. A cross-process serial gate plus a persistent FIFO
job queue, for CLIs and APIs that must never run concurrently.

Some backends are strictly serial: one in-flight call at a time, across
every model, key, or client — overlap and you get rate-limited, corrupted,
or billed twice. Cron jobs don't know about each other, shell scripts
don't coordinate, and "just be careful" stops working at 3am. serialq is
the bouncer: whoever holds the gate is the only thing running.

```sh
pip install serialq

# ad-hoc: blocks until the gate is free, then runs
serialq run --gate qwen -- my-llm-cli ask "summarize this thread"

# fire-and-forget: queue it, a worker runs jobs one by one
serialq enqueue --gate qwen -- my-llm-cli batch jobs/42.json
serialq worker --gate qwen            # drain forever (systemd, tmux, ...)
serialq worker --gate qwen --once     # drain once, then exit (cron-friendly)

serialq list --gate qwen
serialq log 20261005-a3f9c1
serialq status --gate qwen
```

Zero dependencies, standard library only. POSIX only (Linux, macOS) —
it relies on `fcntl` locks.

## Why not just a lock file?

Because lock files go stale. serialq uses `fcntl` advisory locks, which the
kernel releases when the holder process dies — a crashed job can never wedge
the gate. The queue goes further:

- **Crash recovery.** If a worker is kill -9'd mid-job, the next worker
  re-queues the orphaned job instead of losing it. Only one worker per gate
  can run at a time, so the recovery is unambiguous.
- **Timeouts that actually kill.** `--kill-after` terminates the whole
  process group (SIGTERM, then SIGKILL), not just the parent.
- **Retries with backoff.** `enqueue --retries 3` re-queues failures with
  exponential backoff (5s, 10s, 20s … capped at 5 minutes).
- **Atomic queue writes.** The job store is rewritten under an exclusive
  lock with fsync; a corrupt store is quarantined, never silently dropped.
- **Ctrl-C behaves.** `serialq run` forwards SIGINT to the child, so
  interactive commands interrupt the way you'd expect.

## Use cases

- **Serial-only LLM backends.** Some model gateways allow exactly one
  in-flight request across all models. Prefix every call and stop thinking
  about it:
  ```sh
  serialq run --gate qwen --timeout 3600 -- llm chat model-a "prompt one" &
  serialq run --gate qwen --timeout 3600 -- llm chat model-b "prompt two" &
  wait  # they ran sequentially, in order
  ```
- **Overnight batch pipelines.** Enqueue a hundred jobs, let one worker
  chew through them; check `serialq list --all` in the morning.
- **License-limited tools.** One floating license, many cron jobs — put
  the tool behind a gate named after the license.
- **Flaky deploys.** `enqueue --retries 5` on the deploy script; transient
  failures retry themselves with backoff.

## Reference

| Command | What it does |
|---|---|
| `run [-g GATE] [--timeout S] [--kill-after S] -- CMD…` | Run CMD under the gate, blocking until free. Exits with CMD's exit code. |
| `enqueue [-g GATE] [--name N] [--retries N] [--kill-after S] -- CMD…` | Queue CMD, print the job id. |
| `worker [-g GATE] [--once] [--idle-timeout S]` | Run queued jobs FIFO. SIGTERM/SIGINT finish the current job, then stop. |
| `list [-g GATE] [--all]` | Show queued/running jobs (`--all` includes finished). |
| `log [-g GATE] ID` | Print a job's captured output. |
| `cancel [-g GATE] ID` | Cancel a queued job. |
| `status [-g GATE]` | Gate busy/free plus queue counts. |

Gates are just names (`[A-Za-z0-9_-]`, `--gate` or `SERIALQ_GATE` env).
State lives in `SERIALQ_DIR` (default `~/.local/share/serialq`), one
directory per gate: the lock files, `jobs.json`, and per-job logs.

A systemd user unit template ships in `contrib/`:

```sh
cp contrib/serialq-worker@.service ~/.config/systemd/user/
systemctl --user enable --now serialq-worker@qwen
```

## Design notes

- One module, ~500 lines, no dependencies. The whole thing fits in your head.
- The gate and the queue are separate locks: `enqueue`/`list` never block
  behind a long-running job.
- Job ids are `YYYYMMDD-` plus 6 hex chars — sortable, greppable, unique.
- Exit code 124-style semantics aren't faked: timeouts are reported in the
  job log and on stderr.

## Limitations

- POSIX only. Windows would need a different locking primitive.
- FIFO, no priorities — deliberate. If you need priorities you probably
  need a real queue.
- The worker is single-threaded by design: one gate, one job at a time.
  That's the point.

## License

MIT.
