# runs/

One subdirectory per experiment run, named `YYYY-MM-DD` (append `-2`, `-3`,
... if more than one run happens on the same date). Created automatically
by `env/log.sh` the first time `run_logged` or `new_result` is called for
that date.

Each `runs/<date>/` contains:

- `commands.log` — every state-changing command run that day, timestamped, with exit codes. Written by `run_logged`.
- `result.md` — what was run, what was expected, what happened, exit code, and the path to `commands.log`. Scaffolded by `new_result`.
- `raw/` — large or non-reproducible raw output (logs, dumps, screenshots). Gitignored; never committed.

See [`RUNBOOK.md`](../RUNBOOK.md) for the full logging contract.
