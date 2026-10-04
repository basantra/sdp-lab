#!/usr/bin/env bash
# Logging helper for the reproducibility contract described in RUNBOOK.md.
#
# Source this file, then wrap every state-changing command in `run_logged`:
#
#   source env/log.sh
#   run_logged terraform apply -auto-approve
#   run_logged oci compute instance launch ...
#
# Every invocation appends a timestamped entry (command + exit code) to
# runs/<RUN_DATE>/commands.log, where RUN_DATE defaults to today (UTC,
# YYYY-MM-DD) and can be overridden by exporting RUN_DATE before sourcing.
#
# `new_result` scaffolds runs/<RUN_DATE>/result.md with the required
# sections so an experiment's write-up can't skip a field by omission.

set -uo pipefail

RUN_DATE="${RUN_DATE:-$(date -u +%Y-%m-%d)}"
RUN_DIR="runs/${RUN_DATE}"
COMMANDS_LOG="${RUN_DIR}/commands.log"

mkdir -p "${RUN_DIR}"

run_logged() {
    if [ "$#" -eq 0 ]; then
        echo "run_logged: no command given" >&2
        return 2
    fi

    local ts
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '[%s] $ %s\n' "${ts}" "$*" >> "${COMMANDS_LOG}"

    "$@"
    local exit_code=$?

    printf '[%s] exit=%d\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${exit_code}" >> "${COMMANDS_LOG}"
    return "${exit_code}"
}

new_result() {
    local result_file="${RUN_DIR}/result.md"
    if [ -f "${result_file}" ]; then
        echo "new_result: ${result_file} already exists, not overwriting" >&2
        return 1
    fi
    cat > "${result_file}" <<EOF
# Result — ${RUN_DATE}

## What I ran

## What I expected

## What happened

## Exit code

## Log path
${COMMANDS_LOG}
EOF
    echo "${result_file}"
}
