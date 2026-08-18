# Agent Guidance Asset Removal

## Goal

Remove repository-local agent guidance assets that are no longer wanted, without changing the project's verification workflow.

## Scope

- Delete the root `AGENTS.md` file.
- Delete `skills/worktree-flow/review_gate_prompt.txt` and remove its empty parent directories.
- Retain `scripts/verify-worktree.sh` and all existing README references to that verification command.

## Documentation impact

No retained documentation references the deleted agent guidance assets. The existing `README.md` references only the retained verification script, so it remains unchanged.

## Verification

Confirm the deleted paths are absent from the working tree and appear only as intended deletions in `git status`, then run `./gradlew test` through the retained verification script.
