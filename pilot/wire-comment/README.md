# Wire comment pilot (throwaway)

Question: can a read-only PR workflow upload a diff and a separate trusted
`workflow_run` workflow create/update one comment as `github-actions[bot]`?

The two JSON fixtures are synthetic. No Ignite build or compatibility decision.
The publisher never checks out PR code, validates the artifact size/type, and
checks the current PR SHA before posting. Full external-fork authorization is
not established by a same-account test.

## Live test

1. After resolving the GitHub Actions billing lock, merge this setup into the
   default branch of `chesnokoff/ignite` only (not Apache).
2. Open a new PR to that fork changing `pilot/wire-comment/after.json`.
3. Verify build → workflow_run → one comment by github-actions[bot].
4. Push another fixture change; verify the same comment ID is updated.
5. Copy before.json to after.json; verify the comment says no changes.
6. Repeat with an external account and approval enabled to verify fork behavior.

## Observed on 2026-11-06

Re-run of existing Actions run 42003373893, attempt 2, job 127986499507 failed
before executing any step: `The job was not started because your account is
locked due to a billing issue.` No bot comment has been observed for this pilot.

After the experiment, remove both pilot workflows and this directory.
