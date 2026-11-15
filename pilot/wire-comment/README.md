# Wire comment pilot

The two JSON files represent a small wire format change. The PR workflow
compares them and publishes the diff in Job Summary and the `wire-diff`
artifact. The publisher uses the job results to update one PR comment and the
`compatibility` label.

On a successful run with equal fixtures, the publisher removes its comment
and the label it applied. A failed completeness check leaves both alone.

This pilot is scoped to PR #12 in `chesnokoff/ignite`. It uses synthetic JSON
and a PR in the same repository. The build on real Ignite messages is in
PR #11. The publisher runs from the fork's default branch.
