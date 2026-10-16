# heartbeatd source recovery

Task `rd6-t-171006`; source-only candidate. This repository does not configure,
start, switch or supervise any deployed service. Production activation remains a
separate decision. No guard or periodic fallback was added.

## Original identity and projection

Original main `7a7a6a09feb8c7d7810f9a8f0be9f328acde103a` remains an ancestor
of recovered main. Original master
`cb86d08d44517e3a83bbc6f4b1017135e7b9d081` is retained unchanged. The exact
archive/retired heads and remote-tracking mappings, OIDs and per-ref object
closures are in `source-refs.json`. Remote identities are named under
`preserve/source-remotes/`; they must not be overwritten by ordinary origin
tracking. History preservation was completed by task `rd6-h-1mz0jb` before this
source recovery. No old history was filtered or replaced.

`projection-index.json` maps each full source commit to its new target commit.
The starting platform import is
`990253da3b5c5ec83fd43a2a463f39ed6d42aadf`; its heartbeatd subtree is byte/mode
identical to original heartbeatd main. Initial source freeze:
`fb758234da0bd717d511dd41999679fa0bbf4427`. Coverage was then extended through
`4ac4631fbdbf27f3c400805b233dc1d7a9892d46`, a source-owner pi-wrap-only change
with no heartbeatd/helper delta. No dirty source files were exported.

For each first-parent source commit, `projections/<full-SHA>.json` records
Source-Repo, Source-Commit, Source-Parent, Source-Paths, target paths, before/after
blob OIDs and modes, deletion and no-op status. The target commit contains exact
raw module/helper blobs plus its record; no cross-module commit was
cherry-picked. Unrelated commits are explicit record-only no-ops, not silently
skipped. No selected file is deleted. Source
`4dd1b8eaa958ef5f236afed78a74be30e43d030b` removes installed-alias helper
behavior and installer-based tests; that reversal is projected exactly, before
standalone adaptation. Installer/portable changes outside the selected paths
are not copied or resurrected.

The raw source delta affects `README.md`, `heartbeatd.py`, `platform_paths.py`
and `test_platform_paths.py`. Original `.gitignore`, `test_heartbeatd.py` and
`test_reload.py` remain byte/mode identical. `path-audit.json` records each
source/target path, its baseline/raw/adapted blob and mode, and why adapted
content differs. It also checks each projection's exact parent/delta, not just
the final subtree. Recovery metadata is additive and does not alter execution.

## Independent adaptation (after all raw projections)

- `heartbeatd.py`: only local helper lookup changes from the unified repository's
  parent directory to the real entry's own directory. Both direct and symlink
  invocation load the repository-owned helper; no sibling/unified fallback.
  All timer, registry, flock, hot-read, state, process-group and timeout code
  remains untouched.
- `platform_paths.py`: introduced by source
  `9809211b1e91adabb0dfda570bd4a79dc4313d21`, updated by
  `4dd1b8eaa958ef5f236afed78a74be30e43d030b`, then unchanged through the
  initial freeze. Both source deltas are separately projected.
  Drops the integration-only `MODULES` list and marker dependency. Interface:
  `workspace_root(source) -> absolute str`, nonempty PLATFORM_WORKSPACE first
  (expanduser + absolute resolution), otherwise real source repo's parent.
  Explicit relative PLATFORM_WORKSPACE uses invocation cwd; the default does
  not. HEARTBEATD_ROOT and individual path overrides remain at their original
  call sites with their original semantics. Release-directory callers must
  inject the actual workspace.
- `test_platform_paths.py`: replaces platform integration-layout fixtures with
  standalone fixtures and adds seven contracts for no marker/sibling, poisoned
  parent/unified helpers, cwd/space/relative/symlink entries, overrides,
  expanduser/absolute conversion, unchanged module-specific precedence, real
  CLI `check` and quoted shell/helper invocation. No real registry/action is
  used. Fixtures are retained for inspection.
- `README.md`: retains the module's boundary/semantics/hot-read documentation;
  changes examples to standalone paths, removes unified installation and
  workspace-specific policy links, and documents the local root contract.
  Platform root README's heartbeatd responsibility, caller-owned configuration,
  root precedence and per-module tests are covered here. Its six-module
  install/test runner and portable instructions are not heartbeatd mechanisms
  and are not copied. The exact shared README source changes/dispositions are
  recorded in the respective projection records.

## Validation

- `test_heartbeatd.py`: **165 passed, 0 failed**.
- `test_reload.py`: **5 tests, OK**.
- Final `test_platform_paths.py`: **7 tests, OK**, including actual Bash **3.2.0**
  for quoted real, symlink, relative and space-containing helper/CLI entries.
  The Bash interpreter was built under the task's isolated tooling root from
  GNU's `bash-3.2.tar.gz` (HTTPS, SHA256
  `26c99025b59e30779300b68adb764f824974d267a4d7cc1b347d14a2393f9fb4`);
  configure/build succeeded, no system install. This is a real Linux Bash 3.2
  execution, not a syntax-only check or a macOS claim.
- All tests run under a fresh task-owned harness outside production, using conda
  Python; TMPDIR and HEARTBEATD_* fixtures never target a production tree.
  The harness routes legacy test fixture rmtree through the approved
  `safe_rmtree` implementation scoped to that invocation's new temporary base;
  ignore_errors is not propagated. No application behavior is mocked away.
- An initial new root-contract test run failed because its synthetic registry
  had no action profile. Adding a valid inert profile fixed the test fixture;
  daemon validation behavior was not relaxed. Full logs remain in the task
  artifacts. The attempted macOS reverse-channel run returned disconnected
  and no macOS execution/result was confirmed; no macOS result is claimed or
  required for the Linux Bash 3.2 result above.

Original-ref closure/ancestry and per-path audit are independently reproducible
from Git objects and the source provenance records. Separate review must still
accept this source candidate; no production/P1/P6 completion is asserted.
