"""Resolve caller workspaces for an independent source checkout.

Source: agent-platform/platform_paths.py, introduced by
9809211b1e91adabb0dfda570bd4a79dc4313d21 and updated by
4dd1b8eaa958ef5f236afed78a74be30e43d030b (unchanged after that through
fb758234da0bd717d511dd41999679fa0bbf4427).
Standalone adaptation: no platform marker or sibling repository is required.
"""
import os
from pathlib import Path
import sys


def workspace_root(source):
    """Return a string: explicit workspace, or the real source repo's parent.

    A nonempty PLATFORM_WORKSPACE is expanded with expanduser and resolved to an
    absolute path (relative explicit values use the caller's cwd). With no
    override, cwd and lexical symlink parents never affect the workspace.
    Module-specific CLI/root overrides remain the caller's responsibility.
    Fixed-release users must explicitly supply their real caller workspace.
    """
    override = os.environ.get("PLATFORM_WORKSPACE")
    if override:
        return str(Path(override).expanduser().resolve())
    return str(Path(source).resolve().parent.parent)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: platform_paths.py <component entry path>")
    print(workspace_root(sys.argv[1]))
