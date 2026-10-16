"""Discover caller workspaces for canonical module paths."""
import os
from pathlib import Path
import sys

MODULES = ("agentd", "pi-wrap", "agents-sync", "serviced", "heartbeatd", "git-sync")


def workspace_root(source):
    """Return the caller root without depending on cwd or the checkout name.

    Legacy copies without the platform marker retain their parent-root contract.
    """
    lexical = Path(os.path.abspath(source))
    package = lexical.resolve().parent.parent
    if not (package / ".platform-root").is_file():
        return str(lexical.parent.parent)
    override = os.environ.get("PLATFORM_WORKSPACE")
    if override:
        return str(Path(override).expanduser().resolve())
    return str(package.parent)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: platform_paths.py <component entry path>")
    print(workspace_root(sys.argv[1]))
