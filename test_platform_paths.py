"""Standalone root contract; derived from agent-platform test_platform_paths.py.

Fixtures are retained under tempfile.gettempdir() for inspection. A surrounding
harness can authorize cleanup of its own temporary base using safe_rmtree.
BASH_UNDER_TEST selects a real shell; REQUIRE_BASH32=1 requires Bash 3.2.
Only synthetic registries are ever checked; no daemon or real action is run.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from platform_paths import workspace_root

HERE = Path(__file__).resolve().parent


class LayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = Path(tempfile.mkdtemp(prefix="heartbeatd-root-contract-")).resolve()

    def setUp(self):
        self.root = self.base / self._testMethodName
        self.root.mkdir()
        self.ws = self.root / "caller with spaces"
        self.repo = self.ws / "independent source"
        self.repo.mkdir(parents=True)
        for name in ("heartbeatd.py", "platform_paths.py"):
            shutil.copy2(HERE / name, self.repo / name)
        self.entry = self.repo / "heartbeatd.py"
        self.elsewhere = self.root / "other cwd"
        self.elsewhere.mkdir()
        self.alias = self.elsewhere / "linked entry.py"
        self.alias.symlink_to(self.entry)
        # A poisoned parent/sibling proves the local helper owns this import.
        (self.ws / "platform_paths.py").write_text("raise RuntimeError('parent helper imported')\n")
        (self.ws / "agent-platform").mkdir()
        (self.ws / "agent-platform/platform_paths.py").write_text("raise RuntimeError('unified helper imported')\n")

    def env(self, **extra):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("PLATFORM_", "HEARTBEATD_", "PYTHON"))}
        env.update(PYTHONDONTWRITEBYTECODE="1", **extra)
        return env

    def run_ok(self, args, env=None, cwd=None):
        p = subprocess.run(args, env=env if env is not None else self.env(),
                           cwd=cwd or self.elsewhere, capture_output=True, text=True, timeout=15)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return p.stdout.strip()

    def roots(self, entry, **extra):
        code = ("import importlib.util,json,sys; "
                "s=importlib.util.spec_from_file_location('hb_contract',sys.argv[1]); "
                "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
                "print(json.dumps([str(m.ROOT),str(m.REGISTRY),str(m.LOCK_FILE),str(m.STATE_FILE)]))")
        return json.loads(self.run_ok([sys.executable, "-c", code, str(entry)], self.env(**extra)))

    def registry(self, root):
        registry = root / "heartbeats"
        (registry / "profiles").mkdir(parents=True)
        (registry / "timers").mkdir()
        (registry / "profiles/noop.json").write_text(json.dumps({
            "cmd": [sys.executable, "-c", "pass"]}))
        return registry

    def test_no_marker_no_sibling_required(self):
        # Use a standalone source root with no surrounding platform directory.
        alone = self.root / "standalone" / "source"
        alone.mkdir(parents=True)
        for name in ("heartbeatd.py", "platform_paths.py"):
            shutil.copy2(HERE / name, alone / name)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(workspace_root(alone / "heartbeatd.py"), str(alone.parent))
            self.assertIsInstance(workspace_root(alone / "heartbeatd.py"), str)
            self.assertFalse((alone / ".platform-root").exists())
            self.assertFalse((alone.parent / "agent-platform").exists())
        self.assertEqual(self.roots(alone / "heartbeatd.py")[0], str(alone.parent))

    def test_default_uses_real_source_not_cwd_or_lexical_link(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(workspace_root(self.entry), str(self.ws))
            self.assertEqual(workspace_root(self.alias), str(self.ws))
        for entry in (self.entry, self.alias):
            self.assertEqual(self.roots(entry)[0], str(self.ws))
            self.assertEqual(self.run_ok([sys.executable, str(self.repo / "platform_paths.py"), str(entry)]), str(self.ws))
        relative = os.path.relpath(self.entry, self.elsewhere)
        self.assertEqual(self.roots(relative)[0], str(self.ws))

    def test_override_without_marker_and_empty_falls_back(self):
        override = self.root / "separate workspace"
        for entry in (self.entry, self.alias):
            self.assertEqual(self.roots(entry, PLATFORM_WORKSPACE=str(override))[0], str(override))
            self.assertEqual(self.roots(entry, PLATFORM_WORKSPACE="")[0], str(self.ws))
        # Markers are irrelevant, including a historical marker left nearby.
        (self.repo / ".platform-root").touch()
        self.assertEqual(self.roots(self.entry, PLATFORM_WORKSPACE=str(override))[0], str(override))
        self.assertEqual(self.roots(self.entry)[0], str(self.ws))

    def test_explicit_expanduser_and_relative_absolute_contract(self):
        home = self.root / "synthetic home"
        home.mkdir()
        self.assertEqual(self.roots(self.entry, HOME=str(home), PLATFORM_WORKSPACE="~/chosen workspace")[0],
                         str(home / "chosen workspace"))
        self.assertEqual(self.roots(self.alias, PLATFORM_WORKSPACE="../relative workspace")[0],
                         str((self.elsewhere / "../relative workspace").resolve()))

    def test_module_specific_overrides_keep_priority_and_relative_semantics(self):
        specific = self.root / "module caller"
        registry = self.root / "custom registry"
        lock = self.root / "custom lock"
        state = self.root / "custom state"
        self.assertEqual(self.roots(self.alias, PLATFORM_WORKSPACE=str(self.root / "unused"),
                                   HEARTBEATD_ROOT=str(specific), HEARTBEATD_REGISTRY=str(registry),
                                   HEARTBEATD_LOCK=str(lock), HEARTBEATD_STATE=str(state)),
                         list(map(str, (specific, registry, lock, state))))
        # HEARTBEATD_ROOT has always been Path(value), not expanduser/resolve.
        self.assertEqual(self.roots(self.entry, HEARTBEATD_ROOT="relative caller")[0], "relative caller")
        self.assertEqual(self.roots(self.entry, HEARTBEATD_ROOT="~/literal caller")[0], "~/literal caller")
        self.assertEqual(self.roots(self.entry, HEARTBEATD_ROOT="")[0], str(self.ws))

    def test_check_real_symlink_relative_and_space_entries(self):
        self.registry(self.ws)
        for entry in (self.entry, self.alias, Path(os.path.relpath(self.entry, self.elsewhere)),
                      Path(self.alias.name)):
            self.assertIn("ok", self.run_ok([sys.executable, str(entry), "check"]).lower())
        override = self.root / "explicit caller"
        self.registry(override)
        self.assertIn("ok", self.run_ok([sys.executable, str(self.alias), "check"],
                                       self.env(PLATFORM_WORKSPACE=str(override))).lower())

    def test_bash_quoted_helper_and_cli_entries(self):
        bash = os.environ.get("BASH_UNDER_TEST", "/bin/bash")
        version = self.run_ok([bash, "-c", 'printf "%s.%s" "${BASH_VERSINFO[0]}" "${BASH_VERSINFO[1]}"'])
        if os.environ.get("REQUIRE_BASH32") == "1":
            self.assertEqual(version, "3.2")
        self.registry(self.ws)
        # No readlink -f, arrays, eval or word splitting; works on macOS Bash 3.2.
        for entry in (self.entry, self.alias, Path(self.alias.name)):
            output = self.run_ok([bash, "-c", 'set -eu; exec "$1" "$2" check',
                                  "root-contract", sys.executable, str(entry)])
            self.assertIn("ok", output.lower())
            root = self.run_ok([bash, "-c", 'set -eu; exec "$1" "$2" "$3"',
                                "root-contract", sys.executable,
                                str(self.repo / "platform_paths.py"), str(entry)])
            self.assertEqual(root, str(self.ws))


if __name__ == "__main__":
    unittest.main()
