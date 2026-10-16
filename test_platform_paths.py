"""Installation must preserve caller roots and refuse partial destructive changes."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from install import install
from platform_paths import MODULES, workspace_root


class LayoutTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.ws = self.root / "caller"
        self.ws.mkdir()
        self.repo = self.ws / "arbitrary-checkout"
        self.repo.mkdir()
        (self.repo / ".platform-root").touch()
        for name in MODULES:
            (self.repo / name).mkdir()
            (self.repo / name / "entry.py").touch()

    def test_canonical_and_alias_paths_share_workspace(self):
        install(self.ws, self.repo)
        with patch.dict(os.environ, {}, clear=True):
            for name in MODULES:
                self.assertEqual(workspace_root(self.repo / name / "entry.py"), str(self.ws))
                self.assertEqual(workspace_root(self.ws / name / "entry.py"), str(self.ws))

    def test_separate_source_can_install_into_another_caller(self):
        other = self.root / "another-caller"
        other.mkdir()
        install(other, self.repo)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(workspace_root(other / "agentd/entry.py"), str(other))

    def test_explicit_workspace_and_legacy_copy(self):
        legacy = self.root / "legacy" / "agentd"
        legacy.mkdir(parents=True)
        (legacy / "entry.py").touch()
        with patch.dict(os.environ, {"PLATFORM_WORKSPACE": str(self.root / "override")}):
            self.assertEqual(workspace_root(self.repo / "agentd/entry.py"), str(self.root / "override"))
            self.assertEqual(workspace_root(legacy / "entry.py"), str(legacy.parent))

    def test_existing_checkout_prevents_all_writes(self):
        (self.ws / "heartbeatd").mkdir()
        with self.assertRaises(ValueError):
            install(self.ws, self.repo)
        self.assertFalse((self.ws / "agentd").exists())
        self.assertTrue((self.ws / "heartbeatd").is_dir())

    def test_installation_is_idempotent(self):
        before = install(self.ws, self.repo)
        self.assertEqual(install(self.ws, self.repo), before)


if __name__ == "__main__":
    unittest.main()
