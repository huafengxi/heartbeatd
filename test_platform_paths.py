"""Discover workspace roots without depending on cwd or the checkout name."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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

    def test_canonical_module_paths_share_workspace(self):
        with patch.dict(os.environ, {}, clear=True):
            for name in MODULES:
                self.assertEqual(workspace_root(self.repo / name / "entry.py"), str(self.ws))
                self.assertFalse((self.ws / name).exists())

    def test_explicit_workspace_and_legacy_copy(self):
        legacy = self.root / "legacy" / "agentd"
        legacy.mkdir(parents=True)
        (legacy / "entry.py").touch()
        with patch.dict(os.environ, {"PLATFORM_WORKSPACE": str(self.root / "override")}):
            self.assertEqual(workspace_root(self.repo / "agentd/entry.py"), str(self.root / "override"))
            self.assertEqual(workspace_root(legacy / "entry.py"), str(legacy.parent))



if __name__ == "__main__":
    unittest.main()
