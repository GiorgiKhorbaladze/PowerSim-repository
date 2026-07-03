from __future__ import annotations

import copy
import os
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from unittest import mock

from backend.powersim_ai_config import resolve_allowed_path
from backend.powersim_ai_tools import _completed, propose_add_bess, run_solver

class AISafetyTest(unittest.TestCase):
    def test_rejects_parent_paths(self):
        with self.assertRaises(ValueError):
            resolve_allowed_path("../secret.json")

    def test_rejects_absolute_unsafe_paths(self):
        with self.assertRaises(ValueError):
            resolve_allowed_path("/etc/passwd")

    def test_rejects_arbitrary_shell_commands(self):
        with self.assertRaises(ValueError):
            _completed(["sh", "-c", "env"], timeout=1)

    def test_does_not_expose_env_vars(self):
        os.environ["POWERSIM_PRIVATE_TEST_SECRET"] = "do-not-leak"
        res = run_solver("samples/sample_input_168h.json", ".runtime/sessions/safety/out")
        self.assertNotIn("do-not-leak", str(res))

    def test_does_not_mutate_original_input_object(self):
        inp = {"assets": []}
        before = copy.deepcopy(inp)
        propose_add_bess(inp, "B1", 1, 2)
        self.assertEqual(inp, before)

    def test_subprocess_not_shell_true(self):
        with mock.patch("subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = ""
            run.return_value.stderr = ""
            run_solver("samples/sample_input_168h.json", ".runtime/sessions/mock/out", confirmed=True, timeout=1)
            self.assertFalse(run.call_args.kwargs["shell"])

if __name__ == "__main__":
    unittest.main()
