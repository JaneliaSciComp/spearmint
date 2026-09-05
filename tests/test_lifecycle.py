"""Behavioral checks with isolated ledgers and real local worker processes."""

import asyncio
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from spearmint import aio, dagrunner, rundb


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.anchor = patch.object(rundb, "_ANCHOR", None)
        self.anchor.start()
        self.addCleanup(self.anchor.stop)
        self.provenance = patch.object(rundb, "_provenance", return_value=("test", ""))
        self.provenance.start()
        self.addCleanup(self.provenance.stop)
        rundb.anchor(self.tmp.name, repo=str(Path.cwd()))

    def test_plan_creates_nothing(self):
        stage = dagrunner.Stage(name="root", job_key="plan/root", command=lambda: self.fail())
        with contextlib.redirect_stdout(io.StringIO()):
            result = dagrunner.run_experiment([stage], plan=True)
        self.assertEqual(result, {"plan/root": "run"})
        self.assertEqual(list(Path(self.tmp.name).iterdir()), [])

    def test_dependency_changes_and_force_modes(self):
        async def execute(mode=None, dependency=True):
            ctx = aio.Ctx("test", repo=str(Path.cwd()), root=self.tmp.name)
            root = ctx.submit("root", [sys.executable, "-c", "pass"], force=mode)
            leaf = ctx.submit("leaf", [sys.executable, "-c", "pass"],
                              deps=[root] if dependency else [])
            await asyncio.gather(root, leaf)
            return root, leaf

        first = asyncio.run(execute())
        repeat = asyncio.run(execute())
        self.assertTrue(all(j.skipped for j in repeat))
        for mode in ("new", "extend", "replace"):
            current = asyncio.run(execute(mode))
            self.assertTrue(all(j.ran for j in current))
            if mode == "new":
                self.assertNotEqual(current[0].outdir, first[0].outdir)
            else:
                self.assertEqual(current[0].outdir, previous[0].outdir)
            previous = current
        self.assertTrue(asyncio.run(execute(dependency=False))[1].ran)
        self.assertTrue(asyncio.run(execute(dependency=True))[1].ran)

    def test_external_dependency_is_recorded(self):
        rundb.initialize()
        upstream = rundb.start_managed("external", "new", ["true"], [])
        rundb.finish_managed(upstream.run_id, True)
        external = dagrunner.Stage(name="ext", job_key="external", command=lambda: ["true"])
        leaf = dagrunner.Stage(name="leaf", job_key="ext/leaf", command=lambda: ["true"],
                              requires=[external])
        self.assertEqual(dagrunner.run_experiment([leaf])["ext/leaf"], "done")
        self.assertEqual(dagrunner.run_experiment([leaf])["ext/leaf"], "skipped")
        upstream = rundb.start_managed("external", "new", ["true"], [])
        rundb.finish_managed(upstream.run_id, True)
        self.assertEqual(dagrunner.run_experiment([leaf])["ext/leaf"], "done")


if __name__ == "__main__":
    unittest.main()
