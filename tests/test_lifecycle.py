"""Behavioral checks with isolated ledgers and real local worker processes."""

import asyncio
import contextlib
import io
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from spearmint import aio, dashboard, dagrunner, rundb


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

    def test_browser_identifies_empty_and_populated_ledgers(self):
        with self.assertRaises(FileNotFoundError):
            rundb._connect(readonly=True)
        self.assertFalse(Path(self.tmp.name, "rundb.db").exists())
        rundb.initialize()
        handler = dashboard._LedgerHandler.__new__(dashboard._LedgerHandler)
        handler.path = "/"
        with patch.object(handler, "_send") as send:
            handler.do_GET()
            page = send.call_args.args[0]
        self.assertIn(rundb._db_path(), page)
        self.assertIn('id="ledger-tables"', page)
        self.assertIn("No runs in", page)
        row = rundb.start_managed("visible/stage", "new", ["true"], [])
        rundb.finish_managed(row.run_id, True)
        page = dashboard._table()
        self.assertIn('class="exp"', page)
        self.assertIn("visible/stage", page)

    def test_lsf_wrapper_runs_inside_submission(self):
        async def execute():
            ctx = aio.Ctx("lsf", repo=str(Path.cwd()), root=self.tmp.name)
            job = ctx.submit("stage", ["-c", "print('payload on worker')"],
                             cmd_prefix=["bsub", "-K", "-oo", "{}/log.txt", sys.executable])
            await job
            self.assertEqual(rundb.completion(job.run_id, job.outdir)["exit_code"], 0)
            self.assertIn("payload on worker", Path(job.outdir, "log.txt").read_text())
            self.assertIsNotNone(job._jobid)
        fake = Path("spearmint/examples/fake_bin").resolve()
        with patch.dict(os.environ, {"PATH": f"{fake}{os.pathsep}{os.environ['PATH']}"}):
            asyncio.run(execute())

    def test_wrapper_records_termination(self):
        rundb.initialize()
        row = rundb.start_managed("terminated", "new", ["python"], [])
        env = {**os.environ, "SPEARMINT_RUN_ROW": str(row.run_id),
               "SPEARMINT_RUN_OUTDIR": row.outdir}
        with subprocess.Popen([sys.executable, "-m", "spearmint.worker", "--python", "--",
                               "-c", "import time; print('ready', flush=True); time.sleep(60)"],
                              env=env, stdout=subprocess.PIPE, text=True) as worker:
            try:
                self.assertEqual(worker.stdout.readline().strip(), "ready")
                worker.terminate()
                worker.communicate(timeout=5)
            finally:
                if worker.poll() is None:
                    worker.kill()
                    worker.communicate()
        receipt = rundb.completion(row.run_id, row.outdir)
        self.assertIsNotNone(receipt)
        self.assertNotEqual(receipt["exit_code"], 0)
        rundb.reconcile_wip(row.job_key)
        self.assertEqual(rundb._latest("status", row.job_key, None), "failed")

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

    def test_recovery_adopts_worker_completion_without_driver_finish(self):
        rundb.initialize()
        for code in (0, 7):
            row = rundb.start_managed(f"orphan/{code}", "new", ["python"], [])
            env = {**os.environ, "SPEARMINT_RUN_ROW": str(row.run_id),
                   "SPEARMINT_RUN_OUTDIR": row.outdir}
            result = subprocess.run([sys.executable, "-m", "spearmint.worker", "--python", "--",
                                     "-c", f"raise SystemExit({code})"], env=env)
            self.assertEqual(result.returncode, code)
            self.assertEqual(rundb._latest("status", row.job_key, None), "wip")
            rundb.reconcile_wip(row.job_key)
            self.assertEqual(rundb._latest("status", row.job_key, None),
                             "done" if code == 0 else "failed")
            if code == 0:
                resumed = rundb.start_managed(row.job_key, "extend", ["python"], [])
                self.assertEqual(resumed.outdir, row.outdir)
                rundb.reconcile_wip(row.job_key)
                self.assertEqual(rundb._latest("status", row.job_key, None), "wip")

    def test_unknown_lsf_and_malformed_receipt_do_not_fail_or_duplicate(self):
        rundb.initialize()
        row = rundb.start_managed("unknown", "new", ["python"], [])
        rundb.set_lsf_jobid(row.run_id, "123")
        with patch.object(rundb.subprocess, "run", return_value=
                          subprocess.CompletedProcess([], 1, "", "scheduler unavailable")):
            rundb.reconcile_wip(row.job_key)
        self.assertEqual(rundb.decide(row.job_key, []).action, "wait")
        with self.assertRaises(rundb.RunBusy):
            with patch.object(rundb.subprocess, "run", side_effect=OSError):
                rundb.start_managed(row.job_key, "replace", ["python"], [])
        with patch.object(rundb.Path, "read_text", return_value='{"exit_code":0}'):
            self.assertIsNone(rundb.completion(row.run_id, row.outdir))
        with patch.object(rundb.subprocess, "run", return_value=
                          subprocess.CompletedProcess([], 0, "EXIT\n", "")):
            rundb.reconcile_wip(row.job_key)
        self.assertEqual(rundb._latest("status", row.job_key, None), "failed")

    def test_two_claimants_cannot_insert_duplicate_attempts(self):
        rundb.initialize()
        barrier = threading.Barrier(2)
        check = rundb._assert_not_running

        def rendezvous(key):
            check(key)
            barrier.wait(timeout=5)

        def claim():
            try:
                return rundb.start_managed("race", "new", ["true"], [])
            except rundb.RunBusy:
                return None

        with patch.object(rundb, "_assert_not_running", side_effect=rendezvous):
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(lambda _: claim(), range(2)))
        self.assertEqual(sum(row is not None for row in outcomes), 1)
        with contextlib.closing(rundb._connect(readonly=True)) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM runs").fetchone()[0], 1)

    def test_two_driver_processes_run_shared_stage_once(self):
        rundb.initialize()
        program = """
import asyncio, sys
from pathlib import Path
from spearmint import aio, rundb
rundb._provenance = lambda: ('test', '')
aio.WAIT_POLL_SECONDS = 0.02
async def run():
    ctx = aio.Ctx('shared', repo=str(Path.cwd()), root=sys.argv[1])
    job = ctx.submit('stage', [sys.executable, '-c', 'import time; time.sleep(0.3)'])
    await job
asyncio.run(run())
"""
        commands = [sys.executable, "-c", program, self.tmp.name]
        with subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True) as first, \
             subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True) as second:
            try:
                for proc in (first, second):
                    output, _ = proc.communicate(timeout=10)
                    self.assertEqual(proc.returncode, 0, output)
            finally:
                for proc in (first, second):
                    if proc.poll() is None:
                        proc.kill()
                        proc.communicate()
        with contextlib.closing(rundb._connect(readonly=True)) as conn:
            rows = conn.execute("SELECT job_key, status FROM runs").fetchall()
        self.assertEqual(rows, [("shared/stage", "done")])

    def test_failure_blocks_dependents_and_records_exit(self):
        async def execute():
            ctx = aio.Ctx("failure", repo=str(Path.cwd()), root=self.tmp.name)
            root = ctx.submit("root", [sys.executable, "-c", "raise SystemExit(3)"])
            leaf = ctx.submit("leaf", ["true"], deps=[root])
            results = await asyncio.gather(root, leaf, return_exceptions=True)
            self.assertTrue(all(isinstance(r, aio.JobFailed) for r in results))
            self.assertIsNone(leaf.run_id)
            self.assertEqual(rundb.completion(root.run_id, root.outdir)["exit_code"], 3)
        asyncio.run(execute())


if __name__ == "__main__":
    unittest.main()
