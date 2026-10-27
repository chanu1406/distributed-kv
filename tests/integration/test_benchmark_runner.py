"""Run small real clusters to check artifact retention and child cleanup."""
import argparse
import csv
import json
import pathlib
import socket
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class BenchmarkRunner(unittest.TestCase):
    def assert_children_stopped(self, trial):
        for command in trial["node_commands"]:
            port = int(command[command.index("--port") + 1])
            with socket.socket() as sock:
                sock.settimeout(0.2)
                self.assertNotEqual(sock.connect_ex(("127.0.0.1", port)), 0,
                                    f"node still accepts requests on {port}")

    def invoke(self, output, binary_dir, nodes="1,3"):
        return subprocess.run([
            "python3", str(ROOT / "scripts/run_benchmarks.py"),
            "--binary-dir", str(binary_dir), "--output", str(output),
            "--nodes", nodes, "--clients", "1", "--workloads", "get",
            "--repeats", "1", "--seconds", "0.05", "--keys", "4",
            "--warmup-ops", "0"], capture_output=True, text=True, timeout=45)

    def test_small_matrix_retains_results_and_stops_all_children(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "results"
            result = self.invoke(output, BINARY_DIR)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            trials = [json.loads(line) for line in (output / "trials.jsonl").read_text().splitlines()]
            self.assertEqual(len(trials), 2)
            self.assertEqual({trial["nodes"] for trial in trials}, {1, 3})
            for trial in trials:
                self.assertTrue(trial["metrics"]["valid"])
                self.assertGreater(trial["metrics"]["successful"], 0)
                self.assertTrue(trial["cleanup_ok"])
                self.assert_children_stopped(trial)
                self.assertTrue((output / trial["artifacts"] / "node1.log").exists())
            with (output / "summary.csv").open() as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), 2)

    def test_failed_benchmark_is_retained_and_fails_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binaries = root / "bin"
            binaries.mkdir()
            (binaries / "dkv_node").symlink_to(BINARY_DIR / "dkv_node")
            (binaries / "bench_cluster").symlink_to("/usr/bin/false")
            output = root / "results"
            result = self.invoke(output, binaries, nodes="1")
            self.assertNotEqual(result.returncode, 0, result.stdout)
            trials = [json.loads(line) for line in (output / "trials.jsonl").read_text().splitlines()]
            self.assertEqual(len(trials), 1)
            self.assertFalse(trials[0]["metrics"]["valid"])
            self.assertTrue(trials[0]["cleanup_ok"])
            self.assert_children_stopped(trials[0])
            self.assertTrue(trials[0]["error"])
            self.assertTrue((output / trials[0]["artifacts"] / "benchmark.stderr").exists())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary-dir", required=True)
    args, remaining = parser.parse_known_args()
    BINARY_DIR = pathlib.Path(args.binary_dir).resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
