"""Check that plotting summaries do not silently omit failed trials."""
import importlib.util
import pathlib
import unittest


class PlotSummary(unittest.TestCase):
    def test_failed_trial_remains_in_throughput_range(self):
        path = pathlib.Path(__file__).resolve().parents[2] / "scripts/plot_benchmarks.py"
        self.assertTrue(path.exists(), "plotting implementation is missing")
        spec = importlib.util.spec_from_file_location("plots", path)
        plots = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(plots)
        trials = []
        for goodput, latency, valid in [(100, 10, True), (200, 20, True), (0, 0, False)]:
            trials.append({"nodes": 1, "workload": "get", "clients": 4,
                           "metrics": {"success_ops_per_sec": goodput, "p99_us": latency,
                                       "successful": 100 if valid else 0, "valid": valid}})
        summary = plots.summarize(trials)[(1, "get", 4)]
        self.assertEqual(summary["throughput"], (0, 100, 200))
        self.assertEqual(summary["p99_us"], (10, 15, 20))
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["repeats"], 3)


if __name__ == "__main__":
    unittest.main()
