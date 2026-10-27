"""TCP benchmark regressions against controlled socket responses (stdlib only)."""
import argparse
import contextlib
import json
import socket
import socketserver
import subprocess
import threading
import unittest


@contextlib.contextmanager
def peer(reply):
    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            try:
                for index, line in enumerate(self.rfile):
                    response = reply(line, index)
                    if response is None:
                        return
                    self.wfile.write(response)
                    self.wfile.flush()
            except (ConnectionError, OSError):
                pass

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True

    with Server(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server.server_address[1]
        finally:
            server.shutdown()
            thread.join()


class BenchmarkClient(unittest.TestCase):
    def run_client(self, port, *args):
        return subprocess.run([BINARY, "--host", "127.0.0.1", "--port", str(port),
                               "--ops", "10", "--warmup-ops", "0", "--keys", "4",
                               "--json", *args],
                              text=True, capture_output=True, timeout=10)

    def metrics(self, result):
        self.assertTrue(result.stdout.startswith("{"), result.stdout)
        return json.loads(result.stdout)

    def test_connection_failure_does_not_report_success(self):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))  # occupied but not listening
            result = self.run_client(reserved.getsockname()[1])
        self.assertNotEqual(result.returncode, 0, result.stdout)
        metrics = self.metrics(result)
        self.assertEqual(metrics["successful"], 0)
        self.assertEqual(metrics["attempted"], 0)
        self.assertEqual(metrics["success_ops_per_sec"], 0)
        self.assertGreater(metrics["setup_errors"], 0)

    def test_server_errors_fail_the_benchmark(self):
        with peer(lambda line, i: b"-ERR QUORUM_FAILED\n") as port:
            result = self.run_client(port)
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_incorrect_get_value_is_not_a_success(self):
        with peer(lambda line, i: b"+OK\n" if line.startswith(b"SET ")
                  else b"$5 wrong\n") as port:
            result = self.run_client(port, "--workload", "get")
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_failed_preload_aborts_measurement(self):
        with peer(lambda line, i: b"-ERR QUORUM_FAILED\n" if line.startswith(b"SET ")
                  else b"$64 " + b"v" * 64 + b"\n") as port:
            result = self.run_client(port, "--workload", "get")
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_unknown_workload_is_rejected(self):
        result = self.run_client(1, "--workload", "typo")
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_error_responses_are_excluded_from_goodput(self):
        received = 0

        def reply(line, index):
            nonlocal received
            received += 1
            return b"+OK\n" if received <= 4 else b"-ERR QUORUM_FAILED\n"

        with peer(reply) as port:
            result = self.run_client(port)
        metrics = self.metrics(result)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(metrics["attempted"], 10)
        self.assertEqual(metrics["server_errors"], 10)
        self.assertEqual(metrics["successful"], 0)
        self.assertEqual(metrics["success_ops_per_sec"], 0)

    def test_disconnect_counts_only_finished_successes(self):
        # Preparation uses its own connection; count across connections.
        lock = threading.Lock()
        received = 0

        def reply(line, index):
            nonlocal received
            with lock:
                received += 1
                return b"+OK\n" if received <= 4 else None  # 1 preload + 3 successes

        with peer(reply) as port:
            result = self.run_client(port, "--keys", "1")
        metrics = self.metrics(result)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(metrics["successful"], 3)
        self.assertEqual(metrics["attempted"], 4)
        self.assertEqual(metrics["transport_errors"], 1)
        self.assertAlmostEqual(metrics["success_ops_per_sec"] * metrics["elapsed_seconds"], 3, places=4)

    def test_non_divisible_operation_count_is_preserved(self):
        with peer(lambda line, i: b"+OK\n") as port:
            result = self.run_client(port, "--threads", "3")
        self.assertEqual(result.returncode, 0, result.stderr)
        metrics = self.metrics(result)
        self.assertEqual(metrics["successful"], 10)
        self.assertEqual(metrics["attempted"], 10)
        self.assertEqual(metrics["latency_samples"], 10)

    def test_duration_mode_runs_for_requested_interval(self):
        with peer(lambda line, i: b"+OK\n") as port:
            result = self.run_client(port, "--duration-seconds", "0.05")
        self.assertEqual(result.returncode, 0, result.stderr)
        metrics = self.metrics(result)
        self.assertGreaterEqual(metrics["elapsed_seconds"], 0.05)
        self.assertGreater(metrics["successful"], 10)

    def test_pipeline_is_rejected_until_server_can_order_replies(self):
        with peer(lambda line, i: b"+OK\n") as port:
            result = self.run_client(port, "--pipeline", "2")
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_fragmented_large_value_is_fully_validated(self):
        values = {}

        def reply(line, index):
            parts = line.rstrip(b"\n").split(b" ")
            if parts[0] == b"SET":
                values[parts[2]] = parts[4]
                return b"+OK\n"
            value = values[parts[2]]
            return b"$" + str(len(value)).encode() + b" " + value + b"\n"

        with peer(reply) as port:
            result = self.run_client(port, "--workload", "get", "--val-size", "100000")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.metrics(result)["successful"], 10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    args, remaining = parser.parse_known_args()
    BINARY = args.binary
    unittest.main(argv=[__file__, *remaining], verbosity=2)
