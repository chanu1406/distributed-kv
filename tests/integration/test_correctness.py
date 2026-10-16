"""Real-process crash/recovery and replica regressions; standard library only.

Run: python3 tests/integration/test_correctness.py --binary build/bin/dkv_node
Each run retains node logs and raw persistence files in --artifacts (default /tmp).
"""

import argparse
import pathlib
import os
import random
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import unittest


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Cluster:
    def __init__(self, count, read_quorum, write_quorum=1, snapshot_interval=100000):
        self.root = pathlib.Path(tempfile.mkdtemp(prefix="dkv-correctness-", dir=ARTIFACTS))
        self.ports = [free_port() for _ in range(count)]
        self.processes = {}
        self.logs = []
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum
        self.snapshot_interval = snapshot_interval
        self.config = self.root / "cluster.conf"
        self.config.write_text("".join(
            f"node{i + 1} 127.0.0.1:{port}\n" for i, port in enumerate(self.ports)))
        print(f"artifacts: {self.root}", flush=True)

    def start(self, node):
        directory = self.root / str(node)
        directory.mkdir(exist_ok=True)
        log = open(directory / "node.log", "ab", buffering=0)
        self.logs.append(log)
        args = [BINARY, "--node-id", str(node), "--port", str(self.ports[node - 1]),
                "--cluster-conf", str(self.config), "--replication-factor", str(len(self.ports)),
                "--read-quorum", str(self.read_quorum), "--write-quorum", str(self.write_quorum),
                "--snapshot-interval", str(self.snapshot_interval), "--fsync-interval-ms", "1",
                "--heartbeat-interval-ms", "50", "--heartbeat-timeout-ms", "100"]
        for flag, subdir in [("--wal-dir", "wal"), ("--snapshot-dir", "snap"),
                             ("--hints-dir", "hints")]:
            args.extend([flag, str(directory / subdir)])
        self.processes[node] = subprocess.Popen(args, stdout=log, stderr=log)
        self.wait_for(lambda: self.request(node, "PING\n") == "+PONG\n")

    def request(self, node, frame):
        with socket.create_connection(("127.0.0.1", self.ports[node - 1]), timeout=3) as sock:
            sock.sendall(frame.encode())
            response = b""
            while not response.endswith(b"\n"):
                part = sock.recv(65536)
                if not part:
                    raise RuntimeError(f"node {node} disconnected: {response!r}")
                response += part
            return response.decode()

    @staticmethod
    def wait_for(predicate):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except (ConnectionError, OSError):
                pass
            time.sleep(0.01)
        raise AssertionError("condition did not become true within 5 seconds")

    def stop(self, node, crash=False):
        process = self.processes.pop(node)
        if crash:
            process.kill()
        else:
            process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise AssertionError(f"node {node} hung during shutdown")
        if not crash and process.returncode != 0:
            raise AssertionError(f"node {node} exited {process.returncode}")

    def close(self):
        for node in list(self.processes):
            self.stop(node, crash=True)
        for log in self.logs:
            log.close()


class Correctness(unittest.TestCase):
    def cluster(self, *args, **kwargs):
        cluster = Cluster(*args, **kwargs)
        self.addCleanup(cluster.close)
        return cluster

    def test_compacted_wal_sequence_survives_two_restarts(self):
        c = self.cluster(1, 1, snapshot_interval=2)
        c.start(1)
        self.assertEqual(c.request(1, "SET 1 a 1 x\n"), "+OK\n")
        self.assertEqual(c.request(1, "SET 1 b 1 y\n"), "+OK\n")
        self.assertEqual((c.root / "1/wal/wal.bin").stat().st_size, 0)
        c.stop(1)
        c.snapshot_interval = 100000
        c.start(1)
        self.assertEqual(c.request(1, "SET 1 c 1 z\n"), "+OK\n")
        c.stop(1)  # clean shutdown forces fsync; no ack durability assumption
        c.start(1)
        self.assertEqual(c.request(1, "GET 1 c\n"), "$1 z\n")

    def test_wal_recovery_preserves_lww_origin(self):
        c = self.cluster(1, 1)
        c.start(1)
        self.assertEqual(c.request(1, "RSET 1 k 3 old 100 2\n"), "+OK\n")
        self.assertEqual(c.request(1, "RSET 1 k 3 new 100 3\n"), "+OK\n")
        self.assertEqual(c.request(1, "RGET 1 k\n"), "$V 3 new 100 3\n")
        c.stop(1)
        c.start(1)
        self.assertEqual(c.request(1, "RGET 1 k\n"), "$V 3 new 100 3\n")

    def test_read_requires_configured_quorum(self):
        c = self.cluster(3, 3)
        c.start(1)  # two replicas unavailable
        self.assertEqual(c.request(1, "RSET 1 k 3 old 100 1\n"), "+OK\n")
        self.assertEqual(c.request(1, "GET 1 k\n"), "-ERR QUORUM_FAILED\n")

    def test_newer_tombstone_wins_and_repairs_stale_value(self):
        c = self.cluster(2, 2)
        c.start(1)
        c.start(2)
        self.assertEqual(c.request(1, "RSET 1 k 3 old 100 1\n"), "+OK\n")
        self.assertEqual(c.request(2, "RDEL 1 k 200 2\n"), "+OK\n")
        self.assertEqual(c.request(1, "GET 1 k\n"), "-NOT_FOUND\n")
        c.wait_for(lambda: c.request(1, "RGET 1 k\n") == "$T 200 2\n")
        c.stop(1)
        c.start(1)
        self.assertEqual(c.request(1, "RGET 1 k\n"), "$T 200 2\n")

    def test_local_read_repair_is_recoverable(self):
        c = self.cluster(2, 2)
        c.start(1)
        c.start(2)
        self.assertEqual(c.request(2, "RSET 1 k 3 new 200 2\n"), "+OK\n")
        self.assertEqual(c.request(1, "GET 1 k\n"), "$3 new\n")
        c.wait_for(lambda: c.request(1, "RGET 1 k\n") == "$V 3 new 200 2\n")
        c.stop(1)
        c.start(1)
        self.assertEqual(c.request(1, "RGET 1 k\n"), "$V 3 new 200 2\n")

    def test_malformed_replica_reply_does_not_count_as_quorum(self):
        c = self.cluster(2, 2)

        class BadReplica(socketserver.StreamRequestHandler):
            def handle(self):
                for line in self.rfile:
                    self.wfile.write(b"+PONG\n" if line == b"PING\n" else b"$V 1x a 100 2\n")

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

        with Server(("127.0.0.1", c.ports[1]), BadReplica) as peer:
            thread = threading.Thread(target=peer.serve_forever, daemon=True)
            thread.start()
            try:
                c.start(1)
                self.assertEqual(c.request(1, "GET 1 k\n"), "-ERR QUORUM_FAILED\n")
            finally:
                peer.shutdown()
                thread.join()

    def test_local_tombstone_repairs_remote_replica(self):
        c = self.cluster(2, 2)
        c.start(1)
        c.start(2)
        self.assertEqual(c.request(2, "RSET 1 k 3 old 100 1\n"), "+OK\n")
        self.assertEqual(c.request(1, "RDEL 1 k 100 2\n"), "+OK\n")
        self.assertEqual(c.request(1, "GET 1 k\n"), "-NOT_FOUND\n")
        c.wait_for(lambda: c.request(2, "RGET 1 k\n") == "$T 100 2\n")
        c.stop(2)
        c.start(2)
        self.assertEqual(c.request(2, "RGET 1 k\n"), "$T 100 2\n")

    def test_seeded_lww_reorder_duplicate_and_crash_recovery(self):
        c = self.cluster(3, 3)
        rng = random.Random(1729)
        print("LWW model seed: 1729", flush=True)
        for node in (1, 2, 3):
            c.start(node)
        # Python tuple ordering is the independent oracle. No production
        # comparator or serializer is used to derive expected winners.
        operations = []
        winners = {}
        for timestamp in range(1, 41):
            key = f"k{rng.randrange(5)}"
            origin = rng.randrange(1, 4)
            value = None if rng.randrange(3) == 0 else f"v{timestamp}"
            version = (timestamp // 2, origin)
            # Avoid conflicting payloads with equal versions, whose order is
            # intentionally undefined; duplicates below reuse identical data.
            if any(op[:2] == (key, version) for op in operations):
                continue
            operations.append((key, version, value))
            if key not in winners or version > winners[key][0]:
                winners[key] = (version, value)
        for node in (1, 2, 3):
            deliveries = operations + operations[:10]
            rng.shuffle(deliveries)
            for key, (timestamp, origin), value in deliveries:
                frame = (f"RDEL {len(key)} {key} {timestamp} {origin}\n" if value is None
                         else f"RSET {len(key)} {key} {len(value)} {value} {timestamp} {origin}\n")
                self.assertEqual(c.request(node, frame), "+OK\n")
            # Establish a deterministic persisted boundary, then SIGKILL. This
            # does not assume client acknowledgement waits for an fsync.
            with open(c.root / str(node) / "wal/wal.bin", "rb") as wal:
                os.fsync(wal.fileno())
            c.stop(node, crash=True)
            c.start(node)
            for key, ((timestamp, origin), value) in winners.items():
                expected = (f"$T {timestamp} {origin}\n" if value is None else
                            f"$V {len(value)} {value} {timestamp} {origin}\n")
                self.assertEqual(c.request(node, f"RGET {len(key)} {key}\n"), expected)

    def test_hinted_delete_reaches_restarted_replica(self):
        c = self.cluster(3, 2, write_quorum=2)
        for node in (1, 2, 3):
            c.start(node)
        self.assertEqual(c.request(1, "SET 1 k 3 old\n"), "+OK\n")
        c.stop(3, crash=True)
        self.assertEqual(c.request(1, "DEL 1 k\n"), "+OK\n")
        hint = c.root / "1/hints/hints_3.dat"
        c.wait_for(lambda: hint.exists() and hint.stat().st_size > 0)
        # Wait on observable failure detection, not a fixed sleep, so the
        # rejoin callback is guaranteed to be eligible when node 3 returns.
        c.wait_for(lambda: "is DOWN" in (c.root / "1/node.log").read_text())
        c.start(3)
        c.wait_for(lambda: c.request(3, "RGET 1 k\n").startswith("$T "))
        self.assertEqual(c.request(1, "GET 1 k\n"), "-NOT_FOUND\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--artifacts", default=tempfile.gettempdir())
    args, remaining = parser.parse_known_args()
    BINARY = str(pathlib.Path(args.binary).resolve())
    ARTIFACTS = args.artifacts
    pathlib.Path(ARTIFACTS).mkdir(parents=True, exist_ok=True)
    unittest.main(argv=[__file__, *remaining], verbosity=2)
