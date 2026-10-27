#!/usr/bin/env python3
"""Repeat isolated localhost benchmarks and retain every trial, including failures."""
import argparse
import csv
import datetime
import hashlib
import itertools
import json
import os
import pathlib
import platform
import random
import shutil
import socket
import subprocess
import sys
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]


def command_text(args):
    try:
        return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metadata(args):
    cache = args.binary_dir.parent / "CMakeCache.txt"
    flags = {}
    if cache.exists():
        for line in cache.read_text().splitlines():
            if line.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:",
                                "CMAKE_CXX_FLAGS:", "CMAKE_CXX_FLAGS_RELEASE:")):
                key, value = line.split("=", 1)
                flags[key.split(":")[0]] = value
    files = [ROOT / "CMakeLists.txt", ROOT / "bench/bench_cluster.cpp", pathlib.Path(__file__).resolve()]
    files += sorted((ROOT / "src").rglob("*.cpp")) + sorted((ROOT / "include").rglob("*.h"))
    hardware = {"architecture": platform.machine(), "logical_cpus": os.cpu_count()}
    if platform.system() == "Darwin":
        for key, sysctl in [("cpu", "machdep.cpu.brand_string"), ("memory_bytes", "hw.memsize"),
                            ("physical_cpus", "hw.physicalcpu")]:
            hardware[key] = command_text(["sysctl", "-n", sysctl])
        hardware["os"] = command_text(["sw_vers", "-productVersion"])
        hardware["filesystem"] = command_text(["df", "-T", "apfs", str(args.output.parent)])
    else:
        hardware["os"] = platform.platform()
        if pathlib.Path("/proc/cpuinfo").exists():
            hardware["cpu"] = next((line.split(":", 1)[1].strip() for line in
                pathlib.Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown")
        if pathlib.Path("/proc/meminfo").exists():
            hardware["memory"] = pathlib.Path("/proc/meminfo").read_text().splitlines()[0]
    return {
        "schema_version": 1, "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "base_commit": command_text(["git", "rev-parse", "HEAD"]),
        "working_tree_status": command_text(["git", "status", "--short"]),
        "source_sha256": {str(path.relative_to(ROOT)): sha256(path) for path in files},
        "binary_sha256": {name: sha256(args.binary_dir / name) for name in ("dkv_node", "bench_cluster")},
        "compiler": command_text([flags.get("CMAKE_CXX_COMPILER", "c++"), "--version"]),
        "cmake": command_text(["cmake", "--version"]), "build": flags, "hardware": hardware,
        "parameters": {key: str(value) if isinstance(value, pathlib.Path) else value
                       for key, value in vars(args).items()},
        "methodology": {
            "placement": "all nodes and client on one host; IPv4 loopback; client targets node1 only",
            "load": "closed loop; one outstanding request per client; fixed preloaded keyspace; SET updates",
            "timing": "steady_clock; checked preload and per-client warmup excluded; final requests drain",
            "latency": "nearest-rank successful-request percentiles; no coordinated-omission correction",
            "persistence": "WAL enabled; 10 ms timer, 100-append sync threshold; snapshot interval 1000000000",
            "server": "4 request workers per node; 128 virtual nodes; default heartbeat settings",
            "aggregation": "raw per-trial results; plots use median and min/max across repeats; failures retained",
            "resource_scope": "CPU/RSS utilization and network bytes are not measured in this experiment",
        },
    }


class Cluster:
    def __init__(self, binary, directory, count):
        self.binary, self.directory, self.count = binary, directory, count
        self.processes, self.logs, self.ports = [], [], []
        self.commands = []
        self.cleanup_ok = True

    def start(self):
        # Reserve distinct ports together, then release immediately before spawning.
        reserved = []
        try:
            for _ in range(self.count):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reserved.append(sock)
                self.ports.append(sock.getsockname()[1])
            config = self.directory / "cluster.conf"
            config.write_text("".join(f"node{i + 1} 127.0.0.1:{port}\n"
                                      for i, port in enumerate(self.ports)))
        finally:
            for sock in reserved:
                sock.close()
        for i, port in enumerate(self.ports, 1):
            data = self.directory / "data" / f"node{i}"
            quorum = 1 if self.count == 1 else 2
            command = [str(self.binary), "--node-id", str(i), "--port", str(port),
                       "--cluster-conf", str(config), "--replication-factor", str(self.count),
                       "--write-quorum", str(quorum), "--read-quorum", str(quorum),
                       "--worker-threads", "4", "--snapshot-interval", "1000000000",
                       "--fsync-interval-ms", "10", "--log-level", "WARN"]
            for flag, name in [("--wal-dir", "wal"), ("--snapshot-dir", "snapshots"), ("--hints-dir", "hints")]:
                command += [flag, str(data / name)]
            log = (self.directory / f"node{i}.log").open("wb")
            self.logs.append(log)
            self.commands.append(command)
            self.processes.append(subprocess.Popen(command, stdout=log, stderr=log))
        for process, port in zip(self.processes, self.ports):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"node on {port} exited {process.returncode}; inspect log")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5) as sock:
                        sock.sendall(b"PING\n")
                        with sock.makefile("rb") as stream:
                            if stream.readline() == b"+PONG\n":
                                break
                except OSError:
                    pass
                time.sleep(0.02)
            else:
                raise RuntimeError(f"node on {port} did not become ready")

    def close(self):
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                self.cleanup_ok = False
            if process.returncode != 0:
                self.cleanup_ok = False
        for log in self.logs:
            log.close()


def run_trial(args, index, nodes, workload, clients, repeat):
    relative = pathlib.Path("trials") / f"{index:03d}-n{nodes}-{workload}-c{clients}-r{repeat}"
    directory = args.output / relative
    directory.mkdir(parents=True)
    cluster = Cluster(args.binary_dir / "dkv_node", directory, nodes)
    trial = {"index": index, "nodes": nodes, "workload": workload, "clients": clients,
             "repeat": repeat, "artifacts": str(relative), "error": "",
             "metrics": {"valid": False, "successful": 0, "attempted": 0, "errors": 0,
                         "setup_errors": 1, "success_ops_per_sec": 0, "p50_us": 0, "p99_us": 0}}
    try:
        cluster.start()
        command = [str(args.binary_dir / "bench_cluster"), "--host", "127.0.0.1",
                   "--port", str(cluster.ports[0]), "--workload", workload,
                   "--threads", str(clients), "--duration-seconds", str(args.seconds),
                   "--keys", str(args.keys), "--key-size", "16", "--val-size", "64",
                   "--warmup-ops", str(args.warmup_ops), "--pipeline", "1", "--json"]
        trial["benchmark_command"] = command
        with (directory / "benchmark.stdout").open("w") as out, (directory / "benchmark.stderr").open("w") as err:
            completed = subprocess.run(command, stdout=out, stderr=err, timeout=args.seconds + 120)
        trial["benchmark_exit_code"] = completed.returncode
        trial["metrics"] = json.loads((directory / "benchmark.stdout").read_text())
        if completed.returncode != 0 or not trial["metrics"]["valid"]:
            trial["error"] = "benchmark reported a failed or incomplete run"
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        trial["error"] = f"{type(error).__name__}: {error}"
    finally:
        cluster.close()
        trial["cleanup_ok"] = cluster.cleanup_ok
        trial["node_commands"] = cluster.commands
        if not cluster.cleanup_ok:
            trial["error"] += "; node exited abnormally or required forced cleanup"
        if trial["error"]:
            trial["metrics"]["valid"] = False
        (directory / "result.json").write_text(json.dumps(trial, indent=2) + "\n")
        # Only delete persistence files owned by this successful, fresh trial.
        # Failed trials retain them for diagnosis; logs and configs always remain.
        if trial["metrics"]["valid"] and not args.keep_data:
            shutil.rmtree(directory / "data")
    return trial


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-dir", type=pathlib.Path, default=ROOT / "build/release/bin")
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--nodes", default="1,3", help="supported configurations: 1 and 3")
    parser.add_argument("--clients", default="1,2,4,8,16,32")
    parser.add_argument("--workloads", default="get,set,mixed")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--keys", type=int, default=10000)
    parser.add_argument("--warmup-ops", type=int, default=500)
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    args.binary_dir, args.output = args.binary_dir.resolve(), args.output.resolve()
    nodes = [int(value) for value in args.nodes.split(",")]
    clients = [int(value) for value in args.clients.split(",")]
    workloads = args.workloads.split(",")
    if (not set(nodes) <= {1, 3} or min(clients) < 1 or args.repeats < 1 or
            not 0 < args.seconds <= 3600 or args.keys < 1 or args.warmup_ops < 0 or
            not set(workloads) <= {"get", "set", "mixed"}):
        parser.error("invalid experiment parameters")
    for name in ("dkv_node", "bench_cluster"):
        if not os.access(args.binary_dir / name, os.X_OK):
            parser.error(f"missing executable: {args.binary_dir / name}")
    if args.output.exists():
        parser.error("output already exists; choose a fresh directory to preserve prior results")
    args.output.mkdir(parents=True)
    manifest = metadata(args)
    (args.output / "metadata.json").write_text(json.dumps(manifest, indent=2) + "\n")
    matrix = list(itertools.product(nodes, workloads, clients, range(1, args.repeats + 1)))
    random.Random(args.seed).shuffle(matrix)
    fields = ["index", "nodes", "workload", "clients", "repeat", "valid", "successful", "attempted",
              "errors", "setup_errors", "success_ops_per_sec", "p50_us", "p99_us", "error"]
    failures = 0
    with (args.output / "trials.jsonl").open("w") as raw, (args.output / "summary.csv").open("w", newline="") as summary:
        writer = csv.DictWriter(summary, fieldnames=fields)
        writer.writeheader()
        for index, (node_count, workload, client_count, repeat) in enumerate(matrix, 1):
            print(f"[{index}/{len(matrix)}] nodes={node_count} {workload} clients={client_count} repeat={repeat}", flush=True)
            trial = run_trial(args, index, node_count, workload, client_count, repeat)
            raw.write(json.dumps(trial) + "\n")
            raw.flush()
            row = {**trial, **trial["metrics"]}
            writer.writerow({field: row[field] for field in fields})
            summary.flush()
            failures += not trial["metrics"]["valid"]
            metrics = trial["metrics"]
            print(f"  {metrics['success_ops_per_sec']:.0f} successful ops/s; p99 {metrics['p99_us']:.1f} us; "
                  f"{'OK' if metrics['valid'] else trial['error']}", flush=True)
    manifest["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    manifest["trials"] = len(matrix)
    manifest["failed_trials"] = failures
    (args.output / "metadata.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Saved {len(matrix)} trials ({failures} failed) to {args.output}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
