#!/usr/bin/env python3
"""Render throughput and p99 curves from retained benchmark trials."""
import argparse
import collections
import json
import os
import pathlib
import statistics
import tempfile


def summarize(trials):
    grouped = collections.defaultdict(list)
    for trial in trials:
        grouped[(trial["nodes"], trial["workload"], trial["clients"])].append(trial["metrics"])

    def interval(values):
        return (min(values), statistics.median(values), max(values)) if values else (None, None, None)

    return {key: {
        # Failed trials stay in goodput summaries; a zero is not discarded.
        "throughput": interval([row["success_ops_per_sec"] for row in rows]),
        # No successful requests means no defined successful-request percentile.
        "p99_us": interval([row["p99_us"] for row in rows if row["successful"] > 0]),
        "failed": sum(not row["valid"] for row in rows), "repeats": len(rows),
    } for key, rows in grouped.items()}


def render(input_dir, output_dir):
    os.environ.setdefault("MPLCONFIGDIR", str(pathlib.Path(tempfile.gettempdir()) / "dkv-matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(pathlib.Path(tempfile.gettempdir()) / "dkv-cache"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import ScalarFormatter

    metadata = json.loads((input_dir / "metadata.json").read_text())
    trials = [json.loads(line) for line in (input_dir / "trials.jsonl").read_text().splitlines()]
    if not trials:
        raise ValueError("no trials to plot")
    summary = summarize(trials)
    parameters = metadata["parameters"]
    hardware = metadata["hardware"]
    failed = sum(not trial["metrics"]["valid"] for trial in trials)
    errors = sum(trial["metrics"].get("errors", 0) for trial in trials)
    output_dir.mkdir(parents=True, exist_ok=True)
    colors = {1: "#2563eb", 3: "#d97706"}
    labels = {1: "1 node · N/W/R = 1/1/1", 3: "3 replicas · N/W/R = 3/2/2"}
    workloads = [workload for workload in ("get", "set", "mixed")
                 if any(trial["workload"] == workload for trial in trials)]
    nodes = sorted({trial["nodes"] for trial in trials})
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "svg.fonttype": "none", "svg.hashsalt": "distributed-kv",
                         "savefig.facecolor": "white"})
    seconds = parameters["seconds"]
    subtitle = (f"{hardware.get('cpu', hardware['architecture'])} · {hardware['logical_cpus']} CPU cores · "
                f"{parameters['keys']:,} keys · 16 B keys / 64 B values · localhost")
    footer = (f"{parameters['repeats']} trials × {seconds:g} s per point · median line, min–max band · "
              f"{errors:,} request errors / {failed} failed trials\n"
              "WAL enabled (10 ms / 100 appends); no checkpoints during measurement. "
              "One ingress node; one outstanding request per client.")
    definitions = [
        ("throughput", "TCP throughput", "Successful operations / second (thousands)", 1000, "throughput"),
        ("p99_us", "TCP p99 latency", "Successful-request p99 latency (ms)", 1000, "latency"),
    ]
    for metric, title, ylabel, divisor, filename in definitions:
        fig, axes = plt.subplots(1, len(workloads), figsize=(12, 4.7), squeeze=False, sharey=True)
        handles = []
        for axis, workload in zip(axes[0], workloads):
            for count in nodes:
                clients = sorted(key[2] for key in summary if key[:2] == (count, workload))
                intervals = [summary[(count, workload, client)][metric] for client in clients]
                valid_points = [(client, interval) for client, interval in zip(clients, intervals)
                                if interval[1] is not None]
                if not valid_points:
                    continue
                xs = [point[0] for point in valid_points]
                lo, med, hi = zip(*(point[1] for point in valid_points))
                line, = axis.plot(xs, [value / divisor for value in med], color=colors[count],
                                  marker="o", markersize=4, linewidth=2, label=labels[count])
                axis.fill_between(xs, [value / divisor for value in lo], [value / divisor for value in hi],
                                  color=colors[count], alpha=0.14, linewidth=0)
                if workload == workloads[0]:
                    handles.append(line)
                for client, interval in valid_points:
                    if summary[(count, workload, client)]["failed"]:
                        axis.scatter(client, interval[1] / divisor, marker="x", s=65, color="#dc2626", zorder=5)
            axis.set_title({"get": "GET", "set": "SET (updates)", "mixed": "50% GET / 50% SET"}[workload],
                           loc="left", fontweight="bold", pad=12)
            axis.set_xscale("log", base=2)
            ticks = sorted({trial["clients"] for trial in trials if trial["workload"] == workload})
            axis.set_xticks(ticks)
            axis.xaxis.set_major_formatter(ScalarFormatter())
            axis.set_xlabel("Concurrent clients")
            axis.grid(axis="y", color="#e2e8f0", linewidth=0.8)
            axis.set_axisbelow(True)
        # Set the shared lower bound only after every panel has contributed
        # data; setting it earlier freezes autoscaling and clips later panels.
        axes[0][0].set_ylim(bottom=0)
        axes[0][0].set_ylabel(ylabel)
        fig.suptitle(title, x=0.07, y=0.98, ha="left", fontsize=19, fontweight="bold", color="#0f172a")
        fig.text(0.07, 0.905, subtitle, fontsize=9, color="#475569")
        fig.legend(handles=handles, loc="upper right", bbox_to_anchor=(0.98, 1.0), frameon=False, fontsize=9)
        fig.text(0.07, 0.025, footer, fontsize=8, linespacing=1.6, color="#475569")
        fig.subplots_adjust(left=0.07, right=0.98, top=0.78, bottom=0.23, wspace=0.30)
        fig.savefig(output_dir / f"{filename}.svg", metadata={"Date": None})
        fig.savefig(output_dir / f"{filename}.png", dpi=160)
        plt.close(fig)
    print(f"Wrote throughput and latency SVG/PNG plots to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    render(args.input, args.output)


if __name__ == "__main__":
    main()
