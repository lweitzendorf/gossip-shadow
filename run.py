#!/usr/bin/env python3
from dataclasses import asdict
import argparse
import json
import os
import random
import datetime
import subprocess
from network_graph import generate_graph
import experiment
import parse_logs


from analyze_message_deliveries import analyse_message_deliveries

params_file_name = "params.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Generate files but not run Shadow")
    parser.add_argument("--node-count", type=int, required=True)
    parser.add_argument("--seed", type=int, required=False, default=1)
    parser.add_argument("--network", type=str, required=False)
    parser.add_argument("--scenario", type=str, required=True)
    parser.add_argument("--protocol", type=str, required=True, choices=["gossipsub", "dog"])
    parser.add_argument("--parallelism", type=int, required=False, default=24)
    parser.add_argument("--output-dir", type=str, required=False)
    args = parser.parse_args()

    if args.output_dir is None:
        try:
            git_describe = subprocess.check_output(
                ["git", "describe", "--always", "--dirty"]
            ).decode("utf-8").strip()
        except subprocess.CalledProcessError:
            git_describe = "unknown"

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        args.output_dir = f"{args.node_count}-{args.network}-{args.scenario}-{args.protocol}---{args.seed}-{timestamp}-{git_describe}.data"

    random.seed(args.seed)

    binaries = experiment.composition(args.protocol)
    experiment_params = experiment.scenario(args.protocol, args.scenario, args.node_count)

    with open(params_file_name, "w") as f:
        d = asdict(experiment_params)
        d["script"] = [
            instruction.model_dump(exclude_none=True)
            for instruction in experiment_params.script
        ]
        json.dump(d, f)

    # Define the binaries we are running
    binary_paths = random.choices(
        [b.path for b in binaries],
        weights=[b.percent_of_nodes for b in binaries],
        k=args.node_count,
    )

    # Generate the network graph and the Shadow config for the binaries
    generate_graph(
        args.network,
        binary_paths,
        "graph.gml",
        "shadow.yaml",
        params_file_location=os.path.join(os.getcwd(), params_file_name),
    )

    if args.dry_run:
        return

    subprocess.run(["make", "binaries"], check=True)

    subprocess.run(
        ["shadow", "--parallelism", f"{args.parallelism}", "--progress", "true", "-d", args.output_dir, "shadow.yaml"],
        check=True,
    )

    # Move files to output_dir
    os.rename("shadow.yaml", os.path.join(args.output_dir, "shadow.yaml"))
    os.rename("graph.gml", os.path.join(args.output_dir, "graph.gml"))
    os.rename("params.json", os.path.join(args.output_dir, "params.json"))

    logs = parse_logs.parse_log_files(args.output_dir)

    print("Processing logs...")
    warmup_time = datetime.timedelta(minutes=2)
    data_dir = os.path.join(args.output_dir, "data")
    parse_logs.process_logs(logs, warmup_time, data_dir)

    print("Generating graphs...")
    parse_logs.generate_plots(args.output_dir, data_dir)


if __name__ == "__main__":
    main()
