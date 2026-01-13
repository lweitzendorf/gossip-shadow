#!/usr/bin/env python3
from dataclasses import asdict
import argparse
import json
import os
import random
import datetime
import subprocess
from shadow_config import generate_shadow_config
import experiment
import log_analysis
from tqdm import tqdm


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

    print("Generating experiment params...")
    binaries = experiment.composition(args.protocol)
    experiment_params, time_sec = experiment.scenario(args.protocol, args.scenario, args.node_count)

    params_file_location = os.path.join(os.getcwd(), args.output_dir, "params")
    os.makedirs(params_file_location, exist_ok=True)

    print("Writing experiment params...")
    for node_id, node_params in tqdm(experiment_params.items()):
        with open(os.path.join(params_file_location, f"node{node_id}.json"), "w") as f:
            d = asdict(node_params)            
            d["script"] = [
                instruction.model_dump(exclude_none=True)
                for instruction in node_params.script
            ]
            json.dump(d, f)

    # Define the binaries we are running
    binary_paths = random.choices(
        [b.path for b in binaries],
        weights=[b.percent_of_nodes for b in binaries],
        k=args.node_count,
    )
    
    graph_file_path = os.path.join(args.output_dir, "graph.gml")
    shadow_yaml_file_path = os.path.join(args.output_dir, "shadow.yaml")

    # Generate the network graph and the Shadow config for the binaries
    generate_shadow_config(
        args.network,
        binary_paths,
        graph_file_path,
        shadow_yaml_file_path,
        params_file_location=params_file_location,
    )

    if args.dry_run:
        return

    subprocess.run(["make", "binaries"], check=True)

    stop_time = time_sec * 12 // 10 # stop shadow if it runs 20% longer than expected
    shadow_data_dir = os.path.join(args.output_dir, "shadow.data")
    subprocess.run(
        ["shadow", "--parallelism", f"{args.parallelism}", "--stop-time", f"{stop_time}", "--progress", "true", "-d", shadow_data_dir, shadow_yaml_file_path],
        check=True,
    )

    logs = log_analysis.parse_log_files(args.output_dir)

    print("Processing logs...")
    warmup_time = datetime.timedelta(minutes=2)
    data_dir = os.path.join(args.output_dir, "data")
    log_analysis.process_logs(logs, warmup_time, data_dir)

    print("Generating graphs...")
    log_analysis.generate_plots(args.output_dir, data_dir)


if __name__ == "__main__":
    main()
