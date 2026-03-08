import glob
import os
import json
import shutil
import heapq
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional
from tqdm import tqdm

import yaml
import argparse
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import networkx as nx

from dateutil import parser


peer_ids: dict[str, int] = {}
meshes: dict[str, dict[int, set[int]]] = {}

bytes_sent_payload: dict[int, int] = {}
bytes_received_payload: dict[int, int] = {}
bytes_sent_control: dict[int, int] = {}
bytes_received_control: dict[int, int] = {}

msg_send_times: dict[int, tuple[str, int]] = {}
msg_delivery_times: dict[int, dict[int, str]] = defaultdict(dict)


def add_connection(topic: str, from_id: int, to_id: int) -> None:
    if topic not in meshes:
        meshes[topic] = {}

    mesh = meshes[topic]

    if from_id not in mesh:
        mesh[from_id] = set()

    mesh[from_id].add(to_id)

def remove_connection(topic: str, from_id: int, to_id: int) -> None:
    if not (mesh := meshes.get(topic)):
        return

    if not (peers := mesh.get(from_id)):
        return

    if to_id in peers:
        peers.remove(to_id)

def remove_node(node_id: str):
    for mesh in meshes.values():
        if node_id in mesh:
            mesh.pop(node_id)
            
def register_sent_bytes(node_id: int, num_bytes: int, is_payload: bool) -> None:
    bytes_sent = bytes_sent_payload if is_payload else bytes_sent_control
    bytes_sent[node_id] = bytes_sent.get(node_id, 0) + num_bytes
    
def register_received_bytes(node_id: int, num_bytes: int, is_payload: bool) -> None:
    bytes_received = bytes_received_payload if is_payload else bytes_received_control
    bytes_received[node_id] = bytes_received.get(node_id, 0) + num_bytes

def register_message_send(message_id: int, node_id: int, timestamp: str) -> None:
    if message_id not in msg_send_times:
        msg_send_times[message_id] = timestamp, node_id
    register_message_delivery(message_id, node_id, timestamp)

def register_message_delivery(message_id: int, node_id: int, timestamp: str) -> None:
    if (node_id not in msg_delivery_times[message_id]):
        msg_delivery_times[message_id][node_id] = timestamp
            
def get_time(log: dict) -> str:
    return log.get("event_time", log["time"])
            
class LogEntry:
    def __init__(self, node_id: int, log: dict) -> None:
        self.node = node_id
        self.time = parser.isoparse(get_time(log))
        self.log = log
        
    def get(self, arg, default=None) -> Optional[str | int]:
        return self.log.get(arg, default)
        
    def __getitem__(self, arg) -> str | int:
        return self.log[arg]
    
    def __lt__(self, other) -> bool:
        return self.time < other.time


def save_snapshot(output_dir: str, elapsed_time: Optional[timedelta]) -> None:
    print(f"Taking snapshot @ {elapsed_time} ...")

    all_node_idxs = (
        set(bytes_received_payload.keys()) | set(bytes_received_control.keys()) | 
        set(bytes_sent_payload.keys()) | set(bytes_sent_control.keys())
    )
    snapshot_data = {"nodes": {}}
    
    for node_idx in all_node_idxs:
        snapshot_data["nodes"][node_idx] = {
            "bytes_up": {
                "payload": bytes_sent_payload.get(node_idx, 0),
                "control": bytes_sent_control.get(node_idx, 0),
            },
            "bytes_down": {
                "payload": bytes_received_payload.get(node_idx, 0),
                "control": bytes_received_control.get(node_idx, 0),
            }
        }
    
    with open(os.path.join(output_dir, f"{elapsed_time}.json"), 'w') as f:
        json.dump(snapshot_data, f, indent=2)
        

def parse_log_files(root_dir: str) -> Iterable[LogEntry]:
    if not os.path.exists(root_dir):
        raise FileNotFoundError(f"Directory '{root_dir}' does not exist")
    
    shadow_directory = os.path.join(root_dir, "shadow.data")
    if os.path.exists(shadow_directory):
        root_dir = shadow_directory
        
    root_dir = os.path.join(root_dir, "hosts")
    
    def parse_single_file(_node_idx: int, _file_path: str) -> Iterable[LogEntry]:
        with open(_file_path, 'r') as f:
            for log_line in f:
                try:
                    line_data = json.loads(log_line)
                    yield LogEntry(_node_idx, line_data)
                except json.decoder.JSONDecodeError as e:
                    print(f"Failed to parse line in file {_file_path}: {log_line}")
                    raise e
                        
    heads = []            

    for node_dir in os.listdir(root_dir):
        node_idx = int(node_dir.removeprefix("node"))
            
        for log_file in os.listdir(os.path.join(root_dir, node_dir)):
            if not log_file.endswith(".stdout"):
                continue
            
            file_path = os.path.join(root_dir, node_dir, log_file)
            iterator = parse_single_file(node_idx, file_path)
            if head := next(iterator, None):
                heads.append((head, iterator))
                
    heapq.heapify(heads)

    while heads:
        head, iterator = heapq.heappop(heads)
        yield head
        if head := next(iterator, None):
            heapq.heappush(heads, (head, iterator))
            

def process_logs(logs: Iterable[LogEntry], output_dir: str) -> None:
    snapshot_duration = timedelta(seconds=5)
    genesis_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
    next_snapshot = snapshot_duration

    print(f"Creating {output_dir} ...")
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        
    snapshot_dir = os.path.join(output_dir, "snapshots")
    os.makedirs(snapshot_dir)
    
    elapsed_time = None

    for i, log in enumerate(logs):
        node_idx = log.node
        elapsed_time = log.time - genesis_time
        
        if (i % 1_000_000) == 0:
            print(f"Processed log entry {i:,} @ {elapsed_time}")

        match log["msg"]:
            case "PeerID":
                peer_ids[log["id"]] = node_idx
            case "Sent Graft":
                add_connection(log.get("topic", "default"), node_idx, peer_ids[log["to"]])
                register_sent_bytes(node_idx, log["size"], False)
            case "Received Graft":
                add_connection(log.get("topic", "default"), node_idx, peer_ids[log["from"]])
                register_received_bytes(node_idx, log["size"], False)
            case "Added Peer":
                add_connection("default",node_idx, log["id"])
            case "Removed Peer":
                remove_connection("default", node_idx, log["id"])
            case "Sent Prune":
                remove_connection(log.get("topic", "default"), node_idx, peer_ids[log["to"]])
                register_sent_bytes(node_idx, log["size"], False)
            case "Received Prune":
                remove_connection(log.get("topic", "default"), node_idx, peer_ids[log["from"]])
                register_received_bytes(node_idx, log["size"], False)
            case "Sent Message":
                register_message_send(log["id"], node_idx, get_time(log))
                register_sent_bytes(node_idx, log["size"], True)
            case "Received Message":
                register_message_delivery(log["id"], node_idx, get_time(log))
                register_received_bytes(node_idx, log["size"], True)
            case msg if msg.startswith("Sent"):
                register_sent_bytes(node_idx, log["size"], False)
            case msg if msg.startswith("Received"):
                register_received_bytes(node_idx, log["size"], False)
                
        if (elapsed_time >= next_snapshot):
            save_snapshot(snapshot_dir, elapsed_time)
            while elapsed_time >= next_snapshot:
                next_snapshot += snapshot_duration
            
    save_snapshot(snapshot_dir, elapsed_time)
    
    message_data = {}
    
    for msg_id in sorted(msg_send_times.keys()):
        message_data[msg_id] = {
            "source": msg_send_times[msg_id][1],
            "sent": msg_send_times[msg_id][0],
            "received": msg_delivery_times[msg_id]
        }
        
    with open(os.path.join(output_dir, "messages.json"), 'w') as f:
        json.dump(message_data, f, indent=2)
    


def get_message_sources(test_dir: str) -> dict[int, int]:
    message_sources = {}

    for file_name in os.listdir(os.path.join(test_dir, "params")):
        if not file_name.startswith("node") or not file_name.endswith(".json"):
            continue

        with open(os.path.join(test_dir, "params", file_name), "r") as file:
            params = json.load(file)

        node_id = int(file_name.removeprefix("node").removesuffix(".json"))
        for script_instruction in params["script"]:
            for instruction in script_instruction["instructions"]:
                if instruction["type"] == "publish":
                    message_sources[instruction["messageID"]] = node_id

    return message_sources


def get_message_source_locations(test_dir: str) -> dict[int, str]:
    graph_file_path = os.path.join(test_dir, "graph.gml")
    G = nx.read_gml(graph_file_path, label="id")

    for _, _, data in G.edges(data=True):
        data["latency"] = int(data["latency"].removesuffix(" ms"))
    
    node_to_network_node = {}

    with open(os.path.join(test_dir, "shadow.yaml"), "r") as file:
        shadow_config = yaml.safe_load(file)

    for node_name, node_config in shadow_config["hosts"].items():
        node_id = int(node_name.removeprefix("node"))
        network_node_id = node_config["network_node_id"]
        node_to_network_node[node_id] = network_node_id
        
    message_sources = get_message_sources(test_dir)
    source_locations = {}    
    
    for msg_id, node_id in message_sources.items():
        network_node_id = node_to_network_node[node_id]
        node_location = G.nodes[network_node_id]["label"].split("-")[0]
        source_locations[msg_id] = node_location
            
    return source_locations


def plot_total_network_traffic(plots_dir: str, snapshot_data: list[tuple[int, dict]]) -> None:
    elapsed_micros = [0]
    total_bandwidth_up = [0]
    total_bandwidth_down = [0]
    
    for i, (total_microseconds, data) in enumerate(snapshot_data):
        snapshot_bandwidth_up = sum([node_data["bytes_up"]["payload"] for node_data in data["nodes"].values()])
        snapshot_bandwidth_down = sum([node_data["bytes_down"]["payload"] for node_data in data["nodes"].values()])
        elapsed_micros.append(total_microseconds)
        total_bandwidth_up.append(snapshot_bandwidth_up)
        total_bandwidth_down.append(snapshot_bandwidth_down)
      
    x, y_up, y_down = [], [], []
        
    i = 1
    for j in range(i+1, len(total_bandwidth_up)):
        delta_micros = elapsed_micros[j] - elapsed_micros[i]
        delta_bytes_up = total_bandwidth_up[j] - total_bandwidth_up[i]
        delta_bytes_down = total_bandwidth_down[j] - total_bandwidth_down[i]
        if (delta_micros >= 5_000_000):
            x.append(elapsed_micros[j] / 60_000_000) # minutes
            y_up.append(8 * delta_bytes_up / delta_micros) # Mbps
            y_down.append(8 * delta_bytes_down / delta_micros) # Mbps
            i = j

    plt.xlabel("Time (minutes)")
    plt.ylabel("Traffic (Mbps)")
    plt.title("Total Network Traffic")

    # plt.plot(x, y_down, label="Download")
    plt.plot(x, y_up, label="Upload")
    
    # plt.legend()
    plt.xlim((x[0], x[-1]))
    plt.ylim((0, 18))

    plt.savefig(os.path.join(plots_dir, "network_traffic.png"))
    plt.clf()


def plot_payload_traffic_by_node(plots_dir: str, snapshot_data: list[tuple[int, dict]]) -> None:
    x, y_up, y_down = [], {}, {}
    
    y_up["honest"] = []
    y_up["malicious"] = []
    y_down["honest"] = []
    y_down["malicious"] = []
    
    for i, (total_microseconds, data) in enumerate(snapshot_data):
        x.append(total_microseconds / 60_000_000)
        
        sum_malicious_up = sum([node_data["bytes_up"]["payload"] for node_id, node_data in data["nodes"].items() if int(node_id) >= 50])
        sum_malicious_down = sum([node_data["bytes_down"]["payload"] for node_id, node_data in data["nodes"].items() if int(node_id) >= 50])
        sum_honest_up = sum([node_data["bytes_up"]["payload"] for node_id, node_data in data["nodes"].items() if int(node_id) < 50])
        sum_honest_down = sum([node_data["bytes_down"]["payload"] for node_id, node_data in data["nodes"].items() if int(node_id) < 50])
        
        y_up["malicious"].append(sum_malicious_up / 1_000_000_000) # GB
        y_down["malicious"].append(sum_malicious_down / 1_000_000_000) # GB
        y_up["honest"].append(sum_honest_up / 1_000_000_000) # GB
        y_down["honest"].append(sum_honest_down / 1_000_000_000) # GB          
            
            
    plt.xlabel("Time (minutes)")
    plt.ylabel("Traffic (GB)")

    plt.title("Cumulative Upload Traffic by Node Type")
    #for node_id, y in y_up.items():
    #    plt.plot(x, y, label=node_id)
    plt.plot(x, y_up["honest"], label="honest", color="tab:green")
    plt.plot(x, y_up["malicious"], label="malicious", color="tab:red")

    plt.legend()
    plt.savefig(os.path.join(plots_dir, "traffic_by_node_up.png"))
    plt.clf()
    
    plt.xlabel("Time (minutes)")
    plt.ylabel("Traffic (GB)")

    plt.title("Cumulative Download Traffic by Node Type")
    # for node_id, y in y_down.items():
    #    plt.plot(x, y, label=node_id)

    plt.plot(x, y_down["honest"], label="honest", color="tab:green")
    plt.plot(x, y_down["malicious"], label="malicious", color="tab:red")

    plt.legend()
    plt.savefig(os.path.join(plots_dir, "traffic_by_node_down.png"))
    plt.clf()


def plot_message_delivery_times(plots_dir: str, delivery_latencies_ms: list[tuple[int, list[float]]], source_locations: dict[int, str]) -> None:
    locations = sorted(list(set(source_locations.values())))
    latencies = {location: [] for location in locations}
        
    for msg_id, msg_latencies in delivery_latencies_ms:
        location = source_locations[msg_id]
        latencies[location].extend(msg_latencies)
        
    min_latency = float('inf')
    max_latency = 0
    
    for location in latencies.keys():
        x = np.sort(latencies[location])
        if len(x) > 0:
            y = [(j + 1) / len(x) for j in range(len(x))]
            plt.plot(x, y, label=location)
            min_latency = min(min_latency, x[0])
            max_latency = max(max_latency, x[-1])
        
    plt.title("Message Latency by Source Location")
    plt.xscale('log')
    plt.xlabel("Time (seconds)")
    plt.xlim((min_latency, max_latency))
    plt.ylim(0, 1)
    plt.legend(loc="upper left")

    plt.savefig(os.path.join(plots_dir, "message_latency.png"))
    plt.clf()
    
    

def plot_message_delivery_percentiles(plots_dir: str, delivery_latencies_ms: list[tuple[int, list[float]]]) -> None:
    plt.xlabel("Message ID")
    plt.ylabel("Time (milliseconds)")
    plt.title("Message Delivery Percentiles")
    
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    x = []
    y = {
        20: [],
        33: [],
        50: [],
        67: [],
        80: [],
        100: [],
    }
        
    for msg_id, msg_latencies in delivery_latencies_ms:
        x.append(msg_id)
        for percentile in y.keys():
            latency = np.percentile(msg_latencies, percentile)
            y[percentile].append(latency)
            
    colors = {
        100: "tab:green",
        80: "tab:olive",
        67: "tab:orange",
        33: "tab:red",
    }
    
    for percentile, color in colors.items():
        plt.plot(x, y[percentile], color=color, label=f"P{percentile}")
        plt.fill_between(x, y[percentile], color=color)
    
    plt.legend(loc="upper left")
    
    plt.xlim(x[0], x[-1])
    plt.ylim((0, 1700))

    plt.savefig(os.path.join(plots_dir, "message_latency_percentiles.png"))
    plt.clf()
    
def plot_median_delivery_histogram(plots_dir: str, delivery_latencies_ms: list[tuple[int, list[float]]]) -> None:
    plt.xlabel("Time (milliseconds)")
    plt.ylabel("Message count")
    plt.title("Median Message Delivery Times")
    
    x = []
    for _, msg_latencies in delivery_latencies_ms:
        median_latency = np.median(msg_latencies)
        x.append(median_latency)

    counts, bins = np.histogram(x, bins=30)
    plt.stairs(counts, bins)

    plt.savefig(os.path.join(plots_dir, "median_latency_hist.png"))
    plt.clf()


def get_snapshot_data(data_dir: str, warmup_time: timedelta) -> list[tuple[int, dict]]:
    snapshot_data = {}
    snapshot_files = glob.glob(os.path.join(data_dir, "snapshots", "*.json"))
    for file_path in tqdm(snapshot_files, "Parsing data snapshots"):
        file_name = os.path.basename(file_path)
        hours = int(file_name.split(":")[0])
        minutes = int(file_name.split(":")[1])
        seconds = int(file_name.split(":")[2].split(".")[0])
        
        try:
            microseconds = int(file_name.split(":")[2].split(".")[1])
        except ValueError:
            microseconds = 0
            
        total_microseconds = 1_000_000 * (60 * (60 * hours + minutes) + seconds) + microseconds
        if timedelta(microseconds=total_microseconds) <= warmup_time:
            continue

        with open(file_path, 'r') as f:
            snapshot_data[total_microseconds] = json.load(f)
    
    return sorted(snapshot_data.items())


def generate_plots(test_dir: str, data_dir: str, warmup_time: timedelta) -> str:
    plots_dir = os.path.join(test_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    source_locations = get_message_source_locations(test_dir)
    snapshot_data = get_snapshot_data(data_dir, warmup_time)
    
    with open(os.path.join(data_dir, "messages.json"), 'r') as f:
        message_data = json.load(f)
    
    msg_delivery_latencies: list[tuple[int, list[float]]] = []

    for msg_id, msg_data in tqdm(message_data.items(), desc="Calculating message delivery times"):        
        send_ts = parser.isoparse(msg_data["sent"])
        delivery_times = [parser.isoparse(ts) for ts in msg_data["received"].values()]
        delivery_times.sort()
        delivery_latencies = [ts - send_ts for ts in delivery_times if ts > send_ts]  # TODO: use active set info
        delivery_latencies_ms = [t.total_seconds() * 1000 for t in delivery_latencies]
        
        if len(delivery_latencies_ms) > 1:
            msg_delivery_latencies.append((int(msg_id), delivery_latencies_ms))
            
    msg_delivery_latencies.sort(key=lambda x: x[0])
    
    plot_total_network_traffic(plots_dir, snapshot_data)
    plot_payload_traffic_by_node(plots_dir, snapshot_data)
    
    plot_median_delivery_histogram(plots_dir, msg_delivery_latencies)
    plot_message_delivery_times(plots_dir, msg_delivery_latencies, source_locations)
    plot_message_delivery_percentiles(plots_dir, msg_delivery_latencies)    

    return plots_dir

def parse_args():
    parser = argparse.ArgumentParser(description="Generate structured data and plots from simulation logs.")
    parser.add_argument("-d", "--dir", type=str, required=True, help="Path to simulation output directory.")
    parser.add_argument("-o", "--output-dir", type=str, default="data", help="Relative path to simulation directory to use for parsed data.")
    parser.add_argument("-w", "--warmup-time", type=int, default=120, help="Warmup time in seconds to ignore initial logs.")
    parser.add_argument("--use-existing-data", action="store_true", help="Use previously parsed data instead of re-parsing logs.")
    return parser.parse_args()

def main():
    args = parse_args()
    
    if not os.path.exists(args.dir):
        raise FileNotFoundError(f"{args.dir} does not exist")
    
    data_dir = os.path.join(args.dir, args.output_dir)
    warmup_time = timedelta(seconds=args.warmup_time)
    
    if not (args.use_existing_data and os.path.exists(data_dir)):
        print("Parsing log files...")
        logs = parse_log_files(args.dir)
        print("Processing logs...")
        process_logs(logs, data_dir)
    
    print("Generating graphs...")
    generate_plots(args.dir, data_dir, warmup_time)
    
    print("Done.")

        
if __name__ == "__main__":
    main()
