import glob
import os
import sys
import json
import yaml
import numpy as np
from dateutil import parser

import networkx as nx


def print_metrics(data_dir: str) -> None:
    json_data = {}        

    for json_path in glob.glob(os.path.join(data_dir, "data", "*.json")):
        file_name = os.path.basename(json_path)

        hours = int(file_name.split(":")[0])
        minutes = int(file_name.split(":")[1])
        total_minutes = 60 * hours + minutes

        with open(json_path, "r") as f:
            json_data[total_minutes] = json.load(f)
                
    test_data = sorted(json_data.items())[-1][1]
    
    total_bandwidth = sum(test_data["bytes_payload"].values()) / 1_000_000

    msg_send_data = test_data["message_sends"]
    msg_delivery_data = test_data["message_deliveries"]

    graph_file_path = os.path.join(data_dir, "graph.gml")
    G = nx.read_gml(graph_file_path, label="id")

    for u, v, data in G.edges(data=True):
        data["latency"] = int(data["latency"].removesuffix(" ms"))

    node_to_network_node = {}
    message_sources = {}    

    G_full = nx.DiGraph()

    with open(os.path.join(data_dir, "shadow.yaml"), "r") as file:
        shadow_config = yaml.safe_load(file)

    for node_name, node_config in shadow_config["hosts"].items():
        node_id = int(node_name.removeprefix("node"))
        network_node_id = node_config["network_node_id"]
        node_to_network_node[node_id] = network_node_id
        G_full.add_node(node_id)

    with open(os.path.join(data_dir, "params.json"), "r") as file:
        instructions = json.load(file)

    for instruction in instructions["script"]:
        if instruction["type"] != "ifNodeIDEquals":
            continue

        node_id = instruction["nodeID"]
        sub_instruction = instruction["instruction"]

        if sub_instruction["type"] == "connect":
            for other_node_id in sub_instruction["connectTo"]:
                nn_1, nn_2 = node_to_network_node[node_id], node_to_network_node[other_node_id]
                latency = nx.shortest_path_length(G, weight="latency", source=nn_1, target=nn_2)
                if latency == 0:
                    # same network node, but there is still latency
                    latency = G.edges[nn_1, nn_2]["latency"]
                G_full.add_edge(node_id, other_node_id, latency=latency)
                G_full.add_edge(other_node_id, node_id, latency=latency)
        elif sub_instruction["type"] == "publish":
            message_sources[sub_instruction["messageID"]] = node_id

    latencies_ms = []
    optimal_latencies_ms = []

    for msg_id in msg_delivery_data:
        send_ts = parser.isoparse(msg_send_data[msg_id])
        delivery_times = [parser.isoparse(ts) for ts in msg_delivery_data[msg_id].values()]

        if len(delivery_times) <= 1:
            continue

        delivery_times.sort()
        delivery_latencies = [ts - send_ts for ts in delivery_times[1:]]
        data_ms = [t.total_seconds() * 1000 for t in delivery_latencies]
        latencies_ms.append(max(data_ms))

        msg_source = message_sources[int(msg_id)]
        optimal_data_ms = nx.shortest_path_length(G_full, weight="latency", source=msg_source)
        optimal_data_ms = sorted(list(optimal_data_ms.values()))
        optimal_latencies_ms.append(max(optimal_data_ms))

    print(f"Bandwidth: {total_bandwidth:.2f} MB")
    print(f"Latency: {np.mean(latencies_ms):.2f} ms")
    print(f"Optimal Latency: {np.mean(optimal_latencies_ms):.2f} ms")


def main():
    data_dir = sys.argv[1]
    print_metrics(data_dir)

        
if __name__ == "__main__":
    main()
    