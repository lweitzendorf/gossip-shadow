from collections import defaultdict
from dataclasses import dataclass, field
import random
from typing import List, Dict, Set, Optional

import networkx as nx

from script_instruction import GossipSubParams, ScriptInstruction, NodeID
import script_instruction


@dataclass
class Binary:
    path: str
    percent_of_nodes: int


@dataclass
class ExperimentParams:
    script: List[ScriptInstruction] = field(default_factory=list)


def scenario(protocol: str, scenario_name: str, node_count: int) -> tuple[dict[int, ExperimentParams], int]:
    instructions = {node_id: [] for node_id in range(node_count)}
    
    def add_instructions(_instructions: list[ScriptInstruction], _node_id: Optional[int] = None):
        if _node_id is None:
            for _node_id in range(node_count):
                instructions[_node_id].extend(_instructions)
        else:
            instructions[_node_id].extend(_instructions)

    def add_mapped_instructions(_instruction_map: dict[int, list[ScriptInstruction]]):
        for _node_id, _node_instructions in _instruction_map.items():
            instructions[_node_id].extend(_node_instructions)

    num_messages = 1200
    message_size = 1024
    topics = ["topic-a"]
        
    match protocol:
        case "gossipsub":
            gs_params = GossipSubParams()
            init_instruction = script_instruction.InitGossipSub(gossipSubParams=gs_params)
            add_instructions([init_instruction])
        case _:
            pass
    
    def subscribe_to_topics() -> list[ScriptInstruction]:
        return [script_instruction.SubscribeToTopic(topicID=topic) for topic in topics]
    
    match scenario_name:
        case "random":
            number_of_conns_per_node = min(8, node_count - 1)
            add_mapped_instructions(random_network_mesh(node_count, number_of_conns_per_node))
            add_instructions(subscribe_to_topics())
            add_mapped_instructions(
                random_publish(
                    node_count=node_count,
                    num_messages=num_messages,
                    message_size=message_size,
                    topic_strs=topics,
                    interval_ms=12_000
                )
            )
        case "line-feed-in":
            add_mapped_instructions(line_mesh(node_count))
            add_instructions(subscribe_to_topics())
            add_mapped_instructions(
                random_publish(
                    node_count=node_count,
                    num_messages=round(num_messages * 0.2),
                    message_size=message_size,
                    topic_strs=topics,
                    interval_ms=12_000
                )
            )
            add_mapped_instructions(random_network_mesh(node_count, node_count // 2))
            add_mapped_instructions(
                random_publish(
                    node_count=node_count,
                    num_messages=round(num_messages * 0.8),
                    message_size=message_size,
                    topic_strs=topics,
                    interval_ms=12_000
                )
            )
        case "two-cliques":
            degree = min(node_count // 20, 20)
            z = [degree for _ in range(node_count)]
            G = nx.expected_degree_graph(z)

            # clique A
            for i in range(node_count // 2):
                for j in range(node_count // 2):
                    if i != j:
                        G.add_edge(i, j)

            # clique B
            for i in range(node_count // 2, node_count):
                for j in range(node_count // 2, node_count):
                    if i != j:
                        G.add_edge(i, j)

            for node_id, nbrdict in G.adjacency():
                neighbors = list(nbrdict.keys())
                random.shuffle(neighbors)
                add_instructions(
                    [script_instruction.Connect(connectTo=neighbors)],
                    _node_id=node_id
                )

            add_instructions(subscribe_to_topics())
            add_mapped_instructions(
                random_publish(
                    node_count=node_count,
                    num_messages=num_messages,
                    message_size=message_size,
                    topic_strs=topics,
                    interval_ms=12_000
                )
            )
        case "all-to-all":            
            for node_id in range(node_count):
                connections = list(range(node_id)) + list(range(node_id+1, node_count))
                random.shuffle(connections)
                add_instructions(
                    [script_instruction.Connect(connectTo=connections)], 
                    _node_id=node_id
                )

            add_instructions(subscribe_to_topics())
            add_mapped_instructions(
                random_publish(
                    node_count=node_count,
                    num_messages=num_messages,
                    message_size=message_size,
                    topic_strs=topics,
                    interval_ms=12_000
                )
            )
        case "faivre-30-tps":
            num_minutes = 5
            interval_ms = 33
            num_messages = num_minutes * 60 * round(1000 / interval_ms)
            number_of_conns_per_node = min(8, node_count - 1)
            add_mapped_instructions(random_network_mesh(node_count, number_of_conns_per_node))
            add_instructions(subscribe_to_topics())
            add_mapped_instructions(
                all_publish(
                    node_count=node_count,
                    num_messages=num_messages,
                    message_size=message_size,
                    topic_strs=topics,
                    interval_ms=interval_ms
                )
            )
        case "dropout-random-rolling":
            add_mapped_instructions(
                all_publish_with_rolling_dropout(
                    node_count=node_count,
                    topic_strs=topics,
                    interval_ms=10
                )
            )
        case _:
            raise ValueError(f"Unknown scenario name: {scenario_name}")
        
    time_ms = 0
    for node_id, node_instructions in instructions.items():
        for instruction in reversed(node_instructions):
            if isinstance(instruction, script_instruction.WaitUntil):
                time_ms = max(time_ms, instruction.elapsedMillis)
                break
            
    time_sec = (time_ms + 999) // 1000
                
    return {_node_id: ExperimentParams(script=_instructions) for _node_id, _instructions in instructions.items()}, time_sec


def composition(protocol: str) -> List[Binary]:
    match protocol:
        case "gossipsub":
            return [Binary("go-libp2p/gossipsub-bin", percent_of_nodes=100)]
        case "dog":
            return [Binary("libp2p-dog/target/debug/experiment", percent_of_nodes=100)]
    raise ValueError(f"Unknown protocol name: {protocol}")


def line_mesh(num_nodes: int) -> dict[int, list[ScriptInstruction]]:
    instructions = defaultdict(list)

    for node_id in range(num_nodes):
        instructions[node_id].append(
            script_instruction.Connect(
                connectTo=[(node_id + 1) % num_nodes],
            )
        )

    return instructions


def random_network_mesh(
    node_count: int, number_of_connections: int
) -> dict[int, list[ScriptInstruction]]:
    connections: Dict[NodeID, Set[NodeID]] = defaultdict(set)
    connect_to: Dict[NodeID, List[NodeID]] = defaultdict(list)
    for node_id in range(node_count):
        while len(connections[node_id]) < number_of_connections:
            target = random.randint(0, node_count - 1)
            if (target == node_id) or (target in connections[node_id]):
                continue

            connections[node_id].add(target)
            connect_to[node_id].append(target)
            connections[target].add(node_id)

    instructions = defaultdict(list)
    for node_id, node_connections in connect_to.items():
        instructions[node_id].append(
            script_instruction.Connect(
                connectTo=list(node_connections)
            )
        )
    return instructions


def random_publish(
    node_count: int, num_messages: int, message_size: int, topic_strs: List[str], interval_ms: int
) -> dict[int, list[ScriptInstruction]]:
    instructions = {node_id: [] for node_id in range(node_count)}

    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_ms = 120_000
    for node_id in instructions:
        instructions[node_id].append(
            script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
        )

    for i in range(num_messages):
        random_node = random.randint(0, node_count - 1)
        topic_str = random.choice(topic_strs)
        instructions[random_node].append(
            script_instruction.Publish(
                messageID=i,
                topicID=topic_str,
                messageSizeBytes=message_size,
            )
        )
        elapsed_ms += interval_ms
        for node_id in instructions:
            instructions[node_id].append(
                script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
            )

    elapsed_ms += 30_000  # wait a bit more to allow all messages to flush
    for node_id in instructions:
        instructions[node_id].append(
            script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
        )

    return instructions


def all_publish(
        node_count: int, num_messages: int, message_size: int, topic_strs: List[str], interval_ms: int
) -> dict[int, list[ScriptInstruction]]:
    instructions = {node_id: [] for node_id in range(node_count)}

    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_ms = 120_000
    for node_id in instructions:
        instructions[node_id].append(script_instruction.WaitUntil(elapsedMillis=elapsed_ms))
    message_id = 0

    for _ in range(num_messages):
        for topic_str in topic_strs:
            for node_id in range(node_count):
                instructions[node_id].append(
                    script_instruction.Publish(
                        messageID=message_id,
                        topicID=topic_str,
                        messageSizeBytes=message_size,
                    )
                )
                message_id += 1
        elapsed_ms += interval_ms  # add interval for each subsequent message
        for node_id in instructions:
            instructions[node_id].append(
                script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
            )

    elapsed_ms += 120_000  # wait a bit more to allow all messages to flush
    for node_id in instructions:
        instructions[node_id].append(
            script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
        )
    
    return instructions

def all_publish_with_rolling_dropout(
        node_count: int,
        topic_strs: List[str], 
        interval_ms: int
) -> dict[int, list[ScriptInstruction]]:
    instructions = {node_id: [] for node_id in range(node_count)}

    message_size = 1024
    # start dropping nodes after this many messages
    dropout_start_msg = 10_000
    # every this many messages, one node is replaced
    dropout_period_msg = 500
    # keep this many nodes active at a time
    active_set_size = round(node_count // 5)
    
    num_messages = dropout_start_msg + (dropout_period_msg * (node_count)) + dropout_start_msg
        
    active_set = list(range(0, active_set_size))
    ready_set = list(range(active_set_size, node_count))[::-1]
    
    number_of_conns_per_node = min(10, active_set_size - 1)
    
    def node_setup(_node_id: int) -> list[ScriptInstruction]:
        setup_instructions = []
        candidates = [n for n in active_set if n != _node_id]
        connections = random.sample(candidates, k=number_of_conns_per_node)
        setup_instructions.append(
            script_instruction.Connect(connectTo=connections)
        )
        for topic in topic_strs:  
            setup_instructions.append(
                script_instruction.SubscribeToTopic(topicID=topic)
            )
        
        return setup_instructions
    
    for node_id in active_set:
        instructions[node_id].extend(node_setup(node_id))
        
    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_ms = 120_000
    for node_id in instructions:
        instructions[node_id].append(
            script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
        )
    
    publish_start_index: dict[int, int] = {}
    dropped_nodes: set[int] = set()
    
    def instruct_all(_instructions: list[ScriptInstruction]):
        for node_id in instructions:
            if node_id not in dropped_nodes:
                instructions[node_id].extend(_instructions)

    print(f"Total messages to be published: {num_messages:,}")
    for i in range(num_messages):
        if (i >= dropout_start_msg) and (i % dropout_period_msg) == 0 and (len(ready_set) > 0):
            dropout_node = active_set.pop(0)
            dropped_nodes.add(dropout_node)
            # no explicit shutdown instruction
            # node will not receive further instructions
            replacement_node = ready_set.pop()
            instructions[replacement_node].extend(node_setup(replacement_node))
            active_set.append(replacement_node)
            publish_start_index[replacement_node] = i + dropout_period_msg
        
        node_id = random.choice(active_set)
        while i < publish_start_index.get(node_id, 0):
            node_id = random.choice(active_set)
        
        for topic_str in topic_strs:   
            instructions[node_id].append(
                script_instruction.Publish(
                    messageID=i,
                    topicID=topic_str,
                    messageSizeBytes=message_size,
                )
            )
            
        elapsed_ms += interval_ms  # add interval for each subsequent message
        instruct_all([script_instruction.WaitUntil(elapsedMillis=elapsed_ms)])

    elapsed_ms += 300_000  # wait 5 more minutes to allow all messages to flush
    instruct_all([script_instruction.WaitUntil(elapsedMillis=elapsed_ms)])

    return instructions
