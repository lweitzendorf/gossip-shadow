from collections import defaultdict
from dataclasses import dataclass, field
import random
from typing import List, Dict, Set

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


def scenario(protocol: str, scenario_name: str, node_count: int) -> ExperimentParams:
    instructions: List[ScriptInstruction] = []
    
    num_messages = 1200
    message_size = 1024
    topics = ["topic-a"]
    
    match protocol:
        case "gossipsub":
            gs_params = GossipSubParams()
            init_instruction = script_instruction.InitGossipSub(gossipSubParams=gs_params)
            instructions.append(init_instruction)
        case _:
            pass
    
    def subscribe_to_topics() -> list[ScriptInstruction]:
        return [script_instruction.SubscribeToTopic(topicID=topic) for topic in topics]
    
    match scenario_name:
        case "random":
            number_of_conns_per_node = min(8, node_count - 1)
            instructions.extend(random_network_mesh(node_count, number_of_conns_per_node))
            instructions.extend(subscribe_to_topics())
            instructions.extend(random_publish(
                node_count=node_count,
                num_messages=num_messages,
                message_size=message_size,
                topic_strs=topics,
                interval_ms=12_000
            ))
        case "line-feed-in":
            instructions.extend(line_mesh(node_count))
            instructions.extend(subscribe_to_topics())
            instructions.extend(
                random_publish(
                    node_count=node_count,
                    num_messages=round(num_messages * 0.2),
                    message_size=message_size,
                    topic_strs=topics,
                    interval_ms=12_000
                )
            )
            instructions.extend(random_network_mesh(node_count, node_count // 2))
            instructions.extend(
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
                instructions.append(
                    script_instruction.IfNodeIDIn(
                        nodeIDs=[node_id],
                        instructions=[script_instruction.Connect(
                            connectTo=neighbors,
                        )],
                    )
                )

            instructions.extend(subscribe_to_topics())
            instructions.extend(random_publish(
                node_count=node_count,
                num_messages=num_messages,
                message_size=message_size,
                topic_strs=topics,
                interval_ms=12_000
            ))
        case "all-to-all":            
            for node_id in range(node_count):
                connections = list(range(node_id)) + list(range(node_id+1, node_count))
                random.shuffle(connections)
                instructions.append(
                    script_instruction.IfNodeIDIn(
                        nodeIDs=[node_id],
                        instructions=[script_instruction.Connect(
                            connectTo=connections,
                        )],
                    )
                )

            instructions.extend(subscribe_to_topics())
            instructions.extend(random_publish(
                node_count=node_count,
                num_messages=num_messages,
                message_size=message_size,
                topic_strs=topics,
                interval_ms=12_000
            ))
        case "faivre-30-tps":
            num_minutes = 5
            interval_ms = 33
            num_messages = num_minutes * 60 * round(1000 / interval_ms)
            number_of_conns_per_node = min(8, node_count - 1)
            instructions.extend(random_network_mesh(node_count, number_of_conns_per_node))
            instructions.extend(subscribe_to_topics())
            instructions.extend(all_publish(
                node_count=node_count,
                num_messages=num_messages,
                message_size=message_size,
                topic_strs=topics,
                interval_ms=interval_ms
            ))    
        case "dropout-random-rolling":
            num_minutes = 10
            interval_ms = 100
            num_messages = num_minutes * 60 * round(1000 / interval_ms)
            instructions.extend(all_publish_with_rolling_dropout(
                node_count=node_count,
                num_messages=num_messages,
                message_size=message_size,
                topic_strs=topics,
                interval_ms=interval_ms,
                dropout_rate=1/3,
            ))
        case _:
            raise ValueError(f"Unknown scenario name: {scenario_name}")

    return ExperimentParams(script=instructions)


def composition(protocol: str) -> List[Binary]:
    match protocol:
        case "gossipsub":
            return [Binary("go-libp2p/gossipsub-bin", percent_of_nodes=100)]
        case "dog":
            return [Binary("libp2p-dog/target/debug/experiment", percent_of_nodes=100)]
    raise ValueError(f"Unknown protocol name: {protocol}")


def line_mesh(num_nodes: int) -> List[ScriptInstruction]:
    instructions = []

    for node_id in range(num_nodes):
        instructions.append(
            script_instruction.IfNodeIDIn(
                nodeIDs=[node_id],
                instructions=[script_instruction.Connect(
                    connectTo=[(node_id + 1) % num_nodes],
                )],
            )
        )

    return instructions


def random_network_mesh(
    node_count: int, number_of_connections: int
) -> List[ScriptInstruction]:
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


    instructions = []
    for node_id, node_connections in connect_to.items():
        instructions.append(
            script_instruction.IfNodeIDIn(
                nodeIDs=[node_id],
                instructions=[script_instruction.Connect(
                    connectTo=list(node_connections),
                )],
            )
        )
    return instructions


def random_publish(
    node_count: int, num_messages: int, message_size: int, topic_strs: List[str], interval_ms: int
) -> List[ScriptInstruction]:
    instructions = []

    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_ms = 120_000
    instructions.append(script_instruction.WaitUntil(
        elapsedMillis=elapsed_ms))

    for i in range(num_messages):
        random_node = random.randint(0, node_count - 1)
        topic_str = random.choice(topic_strs)
        instructions.append(
            script_instruction.IfNodeIDIn(
                nodeIDs=[random_node],
                instructions=[script_instruction.Publish(
                    messageID=i,
                    topicID=topic_str,
                    messageSizeBytes=message_size,
                )],
            )
        )
        elapsed_ms += interval_ms
        instructions.append(
            script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
        )

    elapsed_ms += 30_000  # wait a bit more to allow all messages to flush
    instructions.append(script_instruction.WaitUntil(
        elapsedMillis=elapsed_ms))

    return instructions


def all_publish(
        node_count: int, num_messages: int, message_size: int, topic_strs: List[str], interval_ms: int
) -> List[ScriptInstruction]:
    instructions = []

    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_ms = 120_000
    instructions.append(script_instruction.WaitUntil(elapsedMillis=elapsed_ms))

    message_id = 0

    for _ in range(num_messages):
        for topic_str in topic_strs:
            for node in range(node_count):
                instructions.append(
                    script_instruction.IfNodeIDIn(
                        nodeIDs=[node],
                        instructions=[script_instruction.Publish(
                            messageID=message_id,
                            topicID=topic_str,
                            messageSizeBytes=message_size,
                        )]
                    ),
                )
                message_id += 1
        elapsed_ms += interval_ms  # add interval for each subsequent message
        instructions.append(
            script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
        )

    elapsed_ms += 120_000  # wait a bit more to allow all messages to flush
    instructions.append(script_instruction.WaitUntil(
        elapsedMillis=elapsed_ms))

    return instructions

def all_publish_with_rolling_dropout(
        node_count: int, 
        num_messages: int, 
        message_size: int, 
        topic_strs: List[str], 
        interval_ms: int, 
        dropout_rate: float
) -> List[ScriptInstruction]:
    instructions = []
    
    total_time_ms = num_messages * interval_ms
    dropout_duration_ms = round(31.125 * total_time_ms * dropout_rate / node_count)
    dropout_rounds  = (num_messages * interval_ms / dropout_duration_ms) + 5
    total_set_size = round(node_count / (dropout_rate * dropout_rounds))
    
    # e.g. 250 nodes, 20 minute simulation, 60 second dropouts, 33.3% dropout rate
    # dropout_rounds = (1200000 / 60000) + 5 = 25 rounds
    # total_set_size = 250 / ((1/3) * 25) = 30 nodes in total set
    assert total_set_size >= 10, "Total set size must be at least 10"
    
    active_set = list(range(0, total_set_size))
    ready_set = list(range(total_set_size, node_count))[::-1]

    dropout_multiple = round(dropout_duration_ms / (interval_ms * dropout_rate * total_set_size))
    # dropout_multiple = 60000 / (100 * (1/3) * 30) = 60 -> every 60 messages
    # drops out 200 / 250 nodes over the whole simulation
    
    number_of_conns_per_node = min(10, total_set_size - 1)
    
    def node_setup(node_id: int) -> list[ScriptInstruction]:
        setup_instructions = []
        candidates = [n for n in active_set if n != node_id]
        connections = random.sample(candidates, k=number_of_conns_per_node)
        setup_instructions.append(
            script_instruction.Connect(connectTo=connections)
        )
        for topic in topic_strs:  
            setup_instructions.append(
                script_instruction.SubscribeToTopic(topicID=topic),
            )
        
        return [script_instruction.IfNodeIDIn(nodeIDs=[node_id], instructions=setup_instructions)]
    
    for node_id in active_set:
        instructions.extend(node_setup(node_id))
        
    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_ms = 120_000
    instructions.append(script_instruction.WaitUntil(elapsedMillis=elapsed_ms))
            
    message_id = 0
    publish_start_index: dict[int, int] = {}

    for i in range(num_messages):
        if (i % dropout_multiple) == 0:
            dropout_node = active_set.pop(0)
            replacement_node = ready_set.pop()
            instructions.append(
                script_instruction.IfNodeIDIn(
                    nodeIDs=[dropout_node],
                    instructions=[script_instruction.ShutDown()],
                )
            )
            instructions.extend(node_setup(replacement_node))
            active_set.append(replacement_node)
            publish_start_index[replacement_node] = i + dropout_multiple
        
        for node in active_set: 
            if i < publish_start_index.get(node, 0):
                continue
            
            node_instructions = []          
            for topic_str in topic_strs:   
                node_instructions.append(
                    script_instruction.Publish(
                        messageID=message_id,
                        topicID=topic_str,
                        messageSizeBytes=message_size,
                    )
                )
                message_id += 1
                
            instructions.append(
                script_instruction.IfNodeIDIn(nodeIDs=[node], instructions=node_instructions)
            )
            
        elapsed_ms += interval_ms  # add interval for each subsequent message
        instructions.append(
            script_instruction.WaitUntil(elapsedMillis=elapsed_ms)
        )

    elapsed_ms += 300_000  # wait 5 more minutes to allow all messages to flush
    instructions.append(script_instruction.WaitUntil(elapsedMillis=elapsed_ms))

    return instructions
