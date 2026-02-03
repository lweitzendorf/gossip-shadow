from collections import defaultdict
from dataclasses import dataclass, field
import random
import json
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
    
@dataclass
class ExperimentState:
    elapsed_time_ms: int = 0
    next_message_id: int = 0
    active_set: list[int] = field(default_factory=list)
    ready_set: list[int] = field(default_factory=list)
    
class ExperimentPhase:
    def __init__(
        self,
        state: ExperimentState,
        warmup_time_ms: int,
        cooldown_time_ms: int,
    ):
        self.state = state
        self.warmup_time_ms = warmup_time_ms
        self.cooldown_time_ms = cooldown_time_ms
        self._main_phase: dict[int, list[ScriptInstruction]] = defaultdict(list)
        self._main_phase_duration_ms: int = 0
    
    def _get_main_phase_end_time_ms(self) -> int:
        return self.state.elapsed_time_ms + self.warmup_time_ms + self._main_phase_duration_ms
    
    def _get_end_time_ms(self) -> int:
        return self._get_main_phase_end_time_ms() + self.cooldown_time_ms
    
    def add_message(self, delay_ms: int, topic: str, node_id: int, size_bytes: int) -> int:
        if delay_ms >= 0:
            self._main_phase_duration_ms += delay_ms
            wait_instruction = script_instruction.WaitUntil(
                elapsedMillis=self._get_main_phase_end_time_ms()
            )
            for _node_id in self.state.active_set:
                self._main_phase[_node_id].append(wait_instruction)
                
        message_id = self.state.next_message_id
        self.state.next_message_id += 1
        publish_instruction = script_instruction.Publish(
            messageID=self.state.next_message_id,
            topicID=topic,
            messageSizeBytes=size_bytes,
        )
        self._main_phase[node_id].append(publish_instruction)
        return message_id
            
    def apply(self, state: ExperimentState, instructions: dict[int, list[ScriptInstruction]]) -> None:                
        for node_id in self.state.active_set:
            if node_id not in instructions:
                instructions[node_id] = []
            
            if self.warmup_time_ms > 0:
                instructions[node_id].append(
                    script_instruction.WaitUntil(
                        elapsedMillis=self.state.elapsed_time_ms + self.warmup_time_ms
                    )
                )
            instructions[node_id].extend(self._main_phase[node_id])
            if self.cooldown_time_ms > 0:
                instructions[node_id].append(
                    script_instruction.WaitUntil(
                        elapsedMillis=self._get_end_time_ms()
                    )
                )
        
        state.elapsed_time_ms = self._get_end_time_ms()
        state.next_message_id = self.state.next_message_id        
        

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
            degree = 6
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
            add_mapped_instructions(random_network_mesh(node_count, 10))
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
        case "malicious-new-connections":
            add_mapped_instructions(
                random_publish_malicious_new_connections(
                    node_count=node_count,
                    topic_strs=topics
                )
            )
        case "rolling-churn":
            add_mapped_instructions(
                random_publish_node_churn(
                    node_count=node_count,
                    topic_strs=topics
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

def random_publish_malicious_new_connections(
        node_count: int,
        topic_strs: List[str],
) -> dict[int, list[ScriptInstruction]]:
    instructions = {node_id: [] for node_id in range(node_count)}

    interval_ms = 100
    message_size = 4200
    active_set_size = 50
    messages_per_phase = 250

    experiment_state = ExperimentState(
        active_set = list(range(0, active_set_size)),
        ready_set = list(range(active_set_size, node_count))[::-1]
    )
    
    def node_setup(_node_id: int, _num_connections: int) -> list[ScriptInstruction]:
        setup_instructions = []
        candidates = [n for n in experiment_state.active_set if n != _node_id]
        connections = random.sample(candidates, k=min(_num_connections, len(candidates)))
        if experiment_state.elapsed_time_ms > 0:
            setup_instructions.append(
                script_instruction.WaitUntil(
                    elapsedMillis=experiment_state.elapsed_time_ms
                )
            )
        setup_instructions.append(
            script_instruction.Connect(connectTo=connections)
        )
        for topic in topic_strs:  
            setup_instructions.append(
                script_instruction.SubscribeToTopic(topicID=topic)
            )
        
        return setup_instructions
    
    for node_id in experiment_state.active_set:
        instructions[node_id].extend(node_setup(node_id, 8))        

    startup_phase = ExperimentPhase(
        state=experiment_state,
        warmup_time_ms=120_000,
        cooldown_time_ms=0
    )
    startup_phase.apply(experiment_state, instructions)    
        
    num_initial_phases = 15
    num_end_phases = 30
    
    warmup_time_ms = 60_000
    
    phase_info = []
    
    for _ in range(num_initial_phases):
        phase = ExperimentPhase(
            state=experiment_state,
            warmup_time_ms=warmup_time_ms,
            cooldown_time_ms=60_000
        )
        
        message_ids = []
        for _ in range(messages_per_phase):
            node_id = random.choice(experiment_state.active_set)
            topic = topic_strs[0]
            message_id = phase.add_message(
                delay_ms=interval_ms,
                topic=topic,
                node_id=node_id,
                size_bytes=message_size
            )
            message_ids.append(message_id)
            
        phase_info.append({
            "active_nodes": experiment_state.active_set.copy(),
            "message_ids": message_ids
        })
        phase.apply(experiment_state, instructions)
                     
    # add malicious nodes
    malicious_nodes = experiment_state.ready_set[:8]
    experiment_state.ready_set = experiment_state.ready_set[8:]
    experiment_state.active_set.extend(malicious_nodes)
    
    # as many connections as possible
    num_connections = len(experiment_state.active_set)
    for new_node in malicious_nodes:
        instructions[new_node].extend(node_setup(new_node, num_connections))
        
    # try replacing nodes to sustain the higher bandwidth 
    for _ in range(len(experiment_state.ready_set)):
        phase = ExperimentPhase(
            state=experiment_state,
            warmup_time_ms=warmup_time_ms,
            cooldown_time_ms=60_000
        )
        
        message_ids = []
        for _ in range(messages_per_phase):
            node_id = random.choice(experiment_state.active_set)
            topic = topic_strs[0]
            message_id = phase.add_message(
                delay_ms=interval_ms,
                topic=topic,
                node_id=node_id,
                size_bytes=message_size
            )
            message_ids.append(message_id)
            
        phase_info.append({
            "active_nodes": experiment_state.active_set.copy(),
            "message_ids": message_ids
        })
        phase.apply(experiment_state, instructions)
        
        # cycle node after phase
        new_node = experiment_state.ready_set.pop()
        experiment_state.active_set.pop(-8)
        experiment_state.active_set.append(new_node)
        instructions[new_node].extend(node_setup(new_node, len(experiment_state.active_set)))
        
    for _ in range(num_end_phases):
        phase = ExperimentPhase(
            state=experiment_state,
            warmup_time_ms=warmup_time_ms,
            cooldown_time_ms=300_000
        )
        
        message_ids = []
        for _ in range(messages_per_phase):
            node_id = random.choice(experiment_state.active_set)
            topic = topic_strs[0]
            message_id = phase.add_message(
                delay_ms=interval_ms,
                topic=topic,
                node_id=node_id,
                size_bytes=message_size
            )
            message_ids.append(message_id)
            
        phase_info.append({
            "active_nodes": experiment_state.active_set.copy(),
            "message_ids": message_ids
        })
        phase.apply(experiment_state, instructions)
     
    with open("phase_info.json", "w") as f:
        json.dump(phase_info, f, indent=2)
            
    return instructions


def random_publish_node_churn(
        node_count: int,
        topic_strs: List[str],
) -> dict[int, list[ScriptInstruction]]:
    instructions = {node_id: [] for node_id in range(node_count)}

    interval_ms = 100
    message_size = 1024
    active_set_size = 50
    messages_per_phase = 250
    replacements_per_phase = 10

    experiment_state = ExperimentState(
        active_set = list(range(0, active_set_size)),
        ready_set = list(range(active_set_size, node_count))[::-1]
    )
    
    num_connections_per_node = 10
    
    def node_setup(_node_id: int) -> list[ScriptInstruction]:
        setup_instructions = []
        candidates = [n for n in experiment_state.active_set if n != _node_id]
        connections = random.sample(candidates, k=min(num_connections_per_node, len(candidates)))
        if experiment_state.elapsed_time_ms > 0:
            setup_instructions.append(
                script_instruction.WaitUntil(
                    elapsedMillis=experiment_state.elapsed_time_ms
                )
            )
        setup_instructions.append(
            script_instruction.Connect(connectTo=connections)
        )
        for topic in topic_strs:  
            setup_instructions.append(
                script_instruction.SubscribeToTopic(topicID=topic)
            )
        
        return setup_instructions
    
    for node_id in experiment_state.active_set:
        instructions[node_id].extend(node_setup(node_id))        

    startup_phase = ExperimentPhase(
        state=experiment_state,
        warmup_time_ms=120_000,
        cooldown_time_ms=0
    )
    startup_phase.apply(experiment_state, instructions)    
        
    num_initial_phases = 30
    num_replacement_phases = len(experiment_state.ready_set) // replacements_per_phase
    num_end_phases = 15
    
    warmup_time_ms = 60_000
    cooldown_time_ms = 60_000
    
    phase_info = []
    
    # initial phases with no churn
    for _ in range(num_initial_phases):
        phase = ExperimentPhase(
            state=experiment_state,
            warmup_time_ms=warmup_time_ms,
            cooldown_time_ms=cooldown_time_ms
        )
        
        message_ids = []
        for _ in range(messages_per_phase):
            node_id = random.choice(experiment_state.active_set)
            topic = topic_strs[0]
            message_id = phase.add_message(
                delay_ms=interval_ms,
                topic=topic,
                node_id=node_id,
                size_bytes=message_size
            )
            message_ids.append(message_id)
            
        phase_info.append({
            "active_nodes": experiment_state.active_set.copy(),
            "message_ids": message_ids
        })
        phase.apply(experiment_state, instructions)
                        
    for _ in range(num_replacement_phases):
        # node replacements
        for _ in range(min(replacements_per_phase, len(experiment_state.ready_set))):
            experiment_state.active_set.pop(0)
            replacement_node = experiment_state.ready_set.pop()
            experiment_state.active_set.append(replacement_node)
            instructions[replacement_node].extend(node_setup(replacement_node))

        phase = ExperimentPhase(
            state=experiment_state,
            warmup_time_ms=warmup_time_ms,
            cooldown_time_ms=cooldown_time_ms
        )
        
        message_ids = []
        for _ in range(messages_per_phase):
            node_id = random.choice(experiment_state.active_set)
            topic = topic_strs[0]
            message_id = phase.add_message(
                delay_ms=interval_ms,
                topic=topic,
                node_id=node_id,
                size_bytes=message_size
            )
            message_ids.append(message_id)
            
        phase_info.append({
            "active_nodes": experiment_state.active_set.copy(),
            "message_ids": message_ids
        })
        phase.apply(experiment_state, instructions)
        
    # final phases with no churn
    for _ in range(num_end_phases):
        phase = ExperimentPhase(
            state=experiment_state,
            warmup_time_ms=warmup_time_ms,
            cooldown_time_ms=cooldown_time_ms
        )
        
        message_ids = []
        for _ in range(messages_per_phase):
            node_id = random.choice(experiment_state.active_set)
            topic = topic_strs[0]
            message_id = phase.add_message(
                delay_ms=interval_ms,
                topic=topic,
                node_id=node_id,
                size_bytes=message_size
            )
            message_ids.append(message_id)
            
        phase_info.append({
            "active_nodes": experiment_state.active_set.copy(),
            "message_ids": message_ids
        })
        phase.apply(experiment_state, instructions)
     
    with open("phase_info.json", "w") as f:
        json.dump(phase_info, f, indent=2)
            
    return instructions
