import random
from collections import defaultdict
from dataclasses import dataclass, field
from abc import ABC
from typing import List, Dict, Set, Sequence, TypeAlias

import networkx as nx

import script_instruction
from script_instruction import ScriptInstruction, NodeConfigParams, GossipSubParams, DOGParams


NodeID: TypeAlias = int

@dataclass
class ExperimentParams:
    config: NodeConfigParams
    script: List[ScriptInstruction]

class Node(ABC):
    def __init__(self, idx: NodeID, binary: str, config: NodeConfigParams) -> None:
        super().__init__()
        self.idx = idx
        self.binary = binary
        self.config = config

class GossipSubNode(Node):
    def __init__(self, idx: int, **params) -> None:
        config = GossipSubParams(**params)
        super().__init__(idx, "go-libp2p/gossipsub-bin", config)

class DOGNode(Node):
    def __init__(self, idx: int, **params) -> None:
        config = DOGParams(**params)
        super().__init__(idx, "libp2p-dog/target/debug/experiment", config)

class MaliciousDOGNode(DOGNode):
    def __init__(self, idx: int, **params) -> None:
        params |= dict(
            send_have_tx=False,
            ignore_have_tx=True,
            forward_transactions=False,
        )
        super().__init__(idx, **params)
    
@dataclass
class ExperimentState:
    elapsed_time_ms: int = 0
    next_message_id: int = 0
    active_set: list[Node] = field(default_factory=list)
    ready_set: list[Node] = field(default_factory=list)
    
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
        self._active_ids: set[int] = {n.idx for n in state.active_set}
    
    def _get_main_phase_end_time_ms(self) -> int:
        return self.state.elapsed_time_ms + self.warmup_time_ms + self._main_phase_duration_ms
    
    def _get_end_time_ms(self) -> int:
        return self._get_main_phase_end_time_ms() + self.cooldown_time_ms
    
    def add_message(self, delay_ms: int, node_id: int, size_bytes: int, topic: str = "default") -> int:
        assert node_id in self._active_ids, f"node {node_id} is not in the active set"
        
        self._main_phase_duration_ms += delay_ms
        message_id = self.state.next_message_id
        self.state.next_message_id += 1

        publish_instruction = script_instruction.Publish(
            messageID=message_id,
            topicID=topic,
            messageSizeBytes=size_bytes,
        )
        self._main_phase[node_id].append(ScriptInstruction(
            elapsedMillis=self._get_main_phase_end_time_ms(),
            instructions=[publish_instruction]
        ))
        return message_id
            
    def _apply(self, instructions: dict[int, list[ScriptInstruction]]) -> None:
        for node in self.state.active_set:
            instructions[node.idx].extend(self._main_phase[node.idx])
            # prevents nodes from shutting down before the end of the phase
            instructions[node.idx].append(
                ScriptInstruction(
                    elapsedMillis=self._get_end_time_ms(),
                    instructions=[]
                )
            )
        self.state.elapsed_time_ms = self._get_end_time_ms()
        
class Experiment:
    def __init__(self, nodes: Sequence[Node]) -> None:
        self.nodes = sorted(nodes, key=lambda n: n.idx)
        self.state = ExperimentState(ready_set=list(nodes))
        self._instructions: dict[int, list[ScriptInstruction]] = defaultdict(list)

    def activate_nodes(self, nodes: Sequence[Node]) -> None:
        for node in nodes:
            self.state.ready_set.remove(node)
            self.state.active_set.append(node)

    def activate_all(self) -> None:
        self.activate_nodes(self.state.ready_set.copy())

    def deactivate_nodes(self, nodes: Sequence[Node]) -> None:
        for node in nodes:
            self.state.active_set.remove(node)

    def add_instruction(self, node_idx: int, instruction: ScriptInstruction) -> None:
        self._instructions[node_idx].append(instruction)

    def setup_node(self, node: Node, peers: list[int], topics: list[str]) -> None:
        self.add_instruction(node.idx, ScriptInstruction(
            elapsedMillis=self.state.elapsed_time_ms,
            instructions=[
                script_instruction.Connect(connectTo=peers),
                *[script_instruction.SubscribeToTopic(topicID=t) for t in topics]
            ]
        ))

    def new_phase(self, warmup_ms: int = 0, cooldown_ms: int = 0) -> ExperimentPhase:
        return ExperimentPhase(self.state, warmup_ms, cooldown_ms)

    def apply_phase(self, phase: ExperimentPhase) -> None:
        phase._apply(self._instructions)

    def finalize(self) -> tuple[dict[int, ExperimentParams], int]:
        time_ms = max(
            instrs[-1].elapsedMillis
            for instrs in self._instructions.values() if instrs
        )
        time_sec = (time_ms + 999) // 1000
        nodes_by_idx = {n.idx: n for n in self.nodes}
        result = {}
        for nid, instrs in self._instructions.items():
            node = nodes_by_idx[nid]
            result[nid] = ExperimentParams(script=instrs, config=node.config)
        return result, time_sec
        
def make_nodes(protocol: str, count: int) -> list[Node]:
    match protocol:
        case "dog":
            return [DOGNode(i) for i in range(count)]
        case "gossipsub":
            return [GossipSubNode(i) for i in range(count)]
        case _:
            raise ValueError(f"Unknown protocol: {protocol}")


def random_mesh_connections(node_count: int, num_connections: int) -> dict[int, list[int]]:
    connections: Dict[NodeID, Set[NodeID]] = defaultdict(set)
    connect_to: Dict[NodeID, List[NodeID]] = defaultdict(list)
    for node_id in range(node_count):
        while len(connections[node_id]) < num_connections:
            target = random.randint(0, node_count - 1)
            if (target == node_id) or (target in connections[node_id]):
                continue
            connections[node_id].add(target)
            connect_to[node_id].append(target)
            connections[target].add(node_id)
    return dict(connect_to)


def scenario(protocol: str, scenario_name: str) -> Experiment:
    match scenario_name:
        case "random":
            return scenario_random(protocol)
        case "two-cliques":
            return scenario_two_cliques(protocol)
        case "faivre-30-tps":
            return scenario_faivre_30_tps(protocol)
        case "malicious-new-connections":
            return scenario_malicious_new_connections(protocol)
        case "rolling-churn":
            return scenario_rolling_churn(protocol)
        case _:
            raise ValueError(f"Unknown scenario: {scenario_name}")


def scenario_random(protocol: str) -> Experiment:
    nodes = make_nodes(protocol, 1000)
    exp = Experiment(nodes)
    exp.activate_all()

    mesh = random_mesh_connections(len(nodes), 8)
    for node in exp.state.active_set:
        exp.setup_node(node, peers=mesh.get(node.idx, []), topics=["default"])

    phase = exp.new_phase(warmup_ms=120_000, cooldown_ms=30_000)
    for _ in range(1200):
        node = random.choice(exp.state.active_set)
        phase.add_message(delay_ms=12_000, node_id=node.idx, size_bytes=1024)
    exp.apply_phase(phase)

    return exp


def scenario_two_cliques(protocol: str) -> Experiment:
    node_count = 1000
    nodes = make_nodes(protocol, node_count)
    exp = Experiment(nodes)
    exp.activate_all()

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

    for node in exp.state.active_set:
        neighbors = list(G.neighbors(node.idx))
        random.shuffle(neighbors)
        exp.setup_node(node, peers=neighbors, topics=["default"])

    phase = exp.new_phase(warmup_ms=120_000, cooldown_ms=30_000)
    for _ in range(1200):
        node = random.choice(exp.state.active_set)
        phase.add_message(delay_ms=12_000, node_id=node.idx, size_bytes=1024)
    exp.apply_phase(phase)

    return exp


def scenario_faivre_30_tps(protocol: str) -> Experiment:
    node_count = 32
    nodes = make_nodes(protocol, node_count)
    exp = Experiment(nodes)
    exp.activate_all()

    mesh = random_mesh_connections(node_count, 10)
    for node in exp.state.active_set:
        exp.setup_node(node, peers=mesh.get(node.idx, []), topics=["default"])

    num_minutes = 5
    interval_ms = 33
    num_rounds = num_minutes * 60 * round(1000 / interval_ms)
    message_size = 1000

    phase = exp.new_phase(warmup_ms=120_000, cooldown_ms=120_000)
    for _ in range(num_rounds):
        for i, node in enumerate(exp.state.active_set):
            delay = interval_ms if (i == 0) else 0
            phase.add_message(
                delay_ms=delay,
                node_id=node.idx,
                size_bytes=message_size,
            )
    exp.apply_phase(phase)

    return exp


def scenario_malicious_new_connections(protocol: str) -> Experiment:
    honest_set_size = 50
    malicious_set_size = 20
    node_count = honest_set_size + malicious_set_size

    honest_nodes = [DOGNode(i) for i in range(honest_set_size)]
    malicious_nodes = [MaliciousDOGNode(i) for i in range(honest_set_size, node_count)]

    exp = Experiment(honest_nodes + malicious_nodes)
    exp.activate_nodes(honest_nodes)

    topics = ["default"]
    interval_ms = 100
    message_size = 4300
    messages_per_phase = 1000
    num_initial_phases = 20
    num_end_phases = 40
    warmup_ms = 60_000

    def random_peers(node_idx: int, num_peers: int) -> list[int]:
        candidates = [n.idx for n in exp.state.active_set if n.idx != node_idx]
        return random.sample(candidates, k=min(num_peers, len(candidates)))

    for node in honest_nodes:
        exp.setup_node(node, peers=random_peers(node.idx, 8), topics=topics)

    # startup warmup
    exp.apply_phase(exp.new_phase(warmup_ms=120_000))

    for _ in range(num_initial_phases):
        phase = exp.new_phase(warmup_ms=warmup_ms, cooldown_ms=60_000)
        for _ in range(messages_per_phase):
            node = random.choice(honest_nodes)
            phase.add_message(delay_ms=interval_ms, node_id=node.idx, size_bytes=message_size)
        exp.apply_phase(phase)

    # add malicious nodes
    exp.activate_nodes(malicious_nodes)
    honest_idxs = [n.idx for n in honest_nodes]
    for node in malicious_nodes:
        exp.setup_node(node, peers=honest_idxs, topics=topics)

    for _ in range(num_end_phases):
        phase = exp.new_phase(warmup_ms=warmup_ms, cooldown_ms=300_000)
        for _ in range(messages_per_phase):
            node = random.choice(honest_nodes)
            phase.add_message(delay_ms=interval_ms, node_id=node.idx, size_bytes=message_size)
        exp.apply_phase(phase)

    return exp


def scenario_rolling_churn(protocol: str) -> Experiment:
    active_set_size = 50
    node_count = 200
    nodes = make_nodes(protocol, node_count)
    exp = Experiment(nodes)

    initial_nodes = [n for n in nodes if n.idx < active_set_size]
    exp.activate_nodes(initial_nodes)

    topics = ["default"]
    interval_ms = 100
    message_size = 1024
    messages_per_phase = 250
    replacements_per_phase = 10
    num_connections_per_node = 10

    def random_peers(node_idx: int, num_peers: int) -> list[int]:
        candidates = [n.idx for n in exp.state.active_set if n.idx != node_idx]
        return random.sample(candidates, k=min(num_peers, len(candidates)))

    for node in initial_nodes:
        exp.setup_node(node, peers=random_peers(node.idx, num_connections_per_node), topics=topics)

    # startup warmup
    exp.apply_phase(exp.new_phase(warmup_ms=120_000))

    num_initial_phases = 30
    num_replacement_phases = len(exp.state.ready_set) // replacements_per_phase
    num_end_phases = 15
    warmup_ms = 60_000
    cooldown_ms = 60_000

    def run_phase():
        phase = exp.new_phase(warmup_ms=warmup_ms, cooldown_ms=cooldown_ms)
        message_ids = []
        for _ in range(messages_per_phase):
            node = random.choice(exp.state.active_set)
            message_id = phase.add_message(
                delay_ms=interval_ms, node_id=node.idx, size_bytes=message_size,
            )
            message_ids.append(message_id)
        exp.apply_phase(phase)

    # initial phases with no churn
    for _ in range(num_initial_phases):
        run_phase()

    # replacement phases
    for _ in range(num_replacement_phases):
        for _ in range(min(replacements_per_phase, len(exp.state.ready_set))):
            oldest = exp.state.active_set[0]
            exp.deactivate_nodes([oldest])
            replacement = exp.state.ready_set[0]
            exp.activate_nodes([replacement])
            exp.setup_node(
                replacement,
                peers=random_peers(replacement.idx, num_connections_per_node),
                topics=topics,
            )
        run_phase()

    # final phases with no churn
    for _ in range(num_end_phases):
        run_phase()

    return exp
