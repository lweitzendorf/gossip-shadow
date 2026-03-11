from __future__ import annotations

from typing import List, Literal, TypeAlias, Union
from pydantic import BaseModel

NodeID: TypeAlias = int    


class Connect(BaseModel):
    type: Literal["connect"] = "connect"
    connectTo: List[NodeID]


class Publish(BaseModel):
    type: Literal["publish"] = "publish"
    messageID: int
    messageSizeBytes: int
    topicID: str


class SubscribeToTopic(BaseModel):
    type: Literal["subscribeToTopic"] = "subscribeToTopic"
    topicID: str


class SetTopicValidationDelay(BaseModel):
    """
    SetTopicValidationDelay is an instruction that lets us mock some
    validation process by delaying the validation results by some number of
    seconds.
    """
    type: Literal["setTopicValidationDelay"] = "setTopicValidationDelay"
    topicID: str
    delaySeconds: float


class GossipSubParams(BaseModel):
    # Overlay parameters
    D: int = 6  # Optimal degree for a GossipSub topic mesh
    Dlo: int = 5  # Lower bound on the number of peers in a topic mesh
    Dhi: int = 12  # Upper bound on the number of peers in a topic mesh
    Dscore: int = 4  # Number of high-scoring peers to retain when pruning
    Dout: int = 2  # Quota for outbound connections to maintain in a topic mesh

    # Gossip parameters
    HistoryLength: int = 5  # Size of the message cache used for gossip
    HistoryGossip: int = 3  # Number of cached message IDs to advertise in IHAVE
    Dlazy: int = 6  # Minimum number of peers to emit gossip to at each heartbeat
    GossipFactor: float = 0.25  # Factor affecting how many peers receive gossip
    GossipRetransmission: int = 3  # Limit for IWANT requests before ignoring a peer

    # Heartbeat parameters (durations in nanoseconds)
    HeartbeatInitialDelay: int = 100_000_000  # 100ms
    HeartbeatInterval: int = 1_000_000_000  # 1s
    SlowHeartbeatWarning: float = 0.1  # Ratio of HeartbeatInterval

    # Fanout and pruning (durations in nanoseconds)
    FanoutTTL: int = 60_000_000_000  # 60s
    PrunePeers: int = 16  # Number of peers to include in prune Peer eXchange
    PruneBackoff: int = 60_000_000_000  # 60s
    UnsubscribeBackoff: int = 10_000_000_000  # 10s

    # Connection management (durations in nanoseconds)
    Connectors: int = 8  # Number of active connection attempts for PX peers
    MaxPendingConnections: int = 128
    ConnectionTimeout: int = 30_000_000_000  # 30s
    DirectConnectTicks: int = 300  # Heartbeat ticks for reconnecting direct peers
    DirectConnectInitialDelay: int = 1_000_000_000  # 1s

    # Opportunistic grafting
    OpportunisticGraftTicks: int = 60  # Heartbeat ticks between grafting attempts
    OpportunisticGraftPeers: int = 2
    GraftFloodThreshold: int = 10_000_000_000  # 10s

    # Message control
    MaxIHaveLength: int = 5000  # Maximum message IDs in an IHAVE
    MaxIHaveMessages: int = 10  # Maximum IHAVE messages per heartbeat
    MaxIDontWantLength: int = 10  # Maximum message IDs in an IDONTWANT
    MaxIDontWantMessages: int = 1000  # Maximum IDONTWANT messages per heartbeat
    IWantFollowupTime: int = 3_000_000_000  # 3s
    IDontWantMessageThreshold: int = 1024  # Size threshold in bytes
    IDontWantMessageTTL: int = 3  # TTL in heartbeats
    
class DOGParams(BaseModel):
    max_transactions_per_rpc: int | None = None  # None means no limit
    connection_handler_queue_len: int = 5000
    cache_time_sec: int = 30
    target_redundancy: float = 1.0
    redundancy_delta_percent: int = 10
    redundancy_interval_sec: int = 1
    connection_handler_publish_duration_sec: int = 5
    connection_handler_forward_duration_sec: int = 1
    deliver_own_transactions: bool = False
    forward_transactions: bool = True
    send_have_tx: bool = True
    ignore_have_tx: bool = False

NodeConfigParams = Union[GossipSubParams, DOGParams]
InstructionParams = Union[
    Connect, Publish, SubscribeToTopic,
    SetTopicValidationDelay,
]

class ScriptInstruction(BaseModel):
    """
    Implementations MUST wait until elapsedMillis is greater than or equal to the specified value.
    They MUST NOT execute any proceeding instruction until the wait is complete.
    They MUST still handle message delivery and forwarding as normal.
    """
    elapsedMillis: int
    instructions: List[InstructionParams]
