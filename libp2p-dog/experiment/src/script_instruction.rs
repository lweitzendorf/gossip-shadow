use std::error::Error;
use std::ffi::OsStr;
use std::fmt::Display;
use std::path::Path;
use std::time::Duration;

use byteorder::{ByteOrder, LittleEndian};
use libp2p::identity::Keypair;
use libp2p_dog::ConfigBuilder;
use serde::{Deserialize, Serialize};

/// NodeID is a unique identifier for a node in the network.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeID(i32);

impl NodeID {
    /// Generate a node from the hostname
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let hostname = hostname::get()?.to_string_lossy().into_owned();

        // Parse "nodeX" format
        let mut chars = hostname.chars();
        // Skip "node" prefix
        for _ in 0..4 {
            if chars.next().is_none() {
                return Err("Invalid hostname format".into());
            }
        }
        // Parse remaining digits as node ID
        let id_str: String = chars.collect();
        Ok(NodeID(id_str.parse::<i32>()?))
    }
}

impl Display for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<NodeID> for Keypair {
    fn from(value: NodeID) -> Self {
        // Create a deterministic seed based on the node ID
        let mut seed = [0u8; 32];
        LittleEndian::write_i32(&mut seed[0..4], value.0);

        // Create a keypair from the seed
        Keypair::ed25519_from_bytes(seed).expect("Failed to create keypair")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DOGParams {
    pub max_transactions_per_rpc: Option<usize>,
    pub connection_handler_queue_len: Option<usize>,
    pub cache_time_sec: Option<u64>,
    pub target_redundancy: Option<f64>,
    pub redundancy_delta_percent: Option<u8>,
    pub redundancy_interval_sec: Option<u64>,
    pub connection_handler_publish_duration_sec: Option<u64>,
    pub connection_handler_forward_duration_sec: Option<u64>,
    pub deliver_own_transactions: Option<bool>,
    pub forward_transactions: Option<bool>,
    pub send_have_tx: Option<bool>,
    pub ignore_have_tx: Option<bool>,
}

impl DOGParams {
    pub fn apply(&self, builder: &mut ConfigBuilder) {
        if let Some(v) = self.max_transactions_per_rpc {
            builder.max_transactions_per_rpc(v);
        }
        if let Some(v) = self.connection_handler_queue_len {
            builder.connection_handler_queue_len(v);
        }
        if let Some(v) = self.cache_time_sec {
            builder.cache_time(Duration::from_secs(v));
        }
        if let Some(v) = self.target_redundancy {
            builder.target_redundancy(v);
        }
        if let Some(v) = self.redundancy_delta_percent {
            builder.redundancy_delta_percent(v);
        }
        if let Some(v) = self.redundancy_interval_sec {
            builder.redundancy_interval(Duration::from_secs(v));
        }
        if let Some(v) = self.connection_handler_publish_duration_sec {
            builder.connection_handler_publish_duration(Duration::from_secs(v));
        }
        if let Some(v) = self.connection_handler_forward_duration_sec {
            builder.connection_handler_forward_duration(Duration::from_secs(v));
        }
        if let Some(v) = self.deliver_own_transactions {
            builder.deliver_own_transactions(v);
        }
        if let Some(v) = self.forward_transactions {
            builder.forward_transactions(v);
        }
        if let Some(v) = self.send_have_tx {
            builder.send_have_tx(v);
        }
        if let Some(v) = self.ignore_have_tx {
            builder.ignore_have_tx(v);
        }
    }
}

/// Instruction represents an instruction that can be executed in a script.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Instruction {
    #[serde(rename = "connect", rename_all = "camelCase")]
    Connect { connect_to: Vec<NodeID> },

    #[serde(rename = "subscribeToTopic", rename_all = "camelCase")]
    SubscribeToTopic {
        #[serde(rename = "topicID")]
        topic_id: String
    },

    #[serde(rename = "publish", rename_all = "camelCase")]
    Publish {
        #[serde(rename = "messageID")]
        message_id: u64,
        message_size_bytes: usize,
        #[serde(rename = "topicID")]
        topic_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptInstruction {
    #[serde(rename = "elapsedMillis")]
    pub elapsed_millis: u64,
    pub instructions: Vec<Instruction>
}

/// ExperimentParams contains all parameters for an experiment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentParams {
    pub config: DOGParams,
    pub script: Vec<ScriptInstruction>,
}

impl ExperimentParams {
    pub fn from_json_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        let path = path.as_ref();

        if path.extension() != Some(OsStr::new("json")) {
            return Err("Params file must be a .json file".into());
        }

        let contents: String = std::fs::read_to_string(path)?;

        serde_json::from_str(&contents).map_err(Into::into)
    }
}
