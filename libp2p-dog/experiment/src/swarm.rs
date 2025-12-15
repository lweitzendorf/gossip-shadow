use crate::behaviour::MyBehaviour;
use libp2p::{noise, tcp, yamux, SwarmBuilder};
use std::time::Duration;
use libp2p_dog::ConfigBuilder;

pub(crate) fn new_swarm() -> libp2p::Swarm<MyBehaviour> {
    let config = ConfigBuilder::default()
        .target_redundancy(1.0)
        .redundancy_delta_percent(10)
        .redundancy_interval(Duration::from_secs(1))
        .build()
        .expect("Failed to build DOG config");

    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .unwrap()
        .with_behaviour(|key| MyBehaviour::new(key, config))
        .unwrap()
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX)))
        .build()
}
