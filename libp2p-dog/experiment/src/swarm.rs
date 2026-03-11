use crate::behaviour::MyBehaviour;
use crate::script_instruction::DOGParams;
use libp2p::{noise, tcp, yamux, SwarmBuilder};
use std::time::Duration;
use libp2p_dog::ConfigBuilder;

pub(crate) fn new_swarm(dog_params: &DOGParams) -> libp2p::Swarm<MyBehaviour> {
    let mut builder = ConfigBuilder::default();
    dog_params.apply(&mut builder);
    let config = builder.build().expect("Failed to build DOG config");

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
