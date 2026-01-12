use libp2p_dog::Transaction;

#[derive(Debug)]
pub(crate) struct State {
    pub should_shutdown: bool,
    pub transactions_received: Vec<Transaction>,
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            should_shutdown: false,
            transactions_received: Vec::new(),
        }
    }
}
