use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(crate) struct ClientSession {
    // pub id: u64,
    // pub udp_addr: SocketAddr,
    pub subscribed_tickers: Vec<String>,
    pub last_ping: Instant,
}

pub(crate) struct ClientManager {
    pub(crate) clients: Arc<RwLock<HashMap<u64, ClientSession>>>,
    next_client_id: u64,
}

impl ClientManager {
    pub(crate) fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            next_client_id: 1,
        }
    }

    pub(crate) fn add_client(
        &mut self,
        // udp_addr: SocketAddr,
        subscribed_tickers: Vec<String>,
    ) -> u64 {
        let id = self.next_client_id;
        self.next_client_id += 1;

        let session = ClientSession {
            // id,
            // udp_addr,
            subscribed_tickers,
            last_ping: Instant::now(),
        };

        self.clients.write().unwrap().insert(id, session);
        id
    }

    pub(crate) fn remove_client(&mut self, id: u64) {
        self.clients
            .write()
            .unwrap()
            .remove(&id)
            // .expect("Client not found")
            ;
    }

    pub(crate) fn update_ping(&mut self, id: u64) -> bool {
        if let Some(client) = self.clients.write().unwrap().get_mut(&id) {
            client.last_ping = Instant::now();
            true
        } else {
            false
        }
    }

    pub(crate) fn get_inactive_clients(&self, timeout: Duration) -> Vec<u64> {
        let now = Instant::now();
        self.clients
            .read()
            .unwrap()
            .iter()
            .filter_map(|(id, client)| {
                if now.duration_since(client.last_ping) > timeout {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn check_client_ticker(&self, id: u64, ticket: &String) -> Option<bool> {
        self.clients
            .read()
            .unwrap()
            .get(&id)
            .map(|client| client.subscribed_tickers.contains(ticket))
    }
}
