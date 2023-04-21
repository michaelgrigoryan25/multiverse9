use log::*;
use std::net::TcpListener;
use std::sync::Arc;

use crate::pooling;
use crate::protocol::Handler;
use crate::settings::Settings;

#[derive(Debug)]
pub struct Node {
    /// Contains the settings of current node.
    pub settings: Settings,
}

impl Node {
    /// Creates a new node from the specified [Settings] struct instance. [Settings] must be
    /// initialized separately.
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    /// Binds a [std::net::TcpListener] to the address specified by the [Settings] struct.
    /// [Node] must use [std::sync::Arc], since its configuration will be shared across
    /// threads. The threads, as of right now do not have the option of changing the settings
    /// internally.
    pub fn start(self, threads: Option<usize>) -> std::io::Result<()> {
        let node = Arc::new(self);
        let pool = pooling::Pool::new(threads.unwrap_or(15));
        let listener = TcpListener::bind(node.settings.addr)?;
        info!("TcpListener bound at {}", listener.local_addr()?);

        let redis = Arc::new(
            redis::Client::open(node.settings.redis_uri.clone())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?,
        );

        for stream in listener.incoming() {
            let stream = stream?;
            let addr = stream.peer_addr()?;
            let node = Arc::clone(&node);
            let redis = Arc::clone(&redis);

            // Spawning a separate thread for each incoming connection. Besides a thread,
            // there will also be an instance of [Handler], which will be the main function
            // the thread tcp executes.
            pool.execute(move || {
                let redis = redis.get_connection().unwrap();
                if let Err(e) = Handler::new(stream).tcp(node, redis) {
                    error!("Stream error from {}: {}", addr, e);
                }
            });
        }

        Ok(())
    }
}
