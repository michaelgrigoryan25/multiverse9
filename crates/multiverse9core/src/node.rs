use log::*;
use std::sync::Arc;

use crate::settings::Settings;

#[derive(Debug)]
pub struct Node {
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
    pub fn start(self: Arc<Self>) -> std::io::Result<()> {
        use crate::protocol::Handler;
        use std::net::TcpListener;

        let listener = TcpListener::bind(self.settings.addr)?;
        info!("TcpListener bound at {}", listener.local_addr()?);

        for stream in listener.incoming() {
            let stream = stream?;
            let addr = stream.peer_addr()?;
            let mut node = Arc::clone(&self);

            // Spawning a separate thread for each incoming connection. Besides a thread,
            // there will also be an instance of [Handler], which will be the main function
            // the thread tcp executes.
            std::thread::spawn(move || {
                if let Err(e) = Handler::new(stream).tcp(&mut node) {
                    error!("Stream error from {}: {}", addr, e);
                }
            });
        }

        Ok(())
    }
}
