use crate::attempt;
use crate::protocol::Handler;
use crate::settings::Settings;
use log::*;
use std::sync::Arc;

#[derive(Debug)]
pub struct Node {
    pub mv_settings: Settings,
}

impl Node {
    /// Creates a new node from the specified [Settings] struct instance. [Settings] must be
    /// initialized separately.
    pub fn new(mv_settings: Settings) -> Self {
        Self { mv_settings }
    }

    /// Binds a [std::net::TcpListener] to the address specified by the [Settings] struct.
    /// [Node] must use [std::sync::Arc], since its configuration will be shared across
    /// threads. The threads, as of right now do not have the option of changing the settings
    /// internally.
    pub fn start(self: Arc<Self>) -> std::io::Result<()> {
        let mut threads = vec![];
        // Binding to the specified address. This is 127.0.0.1:0 by default.
        let listener = std::net::TcpListener::bind(self.mv_settings.mv_addr)?;
        info!("TcpListener bound: {}", listener.local_addr()?);

        {
            let arc = self.clone();
            threads.push(std::thread::spawn(move || {
                if let Err(e) = arc.sync() {
                    error!("Error occurred during sync: {:?}", e);
                }
            }));

            debug!("Sync thread has been spawned...");
        }

        for stream in listener.incoming() {
            let stream = stream?;
            let peer_addr = stream.peer_addr()?;
            debug!("New connection from {}", peer_addr);

            let arc = self.clone();
            // Spawning a separate thread for each incoming connection. Besides a thread,
            // there will also be an instance of [Handler], which will be the main function
            // the thread tcp executes.
            threads.push(std::thread::spawn(move || {
                // Only handling the error, since there is no needed data coming from an Ok(())
                // result.
                if let Err(e) = Handler::new(stream, arc).tcp() {
                    error!("Stream error {}: {}", peer_addr, e);
                }
            }));
        }

        for thread in threads {
            if let Err(e) = thread.join() {
                error!("Error while joining the thread: {:?}", e);
            }
        }

        Ok(())
    }

    fn sync(self: Arc<Self>) -> std::io::Result<()> {
        fn string_err<T: std::fmt::Debug>(e: T) -> String {
            format!("{:?}", e)
        }

        if self.mv_settings.mv_nodes.is_empty() {
            info!("Syncing skipped. No acknowledged remote nodes found");
            if self.mv_settings.mv_open_interactions {
                info!("Syncing will reinstantiate once a remote connection is established with current node");
            }

            return Ok(());
        }

        info!(
            "Started syncing local node with {n} remote nodes...",
            n = self.mv_settings.mv_nodes.len()
        );

        let mut failing = Vec::with_capacity(self.mv_settings.mv_nodes.len());
        for node in &self.mv_settings.mv_nodes {
            info!("Attempting to connect to {}", node);
            // Using a retrying mechanism to repeatedly invoke the callback until failing for
            // the 20th time.
            attempt::<20, 2, String>(
                || {
                    // Connecting to the remote node and checking whether there are no issues with the
                    // incoming stream.
                    if let Ok(stream) = std::net::TcpStream::connect(node) {
                        Handler::new(stream, self.clone())
                            .sync()
                            .map_err(string_err)
                    } else {
                        Err(format!("Could not to connect to `{:?}`", &node))
                    }
                },
                || {
                    // After maximum number of tries is reached, we are appending the node
                    // to the vector of failing nodes, and are notifying the user about
                    // this issue.
                    error!(
                        "Failed to establish connection with the node: `{:?}`",
                        &node
                    );
                    error!("\t\tNode at `{:?}` will be unavailable", &node);
                    failing.push(node);
                },
            );
        }

        // Checking for failing nodes and logging necessary information about them.
        if !failing.is_empty() {
            warn!("Found malfunctioning nodes: {} in total", failing.len());
            failing.iter().for_each(|r| warn!("\t-> {}", r));
        }

        Ok(info!("Node is now in sync"))
    }
}
