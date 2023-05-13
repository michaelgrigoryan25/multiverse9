use log::*;
use std::io;
use std::sync::{Arc, Mutex};

use crate::api;
use crate::node::Node;
use crate::Tcp;

/// Represents a single request packet.
pub struct Packet<'a> {
    /// The request code used to lookup the appropriate handler function.
    pub code: u8,
    /// The stream the request was received on.
    pub stream: std::net::TcpStream,
    /// The request payload. Note that, this buffer does not include the code
    /// prefix which comes from the request.
    pub buffer: &'a [u8],
    pub node: Arc<Mutex<Node>>,
    pub storage: &'a mut redis::Connection,
}

/// Handles incoming TCP requests.
pub(crate) struct Handler {
    /// The TCP stream the request was received on.
    inner: std::net::TcpStream,
}

impl Handler {
    #[inline(always)]
    pub(crate) fn new(stream: std::net::TcpStream) -> Self {
        Self { inner: stream }
    }

    /// Handles incoming TCP requests.
    ///
    /// # Arguments
    ///
    /// * `node` - An Arc containing a mutex to the node configuration.
    /// * `redis` - A mutable reference to the Redis connection.
    ///
    /// # Returns
    ///
    /// An io::Result indicating success or failure.
    ///
    /// # Functionality
    ///
    /// This function reads from the TCP stream in a loop, separating the request
    /// code and payload. It then attempts to lookup a handler function for the
    /// request code in the [api::HANDLER_LOOKUP_TABLE]. If a handler is found, it is
    /// executed and the response is written to the stream. If no handler is found,
    /// the [api::unknown_command] function is called.
    pub(crate) fn tcp(
        &self,
        node: Arc<Mutex<Node>>,
        mut redis: redis::Connection,
    ) -> io::Result<()> {
        while self.inner.peer_addr().is_ok() {
            let buffer = Tcp::read(&self.inner)?;
            if buffer.is_empty() {
                continue;
            }

            // Separating request code (ID) and payload into a separate variable and buffer.
            let (code, buffer) = (&buffer[0], &buffer[1..]);
            let packet = Packet {
                buffer,
                code: *code,
                storage: &mut redis,
                node: Arc::clone(&node),
                stream: self.inner.try_clone()?,
            };

            match api::HANDLER_LOOKUP_TABLE.get(code) {
                Some(handle) => {
                    // Although this operation is safe, it still is a good practice to handle
                    // the error if I somehow managed to not include the code in the lookup
                    // table.
                    let codes = api::CODE_LOOKUP_TABLE.get(code).unwrap();
                    match handle(packet) {
                        Ok(reply) => {
                            let mut buffer = vec![codes.0];
                            buffer.extend(&reply);
                            Tcp::write(&self.inner, &buffer)?;
                        }

                        Err(e) => {
                            // TODO: Implement sending the error as a string with the reply in
                            // some way.
                            error!("{:?}", e);

                            let mut buffer = vec![codes.1];
                            buffer.push(codes.1);
                            buffer.push(00);
                            Tcp::write(&self.inner, &buffer)?;
                        }
                    };
                }

                None => api::unknown_command(packet)?,
            }
        }

        Ok(())
    }
}
