use log::*;
use std::io;
use std::net::TcpStream;
use std::sync::Arc;

use self::api::CODE_LOOKUP_TABLE;
use crate::prelude::Node;
use crate::Tcp;

/// Represents a single request packet.
///
/// # Fields
///
/// * `code` - The request code used to lookup the appropriate handler function.
/// * `stream` - The stream the request was received on.
/// * `buffer` - The raw request payload.
/// * `storage` - A mutable reference to the Redis connection.
pub struct Packet<'a> {
    /// This is the handler code. The code is going to be used for determining
    /// response codes from the [api::CODE_LOOKUP_TABLE].
    pub code: u8,
    pub stream: TcpStream,
    pub buffer: &'a [u8],
    pub storage: &'a mut redis::Connection,
}

/// Handles incoming TCP requests.
///
/// # Fields
///
/// * `inner` - The TCP stream the request was received on.
pub(crate) struct Handler {
    inner: TcpStream,
}

impl Handler {
    #[inline(always)]
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self { inner: stream }
    }

    /// Handles incoming TCP requests.
    ///
    /// # Arguments
    ///
    /// * `_node` - An Arc containing the node settings. Currently unused.
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
    pub(crate) fn tcp(&self, _node: Arc<Node>, mut redis: redis::Connection) -> io::Result<()> {
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
                stream: self.inner.try_clone()?,
            };

            match api::HANDLER_LOOKUP_TABLE.get(code) {
                Some(handle) => {
                    // Although this operation is safe, it still is a good practice to handle
                    // the error if I somehow managed to not include the code in the lookup
                    // table.
                    let codes = CODE_LOOKUP_TABLE.get(code).unwrap();
                    match handle(packet) {
                        Ok(reply) => {
                            let mut buffer = vec![codes.0];
                            buffer.extend(&reply);
                            Tcp::write(&self.inner, &buffer)?;
                        }

                        Err(e) => {
                            let mut buffer = vec![codes.1];
                            error!("{:?}", e);
                            buffer.push(codes.1);
                            // buffer.extend_from_slice(e.to_string().as_bytes());
                            error!("{:?}", e);
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

/// Contains the protocol implementation for communicating between nodes. Defines
/// the request and response codes, as well as the handler functions for each request.
mod api {
    use super::{sdk, Packet, Tcp};

    use redis::Commands;

    use std::io;

    #[derive(Debug)]
    pub enum Error {
        SdkError(sdk::Error),
        PayloadError(&'static str),
        RedisError(redis::RedisError),
    }

    pub type HandlerResult = Result<Vec<u8>, Error>;
    pub type HandlerFn = fn(Packet) -> HandlerResult;

    #[inline(always)]
    pub fn unknown_command(p: Packet) -> io::Result<()> {
        let codes = CODE_LOOKUP_TABLE.get(&0x0000).unwrap();
        Tcp::write(p.stream, &[codes.0, codes.1])
    }

    pub const HANDLER_LOOKUP_TABLE: phf::Map<u8, HandlerFn> = phf::phf_map! {
        0x0001u8 => create,
        0x0002u8 => remove,
        0x0003u8 => aggregate,
    };

    pub const CODE_LOOKUP_TABLE: phf::Map<u8, (u8, u8)> = phf::phf_map! {
        // This code should only be used when no handler is found for the
        // provided command. Either way this code indicates a failure, so
        // both codes are 1.
        0x0000u8 => (1, 1),
        0x0001u8 => (0, 1),
        0x0002u8 => (0, 1),
        0x0003u8 => (0, 1),
        // 0x0004u8 => (0, 1),
    };

    /// Checks that the lengths of the `CODE_LOOKUP_TABLE` and `HANDLER_LOOKUP_TABLE`
    /// maps have the same number of keys. If not, panics with a helpful
    /// error message. This ensures that there are handler functions defined
    /// for all protocol codes, and no unused protocol codes are present. This
    /// check will happen at compile-time.
    const _: () = {
        const CODE_LOOKUP_TABLE_LEN: usize = CODE_LOOKUP_TABLE.len() - 1;
        const HANDLER_LOOKUP_TABLE_LEN: usize = HANDLER_LOOKUP_TABLE.len();
        if CODE_LOOKUP_TABLE_LEN != HANDLER_LOOKUP_TABLE_LEN {
            panic!("Lengths of `CODE_LOOKUP_TABLE` and `HANDLER_LOOKUP_TABLE` do not match!\nMake sure that there are no inconsistencies between them.");
        }
    };

    fn create(p: Packet) -> HandlerResult {
        let id = ulid::Ulid::new().to_string();
        p.storage.set(&id, p.buffer).map_err(Error::RedisError)?;
        Ok(id.as_bytes().to_vec())
    }

    fn remove(p: Packet) -> HandlerResult {
        let keys: Vec<&[u8]> = p.buffer.split(|c| *c == 00).collect();
        if keys.is_empty() {
            return Err(Error::PayloadError("No keys were found. Aborting..."));
        }

        p.storage.del(keys).map_err(Error::RedisError)?;
        Ok(Vec::with_capacity(0))
    }

    fn aggregate(p: Packet) -> HandlerResult {
        /// Extracts target keys from the provided buffer.
        ///
        /// # Arguments
        ///
        /// * `buffer` - The buffer containing the target keys and delimiters.
        ///
        /// # Returns
        ///
        /// A vector containing the extracted target keys.
        ///
        /// # Functionality
        ///
        /// This function iterates over the provided buffer, extracting target keys
        /// and storing them in a vector. The keys are separated by a null byte (00),
        /// so whenever a null byte is encountered, the current target key is pushed
        /// to the vector of targets and a new target key is started. If there are no
        /// null bytes, the entire buffer is treated as a single target key.
        fn extract_targets(buffer: &[u8]) -> Vec<Vec<u8>> {
            let mut targets: Vec<Vec<u8>> = vec![];
            let mut current_target: Vec<u8> = vec![];
            for chunk in buffer {
                if *chunk == 00 {
                    targets.push(current_target.clone());
                    current_target.clear();
                } else {
                    current_target.push(*chunk);
                }
            }

            if !current_target.is_empty() {
                targets.push(current_target);
            }

            targets
        }

        let targets = extract_targets(p.buffer);
        if targets.is_empty() {
            return Err(Error::PayloadError("No keys were found. Aborting..."));
        }

        let mut aggregated: Vec<u8> = vec![];
        for target in targets {
            if target.is_empty() {
                continue;
            }

            let target = target.to_vec();
            let target: Vec<&[u8]> = target.split(|c| *c == b'@').collect();
            // The key is required, however, the address of the key is not, since the
            // default instance where the key is going to be looked for is the current
            // node.
            let key = String::from_utf8_lossy(target.first().unwrap()).to_string();
            match target
                .get(1)
                .map(|chunks| String::from_utf8_lossy(chunks).to_string())
            {
                Some(addr) => {
                    let reply = sdk::aggregate(addr, vec![key.clone()]).map_err(Error::SdkError)?;
                    aggregated.extend(reply);
                }

                None => {
                    let buffer: Option<Vec<u8>> = p.storage.get(&key).map_err(Error::RedisError)?;
                    let buffer = buffer.unwrap_or(b"Unknown key".to_vec());
                    aggregated.extend(key.as_bytes());
                    aggregated.push(b':');
                    aggregated.extend(buffer);
                    aggregated.push(00);
                }
            }
        }

        Ok(aggregated)
    }
}

pub mod sdk {
    use std::net::TcpStream;

    use super::Tcp;

    #[derive(Debug)]
    pub enum Error {
        IoError(std::io::Error),
    }

    type SdkResult = Result<Vec<u8>, Error>;

    /// Aggregates the values of the specified keys from the node at the given address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the node to aggregate from.
    /// * `keys` - The keys to aggregate.
    ///
    /// # Returns
    ///
    /// The aggregated values of the keys.
    ///
    /// # Errors
    ///
    /// Returns an [Error::IoError] if there is an issue connecting to the node or reading the response.
    pub fn aggregate(addr: String, keys: Vec<String>) -> SdkResult {
        let stream = TcpStream::connect(&addr).map_err(Error::IoError)?;
        let mut buffer: Vec<u8> = vec![0x0003];

        for key in keys {
            buffer.extend_from_slice(key.as_bytes());
            buffer.push(00);
        }

        Tcp::write(&stream, &buffer).map_err(Error::IoError)?;
        Tcp::read(stream).map_err(Error::IoError)
    }
}
