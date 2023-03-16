use crate::node::Node;
use log::*;
use std::io::prelude::*;
use std::sync::Arc;

/// This protocol code indicates that a remote node wants to initiate synchronization with current
/// node.
pub const PROTO_SYNC_REQ: &[u8] = &[0o000001];
/// This protocol code indicates that remote synchronization is available for remote nodes which want
/// to connect to current node.
pub const PROTO_SYNC_OK: &[u8] = &[0x10, 0x10];
/// This protocol code indicates that remote synchronization is not available for remote nodes
/// which want to connect to current node.
pub const PROTO_SYNC_NA: &[u8] = &[0x10, 0x11];

pub(crate) struct Handler {
    /// Keeping an immutable reference to the node data.
    mv_node: Arc<Node>,
    /// The streams will be moved and owned by the [Handler] for maximum isolation.
    stream: std::net::TcpStream,
}

impl Handler {
    pub(crate) fn new(stream: std::net::TcpStream, mv_node: Arc<Node>) -> Self {
        Self { stream, mv_node }
    }

    /// This is the main TCP request handler which will be spawned from [Node::start].
    ///
    /// # Blocking
    ///
    /// This function is blocking, as it should never return, unless the TCP stream is disconnected
    /// or there is an error coming from this function.
    pub(crate) fn tcp(self) -> std::io::Result<()> {
        let peer_addr = self.stream.peer_addr()?;
        let buffer = self.read()?;
        match buffer.as_slice() {
            PROTO_SYNC_REQ => {
                if self.mv_node.mv_settings.mv_open_interactions {
                    // Add remote node to the "mv_remotes" slice of the configuration. As of right
                    // now, this is impossible, since we are using an Arc'ed pointer, which points
                    // to Node and all its configuration.
                    debug!("Sync request from {} was accepted...", peer_addr);
                    self.write(PROTO_SYNC_OK)?;
                } else {
                    debug!("Sync request from {} was denied...", peer_addr);
                    self.write(PROTO_SYNC_NA)?;
                }
            }

            _ => (),
        }

        Ok(())
    }

    pub(crate) fn sync(self) -> std::io::Result<()> {
        // Sending a sync request to the remote node
        self.write(PROTO_SYNC_REQ)?;
        // Blocking indefinitely and getting back the response
        let reply = self.read()?;
        match reply.as_slice() {
            PROTO_SYNC_OK => {
                debug!("Full access granted during sync. Adding node to acknowledged nodes");
                unimplemented!();
            }

            PROTO_SYNC_NA => {
                debug!("Partial access granted during sync.");
                unimplemented!();
            }

            unknown => unimplemented!(
                "Unknown cases are currently not handled. Got: {:?}",
                unknown
            ),
        };
    }

    fn read(&self) -> std::io::Result<Vec<u8>> {
        const READ_BYTES_CAP: usize = 8;

        let mut buffer: Vec<u8> = vec![];
        let mut rx_bytes: [u8; READ_BYTES_CAP] = Default::default();
        loop {
            let bytes_read = (&self.stream).read(&mut rx_bytes)?;
            buffer.extend_from_slice(&rx_bytes[..bytes_read]);
            if bytes_read < READ_BYTES_CAP {
                break;
            }
        }

        (&self.stream).flush()?;
        Ok(buffer)
    }

    fn write(&self, buf: &[u8]) -> std::io::Result<()> {
        (&self.stream).write_all(buf)?;
        (&self.stream).flush()
    }
}
