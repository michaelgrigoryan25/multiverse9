use std::io::prelude::*;
use std::net::TcpStream;

pub(crate) struct Handler(TcpStream);

impl std::ops::Deref for Handler {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Handler {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self(stream)
    }

    /// This is the main TCP request handler which will be spawned from [Node::start].
    ///
    /// # Blocking
    ///
    /// This function is blocking, as it should never return, unless the TCP stream is disconnected
    /// or there is an error coming from this function.
    pub(crate) fn tcp(&mut self) -> std::io::Result<()> {
        unimplemented!()
    }
}

pub(crate) fn write(mut stream: &std::net::TcpStream, buf: &[u8]) -> std::io::Result<()> {
    stream.write_all(buf)?;
    stream.flush()
}

pub(crate) fn read(mut stream: &std::net::TcpStream) -> std::io::Result<Vec<u8>> {
    const READ_BYTES_CAP: usize = 8;

    let mut buffer: Vec<u8> = vec![];
    let mut rx_bytes = [0u8; READ_BYTES_CAP];
    loop {
        let bytes_read = stream.read(&mut rx_bytes)?;
        buffer.extend_from_slice(&rx_bytes[..bytes_read]);
        if bytes_read < READ_BYTES_CAP {
            break;
        }
    }

    stream.flush()?;
    Ok(buffer)
}
