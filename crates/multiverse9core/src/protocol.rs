use log::*;
use std::io;
use std::net::TcpStream;
use std::sync::Arc;

use crate::prelude::Node;

use self::api::CODE_LOOKUP_TABLE;

pub struct Packet<'a, T>
where
    T: io::Read + io::Write,
{
    /// This is the handler code. The code is going to be used for determining
    /// response codes from the [api::CODE_LOOKUP_TABLE].
    pub code: u8,
    pub stream: T,
    pub buffer: &'a [u8],
    pub storage: &'a mut redis::Connection,
}

impl<'a, T> Packet<'a, T>
where
    T: io::Read + io::Write,
{
    #[inline(always)]
    pub fn new(stream: T, buffer: &'a [u8], code: u8, storage: &'a mut redis::Connection) -> Self {
        Self {
            code,
            buffer,
            stream,
            storage,
        }
    }
}

pub(crate) struct Handler {
    inner: TcpStream,
}

impl Handler {
    #[inline(always)]
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self { inner: stream }
    }

    pub(crate) fn tcp(&self, _node: Arc<Node>, mut redis: redis::Connection) -> io::Result<()> {
        while self.inner.peer_addr().is_ok() {
            let buffer = Tcp::read(&self.inner)?;
            if buffer.is_empty() {
                continue;
            }

            // Separating request code (ID) and payload into a separate variable and buffer.
            let (code, buffer) = (&buffer[0], &buffer[1..]);
            let packet = Packet::new(self.inner.try_clone()?, buffer, *code, &mut redis);
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
                            buffer.extend_from_slice(e.to_string().as_bytes());
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

pub(crate) struct Tcp;

impl Tcp {
    #[inline(always)]
    pub(crate) fn write<T: io::Read + io::Write>(mut stream: T, buffer: &[u8]) -> io::Result<()> {
        stream.write_all(buffer)?;
        stream.flush()
    }

    pub(crate) fn read<T: io::Read + io::Write>(mut stream: T) -> io::Result<Vec<u8>> {
        const MAX_READ_BYTES: usize = 8;

        let mut buffer: Vec<u8> = vec![];
        let mut rx_bytes = [0u8; MAX_READ_BYTES];
        loop {
            let bytes_read = stream.read(&mut rx_bytes)?;
            buffer.extend_from_slice(&rx_bytes[..bytes_read]);
            if bytes_read < MAX_READ_BYTES {
                // Stopping if all data was read from the stream
                break;
            }
        }

        stream.flush()?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::{TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn test_tcp_write() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let buffer = b"Hello, world!";

        let handle = thread::spawn(move || -> io::Result<()> {
            let (stream, _) = listener.accept()?;
            Tcp::write(&stream, buffer)
        });

        let stream = TcpStream::connect(addr)?;
        Tcp::write(&stream, buffer)?;
        handle
            .join()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))??;
        Ok(())
    }

    #[test]
    fn test_tcp_read() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let buffer = b"Hello, world!";

        let handle = thread::spawn(move || -> io::Result<()> {
            let (stream, _) = listener.accept()?;
            Tcp::write(&stream, buffer)
        });

        let stream = TcpStream::connect(addr)?;
        let mut expected_buffer = Vec::new();
        expected_buffer.extend_from_slice(buffer);
        let actual_buffer = Tcp::read(&stream)?;
        handle
            .join()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))??;
        assert_eq!(actual_buffer, expected_buffer);
        Ok(())
    }
}

mod api {
    use super::{Packet, Tcp};

    use lazy_static::lazy_static;
    use redis::Commands;

    use std::collections::HashMap;
    use std::io;
    use std::net::TcpStream;

    #[derive(Debug)]
    pub enum HandlerError {
        RedisError(redis::RedisError),
    }

    impl ToString for HandlerError {
        fn to_string(&self) -> String {
            match self {
                // HandlerError::PayloadError(e) => e.clone(),
                HandlerError::RedisError(e) => e.to_string(),
            }
        }
    }

    pub type HandlerResult = Result<Vec<u8>, HandlerError>;
    pub type HandlerFn<T> = fn(Packet<T>) -> HandlerResult;

    pub static UNKNOWN_HANDLER_CODE: u8 = 0x0000;
    pub fn unknown_command<T: io::Read + io::Write>(p: Packet<T>) -> io::Result<()> {
        let codes = CODE_LOOKUP_TABLE.get(&UNKNOWN_HANDLER_CODE).unwrap();
        Tcp::write(p.stream, &[codes.0, codes.1])
    }

    lazy_static! {
        pub static ref HANDLER_LOOKUP_TABLE: HashMap<u8, HandlerFn<TcpStream>> =
            [
                (0x0001, create as HandlerFn<TcpStream>),
                (0x0002, remove),
                (0x0003, aggregate)
                //
            ].iter().cloned().collect();

        pub static ref CODE_LOOKUP_TABLE: HashMap<u8, (u8, u8)> =
            [
                // This code should only be used when no handler is found for the
                // provided command. Either way this code indicates a failure, so
                // both codes are 1.
                (UNKNOWN_HANDLER_CODE, (1, 1)),
                (0x0001, (0, 1)),
                (0x0002, (0, 1)),
                (0x0003, (0, 1))
            ]
            .iter().cloned().collect();
    }

    fn create<T: io::Read + io::Write>(p: Packet<T>) -> HandlerResult {
        let id = ulid::Ulid::new().to_string();
        p.storage
            .set(&id, p.buffer)
            .map_err(HandlerError::RedisError)?;

        Ok(id.as_bytes().to_vec())
    }

    fn remove<T: io::Read + io::Write>(p: Packet<T>) -> HandlerResult {
        p.storage.del(p.buffer).map_err(HandlerError::RedisError)?;
        Ok(Default::default())
    }

    fn aggregate<T: io::Read + io::Write>(p: Packet<T>) -> HandlerResult {
        unimplemented!()
    }
}
