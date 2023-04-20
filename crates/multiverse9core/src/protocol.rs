use log::*;
use std::io;
use std::net::TcpStream;
use std::sync::Arc;

use crate::prelude::Node;
use crate::Hasher;

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
    pub storage: redis::Connection,
}

impl<'a, T> Packet<'a, T>
where
    T: io::Read + io::Write,
{
    #[inline(always)]
    pub fn new(stream: T, buffer: &'a [u8], code: u8, storage: redis::Connection) -> Self {
        Self {
            code,
            buffer,
            stream,
            storage,
        }
    }
}

pub(crate) struct Handler(TcpStream);

impl Handler {
    #[inline(always)]
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self(stream)
    }

    pub(crate) fn tcp(&self, _node: Arc<Node>, redis: redis::Connection) -> io::Result<()> {
        let buffer = Tcp::read(&self.0)?;
        if buffer.is_empty() {
            debug!("Request {} is empty. Ignoring...", Hasher::hash(&buffer));
            return Ok(());
        }

        let (code, buffer) = (&buffer[0], &buffer[1..]);
        let packet = Packet::new(self.0.try_clone()?, buffer, *code, redis);
        match api::HANDLER_LOOKUP_TABLE.get(code) {
            Some(handle) => {
                let codes = unsafe { CODE_LOOKUP_TABLE.get(code).unwrap_unchecked() };
                let mut buffer = vec![];
                match handle(packet) {
                    Ok(reply) => {
                        buffer.push(codes.0);
                        buffer.extend(&reply);
                        Tcp::write(&self.0, &buffer)
                    }

                    Err(e) => {
                        error!("{}", e);
                        buffer.push(codes.1);
                        Tcp::write(&self.0, &buffer)
                    }
                }
            }

            None => {
                let codes = unsafe { CODE_LOOKUP_TABLE.get(&0x0000).unwrap_unchecked() };
                Tcp::write(&self.0, &[codes.0, codes.1])
            }
        }
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
    use super::Packet;
    use crate::Hasher;

    use lazy_static::lazy_static;
    use redis::Commands;

    use std::collections::HashMap;
    use std::io;
    use std::net::TcpStream;

    pub type HandlerFn<T> = fn(Packet<T>) -> Result<Vec<u8>, redis::RedisError>;

    lazy_static! {
        pub static ref HANDLER_LOOKUP_TABLE: HashMap<u8, HandlerFn<TcpStream>> =
            [
                (0x0001, create as HandlerFn<TcpStream>)
                //
            ].iter().cloned().collect();

        pub static ref CODE_LOOKUP_TABLE: HashMap<u8, (u8, u8)> =
            [
                // This code should only be used when no handler is found for the
                // provided command.
                (0x0000, (1, 1)),
                (0x0001, (0, 1)),
            ]
            .iter().cloned().collect();
    }

    fn create<T: io::Read + io::Write>(mut p: Packet<T>) -> Result<Vec<u8>, redis::RedisError> {
        let id = Hasher::hash(p.buffer);
        let _: () = p.storage.set(id.clone(), p.buffer)?;
        Ok(id.as_bytes().to_vec())
    }

    // End-to-end tests
    // #[cfg(test)]
    // mod tests {
    //     use std::net::{TcpListener, TcpStream};

    //     use crate::protocol::{Packet, Tcp};

    //     #[test]
    //     fn test_api_create() -> Result<(), Box<dyn std::error::Error>> {
    //         let redis = redis::Client::open("redis://127.0.0.1:5555")?.get_connection()?;
    //         let listener = TcpListener::bind("127.0.0.1:0")?;
    //         let stream = TcpStream::connect(listener.local_addr()?)?;
    //         let mut buffer = vec![0x0001];
    //         buffer.extend_from_slice(b"Hello, World!");
    //         let packet = Packet::new(&stream, &buffer, buffer[0], redis);
    //         super::create(packet)?;
    //         let reply = Tcp::read(stream)?;
    //         assert_eq!(reply[0], 0);
    //         Ok(())
    //     }
    // }
}
