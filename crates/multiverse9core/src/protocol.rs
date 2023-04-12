use log::*;
use std::io::{self, prelude::*};
use std::net::TcpStream;
use std::sync::Arc;

use crate::prelude::Node;
use crate::Hasher;

pub struct Pack<'a> {
    pub buffer: &'a [u8],
    pub stream: &'a TcpStream,
    pub node: &'a mut Arc<Node>,
}

impl<'a> Pack<'a> {
    pub fn new(stream: &'a TcpStream, buffer: &'a [u8], node: &'a mut Arc<Node>) -> Self {
        Self {
            stream,
            buffer,
            node,
        }
    }
}

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

    pub(crate) fn tcp(&self, node: &mut Arc<Node>) -> io::Result<()> {
        let buffer = read(&self)?;
        if buffer.is_empty() {
            debug!("Request {} is empty. Ignoring...", Hasher::hash(&buffer));
            return Ok(());
        }

        let pack = Pack::new(self, &buffer[1..], node);
        match api::LOOKUP_TABLE.get(&buffer[0]) {
            Some(h) => h(pack),
            None => write(self, &[0x0]),
        }
    }
}

pub(crate) fn write(mut stream: &TcpStream, buf: &[u8]) -> io::Result<()> {
    stream.write_all(buf)?;
    stream.flush()
}

pub(crate) fn read(mut stream: &TcpStream) -> io::Result<Vec<u8>> {
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

mod api {
    use super::{write, Pack};
    use lazy_static::lazy_static;
    use std::collections::HashMap;
    use std::io;

    pub(crate) type HandlerFn = fn(Pack) -> io::Result<()>;

    lazy_static! {
        pub static ref LOOKUP_TABLE: HashMap<u8, HandlerFn> = [
            (0x0001, create as HandlerFn),
            (0x0002, remove),
            (0x0003, search),
            (0x0004, get)
        ]
        .iter()
        .cloned()
        .collect();
    }

    fn create(p: Pack) -> io::Result<()> {
        unimplemented!()
    }

    fn remove(p: Pack) -> io::Result<()> {
        unimplemented!()
    }

    fn search(p: Pack) -> io::Result<()> {
        unimplemented!()
    }

    fn get(p: Pack) -> io::Result<()> {
        unimplemented!()
    }
}
