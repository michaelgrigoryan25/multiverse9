#![forbid(unsafe_code)]

/// Contains the main node implementation which handles incoming TCP connections
/// and delegates the requests to the appropriate handler functions.
pub mod node;
/// Contains the protocol implementation for communicating between nodes. Defines
/// the request and response codes, as well as the handler functions for each request.
pub mod protocol;
/// Contains the SDK for interacting with the multiverse9 network.
pub mod sdk;
/// Contains the settings struct which holds configuration for a node instance.
pub mod settings;
pub mod prelude {
    pub use super::node::Node;
    pub use super::sdk;
    pub use super::settings::Settings;
}

/// Contains the protocol implementation for communicating between nodes. Defines
/// the request and response codes, as well as the handler functions for each request.
pub(crate) mod api;
/// Contains a thread pool implementation. The thread pool spawns a fixed number
/// of threads on initialization. Jobs can then be submitted to the pool, and will
/// be executed on the next available thread.
pub(crate) mod pooling;

/// Contains utility functions for interacting with TCP streams.
pub(crate) struct Tcp;

impl Tcp {
    /// This indicates how many bytes will be read at once when reading from an io stream.
    const MAX_READ_BYTES: usize = 16;

    /// Writes the given buffer to the stream.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream to write to.
    /// * `buffer` - The buffer containing the data to write.
    #[inline(always)]
    pub(crate) fn write<T: std::io::Read + std::io::Write>(
        mut stream: T,
        buffer: &[u8],
    ) -> std::io::Result<()> {
        stream.write_all(buffer)?;
        stream.flush()
    }

    /// Reads data from the given stream into a buffer. Reads up to
    /// [Self::MAX_READ_BYTES] at a time from the stream until there is no more data to read.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream to read from.
    ///
    /// # Returns
    ///
    /// The data read from the stream.
    pub(crate) fn read<T: std::io::Read + std::io::Write>(
        mut stream: T,
    ) -> std::io::Result<Vec<u8>> {
        let mut buffer: Vec<u8> = vec![];
        let mut rx_bytes = [0u8; Self::MAX_READ_BYTES];
        loop {
            let bytes_read = stream.read(&mut rx_bytes)?;
            buffer.extend_from_slice(&rx_bytes[..bytes_read]);
            if bytes_read < Self::MAX_READ_BYTES {
                // Stopping if all data was read from the stream
                break;
            }
        }

        stream.flush()?;
        Ok(buffer)
    }
}

/// Defines a macro that generates an enum with a ToString implementation and optional derives.
///
/// # Arguments
///
/// * `$enum_name` - The name of the enum to generate.
/// * `$variant_name` - The name of a variant in the enum.
/// * `$variant_type` - The type of the variant.
/// * `$derive_name` - Optional derives to apply to the enum.
///
/// # Example
///
/// ```rs
/// enum_with_impl_to_string!(
///     Color,  
///     .Red(String),
///     .Green(u8),
///     .Blue(f64),
///     ~Debug
/// );
///
/// let c = Color::Red("Crimson".to_string());
/// println!("{:?}", c); // Prints "Color::Red(Crimson)"
/// println!("{}", c.to_string()); // Prints "Crimson"
/// ```
///
/// This will generate an enum like:
///
/// ```rs
/// #[derive(Debug)]
/// pub enum Color {
///     Red(String),  
///     Green(u8),
///     Blue(f64)
/// }
///
/// impl std::string::ToString for Color {
///     fn to_string(&self) -> String {
///         match self {
///             Self::Red(val) => val.to_string(),  
///             Self::Green(val) => val.to_string(),
///             Self::Blue(val) => val.to_string()
///         }
///     }
/// }
/// ```
macro_rules! enum_with_impl_to_string {
    (
        $(#[doc = $doc:expr])*
        $visibility:vis $enum_name:ident,
        $(.$variant_name:ident($variant_type:ty))*
        $(~$derive_name:ident)*
    ) => {
        $(#[doc = $doc])*
        #[derive($($derive_name),*)]
        $visibility enum $enum_name {
            $($variant_name($variant_type)),*
        }

        #[automatically_derived]
        impl std::string::ToString for $enum_name {
            fn to_string(&self) -> String {
                match self {
                    $(Self::$variant_name(val) => val.to_string(),)*
                }
            }
        }
    };
}

pub(crate) use enum_with_impl_to_string;

#[cfg(test)]
mod tests {
    #[cfg(test)]
    mod tests_tcp_rw {
        use crate::Tcp;

        use std::net::{TcpListener, TcpStream};
        use std::thread;

        #[test]
        fn test_tcp_write() -> std::io::Result<()> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let addr = listener.local_addr()?;
            let buffer = b"Hello, world!";

            let handle = thread::spawn(move || -> std::io::Result<()> {
                let (stream, _) = listener.accept()?;
                Tcp::write(&stream, buffer)
            });

            let stream = TcpStream::connect(addr)?;
            Tcp::write(&stream, buffer)?;
            handle.join().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))
            })??;

            Ok(())
        }

        #[test]
        fn test_tcp_read() -> std::io::Result<()> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let addr = listener.local_addr()?;
            let buffer = b"Hello, world!";

            let handle = thread::spawn(move || -> std::io::Result<()> {
                let (stream, _) = listener.accept()?;
                Tcp::write(&stream, buffer)
            });

            let stream = TcpStream::connect(addr)?;
            let mut expected_buffer = Vec::new();
            expected_buffer.extend_from_slice(buffer);
            let actual_buffer = Tcp::read(&stream)?;
            handle.join().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))
            })??;
            assert_eq!(actual_buffer, expected_buffer);
            Ok(())
        }
    }
}
