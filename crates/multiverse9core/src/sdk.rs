use std::net::TcpStream;

use super::Tcp;

crate::enum_with_impl_to_string! {
    pub Error,
    .Io(std::io::Error)
    ~Debug
}

type SdkResult = Result<Vec<u8>, Error>;

/// Aggregates the values of the specified keys from the node at the given address.
///
/// # Arguments
///
/// * `addr` - The address of the node to aggregate from.
/// * `key` - The key to aggregate.
///
/// # Returns
///
/// The aggregated values of the keys.
///
/// # Errors
///
/// Returns an [Error::Io] if there is an issue connecting to the node or reading
/// the response.
pub fn aggregate(addr: String, key: String) -> SdkResult {
    let stream = TcpStream::connect(addr).map_err(Error::Io)?;
    let mut buffer: Vec<u8> = vec![0x0003];

    // for key in keys {
    buffer.extend_from_slice(key.as_bytes());
    buffer.push(00);
    // }

    Tcp::write(&stream, &buffer).map_err(Error::Io)?;
    Tcp::read(stream).map_err(Error::Io)
}
