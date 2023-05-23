use crate::protocol::Packet;
use crate::{sdk, Tcp};

use redis::Commands;
use std::io;

/// This module contains private helper functions used within [api](crate::api).
mod internal {
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
    #[inline(always)]
    pub fn buf_extract_targets(buffer: &[u8]) -> Vec<Vec<u8>> {
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

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_extract_keys() {
            let buffer = b"key1@addr1\x00key2\x00key3@addr3";
            let expected = vec![
                b"key1@addr1".to_vec(),
                b"key2".to_vec(),
                b"key3@addr3".to_vec(),
            ];
            assert_eq!(super::buf_extract_targets(buffer), expected);
            let buffer = b"key1@addr1\x00key2\x00key3@addr3\x00";
            assert_eq!(super::buf_extract_targets(buffer), expected);
        }
    }
}

crate::enum_with_impl_to_string! {
    pub Error,
    .Sdk(sdk::Error)
    .InvalidKey(String)
    .EmptyKeys(&'static str)
    .Redis(redis::RedisError)
    .EmptyBuffer(&'static str)
    ~Debug
}

pub type HandlerOutputCodes = (u8, u8);
pub type HandlerResult = Result<Vec<u8>, Error>;
pub type HandlerFn = fn(Packet) -> HandlerResult;

#[inline(always)]
/// Sends an error response `(1, 1)` if an unknown command is received.
pub fn unknown_command(p: Packet) -> io::Result<()> {
    Tcp::write(p.stream, &[1, 1])
}

/// A lookup table mapping request codes to handler functions. Used to determine
/// the appropriate handler function to call for a given request.
pub const HANDLER_LOOKUP_TABLE: phf::Map<u8, HandlerFn> = phf::phf_map! {
    0x0001u8 => create,
    0x0002u8 => remove,
    0x0003u8 => aggregate,
};

/// A lookup table mapping request codes to response codes. Used to determine
/// the appropriate response for a given request.
pub const CODE_LOOKUP_TABLE: phf::Map<u8, HandlerOutputCodes> = phf::phf_map! {
    0x0001u8 => (0, 1),
    0x0002u8 => (0, 1),
    0x0003u8 => (0, 1),
};

/// Compile-time length equality assertion for the lookup tables.
const _: () = assert!(CODE_LOOKUP_TABLE.len() == HANDLER_LOOKUP_TABLE.len());

fn create(p: Packet) -> HandlerResult {
    // The buffer cannot be empty when creating data
    if p.buffer.is_empty() {
        return Err(Error::EmptyBuffer(""));
    }

    // Generating a unique ID for the data
    let id = ulid::Ulid::new().to_string();
    p.storage.set(&id, p.buffer).map_err(Error::Redis)?;
    Ok(id.as_bytes().to_vec())
}

fn remove(p: Packet) -> HandlerResult {
    // As of right now, only local removals are supported. However,
    // remote removals might also become supported.
    let keys: Vec<_> = internal::buf_extract_targets(p.buffer);
    // If the keys vector is empty after extraction, then this operation
    // is invalid.
    if keys.is_empty() {
        return Err(Error::EmptyKeys(""));
    }

    p.storage.del(keys).map_err(Error::Redis)?;
    Ok(Vec::with_capacity(0))
}

fn aggregate(p: Packet) -> HandlerResult {
    let targets = internal::buf_extract_targets(p.buffer);
    if targets.is_empty() {
        return Err(Error::EmptyKeys(""));
    }

    let mut aggregated: Vec<u8> = vec![];
    for target in targets {
        if target.is_empty() {
            dbg!(target);
            panic!("`internal::buf_extract_keys` contains a bug. Cannot append empty vectors to `aggregated`.");
        }

        let target: Vec<&[u8]> = target.split(|c: &u8| *c == b'@').collect();
        // The key is required, however, the address of the key is not, since the
        // default instance where the key is going to be looked for is the current
        // node.
        let key: String = String::from_utf8_lossy(target.first().unwrap()).to_string();
        if key.len() != ulid::ULID_LEN {
            return Err(Error::InvalidKey(key));
        }

        // Attempts to extract the address of the key and convert it to a String.
        match target
            .get(1)
            .map(|chunks| String::from_utf8_lossy(chunks).to_string())
        {
            Some(addr) => {
                // If the key came with an address, then we are going to make an external
                // request to the remote node via the SDK and push the aggregated resposne
                // bytes to the reply.
                let reply = sdk::aggregate(addr, key).map_err(Error::Sdk)?;
                aggregated.extend(reply);
                Ok(())

                // TODO: Implement a HashMap, which would collect all the keys which are
                // registered under one address. This is used to send bulk read requests
                // instead of separate smaller requests. This would also require sdk::aggregate
                // to be changed accordingly.
            }
            None => {
                let buffer: Option<Vec<u8>> = p.storage.get(&key).map_err(Error::Redis)?;
                let buffer = buffer.unwrap_or(b"Unknown key".to_vec());
                aggregated.extend(key.as_bytes());
                aggregated.push(b':');
                aggregated.extend(buffer);
                aggregated.push(00);
                Ok(())
            }
        }?;
    }

    Ok(aggregated)
}
