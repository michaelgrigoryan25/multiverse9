#![forbid(unsafe_code)]

pub mod node;
pub mod protocol;
pub mod settings;
pub mod prelude {
    pub use super::node::Node;
    pub use super::settings::Settings;
}

pub(crate) mod pooling;

/// Implementations of various hashing functions for internal usage purposes.
pub(crate) struct Hasher;

impl Hasher {
    #[inline(always)]
    /// Simple hasher function for generating random strings. Since memory
    /// addresses are pseudo-random, this will work fine for current case.
    pub(crate) fn hash<T>(raw: T) -> String {
        format!("{:p}", std::ptr::addr_of!(raw))
    }
}

#[cfg(test)]
mod tests {
    use crate::Hasher;

    #[test]
    fn test_hasher_hash() {
        assert_ne!(Hasher::hash("12345"), Hasher::hash("12345"))
    }
}
