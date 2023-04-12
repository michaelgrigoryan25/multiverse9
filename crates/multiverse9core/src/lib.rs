pub mod node;
pub mod protocol;
pub mod settings;
pub mod prelude {
    pub use super::node::Node;
    pub use super::settings::Settings;
}

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
