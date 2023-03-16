use log::*;

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

/// A simple retrying mechanism implementation which will retry a function in case it fails for 20
/// times at max. The function accepts two closure arguments. The first function will be executed on
/// each attempt, and the second function will be called in case max number of attempts has been
/// reached.
///
/// # Blocking
///
/// The function is blocking, since it _may_ call [std::thread::sleep] for more than 1 time under the
/// hood. The function will automatically break after the **first** successful attempt.
pub(crate) fn attempt<const RETRIES: usize, const SLEEP: usize, T>(
    callback: impl FnOnce() -> Result<(), T> + Copy,
    fallback: impl FnOnce(),
) where
    T: std::fmt::Debug,
{
    use std::time::Duration;

    let mut attempts = 0;
    while attempts < RETRIES {
        let duration = Duration::from_secs((attempts * SLEEP) as u64);
        std::thread::sleep(duration);

        match callback() {
            Err(e) => {
                attempts += 1;
                error!("{:?}", e);
                continue;
            }

            _ => return,
        }
    }

    fallback()
}
