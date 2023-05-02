use log::*;
use serde::Deserialize;
use serde::Serialize;
use std::io::prelude::*;

/// Default address when binding the [std::net::TcpListener] locally.
const DEFAULT_HOST_ADDRESS: &str = "127.0.0.1:0";
/// Default instance name prefix.
const DEFAULT_INSTANCE_PREFIX: &str = "multiverse9";

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    /// Human-readable identifier of current instance.
    pub name: String,
    /// Redis connection string.
    pub redis_uri: String,
    /// The version of current node.
    pub version: String,
    /// Internal IP address of the node.
    pub addr: std::net::SocketAddr,
    /// Whether the instance allows anyone to request for its metadata.
    pub open_metadata: bool,
    /// Whether the instance is open for any kind of interaction from any
    /// remote instance. This essentially grants unrestricted access for
    /// posting, interacting, etc. on current instance for remote nodes.
    pub open_interactions: bool,
    /// Acknowledged list of nodes which are allowed to have any type of
    /// interaction with current node. Essentially, this is a list of the
    /// nodes which are directly connected with current node.
    pub nodes: Vec<std::net::SocketAddr>,
}

impl Settings {
    /// Creates a new settings struct for controlling an instance. The function
    /// will create a new directory in the filesystem for keeping the data for
    /// current instance.
    pub fn new(redis_uri: String) -> Result<Self, Error> {
        redis::Client::open(redis_uri.clone()).map_err(Error::RedisError)?;

        let hash = ulid::Ulid::new().to_string();
        let name = format!("{}_{}", DEFAULT_INSTANCE_PREFIX, hash);

        Ok(Self {
            name,
            redis_uri,
            nodes: vec![],
            open_metadata: true,
            open_interactions: true,
            version: env!("CARGO_PKG_VERSION").into(),
            addr: DEFAULT_HOST_ADDRESS.parse().unwrap(),
        })
    }
}

impl ToString for Settings {
    fn to_string(&self) -> String {
        serde_json::to_string_pretty(&self).unwrap()
    }
}

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    ParseError(serde_json::Error),
    RedisError(redis::RedisError),
    ConversionError(serde_json::Error),
}

impl TryFrom<std::path::PathBuf> for Settings {
    type Error = Error;

    fn try_from(path: std::path::PathBuf) -> Result<Self, Self::Error> {
        let mut settings = std::fs::File::open(&path).map_err(Error::IoError)?;
        let mut contents = String::new();
        settings
            .read_to_string(&mut contents)
            .map_err(Error::IoError)?;
        // The contents of the settings file will be kept in memory until the program ends running.
        let contents: &'static str = Box::leak(contents.into_boxed_str());
        let settings = serde_json::from_str(contents).map_err(Error::ParseError);
        debug!("Settings from {:?} have been loaded successfully", &path);
        settings
    }
}
