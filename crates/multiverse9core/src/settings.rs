use log::*;
use serde::Deserialize;
use serde::Serialize;
use std::io::prelude::*;

/// Default address when binding the [std::net::TcpListener] locally.
const DEFAULT_HOST_ADDRESS: &str = "127.0.0.1:0";
/// Default instance name prefix.
const DEFAULT_INSTANCE_PREFIX: &str = "multiverse9";
/// Default settings file name.
const DEFAULT_SETTINGS_FILENAME: &str = "settings.json";
/// Default storage path.
const DEFAULT_DATA_PATHNAME: &str = DEFAULT_INSTANCE_PREFIX;

#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    /// Human-readable identifier of current instance.
    pub name: String,
    /// The path for storing information, such as posts and metadata.
    pub data: String,
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
    pub fn new(mv_data: Option<String>) -> Result<Self, SettingsError> {
        let mv_hash = crate::Hasher::hash(DEFAULT_INSTANCE_PREFIX);
        let mv_name = format!("{}_{}", DEFAULT_INSTANCE_PREFIX, mv_hash);
        let mv_data = mv_data.unwrap_or_else(|| String::from(DEFAULT_DATA_PATHNAME));

        std::fs::create_dir_all(&mv_data).map_err(SettingsError::IoError)?;
        debug!("Created a data directory at {:?}", &mv_data);

        // Getting the absolute path to the data directory.
        let mv_data = std::path::Path::new(&mv_data)
            .canonicalize()
            .map_err(SettingsError::IoError)?
            .display()
            .to_string();

        Ok(Self {
            data: mv_data,
            name: mv_name,
            nodes: vec![],
            open_metadata: true,
            open_interactions: true,
            version: env!("CARGO_PKG_VERSION").into(),
            addr: DEFAULT_HOST_ADDRESS.parse().unwrap(),
        })
    }

    /// Generates a settings file from the provided [Settings] struct and
    /// persists it in the filesystem at the path provided when initiating
    /// the struct.
    pub fn persist(&self) -> Result<(), SettingsError> {
        std::fs::create_dir_all(&self.data).map_err(SettingsError::IoError)?;
        debug!("Created a data directory at {:?}", &self.data);

        let mut settings = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(false)
            .open(format!("{}/{}", self.data, DEFAULT_SETTINGS_FILENAME))
            .map_err(SettingsError::IoError)?;

        settings
            .write_all(
                serde_json::to_string_pretty(&self)
                    .map_err(SettingsError::ConversionError)?
                    .as_bytes(),
            )
            .map_err(SettingsError::IoError)?;

        Ok(())
    }
}

/// Error enum for dealing with issues related to [Settings] parsing, initialization, and
/// conversion.
#[derive(Debug)]
pub enum SettingsError {
    IoError(std::io::Error),
    ParseError(serde_json::Error),
    ConversionError(serde_json::Error),
}

impl TryFrom<std::path::PathBuf> for Settings {
    type Error = SettingsError;

    fn try_from(path: std::path::PathBuf) -> Result<Self, Self::Error> {
        let mut settings = std::fs::File::open(&path).map_err(SettingsError::IoError)?;
        let mut contents = String::new();
        settings
            .read_to_string(&mut contents)
            .map_err(SettingsError::IoError)?;
        // The contents of the settings file will be kept in memory until the program ends running.
        let contents: &'static str = Box::leak(contents.into_boxed_str());
        let settings = serde_json::from_str(contents).map_err(SettingsError::ParseError);
        debug!("Settings from {:?} have been loaded successfully", &path);
        settings
    }
}
