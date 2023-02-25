use log::*;
use serde::Deserialize;
use serde::Serialize;
use std::io::prelude::*;
use std::sync::Arc;

/// Default address when binding the [std::net::TcpListener] locally.
const DEFAULT_HOST_ADDRESS: &str = "127.0.0.1:0";
/// Default storage path.
const DEFAULT_DATA_PATHNAME: &str = "multiverse9";
/// Default instance name prefix.
const DEFAULT_INSTANCE_PREFIX: &str = "multiverse9";
/// Default settings file name.
const DEFAULT_SETTINGS_FILENAME: &str = "settings.json";

/// This protocol code indicates that a remote node wants to initiate synchronization with current
/// node.
const PROTO_SYNC_REQ: &[u8] = &[0o000001];
/// This protocol code indicates that remote synchronization is available for remote nodes which want
/// to connect to current node.
const PROTO_SYNC_OK: &[u8] = &[0x10, 0x10];
/// This protocol code indicates that remote synchronization is not available for remote nodes
/// which want to connect to current node.
const PROTO_SYNC_NA: &[u8] = &[0x10, 0x11];

#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    /// Human-readable identifier of current instance.
    pub mv_name: String,
    /// The path for storing information, such as posts and metadata.
    pub mv_data: String,
    /// The version of current node.
    pub mv_version: String,
    /// Internal IP address of the node.
    pub mv_addr: std::net::SocketAddr,
    /// Whether the instance allows anyone to request for its metadata.
    pub mv_open_metadata: bool,
    /// Whether the instance is open for any kind of interaction from any
    /// remote instance. This essentially grants unrestricted access for
    /// posting, interacting, etc. on current instance for remote nodes.
    pub mv_open_interactions: bool,
    /// Acknowledged list of nodes which are allowed to have any type of
    /// interaction with current node. Essentially, this is a list of the
    /// nodes which are directly connected with current node.
    pub mv_nodes: Vec<std::net::SocketAddr>,
}

impl Settings {
    /// Creates a new settings struct for controlling an instance. The function
    /// will create a new directory in the filesystem for keeping the data for
    /// current instance.
    pub fn new(mv_data: Option<String>) -> Result<Self, SettingsError> {
        let mv_hash = Hasher::hash(DEFAULT_INSTANCE_PREFIX);
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
            mv_data,
            mv_name,
            mv_nodes: vec![],
            mv_open_metadata: true,
            mv_open_interactions: true,
            mv_version: env!("CARGO_PKG_VERSION").into(),
            mv_addr: DEFAULT_HOST_ADDRESS.parse().unwrap(),
        })
    }

    /// Generates a settings file from the provided [Settings] struct and
    /// persists it in the filesystem at the path provided when initiating
    /// the struct.
    pub fn generate(self) -> Result<(), SettingsError> {
        std::fs::create_dir_all(&self.mv_data).map_err(SettingsError::IoError)?;
        debug!("Created a data directory at {:?}", &self.mv_data);

        let mut settings = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(false)
            .open(format!("{}/{}", self.mv_data, DEFAULT_SETTINGS_FILENAME))
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
        debug!("Settings from `{:?}` have been loaded successfully", &path);
        settings
    }
}

/// Implementations of various hashing functions for internal usage purposes.
struct Hasher;

impl Hasher {
    #[inline(always)]
    /// Simple hasher function for generating random strings. Since memory
    /// addresses are pseudo-random, this will work fine for current case.
    fn hash<T>(raw: T) -> String {
        format!("{:p}", std::ptr::addr_of!(raw))
    }
}

struct Handler {
    /// Keeping an immutable reference to the node data.
    mv_node: Arc<Node>,
    /// The streams will be moved and owned by the [Handler] for maximum isolation.
    stream: std::net::TcpStream,
}

impl Handler {
    fn new(stream: std::net::TcpStream, mv_node: Arc<Node>) -> Self {
        Self { stream, mv_node }
    }

    /// This is the main TCP request handler which will be spawned from [Node::start].
    ///
    /// # Blocking
    ///
    /// This function is blocking, as it should never return, unless the TCP stream is disconnected
    /// or there is an error coming from this function.
    fn tcp(self) -> std::io::Result<()> {
        let buffer = read_tcp_stream(&self.stream)?;
        match buffer.as_slice() {
            PROTO_SYNC_REQ => {
                if self.mv_node.mv_settings.mv_open_interactions {
                    // Add remote node to the "mv_remotes" slice of the configuration. As of right
                    // now, this is impossible, since we are using an Arc'ed pointer, which points
                    // to Node and all its configuration.

                    write_tcp_stream(&self.stream, PROTO_INTERACT_OK)?;
                } else {
                    write_tcp_stream(&self.stream, PROTO_INTERACT_NO)?;
                }
            }

            _ => {}
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Node {
    pub mv_settings: Settings,
}

impl Node {
    /// Creates a new node from the specified [Settings] struct instance. [Settings] must be
    /// initialized separately.
    pub fn new(mv_settings: Settings) -> Self {
        Self { mv_settings }
    }

    /// Binds a [std::net::TcpListener] to the address specified by the [Settings] struct.
    /// [Node] must use [std::sync::Arc], since its configuration will be shared across
    /// threads. The threads, as of right now do not have the option of changing the settings
    /// internally.
    pub fn start(self: std::sync::Arc<Self>) -> std::io::Result<()> {
        // Binding to the specified address. This is 127.0.0.1:0 by default.
        let listener = std::net::TcpListener::bind(self.mv_settings.mv_addr)?;
        info!("TcpListener bound: {}", listener.local_addr()?);

        let ptr = self.clone();
        // Spawning a separate thread for handling the syncing process of nodes.
        std::thread::spawn(move || ptr.sync());
        debug!("Sync daemon has been spawned");

        for stream in listener.incoming() {
            let stream = stream?; // Acquiring the connection
            debug!("New connection from {}", stream.peer_addr()?);

            let ptr = self.clone();
            // Spawning a separate thread for each incoming connection. Besides a thread,
            // there will also be an instance of [Handler], which will be the main function
            // the thread tcp executes.
            std::thread::spawn(move || {
                // Only handling the error, since there is no needed data coming from an Ok(())
                // result.
                if let Err(e) = Handler::new(stream, ptr).tcp() {
                    error!("{}", e);
                }
            });
        }

        Ok(())
    }

    /// A safe and retriable handler function for syncing remote nodes with
    /// current node.
    ///
    /// # Tasks
    ///
    /// A non-exhaustive set of tasks this function is responsible for:
    ///
    /// - Collecting metadata from the connected nodes. (experimental)
    /// - Sending connection requests to the remote nodes. (experimental)
    /// - Handling incoming node connections and adding them to the configuration if allowed.
    /// (experimental)
    fn sync(&self) -> std::io::Result<()> {
        fn string_err<T: std::fmt::Debug>(e: T) -> String {
            format!("{:?}", e)
        }

        if self.mv_settings.mv_nodes.is_empty() {
            info!("Syncing skipped since there are no acknowledged remote nodes attached");
            if self.mv_settings.mv_open_interactions {
                info!("Syncing will re-initiate once any remote connection is established with the host");
            }

            return Ok(());
        }

        info!(
            "Started syncing local node with {n} remote nodes...",
            n = self.mv_settings.mv_nodes.len()
        );

        let mut failing = Vec::with_capacity(self.mv_settings.mv_nodes.len());
        // Iterating through all acknowledged nodes specified in the configuration file and sending
        // a CONN request to their nodes.
        for node in &self.mv_settings.mv_nodes {
            info!("Attempting to connect to {}", node);
            // Using a pull-back mechanism for repeatedly invoking the callback until failing for
            // the 20th time. This function is blocking.
            with_pullback::<20, 2, String>(
                || {
                    // Connecting to the remote node and checking whether there are no issues with the
                    // incoming stream.
                    if let Ok(stream) = std::net::TcpStream::connect(node) {
                        write_tcp_stream(&stream, PROTO_SYNC_REQ).map_err(string_err)?;
                        let reply = read_tcp_stream(&stream).map_err(string_err)?;
                        match *reply.as_slice() {
                            [0x10, 0x10] => {
                                debug!("Full access granted during sync. Adding node to acknowledged nodes");
                            }

                            [0x10, 0x11] => {
                                debug!("Partial access granted during sync.");
                            }

                            _ => (),
                        };

                        Ok(())
                    } else {
                        Err(format!("Could not to connect to `{:?}`", &node))
                    }
                },
                || {
                    // After maximum number of tries is reached, we are appending the node
                    // to the vector of failing nodes, and are notifying the user about
                    // this issue.
                    error!(
                        "Failed to establish connection with the node: `{:?}`",
                        &node
                    );
                    error!("\t\tNode at `{:?}` will be unavailable", &node);
                    failing.push(node);
                },
            );
        }

        // Checking for failing nodes and logging necessary information about them.
        if !failing.is_empty() {
            warn!("Found malfunctioning nodes: {} in total", failing.len());
            failing.iter().for_each(|r| warn!("\t-> {}", r));
        }

        info!("Node is now in sync");
        Ok(())
    }
}

/// A simple pull-back mechanism implementation which will retry a function in case it fails for 20
/// times at max. The function accepts two closure arguments. The first function will be executed on
/// each retry iteration, and the second function will be called in case max number of retries has
/// been reached.
///
/// # Blocking
///
/// The function is blocking, since it _may_ call [std::thread::sleep] for more than 1 time under the
/// hood. _**May**_ call, because the mechanism will automatically stop after the first successful
/// attempt.
fn with_pullback<const RETRIES: usize, const SLEEP: usize, T>(
    callback: impl FnOnce() -> Result<(), T> + Copy,
    fallback: impl FnOnce(),
) where
    T: std::fmt::Debug,
{
    let mut iterations = 0; // keeping track of current number of retries.
    while iterations < RETRIES {
        let duration = std::time::Duration::from_secs((iterations * SLEEP) as u64);
        std::thread::sleep(duration);

        if let Err(e) = callback() {
            error!("{:?}", e);
            iterations += 1;
            debug!(target: "with_pullback", "Sleeping for {:?}...", duration);
            continue;
        } else {
            debug!("Callback execution finished successfully. Stoping the pull-back");
            return;
        }
    }

    debug!("Pull-back failed after maximum number of tries. Executing the fallback function");
    fallback();
}

fn write_tcp_stream(mut stream: &std::net::TcpStream, buffer: &[u8]) -> std::io::Result<()> {
    stream.write_all(buffer)?;
    stream.flush()
}

/// This function helps us to avoid writing boilerplate code when executing TCP read requests.
fn read_tcp_stream(mut stream: &std::net::TcpStream) -> std::io::Result<Vec<u8>> {
    const READ_BYTES_CAP: usize = 8;

    let mut buffer: Vec<u8> = vec![];
    let mut rx_bytes: [u8; READ_BYTES_CAP] = Default::default();
    loop {
        let bytes_read = stream.read(&mut rx_bytes)?;
        buffer.extend_from_slice(&rx_bytes[..bytes_read]);
        if bytes_read < READ_BYTES_CAP {
            break;
        }
    }

    debug!("Read bytes: {:?}", buffer);
    stream.flush()?;
    Ok(buffer)
}
