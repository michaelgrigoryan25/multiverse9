use log::*;
use serde::Deserialize;
use serde::Serialize;
use std::io::prelude::*;

const DEFAULT_HOST_ADDRESS: &str = "127.0.0.1:0";
const DEFAULT_DATA_PATHNAME: &str = "multiverse9";
const DEFAULT_INSTANCE_PREFIX: &str = "multiverse9";
const DEFAULT_SETTINGS_FILENAME: &str = "settings.json";

const PROTOCOL_CONN_REQ: &[u8] = &[0o0001];
const PROTOCOL_INIT_RES: &[u8] = &[0o0010];
const PROTOCOL_AGGR_REQ: &[u8] = &[0o0002];
const PROTOCOL_META_REQ: &[u8] = &[0o0003];

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
    pub fn new(mv_data: Option<String>) -> std::io::Result<Self> {
        let mv_hash = Hasher::hash(DEFAULT_INSTANCE_PREFIX);
        let mv_name = format!("{}_{}", DEFAULT_INSTANCE_PREFIX, mv_hash);
        let mv_data = mv_data.unwrap_or_else(|| String::from(DEFAULT_DATA_PATHNAME));

        std::fs::create_dir_all(&mv_data)?;
        debug!("Created a data directory at {:?}", &mv_data);

        // Getting the absolute path to the data directory.
        let mv_data = std::path::Path::new(&mv_data)
            .canonicalize()?
            .display()
            .to_string();

        Ok(Self {
            mv_data,
            mv_name,
            mv_nodes: vec![],
            mv_open_metadata: true,
            mv_open_interactions: true,
            // We don't have to define the version of the node manually,
            // since it is done at build-time by cargo.
            mv_version: env!("CARGO_PKG_VERSION").into(),
            mv_addr: DEFAULT_HOST_ADDRESS.parse().unwrap(),
        })
    }

    /// Generates a settings file from the provided [Settings] struct and
    /// persists it in the filesystem at the path provided when initiating
    /// the struct.
    pub fn generate(self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.mv_data)?;
        debug!("Created a data directory at {:?}", &self.mv_data);

        let mut settings = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(false)
            .open(format!("{}/{}", self.mv_data, DEFAULT_SETTINGS_FILENAME))?;

        settings.write_all(
            serde_json::to_string_pretty(&self)
                .expect("Could not convert settings into JSON")
                .as_bytes(),
        )?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum ParseSettingsError {
    IoError(std::io::Error),
    ParseError(serde_json::Error),
}

impl TryFrom<std::path::PathBuf> for Settings {
    type Error = ParseSettingsError;

    fn try_from(path: std::path::PathBuf) -> Result<Self, Self::Error> {
        let mut settings = std::fs::File::open(&path).map_err(ParseSettingsError::IoError)?;
        let mut contents = String::new();
        settings
            .read_to_string(&mut contents)
            .map_err(ParseSettingsError::IoError)?;
        // The contents of the settings file will be kept in memory until the program ends running.
        let contents: &'static str = Box::leak(contents.into_boxed_str());
        let settings = serde_json::from_str(contents).map_err(ParseSettingsError::ParseError);
        debug!("Settings from `{:?}` have been loaded successfully", &path);
        settings
    }
}

/// Implementations of hashing functions for internal usage purposes.
struct Hasher;

impl Hasher {
    #[inline(always)]
    /// Simple hasher function for generating random strings. Since memory
    /// addresses are _almost_ random, this will work fine for current case.
    fn hash<T>(raw: T) -> String {
        format!("{:p}", std::ptr::addr_of!(raw))
    }
}

struct Handler {
    stream: std::net::TcpStream,
}

impl Handler {
    fn new(stream: std::io::Result<std::net::TcpStream>) -> Self {
        Self {
            stream: stream.expect("Cannot acquire TcpStream"),
        }
    }

    /// This is the main TCP request handler which will be spawned from [Node::start].
    fn tcp(mut self) -> std::io::Result<()> {
        use std::io::{Error, ErrorKind::UnexpectedEof};

        loop {
            // Parsing the size of the received packet
            match packet_size(&self.stream) {
                Ok(size) => {
                    // Allocating exactly `size` bytes of space for reading and parsing the request
                    // received from the remote node.
                    let mut buffer = vec![0; size];
                    // If read bytes do not match the size of the parsed request, then the request will
                    // be aborted with an Unexpected EOF error.
                    if self.stream.read(&mut buffer)? != size {
                        let error =
                            Error::new(UnexpectedEof, "Header size does not match buffer size");
                        self.stream.write_all(format!("{}", error).as_bytes())?;
                        return Err(error);
                    }

                    match buffer.as_slice() {
                        PROTOCOL_INIT_REQ => {
                            debug!("Received an initialization request");
                        }

                        PROTOCOL_CONN_REQ => {
                            debug!("Received a connection request");
                        }

                        PROTOCOL_AGGR_REQ => {
                            debug!("Received an aggregation request");
                        }

                        PROTOCOL_META_REQ => {
                            debug!("Received a metadata request");
                        }

                        unknown => {
                            debug!("Received an unknown request: `{:?}`", unknown);
                        }
                    };
                }

                Err(e) => {
                    self.stream.write_all(format!("{}", e).as_bytes())?;
                    return Err(e);
                }
            }
        }
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
        // Spawning a separate thread for handling the syncing process of nodes.
        std::thread::spawn(move || self.sync());
        for stream in listener.incoming() {
            // Spawning a separate thread for each incoming connection. Besides a thread,
            // there will also be an instance of [Handler], which will be the main function
            // the thread tcp executes.
            std::thread::spawn(move || {
                // Only handling the error, since there is no needed data coming from an Ok(())
                // result.
                if let Err(e) = Handler::new(stream).tcp() {
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
    /// - Pinging remote nodes.
    /// - Collecting metadata from the connected nodes.
    fn sync(&self) {
        if self.mv_settings.mv_nodes.is_empty() {
            info!("Syncing skipped since there are no remote nodes attached");
            if self.mv_settings.mv_open_interactions {
                info!("Syncing will re-initiate once a remote connection is established with the host");
            }

            return;
        }

        info!(
            "Started syncing local node with {n} remote nodes...",
            n = self.mv_settings.mv_nodes.len()
        );

        let mut failing = Vec::with_capacity(self.mv_settings.mv_nodes.len());
        // Iterating through all acknowledged nodes specified in the configuration file and sending
        // a ping+metadata request to their servers.
        for node in &self.mv_settings.mv_nodes {
            debug!("Attempting to connect to {}", node);
            // Using a pull-back mechanism for repeatedly invoking the callback until failing for
            // the 20th time. This function is blocking.
            with_pullback::<20, 2, String>(
                || {
                    // Connecting to the remote node and checking whether there are no issues with the
                    // incoming stream.
                    if let Ok(mut stream) = std::net::TcpStream::connect(node) {
                        let size = packet_size(&stream).unwrap();
                        let mut buffer: Vec<u8> = vec![0; size];
                        match stream.read(&mut buffer) {
                            Ok(n) => {
                                if n != size {
                                    unimplemented!("The reader might have read less or 0, which means that the header is corrupted.");
                                } else {
                                    unimplemented!("Checksums matched and processing the request further is safe.");
                                }
                            }

                            Err(e) => Err(format!(
                                "Failed to read sync response from `{:?}`: `{}`",
                                &node, e
                            )),
                        }
                    } else {
                        Err(format!("Could not to connect to `{:?}`", &node))
                    }
                },
                || {
                    // After maximum number of tries is reached, we are appending the node
                    // to the vector of failing nodes, and are notifying the user about
                    // this issue.
                    error!("Failed to establish connection with node: `{:?}`", &node);
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
            continue;
        }

        break;
    }

    fallback();
}

/// Gets the size of the packet from the header, defined in the beginning
/// of the request, as defined by the protocol.
fn packet_size(mut stream: &std::net::TcpStream) -> std::io::Result<usize> {
    use std::io;

    // The usize string will include 32 digits of numbers at its maximum. We parse it as a
    // usize for maximum flexibility.
    let mut header = [0u8; 32];
    stream.read_exact(&mut header)?;
    let header = String::from_utf8_lossy(&header);
    let size = header
        .trim_end_matches('\0')
        .parse::<usize>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid header format"))?;

    Ok(size)
}
