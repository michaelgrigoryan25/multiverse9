use log::*;
use serde::Deserialize;
use serde::Serialize;
use std::io::prelude::*;

const DEFAULT_HOST_ADDRESS: &str = "127.0.0.1:0";
const DEFAULT_DATA_PATHNAME: &str = "multiverse9";
const DEFAULT_INSTANCE_PREFIX: &str = "multiverse9";
const DEFAULT_SETTINGS_FILENAME: &str = "settings.json";

const PROTO_CONN_REQ: &[u8] = &[0o0001];
const PROTO_SYNC_REQ: &[u8] = &[0o0010];
const PROTO_INIT_RES: &[u8] = &[0o0100];
const PROTO_AGGR_REQ: &[u8] = &[0o0002];
const PROTO_META_REQ: &[u8] = &[0o0003];

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
            // We don't have to define the version of the node manually, since it is done
            // at build-time by cargo.
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
    /// The streams will be moved and owned by the [Handler] for maximum isolation.
    stream: std::net::TcpStream,
}

impl Handler {
    fn new(stream: std::net::TcpStream) -> Self {
        Self { stream }
    }

    /// This is the main TCP request handler which will be spawned from [Node::start].
    ///
    /// # Blocking
    ///
    /// This function is blocking, as it should never return, unless the TCP stream is disconnected
    /// or there is an error coming from this function.
    fn tcp(mut self) -> std::io::Result<()> {
        loop {
            debug!("Awaiting a new request...");
            let buffer = read_tcp_stream(&self.stream)?;
            debug!("Received buffer: {:?}", buffer);
            match buffer.as_slice() {
                PROTO_SYNC_REQ => {
                    debug!("This is a sync request");
                }

                _ => {}
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
        debug!("Sync daemon has been spawned");

        for stream in listener.incoming() {
            let stream = stream?; // Acquiring the connection
            debug!("New stream connection from {}", stream.peer_addr()?);
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
    /// - Collecting metadata from the connected nodes. (experimental)
    /// - Sending connection requests to the remote nodes. (experimental)
    /// - Handling incoming node connections and adding them to the configuration if allowed.
    /// (experimental)
    fn sync(&self) {
        if self.mv_settings.mv_nodes.is_empty() {
            info!("Syncing skipped since there are no acknowledged remote nodes attached");
            if self.mv_settings.mv_open_interactions {
                info!("Syncing will re-initiate once any remote connection is established with the host");
            }

            return;
        }

        info!(
            "Started syncing local node with {n} remote nodes...",
            n = self.mv_settings.mv_nodes.len()
        );

        let mut failing = Vec::with_capacity(self.mv_settings.mv_nodes.len());
        // Iterating through all acknowledged nodes specified in the configuration file and sending
        // a CONN request to their nodes.
        for node in &self.mv_settings.mv_nodes {
            debug!("Attempting to connect to {}", node);
            // Using a pull-back mechanism for repeatedly invoking the callback until failing for
            // the 20th time. This function is blocking.
            with_pullback::<20, 2, String>(
                || {
                    // Connecting to the remote node and checking whether there are no issues with the
                    // incoming stream.
                    if let Ok(mut stream) = std::net::TcpStream::connect(node) {
                        write_tcp_stream(&stream, PROTO_SYNC_REQ).unwrap();
                        let reply = read_tcp_stream(&stream).unwrap();
                        debug!("Sync reply: {:?}", reply);
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
        debug!("Sleeping for {:?}...", duration);
        std::thread::sleep(duration);

        if let Err(e) = callback() {
            error!("{:?}", e);
            iterations += 1;
            continue;
        }

        debug!("Callback execution finished successfully. Stoping the pull-back");
        break;
    }

    debug!("Pull-back failed after maximum number of tries. Executing the fallback function");
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
    debug!("The packet size is {}", size);

    Ok(size)
}

fn write_tcp_stream(mut stream: &std::net::TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let size_be = payload.len().to_be_bytes();
    // This part is a little tricky. We first want to allocate space for storing the header of
    // the TCP request. Since the header is going to be an encoded big-endian slice, we will need
    // to add it into account. Now, we are at the following part of the request:
    //
    // -------------------------------------------------------------
    // 0x123456\0payload information
    // |----|-------------------------------------------------------
    // \____/
    //  |-> [ 0x12, 0x34, 0x56 ]
    //
    // Basically, we now have space for storing the actual number which keeps track of the length.
    // Now, we need to add one more slot for keeping the NULL terminator, which is represented as
    // `[0]` in memory:
    //
    // -------------------------------------------------------------
    // 123456\0payload information
    // -----|-|-----------------------------------------------------
    //      \_/
    //       |-> [0] We are using ascii representation for a terminator here, which is is OK, since
    //               the number is represented in big-endian and has a length of 8 bytes at most.
    //               This is documented at https://doc.rust-lang.org/std/primitive.usize.html#method.to_be_bytes
    //
    //
    // And finally, we are allocating space for the actual payload, by getting its
    // length:
    //
    // -------------------------------------------------------------
    // 123456\0payload information
    // -------|------------------|----------------------------------
    //        \__________________/
    //                  |-> [...]
    //
    let mut buffer = Vec::with_capacity(size_be.len() + 1 + payload.len());
    buffer.extend(size_be);
    buffer.extend(&[0]);
    buffer.extend(payload);
    debug!(
        "Planned a new TCP request. Length is {}: `{:?}`",
        buffer.len(),
        buffer
    );

    stream.write_all(&buffer)?;
    debug!("TCP request executed successfully");
    Ok(())
}

/// This function helps us to avoid writing boilerplate code when executing TCP read requests.
/// Under the hood, this function consumes the [packet_size] function.
fn read_tcp_stream(mut stream: &std::net::TcpStream) -> std::io::Result<Vec<u8>> {
    let size = packet_size(stream)?;
    let mut buffer: Vec<u8> = vec![0; size];
    stream.read(&mut buffer)?;
    Ok(buffer)
}
