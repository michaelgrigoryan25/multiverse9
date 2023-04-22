use std::process::ExitCode;

use clap::Parser;
use log::error;
use multiverse9core::prelude::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    debug: bool,

    #[command(subcommand)]
    action: Action,
}

#[derive(clap::Subcommand, Debug)]
enum Action {
    /// One-time setup and configuration generation
    Setup {
        #[arg(long)]
        redis_uri: String,
    },

    /// Start a TcpListener and bind to `127.0.0.1:<port>`
    Run {
        #[arg(short)]
        settings: String,

        #[arg(short, long)]
        threads: Option<usize>,
    },
}

impl Action {
    pub fn execute(self) {
        match self {
            Self::Setup { redis_uri } => {
                match Settings::new(redis_uri) {
                    Ok(settings) => {
                        // We are only printing the generated settings as a JSON file. It is the
                        // responsibility of the server maintainer to decide the directory where
                        // it is going to be stored.
                        println!("{}", settings.to_string());
                    }

                    Err(e) => error!("{}", e),
                }
            }

            Self::Run { settings, threads } => {
                let path = std::path::PathBuf::from(settings);
                let settings = Settings::try_from(path).expect("Could not read settings");
                Node::new(settings)
                    .start(threads)
                    .expect("Could not start the node");
            }
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(e) = logger::setup(args.debug) {
        eprintln!("Logger failed to start: {:?}", e);
        return ExitCode::FAILURE;
    }

    args.action.execute();
    ExitCode::SUCCESS
}

mod logger {
    use log::LevelFilter;
    use std::env;
    use std::str::FromStr;

    pub fn setup(debug: bool) -> Result<(), fern::InitError> {
        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!("{}: {}", record.level(), message))
            })
            .level(if debug {
                LevelFilter::Debug
            } else {
                LevelFilter::from_str(&env::var("RUST_LOG").unwrap_or_default())
                    .unwrap_or(LevelFilter::Info)
            })
            .chain(std::io::stdout())
            .apply()?;

        Ok(())
    }
}
