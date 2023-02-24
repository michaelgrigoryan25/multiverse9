use std::process::ExitCode;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Enable verbose logging (ignores `RUST_LOG`)
    #[arg(short, long)]
    debug: bool,

    #[command(subcommand)]
    action: Action,
}

#[derive(clap::Subcommand, Debug)]
enum Action {
    /// One-time setup and configuration generation
    Setup {
        #[arg(short, long)]
        path: Option<String>,
    },

    /// Start a TcpListener and bind to `127.0.0.1:<port>`
    Run {
        #[arg(short, long)]
        settings: String,
    },
}

impl Action {
    pub fn execute(&self) {
        match self {
            Self::Setup { path } => {
                let settings = multiverse9core::Settings::new(path.to_owned()).unwrap();
                settings.generate().unwrap();
            }

            Self::Run { settings } => {
                let settings = std::path::PathBuf::from(format!("{}/settings.json", settings));
                let settings =
                    multiverse9core::Settings::try_from(settings).expect("Could not read settings");
                let node = multiverse9core::Node::new(settings);
                std::sync::Arc::new(node)
                    .start()
                    .expect("Could not start the node");
            }
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(e) = logger::setup(args.debug) {
        eprintln!("error: logger failed to start. reason: {:?}", e);
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
