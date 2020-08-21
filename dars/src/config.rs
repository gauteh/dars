use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use getopts::Options;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub db: Db,
    pub data: PathBuf,
    pub address: SocketAddr,
    pub root_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Db {
    pub path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db: Db::default(),
            data: "data/".into(),
            address: "127.0.0.1:8001".parse().unwrap(),
            root_url: None,
        }
    }
}

impl Default for Db {
    fn default() -> Self {
        Db {
            path: "./dars.db".into(),
        }
    }
}

pub fn load_config_with_args() -> anyhow::Result<Config> {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "c",
        "config",
        "configuration file (default: ./dars.toml)",
        "FILE",
    );
    opts.optopt(
        "a",
        "address",
        "listening socket address (default: 127.0.0.1:8001)",
        "ADDR",
    );
    opts.optopt(
        "",
        "root-url",
        "root URL of service (default: empty)",
        "ROOT",
    );
    opts.optflag("h", "help", "print this help");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options] [data..]", program);
        print!("{}", opts.usage(&brief));
        println!(
            r#"
The directories specified with DATA is searched for supported datasets.
If DATA is specified with a trailing "/" (e.g. "data/"), the folder
name is not included at the end-point for the dataset. All datasets are
available under the /data root. A list of datasets may be queried at /data.

If no DATA is specified, "data/" is used."#
        );
        return Err(anyhow!("argument help"));
    }

    let mut config = if let Some(f) = matches.opt_get::<PathBuf>("config")? {
        info!("reading configuration from: {:?}", f);
        let config = std::fs::read_to_string(f)?;
        toml::from_str(&config)?
    } else {
        if std::fs::metadata("./dars.toml").is_ok() {
            info!("reading configuration from default: ./dars.toml");
            let config = std::fs::read_to_string("./dars.toml")?;
            toml::from_str(&config)?
        } else {
            debug!("using default configuration");
            Config::default()
        }
    };

    // Override configuration options with arguments
    if !matches.free.is_empty() {
        config.data = matches.free[0].clone().into();
    };

    matches
        .opt_get("a")?
        .iter()
        .for_each(|a| config.address = *a);
    matches
        .opt_get::<String>("root-url")?
        .iter()
        .for_each(|r| config.root_url = Some((*r).clone()));

    Ok(config)
}
