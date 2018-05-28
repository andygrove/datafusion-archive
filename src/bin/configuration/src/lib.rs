//! Datafusion configuration system.
//!
//! This crate is responsible for the datafusuion configuration file,
//! for now, enabling configs related to Worker and Console.
//!
//! # Examples
//! ## Configuration File
//! ```toml
//! # config.toml
//!
//! etcd = "http://127.0.0.1:2379"
//!
//! [worker]
//! bind = "0.0.0.0:8080"
//! data_dir = "./path/data_dir"
//! web_root = "./src/bin/worker"
//! ```
#[macro_use]
extern crate serde_derive;
extern crate toml;

use std::error;
use std::fmt;
use std::fs::File;
use std::io::{Error, Read};
use std::str::FromStr;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug)]
pub enum ConfigError {
    Parse(toml::de::Error),
    Io(Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConfigError::Parse(ref err) => write!(f, "Parse error: {}", err),
            ConfigError::Io(ref err) => write!(f, "IO error: {}", err),
        }
    }
}

impl error::Error for ConfigError {
    fn description(&self) -> &str {
        match *self {
            ConfigError::Parse(ref err) => err.description(),
            ConfigError::Io(ref err) => err.description(),
        }
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(error: toml::de::Error) -> Self {
        ConfigError::Parse(error)
    }
}

impl From<Error> for ConfigError {
    fn from(error: Error) -> Self {
        ConfigError::Io(error)
    }
}

/// The base config struct, all configuration is wrapped inside here
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    /// etcd endpoint
    pub etcd: String,
    /// `Worker` configs
    pub worker: Worker,
}

/// Worker configuration parameters
#[derive(Serialize, Deserialize, Debug)]
pub struct Worker {
    /// IP address and port to bind to
    pub bind: String,
    /// Location of data files
    pub data_dir: PathBuf,
    /// Location of HTML files
    pub web_root: PathBuf,
}

impl FromStr for Config {
    type Err = ConfigError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let config: Config = toml::from_str(str)?;
        Ok(config)
    }
}


impl Config {
    /// Load a configuration file from a given path
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_configuration::Config;
    ///
    /// let config = Config::from_path("../../config_example.toml");
    /// ```
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let mut buffer: Vec<u8> = Vec::new();
        let mut file = File::open(path)?;

        file.read_to_end(&mut buffer)?;
        let config: Config = toml::from_slice(&buffer[..])?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parser_config_from_str() {
        let config_str = r#"
            etcd = 'http://127.0.0.1:2379'
            [worker]
            bind = "0.0.0.0:8080"
            data_dir = "./path/data_dir"
            web_root = "./src/bin/worker"
        "#;

        let config = Config::from_str(&config_str).unwrap();

        assert_eq!(config.etcd, "http://127.0.0.1:2379");
        assert_eq!(config.worker.bind, "0.0.0.0:8080");
        assert_eq!(config.worker.data_dir, PathBuf::from("./path/data_dir"));
        assert_eq!(config.worker.web_root, PathBuf::from("./src/bin/worker"));
    }

    #[test]
    fn it_parser_config_from_file() {
        let path = Path::new(file!())
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("config_example.toml");
        println!("path: {:?}", &path);

        let config = Config::from_path(path).unwrap();

        assert_eq!(config.etcd, "http://127.0.0.1:2379");
        assert_eq!(config.worker.bind, "0.0.0.0:8080");
        assert_eq!(config.worker.data_dir, PathBuf::from("./path/data_dir"));
        assert_eq!(config.worker.web_root, PathBuf::from("./src/bin/worker"));
    }
}
