use displaydoc::Display;
use std::path::Path;

#[derive(thiserror::Error, Debug, Display)]
pub enum Error {
    /// failed to parse config file contents: {0}
    ConfigParse(#[from] toml::de::Error),
    /// failed to read config file from {1}: {0}
    ConfigRead(#[source] std::io::Error, Box<Path>),
    /// a Twitter API call failed: {0}
    EggMode(#[from] egg_mode::error::Error),
    /// unexpected Twitter API error code: {0}
    UnexpectedTwitterErrorCode(i32),
    /// an error occurred in the HTTP client: {0}
    HttpClient(#[from] reqwest::Error),
    /// a failure occurred when parsing a tweet id string: {0}
    TweetIDParse(String),
}
