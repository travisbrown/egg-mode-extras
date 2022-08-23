use displaydoc::Display;
use egg_mode::error::{Error as EggModeError, TwitterErrors};
use hyper::StatusCode;
use std::path::Path;

#[derive(thiserror::Error, Debug, Display)]
pub enum Error {
    /// failed to parse config file contents: {0}
    ConfigParse(#[from] toml::de::Error),
    /// failed to read config file from {1}: {0}
    ConfigRead(#[source] std::io::Error, Box<Path>),
    /// a Twitter API call failed: {0}
    EggMode(#[from] EggModeError),
    /// unexpected Twitter API error code: {0}
    UnexpectedTwitterErrorCode(i32),
    /// an error occurred in the HTTP client: {0}
    HttpClient(#[from] reqwest::Error),
    /// a failure occurred when parsing a tweet id string: {0}
    TweetIDParse(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailableReason {
    Unauthorized,
    Deactivated,
    Suspended,
    DoesNotExist,
    Other,
}

impl From<&EggModeError> for UnavailableReason {
    fn from(error: &EggModeError) -> Self {
        match error {
            EggModeError::BadStatus(StatusCode::UNAUTHORIZED) => Self::Unauthorized,
            EggModeError::TwitterError(_, TwitterErrors { ref errors }) => {
                if errors.len() == 1 {
                    match errors[0].code {
                        34 => Self::DoesNotExist,
                        50 => Self::Deactivated,
                        63 => Self::Suspended,
                        _ => Self::Other,
                    }
                } else {
                    Self::Other
                }
            }
            _ => Self::Other,
        }
    }
}
