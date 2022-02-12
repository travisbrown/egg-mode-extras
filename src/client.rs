use super::config::Config;
use super::error::Error;
use super::limits::{RateLimitTracker, TimelineScrollback};
use super::method::Method;

use egg_mode::{
    error::{Error as EggModeError, TwitterErrors},
    tweet::{Timeline, Tweet},
    user::{TwitterUser, UserID},
    KeyPair, RateLimit, Response, Token,
};
use futures::{stream::LocalBoxStream, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::convert::TryFrom;
use std::path::Path;
use std::time::Duration;

pub type EggModeResult<T> = Result<T, EggModeError>;

const TWEET_LOOKUP_PAGE_SIZE: usize = 100;
const TWEET_LOOKUP_PARALLELISM: usize = 100;
const USER_FOLLOWER_IDS_PAGE_SIZE: i32 = 5000;
const USER_FOLLOWED_IDS_PAGE_SIZE: i32 = 5000;
const USER_LOOKUP_PAGE_SIZE: usize = 100;
const USER_TIMELINE_PAGE_SIZE: i32 = 200;

const USER_LOOKUP_URL: &str = "https://api.twitter.com/1.1/users/lookup.json";

/// Represents the type of token.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TokenType {
    User,
    App,
}

/// Represents the status of a suspended or deactivated user account.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FormerUserStatus {
    Deactivated,
    Suspended,
}

impl FormerUserStatus {
    pub fn code(&self) -> u8 {
        match self {
            Self::Deactivated => 50,
            Self::Suspended => 63,
        }
    }
}

impl TryFrom<i32> for FormerUserStatus {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            50 => Ok(Self::Deactivated),
            63 => Ok(Self::Suspended),
            other => Err(Error::UnexpectedTwitterErrorCode(other)),
        }
    }
}

/// Twitter API client that supports rate-limit-aware asynchronous streams.
pub struct Client {
    user: TwitterUser,
    user_token: Token,
    app_token: Token,
    user_limit_tracker: RateLimitTracker,
    app_limit_tracker: RateLimitTracker,
}

impl Client {
    async fn new(
        user: TwitterUser,
        user_token: Token,
        app_token: Token,
    ) -> egg_mode::error::Result<Client> {
        let user_limit_tracker = RateLimitTracker::new(user_token.clone()).await?;
        let app_limit_tracker = RateLimitTracker::new(app_token.clone()).await?;

        Ok(Client {
            user,
            user_token,
            app_token,
            user_limit_tracker,
            app_limit_tracker,
        })
    }

    /// Create a client from two key pairs.
    pub async fn from_key_pairs(
        consumer: KeyPair,
        access: KeyPair,
    ) -> std::result::Result<Self, egg_mode::error::Error> {
        let app_token = egg_mode::auth::bearer_token(&consumer).await?;
        let token = Token::Access { consumer, access };
        let user = egg_mode::auth::verify_tokens(&token).await?.response;
        Self::new(user, token, app_token).await
    }

    /// Create a client from two key pairs read from a TOML config file.
    pub async fn from_config_file<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        let contents = std::fs::read_to_string(&path)
            .map_err(|e| Error::ConfigRead(e, path.into_boxed_path()))?;
        let config = toml::from_str::<Config>(&contents)?;
        let (consumer, access) = config.twitter_key_pairs();

        Ok(Self::from_key_pairs(consumer, access).await?)
    }

    /// Stream user IDs blocked by the authenticated user.
    pub fn blocked_ids(&self) -> LocalBoxStream<EggModeResult<u64>> {
        let cursor = egg_mode::user::blocks_ids(&self.user_token);

        self.user_limit_tracker
            .make_stream(cursor, Method::USER_BLOCKS_IDS)
    }

    fn choose_token(&self, token_type: TokenType) -> &Token {
        match token_type {
            TokenType::User => &self.user_token,
            TokenType::App => &self.app_token,
        }
    }

    fn choose_limit_tracker(&self, token_type: TokenType) -> &RateLimitTracker {
        match token_type {
            TokenType::User => &self.user_limit_tracker,
            TokenType::App => &self.app_limit_tracker,
        }
    }

    /// Stream user IDs of followers for the given account.
    pub fn follower_ids<T: Into<UserID>>(
        &self,
        account: T,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<u64>> {
        let token = self.choose_token(token_type);
        let cursor = egg_mode::user::followers_ids(account, token)
            .with_page_size(USER_FOLLOWER_IDS_PAGE_SIZE);

        self.choose_limit_tracker(token_type)
            .make_stream(cursor, Method::USER_FOLLOWER_IDS)
    }

    /// Stream user IDs of followers of the authenticated user.
    pub fn self_follower_ids(&self) -> LocalBoxStream<EggModeResult<u64>> {
        let cursor = egg_mode::user::followers_ids(self.user.id, &self.user_token)
            .with_page_size(USER_FOLLOWER_IDS_PAGE_SIZE);

        self.user_limit_tracker
            .make_stream(cursor, Method::USER_FOLLOWER_IDS)
    }

    /// Stream user IDs followed by the given account.
    pub fn followed_ids<T: Into<UserID>>(
        &self,
        account: T,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<u64>> {
        let token = self.choose_token(token_type);
        let cursor =
            egg_mode::user::friends_ids(account, token).with_page_size(USER_FOLLOWED_IDS_PAGE_SIZE);

        self.choose_limit_tracker(token_type)
            .make_stream(cursor, Method::USER_FOLLOWED_IDS)
    }

    /// Stream user IDs followed by the authenticated user.
    pub fn self_followed_ids(&self) -> LocalBoxStream<EggModeResult<u64>> {
        let cursor = egg_mode::user::friends_ids(self.user.id, &self.user_token)
            .with_page_size(USER_FOLLOWED_IDS_PAGE_SIZE);

        self.user_limit_tracker
            .make_stream(cursor, Method::USER_FOLLOWED_IDS)
    }

    /// Stream tweets by the given user.
    pub fn user_tweets<T: Into<UserID>>(
        &self,
        account: T,
        with_replies: bool,
        with_rts: bool,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<Tweet>> {
        let token = self.choose_token(token_type);

        self.choose_limit_tracker(token_type).make_stream(
            TimelineScrollback::new(
                egg_mode::tweet::user_timeline(account, with_replies, with_rts, token)
                    .with_page_size(USER_TIMELINE_PAGE_SIZE),
            ),
            Method::USER_TIMELINE,
        )
    }

    /// Stream the given user's timeline.
    pub fn user_timeline_stream<T: Into<UserID>>(
        &self,
        account: T,
        wait: Duration,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<Tweet>> {
        let token = self.choose_token(token_type);
        let timeline = egg_mode::tweet::user_timeline(account, true, true, token);

        make_timeline_stream(timeline, wait)
    }

    /// Look up a single user.
    pub async fn lookup_user<T: Into<UserID>>(
        &self,
        account: T,
        token_type: TokenType,
    ) -> EggModeResult<TwitterUser> {
        let token = self.choose_token(token_type);
        egg_mode::user::show(account, token)
            .map_ok(|response| response.response)
            .await
    }

    /// Stream users.
    pub fn lookup_users<T: Into<UserID> + Unpin + Send, I: IntoIterator<Item = T>>(
        &self,
        accounts: I,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<TwitterUser>> {
        let token = self.choose_token(token_type);
        let mut lookups = vec![];

        let user_ids = accounts
            .into_iter()
            .map(Into::into)
            .collect::<Vec<UserID>>();
        let chunks = user_ids.chunks(USER_LOOKUP_PAGE_SIZE);

        for chunk in chunks {
            lookups.push(
                egg_mode::user::lookup(chunk.to_vec(), token)
                    .map(recover_user_lookup)
                    .boxed_local(),
            );
        }

        self.choose_limit_tracker(token_type)
            .make_stream(lookups.into_iter().peekable(), Method::USER_LOOKUP)
    }

    /// Stream user JSON objects.
    pub fn lookup_users_json<T: Into<UserID> + Unpin + Send, I: IntoIterator<Item = T>>(
        &self,
        accounts: I,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<serde_json::Value>> {
        let token = self.choose_token(token_type);
        let mut lookups = vec![];

        let user_ids = accounts
            .into_iter()
            .map(Into::into)
            .collect::<Vec<UserID>>();
        let chunks = user_ids.chunks(USER_LOOKUP_PAGE_SIZE);

        for chunk in chunks {
            lookups.push(
                user_lookup_json(chunk.to_vec(), token)
                    .map(recover_user_lookup)
                    .boxed_local(),
            );
        }

        self.choose_limit_tracker(token_type)
            .make_stream(lookups.into_iter().peekable(), Method::USER_LOOKUP)
    }

    /// Stream either user profiles or former user statuses.
    pub fn lookup_users_or_status<T: Into<UserID> + Unpin + Send, I: IntoIterator<Item = T>>(
        &self,
        accounts: I,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<Result<TwitterUser, (UserID, FormerUserStatus)>>> {
        let token = self.choose_token(token_type);
        let mut lookups = vec![];
        let user_ids = accounts.into_iter().map(Into::into);

        for id in user_ids {
            lookups.push(
                egg_mode::user::show(id.clone(), token)
                    .map(move |result| match result {
                        Ok(response) => Ok(Response::map(response, |user| vec![Ok(user)])),
                        Err(error) => {
                            match error {
                                EggModeError::TwitterError(
                                    ref headers,
                                    TwitterErrors { ref errors },
                                ) if errors.len() == 1 => {
                                    let limit = extract_rate_limit(headers);

                                    // If the error code isn't 50 or 63 we just pass along the
                                    // error.
                                    let user_status =
                                        errors[0].code.try_into().map_err(|_| error)?;

                                    Ok(Response::new(limit, vec![Err((id, user_status))]))
                                }
                                other => Err(other),
                            }
                        }
                    })
                    .boxed_local(),
            );
        }

        self.choose_limit_tracker(token_type)
            .make_stream(lookups.into_iter().peekable(), Method::USER_SHOW)
    }

    /// Attempt to find a tweet that the tweet with the given status ID is a reply to.
    pub async fn lookup_reply_parent(
        &self,
        status_id: u64,
        token_type: TokenType,
    ) -> EggModeResult<Option<(String, u64)>> {
        let token = self.choose_token(token_type);
        let result = egg_mode::tweet::lookup(vec![status_id], token).await?;
        let tweet = result.response.get(0);

        Ok(tweet.and_then(|tweet| {
            tweet
                .in_reply_to_screen_name
                .as_ref()
                .cloned()
                .zip(tweet.in_reply_to_status_id)
        }))
    }

    /// Stream tweets for the given status IDs.
    pub fn lookup_tweets<I: IntoIterator<Item = u64>>(
        &self,
        status_ids: I,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<(u64, Option<Tweet>)>> {
        let token = self.choose_token(token_type);
        let mut ids = status_ids.into_iter().collect::<Vec<u64>>();
        ids.sort_unstable();
        ids.dedup();
        ids.reverse();

        futures::stream::unfold(ids, move |mut ids| async move {
            if ids.is_empty() {
                None
            } else if ids.len() <= TWEET_LOOKUP_PAGE_SIZE {
                Some((egg_mode::tweet::lookup_map(ids, token), vec![]))
            } else {
                let chunk = ids.split_off(ids.len() - TWEET_LOOKUP_PAGE_SIZE);
                Some((egg_mode::tweet::lookup_map(chunk, token), ids))
            }
        })
        .map(Ok)
        .try_buffer_unordered(TWEET_LOOKUP_PARALLELISM)
        .map_ok(|res| futures::stream::iter(res.response.into_iter().map(Ok)))
        .try_flatten()
        .boxed_local()
    }
}

/// Asynchronously stream a timeline.
pub fn make_timeline_stream(
    timeline: Timeline,
    wait: Duration,
) -> LocalBoxStream<'static, Result<Tweet, EggModeError>> {
    futures::stream::try_unfold(
        (timeline, None, false),
        move |(timeline, mut max_id, mut started)| async move {
            let (mut timeline, response) = if !started {
                started = true;
                timeline.start().await?
            } else {
                tokio::time::sleep(wait).await;
                timeline.newer(None).await?
            };

            if !response.is_empty() {
                max_id = timeline.max_id;
            } else {
                timeline.max_id = max_id;
            }

            let result: Result<Option<_>, EggModeError> =
                Ok(Some((response.response, (timeline, max_id, started))));
            result
        },
    )
    .map_ok(|vs| futures::stream::iter(vs).map(Ok))
    .try_flatten()
    .boxed_local()
}

/// Convenience method copied from egg-mode.
fn multiple_names_param<T: Into<UserID>, I: IntoIterator<Item = T>>(
    accounts: I,
) -> (String, String) {
    let mut ids = Vec::new();
    let mut screen_names = Vec::new();

    for account in accounts {
        match account.into() {
            UserID::ID(id) => ids.push(id.to_string()),
            UserID::ScreenName(screen_name) => screen_names.push(screen_name),
        }
    }

    (ids.join(","), screen_names.join(","))
}

/// Essentially `egg_mode::user::lookup` but returning raw JSON.
async fn user_lookup_json<T: Into<UserID>, I: IntoIterator<Item = T>>(
    accounts: I,
    token: &Token,
) -> EggModeResult<Response<Vec<serde_json::Value>>> {
    let (id_param, screen_name_param) = multiple_names_param(accounts);

    let params = egg_mode::raw::ParamList::new()
        .extended_tweets()
        .add_param("user_id", id_param)
        .add_param("screen_name", screen_name_param);

    let request = egg_mode::raw::request_post(USER_LOOKUP_URL, token, Some(&params));

    egg_mode::raw::response_json(request).await
}

/// We just use the defaults if the headers are malformed for some reason.
fn extract_rate_limit(headers: &egg_mode::raw::Headers) -> RateLimit {
    RateLimit::try_from(headers).unwrap_or(RateLimit {
        limit: -1,
        remaining: -1,
        reset: -1,
    })
}

/// Recover from "No user matches" errors.
fn recover_user_lookup<T>(
    result: EggModeResult<Response<Vec<T>>>,
) -> EggModeResult<Response<Vec<T>>> {
    result.or_else(|error| match error {
        EggModeError::TwitterError(ref headers, TwitterErrors { ref errors }) => {
            if errors.len() == 1 && errors[0].code == 17 {
                let limit = extract_rate_limit(headers);

                Ok(Response::new(limit, vec![]))
            } else {
                Err(error)
            }
        }
        other => Err(other),
    })
}
