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
use futures::{
    sink::SinkExt, stream::LocalBoxStream, FutureExt, StreamExt, TryFutureExt, TryStreamExt,
};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::path::Path;
use std::time::Duration;

pub type EggModeResult<T> = Result<T, EggModeError>;

const NO_USER_MATCHES_ERROR_CODE: i32 = 17;

const TWEET_LOOKUP_PAGE_SIZE: usize = 100;
const TWEET_LOOKUP_PARALLELISM: usize = 100;
const USER_FOLLOWER_IDS_PAGE_SIZE: i32 = 5000;
const USER_FOLLOWED_IDS_PAGE_SIZE: i32 = 5000;
const USER_LOOKUP_PAGE_SIZE: usize = 100;
const USER_TIMELINE_PAGE_SIZE: i32 = 200;
const MISSING_USER_ID_BUFFER_SIZE: usize = 1024 * 1024;

const USER_LOOKUP_URL: &str = "https://api.twitter.com/1.1/users/lookup.json";
const USER_SHOW_URL: &str = "https://api.twitter.com/1.1/users/show.json";

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
            .wrap_stream(futures::stream::iter(lookups), Method::USER_LOOKUP, true)
            .map_ok(|values| futures::stream::iter(values.into_iter().map(Ok)))
            .try_flatten()
            .boxed_local()
    }

    /// Stream user JSON objects (or their IDs if they are unavailable).
    pub fn lookup_users_json_or_missing<
        T: Into<UserID> + Unpin + Send,
        I: IntoIterator<Item = T>,
    >(
        &self,
        accounts: I,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<Result<serde_json::Value, UserID>>> {
        let token = self.choose_token(token_type);
        let mut lookups = vec![];

        let user_ids = accounts
            .into_iter()
            .map(Into::into)
            .collect::<Vec<UserID>>();
        let chunks = user_ids.chunks(USER_LOOKUP_PAGE_SIZE);

        for chunk in chunks {
            lookups.push(user_lookup_json_flat(chunk.to_vec(), token).boxed_local());
        }

        self.choose_limit_tracker(token_type)
            .wrap_stream(futures::stream::iter(lookups), Method::USER_LOOKUP, true)
            .map_ok(|values| futures::stream::iter(values.into_iter().map(Ok)))
            .try_flatten()
            .boxed_local()
    }

    /// Stream user JSON objects.
    pub fn lookup_users_json<T: Into<UserID> + Unpin + Send, I: IntoIterator<Item = T>>(
        &self,
        accounts: I,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<serde_json::Value>> {
        self.lookup_users_json_or_missing(accounts, token_type)
            .try_filter_map(|result| futures::future::ok(result.ok()))
            .boxed_local()
    }

    /// Stream user JSON objects (or their statuses if they are unavailable).
    pub fn lookup_users_json_or_status<
        T: Into<UserID> + Unpin + Send,
        I: IntoIterator<Item = T>,
    >(
        &self,
        accounts: I,
        token_type: TokenType,
    ) -> LocalBoxStream<EggModeResult<Result<serde_json::Value, (UserID, FormerUserStatus)>>> {
        let (sender, receiver) = futures::channel::mpsc::channel(MISSING_USER_ID_BUFFER_SIZE);
        let mut closing_sender = sender.clone();

        let values = self
            .lookup_users_json_or_missing(accounts, token_type)
            .and_then(move |result| {
                let mut sender = sender.clone();

                async move {
                    match result {
                        Ok(value) => Ok(Some(Ok(value))),
                        Err(user_id) => {
                            // TODO: This shouldn't happen but should also be handled better.
                            sender.feed(user_id).await.unwrap();
                            Ok(None)
                        }
                    }
                }
            })
            .chain(futures::stream::once(async move {
                closing_sender.close_channel();
                Ok(None)
            }))
            .try_filter_map(futures::future::ok);

        let missing = self.choose_limit_tracker(token_type).wrap_stream(
            receiver.map(move |user_id| {
                user_show_json(user_id.clone(), self.choose_token(token_type))
                    .map_ok(move |response| {
                        Response::map(response, |result| {
                            result.map_err(|status| (user_id, status))
                        })
                    })
                    .boxed_local()
            }),
            Method::USER_SHOW,
            true,
        );

        futures::stream::select(values, missing).boxed_local()
    }

    /// Stream users (or their statuses if they are unavailable).
    ///
    /// Note that `lookup_users_json_or_status` will generally be more efficient.
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
                        Ok(response) => Ok(Response::map(response, Ok)),
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

                                    Ok(Response::new(limit, Err((id, user_status))))
                                }
                                other => Err(other),
                            }
                        }
                    })
                    .boxed_local(),
            );
        }

        self.choose_limit_tracker(token_type)
            .wrap_stream(futures::stream::iter(lookups), Method::USER_SHOW, true)
            .boxed_local()
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

/// Essentially `egg_mode::user::show` but returning raw JSON or status of missing user.
async fn user_show_json<T: Into<UserID>>(
    account: T,
    token: &Token,
) -> EggModeResult<Response<Result<serde_json::Value, FormerUserStatus>>> {
    let params = egg_mode::raw::ParamList::new()
        .extended_tweets()
        .add_user_param(account.into());

    let request = egg_mode::raw::request_get(USER_SHOW_URL, token, Some(&params));

    match egg_mode::raw::response_json::<serde_json::Value>(request).await {
        Ok(response) => Ok(Response::map(response, Ok)),
        Err(error) => {
            match error {
                EggModeError::TwitterError(ref headers, TwitterErrors { ref errors })
                    if errors.len() == 1 =>
                {
                    let limit = extract_rate_limit(headers);

                    // If the error code isn't 50 or 63 we just pass along the
                    // error.
                    let user_status = errors[0].code.try_into().map_err(|_| error)?;

                    Ok(Response::new(limit, Err(user_status)))
                }
                other => Err(other),
            }
        }
    }
}

/// Essentially `egg_mode::user::lookup` but returning raw JSON and IDs of missing users.
async fn user_lookup_json<T: Into<UserID>, I: IntoIterator<Item = T>>(
    accounts: I,
    token: &Token,
) -> EggModeResult<Response<(Vec<serde_json::Value>, Vec<UserID>)>> {
    let mut ids = HashSet::new();
    let mut screen_names = HashSet::new();

    let mut id_param = String::new();
    let mut screen_name_param = String::new();

    for account in accounts {
        match account.into() {
            UserID::ID(id) => {
                ids.insert(id);
                id_param.push_str(&id.to_string());
                id_param.push(',');
            }
            UserID::ScreenName(screen_name) => {
                screen_names.insert(screen_name.to_lowercase());
                screen_name_param.push_str(&screen_name);
                screen_name_param.push(',');
            }
        }
    }

    // Remove the trailing commas if needed
    id_param.pop();
    screen_name_param.pop();

    let params = egg_mode::raw::ParamList::new()
        .extended_tweets()
        .add_param("user_id", id_param)
        .add_param("screen_name", screen_name_param);

    let request = egg_mode::raw::request_post(USER_LOOKUP_URL, token, Some(&params));
    let response =
        recover_user_lookup(egg_mode::raw::response_json::<Vec<serde_json::Value>>(request).await)?;

    for value in &response.response {
        let user_id = value
            .get("id_str")
            .and_then(|id_str_value| id_str_value.as_str())
            .and_then(|id_str| id_str.parse::<u64>().ok())
            .ok_or_else(|| {
                EggModeError::InvalidResponse("Missing id_str", Some(value.to_string()))
            })?;

        // If we didn't find the user ID, we need to try the screen name
        if !ids.remove(&user_id) {
            let screen_name = value
                .get("screen_name")
                .and_then(|screen_name_value| screen_name_value.as_str())
                .ok_or_else(|| {
                    EggModeError::InvalidResponse("Missing screen name", Some(value.to_string()))
                })?;

            screen_names.remove(&screen_name.to_lowercase());
        }
    }

    let mut missing = Vec::with_capacity(ids.len() + screen_names.len());

    for id in ids {
        missing.push(UserID::ID(id));
    }

    for screen_name in screen_names {
        missing.push(UserID::ScreenName(screen_name.into()));
    }

    Ok(Response::map(response, |values| (values, missing)))
}

/// Essentially `egg_mode::user::lookup` but returning intermixed raw JSON and IDs of missing users.
async fn user_lookup_json_flat<T: Into<UserID>, I: IntoIterator<Item = T>>(
    accounts: I,
    token: &Token,
) -> EggModeResult<Response<Vec<Result<serde_json::Value, UserID>>>> {
    let response = user_lookup_json(accounts, token).await?;

    Ok(Response::map(response, |(values, missing)| {
        let mut results: Vec<Result<serde_json::Value, UserID>> =
            Vec::with_capacity(values.len() + missing.len());

        for value in values {
            results.push(Ok(value));
        }

        for id in missing {
            results.push(Err(id));
        }

        results
    }))
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
            if errors.len() == 1 && errors[0].code == NO_USER_MATCHES_ERROR_CODE {
                let limit = extract_rate_limit(headers);

                Ok(Response::new(limit, vec![]))
            } else {
                Err(error)
            }
        }
        other => Err(other),
    })
}
