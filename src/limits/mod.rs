mod method_limit;
mod stream;

use stream::ResponseFuture;
pub use stream::{Pageable, TimelineScrollback};

use super::method::Method;
use egg_mode::{
    error::{Error, TwitterErrors},
    service::rate_limit_status,
    Token,
};
use futures::{stream::LocalBoxStream, Stream, StreamExt, TryStreamExt};
use std::time::Duration;

const OVER_CAPACITY_DELAY_SECS: u64 = 60;
const OVER_CAPACITY_ERROR_CODE: i32 = 130;

pub struct RateLimitTracker {
    limits: method_limit::MethodLimitStore,
    over_capacity_delay: Duration,
}

impl RateLimitTracker {
    pub async fn new(token: Token) -> Result<Self, Error> {
        let status = rate_limit_status(&token).await?.response;
        let limits = method_limit::MethodLimitStore::from(status);
        let over_capacity_delay = Duration::from_secs(OVER_CAPACITY_DELAY_SECS);

        Ok(Self {
            limits,
            over_capacity_delay,
        })
    }

    pub fn make_stream<'a, L: Pageable<'a> + 'a>(
        &self,
        loader: L,
        method: &Method,
    ) -> LocalBoxStream<'a, Result<L::Item, Error>> {
        let limit = self.limits.get(method);
        let over_capacity_delay = self.over_capacity_delay;

        futures::stream::try_unfold(
            (loader, false, false),
            move |(mut this, is_done, is_over_capacity)| {
                let limit = limit.clone();
                async move {
                    if is_done {
                        let res: Result<Option<_>, Error> = Ok(None);
                        res
                    } else {
                        if is_over_capacity {
                            tokio::time::sleep(over_capacity_delay).await;
                        }

                        if let Some(delay) = limit.wait_duration() {
                            log::warn!(
                                "Waiting for {:?} for rate limit reset at {:?}",
                                delay,
                                limit.reset_time()
                            );
                            tokio::time::sleep(delay).await;
                        }

                        limit.decrement();
                        let mut response = match this.load().await {
                            Ok(response) => Ok(response),
                            Err(Error::TwitterError(headers, TwitterErrors { errors })) => {
                                if errors.len() == 1 && errors[0].code == OVER_CAPACITY_ERROR_CODE {
                                    return Ok(Some((None, (this, false, true))));
                                } else {
                                    Err(Error::TwitterError(headers, TwitterErrors { errors }))
                                }
                            }
                            Err(other) => Err(other),
                        }?;

                        let is_done = this.update(&mut response);

                        limit.update(
                            response.rate_limit_status.remaining,
                            response.rate_limit_status.reset,
                        );

                        Ok(Some((
                            Some(L::extract(response.response)),
                            (this, is_done, false),
                        )))
                    }
                }
            },
        )
        .try_filter_map(futures::future::ok)
        .map_ok(|items| futures::stream::iter(items).map(Ok))
        .try_flatten()
        .boxed_local()
    }

    pub fn wrap_stream<'a, T: 'a, S: Stream<Item = ResponseFuture<'a, T>> + 'a>(
        &self,
        stream: S,
        method: &Method,
        ignore_over_capacity_errors: bool,
    ) -> LocalBoxStream<'a, Result<T, Error>> {
        let limit = self.limits.get(method);
        let over_capacity_delay = self.over_capacity_delay;

        stream
            .filter_map(move |future| {
                let limit = limit.clone();
                async move {
                    if let Some(delay) = limit.wait_duration() {
                        log::warn!(
                            "Waiting for {:?} for rate limit reset at {:?}",
                            delay,
                            limit.reset_time()
                        );
                        tokio::time::sleep(delay).await;
                    }

                    limit.decrement();

                    match future.await {
                        Ok(response) => Some(Ok(response.response)),
                        Err(Error::TwitterError(headers, TwitterErrors { errors }))
                            if ignore_over_capacity_errors =>
                        {
                            if errors.len() == 1 && errors[0].code == OVER_CAPACITY_ERROR_CODE {
                                tokio::time::sleep(over_capacity_delay).await;
                                None
                            } else {
                                Some(Err(Error::TwitterError(headers, TwitterErrors { errors })))
                            }
                        }
                        Err(error) => Some(Err(error)),
                    }
                }
            })
            .boxed_local()
    }
}
