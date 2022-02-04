mod method_limit;
mod stream;

pub use stream::{Pageable, TimelineScrollback};

use super::method::Method;
use egg_mode::{error::Error, service::rate_limit_status, Token};
use futures::{stream::LocalBoxStream, StreamExt, TryStreamExt};

pub struct RateLimitTracker {
    limits: method_limit::MethodLimitStore,
}

impl RateLimitTracker {
    pub async fn new(token: Token) -> Result<Self, Error> {
        let status = rate_limit_status(&token).await?.response;
        let limits = method_limit::MethodLimitStore::from(status);

        Ok(Self { limits })
    }

    pub fn make_stream<'a, L: Pageable<'a> + 'a>(
        &self,
        loader: L,
        method: &Method,
    ) -> LocalBoxStream<'a, Result<L::Item, Error>> {
        let limit = self.limits.get(method);

        futures::stream::try_unfold((loader, false), move |(mut this, is_done)| {
            let limit = limit.clone();
            async move {
                if is_done {
                    let res: Result<Option<_>, Error> = Ok(None);
                    res
                } else {
                    if let Some(delay) = limit.wait_duration() {
                        log::warn!(
                            "Waiting for {:?} for rate limit reset at {:?}",
                            delay,
                            limit.reset_time()
                        );
                        tokio::time::sleep(delay).await;
                    }

                    limit.decrement();
                    let mut response = this.load().await?;
                    let is_done = this.update(&mut response);

                    limit.update(
                        response.rate_limit_status.remaining,
                        response.rate_limit_status.reset,
                    );

                    Ok(Some((L::extract(response.response), (this, is_done))))
                }
            }
        })
        .map_ok(|items| futures::stream::iter(items).map(Ok))
        .try_flatten()
        .boxed_local()
    }
}
