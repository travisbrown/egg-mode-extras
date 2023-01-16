# egg-mode-extras

[![Rust build status](https://img.shields.io/github/actions/workflow/status/travisbrown/egg-mode-extras/ci.yaml?branch=main)](https://github.com/travisbrown/egg-mode-extras/actions)
[![Coverage status](https://img.shields.io/codecov/c/github/travisbrown/egg-mode-extras/main.svg)](https://codecov.io/github/travisbrown/egg-mode-extras)

This project includes some tools that can be useful for working with [egg-mode][egg-mode],
a Rust library for accessing the Twitter API.

In particular it includes rate-limit-aware asynchronous streams, which make it easy to request
e.g. user profiles for millions of Twitter accounts without worrying about Twitter's rate limits:

```rust
use egg_mode_extras::{client::TokenType, Client};
use futures::stream::TryStreamExt;
use std::io::BufRead;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::from_config_file("keys.toml").await?;

    let user_ids = std::io::stdin()
        .lock()
        .lines()
        .filter_map(|line| {
            line.map_or_else(
                |error| Some(Err(error)),
                |line| line.parse::<u64>().ok().map(Ok),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    client.lookup_users_json(user_ids, TokenType::App).try_for_each(|user| async move {
        println!("{}", user);

        Ok(())
    }).await?;

    Ok(())
}
```

The code is a mess and is largely untested, although most of it has been abstracted out of ✨[cancel-culture]✨,
where it has been used fairly extensively.

## License

This project is licensed under the Mozilla Public License, version 2.0. See the LICENSE file for details.

Please note that we are only using the MPL in order to support use from ✨[cancel-culture]✨, which is currently
published under the MPL. Future versions of both ✨[cancel-culture]✨ and this project are likely to be published
under the [Anti-Capitalist Software License][acsl].

[acsl]: https://anticapitalist.software/
[cancel-culture]: https://github.com/travisbrown/cancel-culture
[egg-mode]: https://docs.rs/egg-mode/latest/egg_mode/
