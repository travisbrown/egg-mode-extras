const STATUS_PATTERN: &str = r"^http[s]?://twitter\.com/[^/]+/status/(\d+)(?:\?.+)?$";

/// Attempt to parse a status URL and extract the status ID.
pub fn extract_status_id(url: &str) -> Option<u64> {
    lazy_static::lazy_static! {
        static ref STATUS_RE: regex::Regex = regex::Regex::new(STATUS_PATTERN).unwrap();
    }

    STATUS_RE
        .captures(url)
        .and_then(|groups| groups.get(1).and_then(|m| m.as_str().parse::<u64>().ok()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn extract_status_id() {
        let pairs = vec![
            (
                "https://twitter.com/martinshkreli/status/446273988780904448?lang=da",
                Some(446273988780904448),
            ),
            (
                "https://twitter.com/ChiefScientist/status/1270099974559154177",
                Some(1270099974559154177),
            ),
            ("abcdef", None),
        ];

        for (url, expected) in pairs {
            assert_eq!(super::extract_status_id(url), expected);
        }
    }
}
