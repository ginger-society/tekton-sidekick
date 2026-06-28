// src/handlers/label_selector.rs
//
// Parses the `labels` query param shared by the live (k8s) and archive
// (Postgres) halves of runs-by-label.
//
// Format: comma-separated `key=value` pairs, e.g.
//   labels=ginger-gitter/branch=main,tekton.dev/pipeline=my-pipeline
//
// Label KEYS in Kubernetes commonly carry a `/`-delimited prefix
// (`ginger-gitter/branch`, `tekton.dev/pipeline`) — that's normal and
// expected, so we must NOT split on the first `/`. We only split each
// pair on its first `=`, which is safe because K8s label keys can't
// contain `=` (DNS-subdomain-prefix + name, no `=` in the grammar) while
// values technically could in odd cases — splitting on the *first* `=`
// keeps any extra `=` characters as part of the value rather than
// truncating it.
//
// This also means we deliberately do NOT use Rocket's `<label..>` path
// segment capture for this — the route takes labels as a query param
// instead, so `/` in a key never collides with path segment boundaries
// in the first place.

#[derive(Debug)]
pub struct LabelSelectorError {
    pub bad_pair: String,
}

impl std::fmt::Display for LabelSelectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid label selector segment '{}', expected key=value",
            self.bad_pair
        )
    }
}

/// Parse `k1=v1,k2=v2` into `[(k1, v1), (k2, v2)]`. Empty segments
/// (e.g. a trailing comma) are skipped rather than erroring, to be
/// forgiving of trailing-comma typos in hand-built URLs.
pub fn parse_label_selector(raw: &str) -> Result<Vec<(String, String)>, LabelSelectorError> {
    let mut pairs = Vec::new();

    for segment in raw.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }

        match segment.split_once('=') {
            Some((k, v)) if !k.is_empty() => {
                pairs.push((k.to_string(), v.to_string()));
            }
            _ => {
                return Err(LabelSelectorError {
                    bad_pair: segment.to_string(),
                })
            }
        }
    }

    Ok(pairs)
}

/// Render pairs back into a k8s-style label selector string
/// (`k1=v1,k2=v2`) for use with kube-rs `ListParams::labels(...)`.
pub fn to_k8s_selector(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_pair_with_slash_in_key() {
        let pairs = parse_label_selector("ginger-gitter/branch=main").unwrap();
        assert_eq!(
            pairs,
            vec![("ginger-gitter/branch".to_string(), "main".to_string())]
        );
    }

    #[test]
    fn parses_multiple_pairs() {
        let pairs =
            parse_label_selector("ginger-gitter/branch=main,tekton.dev/pipeline=my-pipeline")
                .unwrap();
        assert_eq!(
            pairs,
            vec![
                ("ginger-gitter/branch".to_string(), "main".to_string()),
                ("tekton.dev/pipeline".to_string(), "my-pipeline".to_string()),
            ]
        );
    }

    #[test]
    fn value_containing_equals_is_preserved() {
        // first '=' splits key/value; any further '=' stays in the value
        let pairs = parse_label_selector("k=a=b").unwrap();
        assert_eq!(pairs, vec![("k".to_string(), "a=b".to_string())]);
    }

    #[test]
    fn trailing_comma_is_forgiven() {
        let pairs = parse_label_selector("k=v,").unwrap();
        assert_eq!(pairs, vec![("k".to_string(), "v".to_string())]);
    }

    #[test]
    fn missing_equals_errors() {
        assert!(parse_label_selector("not-a-pair").is_err());
    }

    #[test]
    fn to_k8s_selector_roundtrips() {
        let pairs = vec![
            ("a/b".to_string(), "c".to_string()),
            ("d".to_string(), "e".to_string()),
        ];
        assert_eq!(to_k8s_selector(&pairs), "a/b=c,d=e");
    }
}