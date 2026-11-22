use crate::drpc::{Metadata, metadata::Entry};
use base64::{Engine, engine::general_purpose::STANDARD};
use connectrpc::ConnectError;
use http::{HeaderMap, HeaderName, HeaderValue};

pub(crate) fn headers(md: &Metadata) -> Result<HeaderMap, ConnectError> {
    let mut out = HeaderMap::new();
    for entry in &md.entries {
        let name = HeaderName::from_bytes(entry.key.as_deref().unwrap_or_default().as_bytes())
            .map_err(|_| ConnectError::invalid_argument("invalid metadata key"))?;
        for bytes in &entry.values {
            let value = if name.as_str().ends_with("-bin") {
                HeaderValue::from_str(&STANDARD.encode(bytes))
            } else {
                HeaderValue::from_bytes(bytes)
            }
            .map_err(|_| ConnectError::invalid_argument("invalid metadata value"))?;
            out.append(&name, value);
        }
    }
    Ok(out)
}

pub(crate) fn metadata(headers: &HeaderMap) -> Metadata {
    let mut entries: Vec<_> = headers
        .keys()
        .map(|key| Entry {
            key: Some(key.to_string()),
            values: headers
                .get_all(key)
                .iter()
                .map(|v| {
                    if key.as_str().ends_with("-bin") {
                        STANDARD
                            .decode(v.as_bytes())
                            .unwrap_or_else(|_| v.as_bytes().to_vec())
                    } else {
                        v.as_bytes().to_vec()
                    }
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    Metadata {
        entries,
        ..Default::default()
    }
}
