use crate::i18n::{I18nKey, Language, t};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use reqwest::Method;
use reqwest::header::{HOST, HeaderMap, HeaderName, HeaderValue};
use std::str::FromStr;
use url::Url;

#[derive(Clone, Debug)]
pub struct RequestTemplate {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
}

pub fn parse_curl(input: &str, language: Language) -> Result<RequestTemplate> {
    let parts =
        shlex::split(input).ok_or_else(|| anyhow!(t(language, I18nKey::InvalidShellString)))?;
    if parts.is_empty() {
        return Err(anyhow!(t(language, I18nKey::EmptyInput)));
    }

    let mut method = Method::GET;
    let mut headers = HeaderMap::new();
    let mut body: Option<Bytes> = None;
    let mut url: Option<String> = None;

    let mut idx = 0usize;
    while idx < parts.len() {
        let token = &parts[idx];
        match token.as_str() {
            "curl" => {}
            "-X" | "--request" => {
                idx += 1;
                let m = parts
                    .get(idx)
                    .ok_or_else(|| anyhow!(t(language, I18nKey::MissingHttpMethod)))?;
                method = Method::from_str(&m.to_uppercase())
                    .with_context(|| t(language, I18nKey::InvalidHttpMethod))?;
            }
            "-H" | "--header" => {
                idx += 1;
                let hv = parts
                    .get(idx)
                    .ok_or_else(|| anyhow!(t(language, I18nKey::MissingHeaderValue)))?;
                if let Some((k, v)) = hv.split_once(':') {
                    let key = HeaderName::from_str(k.trim())
                        .with_context(|| t(language, I18nKey::InvalidHeaderName))?;
                    let val = HeaderValue::from_str(v.trim())
                        .with_context(|| t(language, I18nKey::InvalidHeaderValue))?;
                    headers.insert(key, val);
                } else {
                    return Err(anyhow!(t(language, I18nKey::HeaderFormatInvalid)));
                }
            }
            "-d" | "--data" | "--data-raw" | "--data-binary" => {
                idx += 1;
                let b = parts
                    .get(idx)
                    .ok_or_else(|| anyhow!(t(language, I18nKey::MissingBody)))?;
                body = Some(Bytes::from(b.as_bytes().to_vec()));
                if method == Method::GET {
                    method = Method::POST;
                }
            }
            t if t.starts_with("http://") || t.starts_with("https://") => {
                url = Some(token.clone());
            }
            _ => {}
        }
        idx += 1;
    }

    let target = url.ok_or_else(|| anyhow!(t(language, I18nKey::UrlNotFound)))?;
    let parsed = Url::parse(&target).with_context(|| t(language, I18nKey::InvalidUrl))?;
    if !headers.contains_key(HOST) {
        if let Some(host) = parsed.host_str() {
            let h = if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_owned()
            };
            headers.insert(HOST, HeaderValue::from_str(&h)?);
        }
    }

    Ok(RequestTemplate {
        method,
        url: target,
        headers,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_post_request() {
        let template = parse_curl(
            "curl -X POST -H 'Content-Type: application/json' -d '{\"key\":\"value\"}' https://api.example.com/endpoint",
            Language::ZhCn,
        )
        .unwrap();

        assert_eq!(template.method, Method::POST);
        assert_eq!(template.url, "https://api.example.com/endpoint");
        assert_eq!(
            template
                .headers
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "application/json"
        );
        assert_eq!(
            template.headers.get(HOST).unwrap().to_str().unwrap(),
            "api.example.com"
        );
        assert_eq!(
            template.body.as_ref().map(|body| body.as_ref()),
            Some(br#"{"key":"value"}"# as &[u8])
        );
    }

    #[test]
    fn infers_post_when_data_is_present() {
        let template = parse_curl(
            "curl --data 'name=reqwave' https://example.com/api",
            Language::EnUs,
        )
        .unwrap();

        assert_eq!(template.method, Method::POST);
        assert_eq!(
            template.body.as_ref().map(|body| body.as_ref()),
            Some(b"name=reqwave" as &[u8])
        );
    }

    #[test]
    fn rejects_malformed_header() {
        let err = parse_curl(
            "curl -H 'broken-header' https://example.com",
            Language::EnUs,
        )
        .unwrap_err();

        assert!(err.to_string().contains("Header"));
    }
}
