use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, HOST};
use std::str::FromStr;
use url::Url;

#[derive(Clone, Debug)]
pub struct RequestTemplate {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
}

pub fn parse_curl(input: &str) -> Result<RequestTemplate> {
    let parts = shlex::split(input).ok_or_else(|| anyhow!("无效的 shell 字符串"))?;
    if parts.is_empty() {
        return Err(anyhow!("输入为空"));
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
                    .ok_or_else(|| anyhow!("缺少 HTTP Method"))?;
                method = Method::from_str(&m.to_uppercase()).context("无效 Method")?;
            }
            "-H" | "--header" => {
                idx += 1;
                let hv = parts.get(idx).ok_or_else(|| anyhow!("缺少 Header 值"))?;
                if let Some((k, v)) = hv.split_once(':') {
                    let key = HeaderName::from_str(k.trim()).context("Header 名非法")?;
                    let val = HeaderValue::from_str(v.trim()).context("Header 值非法")?;
                    headers.insert(key, val);
                } else {
                    return Err(anyhow!("Header 格式必须是 Key: Value"));
                }
            }
            "-d" | "--data" | "--data-raw" | "--data-binary" => {
                idx += 1;
                let b = parts.get(idx).ok_or_else(|| anyhow!("缺少 Body"))?;
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

    let target = url.ok_or_else(|| anyhow!("未找到 URL"))?;
    let parsed = Url::parse(&target).context("URL 非法")?;
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
