use crate::curl_parser::RequestTemplate;
use anyhow::{Result, anyhow};
use hdrhistogram::Histogram;
use reqwest::Client;
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinSet;

#[derive(Clone, Debug)]
pub struct LoadTestSettings {
    pub concurrency: usize,
    pub duration_secs: u64,
    pub interval_ms: u64,
    pub timeout_secs: u64,
    pub keep_alive: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeMetrics {
    pub elapsed_secs: f64,
    pub total_requests: u64,
    pub success_requests: u64,
    pub failed_requests: u64,
    pub timeout_requests: u64,
    pub qps: f64,
    pub avg_latency_ms: f64,
    pub status_code_counts: Vec<(u16, u64)>,
    pub transport_error_requests: u64,
}

#[derive(Clone, Debug, Default)]
pub struct FinalMetrics {
    pub elapsed_secs: f64,
    pub total_requests: u64,
    pub success_requests: u64,
    pub failed_requests: u64,
    pub timeout_requests: u64,
    pub qps: f64,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub max_latency_ms: f64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub enum EngineEvent {
    Progress(RuntimeMetrics),
    Completed(FinalMetrics),
    Failed(String),
}

#[derive(Default)]
struct SharedCounters {
    total: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
    timeout: AtomicU64,
    latency_total_us: AtomicU64,
    status_codes: Mutex<HashMap<u16, u64>>,
    transport_error: AtomicU64,
}

#[derive(Default)]
struct WorkerResult {
    histogram: Option<Histogram<u64>>,
    last_error: Option<String>,
}

pub async fn run_load_test(
    template: RequestTemplate,
    settings: LoadTestSettings,
    events: UnboundedSender<EngineEvent>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    if settings.concurrency == 0 {
        return Err(anyhow!("并发数必须大于 0"));
    }
    if settings.duration_secs == 0 {
        return Err(anyhow!("持续时间必须大于 0"));
    }

    let begin = Instant::now();
    let deadline = begin + Duration::from_secs(settings.duration_secs);
    let counters = Arc::new(SharedCounters::default());
    let report_stop = stop.clone();
    let report_counters = counters.clone();
    let progress_tx = events.clone();
    let ticker_begin = begin;

    let reporter = tokio::spawn(async move {
        while !report_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let elapsed = ticker_begin.elapsed().as_secs_f64().max(0.001);
            let total = report_counters.total.load(Ordering::Relaxed);
            let success = report_counters.success.load(Ordering::Relaxed);
            let failed = report_counters.failed.load(Ordering::Relaxed);
            let timeout = report_counters.timeout.load(Ordering::Relaxed);
            let transport_error = report_counters.transport_error.load(Ordering::Relaxed);
            let latency_total_us = report_counters.latency_total_us.load(Ordering::Relaxed);
            let avg_latency_ms = if total > 0 {
                latency_total_us as f64 / total as f64 / 1000.0
            } else {
                0.0
            };
            let mut status_code_counts: Vec<(u16, u64)> = report_counters
                .status_codes
                .lock()
                .ok()
                .map(|map| map.iter().map(|(code, count)| (*code, *count)).collect())
                .unwrap_or_default();
            status_code_counts.sort_by_key(|(code, _)| *code);
            let _ = progress_tx.send(EngineEvent::Progress(RuntimeMetrics {
                elapsed_secs: elapsed,
                total_requests: total,
                success_requests: success,
                failed_requests: failed,
                timeout_requests: timeout,
                qps: total as f64 / elapsed,
                avg_latency_ms,
                status_code_counts,
                transport_error_requests: transport_error,
            }));
        }
    });

    let mut join_set = JoinSet::new();
    let interval = Duration::from_millis(settings.interval_ms);
    let timeout = Duration::from_secs(settings.timeout_secs);
    let mut builder = Client::builder()
        .pool_max_idle_per_host(settings.concurrency.saturating_mul(2))
        .tcp_nodelay(true)
        .timeout(timeout);

    if settings.keep_alive {
        builder = builder.pool_idle_timeout(Duration::from_secs(30));
    } else {
        builder = builder.pool_idle_timeout(None);
    }

    let client = Arc::new(builder.build()?);

    for _ in 0..settings.concurrency {
        let c = client.clone();
        let counters = counters.clone();
        let t = template.clone();
        let stop_flag = stop.clone();
        join_set.spawn(async move { worker_loop(c, t, deadline, interval, stop_flag, counters).await });
    }

    let mut merged = Histogram::<u64>::new(3)?;
    let mut last_error = None;
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(worker)) => {
                if let Some(h) = worker.histogram {
                    let _ = merged.add(&h);
                }
                if worker.last_error.is_some() {
                    last_error = worker.last_error;
                }
            }
            Ok(Err(e)) => {
                last_error = Some(e.to_string());
            }
            Err(e) => {
                last_error = Some(e.to_string());
            }
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = reporter.await;

    let elapsed = begin.elapsed().as_secs_f64().max(0.001);
    let total = counters.total.load(Ordering::Relaxed);
    let success = counters.success.load(Ordering::Relaxed);
    let failed = counters.failed.load(Ordering::Relaxed);
    let timeout_count = counters.timeout.load(Ordering::Relaxed);

    let final_metrics = if merged.len() > 0 {
        FinalMetrics {
            elapsed_secs: elapsed,
            total_requests: total,
            success_requests: success,
            failed_requests: failed,
            timeout_requests: timeout_count,
            qps: total as f64 / elapsed,
            avg_latency_ms: merged.mean() / 1000.0,
            p50_latency_ms: merged.value_at_quantile(0.50) as f64 / 1000.0,
            p95_latency_ms: merged.value_at_quantile(0.95) as f64 / 1000.0,
            p99_latency_ms: merged.value_at_quantile(0.99) as f64 / 1000.0,
            max_latency_ms: merged.max() as f64 / 1000.0,
            last_error,
        }
    } else {
        FinalMetrics {
            elapsed_secs: elapsed,
            total_requests: total,
            success_requests: success,
            failed_requests: failed,
            timeout_requests: timeout_count,
            qps: total as f64 / elapsed,
            last_error,
            ..Default::default()
        }
    };

    let _ = events.send(EngineEvent::Completed(final_metrics));
    Ok(())
}

async fn worker_loop(
    client: Arc<Client>,
    template: RequestTemplate,
    deadline: Instant,
    interval: Duration,
    stop: Arc<AtomicBool>,
    counters: Arc<SharedCounters>,
) -> Result<WorkerResult> {
    let mut hist = Histogram::<u64>::new(3)?;
    let mut last_error = None;

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let started = Instant::now();
        let req = build_request(client.as_ref(), &template);
        let resp = req.send().await;
        counters.total.fetch_add(1, Ordering::Relaxed);

        match resp {
            Ok(r) => {
                let status = r.status();
                if let Ok(mut map) = counters.status_codes.lock() {
                    *map.entry(status.as_u16()).or_insert(0) += 1;
                }
                let _ = r.bytes().await;
                let latency_us = started.elapsed().as_micros() as u64;
                let _ = hist.record(latency_us.max(1));
                counters
                    .latency_total_us
                    .fetch_add(latency_us.max(1), Ordering::Relaxed);
                if status.is_success() {
                    counters.success.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                    last_error = Some(format!("HTTP {}", status.as_u16()));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                counters.transport_error.fetch_add(1, Ordering::Relaxed);
                let latency_us = started.elapsed().as_micros() as u64;
                let _ = hist.record(latency_us.max(1));
                counters
                    .latency_total_us
                    .fetch_add(latency_us.max(1), Ordering::Relaxed);
                if msg.contains("timed out") {
                    counters.timeout.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                }
                last_error = Some(msg);
            }
        }

        if !interval.is_zero() {
            tokio::time::sleep(interval).await;
        }
    }

    Ok(WorkerResult {
        histogram: Some(hist),
        last_error,
    })
}

fn build_request(client: &Client, template: &RequestTemplate) -> reqwest::RequestBuilder {
    let mut rb = client.request(template.method.clone(), &template.url);
    rb = set_headers(rb, &template.headers);
    if let Some(body) = &template.body {
        rb = rb.body(body.clone());
    }
    rb
}

fn set_headers(rb: reqwest::RequestBuilder, headers: &HeaderMap) -> reqwest::RequestBuilder {
    let mut out = rb;
    for (k, v) in headers {
        out = out.header(k, v);
    }
    out
}
