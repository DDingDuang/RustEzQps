use crate::curl_parser::RequestTemplate;
use crate::i18n::{I18nKey, Language, t};
use anyhow::{Result, anyhow};
use hdrhistogram::{
    Histogram,
    sync::{Recorder, SyncHistogram},
};
use reqwest::Client;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub max_latency_ms: f64,
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
    pub status_code_counts: Vec<(u16, u64)>,
}

#[derive(Clone, Debug)]
pub enum EngineEvent {
    Progress(RuntimeMetrics),
    Completed(FinalMetrics),
    Failed(String),
}

struct SharedCounters {
    total: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
    timeout: AtomicU64,
    status_codes: [AtomicU64; 600],
    transport_error: AtomicU64,
}

impl Default for SharedCounters {
    fn default() -> Self {
        Self {
            total: AtomicU64::new(0),
            success: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            timeout: AtomicU64::new(0),
            status_codes: std::array::from_fn(|_| AtomicU64::new(0)),
            transport_error: AtomicU64::new(0),
        }
    }
}

pub async fn run_load_test(
    template: RequestTemplate,
    settings: LoadTestSettings,
    language: Language,
    events: UnboundedSender<EngineEvent>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    if settings.concurrency == 0 {
        return Err(anyhow!(t(language, I18nKey::ConcurrencyMustGtZero)));
    }
    if settings.duration_secs == 0 {
        return Err(anyhow!(t(language, I18nKey::DurationMustGtZero)));
    }

    let begin = Instant::now();
    let deadline = begin + Duration::from_secs(settings.duration_secs);
    let counters = Arc::new(SharedCounters::default());
    let latency_hist = Arc::new(Mutex::new(SyncHistogram::from(Histogram::<u64>::new(3)?)));
    let report_stop = stop.clone();
    let report_counters = counters.clone();
    let report_hist = latency_hist.clone();
    let progress_tx = events.clone();
    let ticker_begin = begin;

    let reporter = tokio::spawn(async move {
        while !report_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let metrics = {
                let mut hist = report_hist
                    .lock()
                    .expect("latency histogram mutex poisoned");
                hist.refresh_timeout(Duration::from_millis(25));
                build_runtime_metrics(
                    &report_counters,
                    &hist,
                    ticker_begin.elapsed().as_secs_f64(),
                )
            };
            let _ = progress_tx.send(EngineEvent::Progress(metrics));
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
        builder = builder
            .pool_idle_timeout(Some(Duration::ZERO))
            .pool_max_idle_per_host(0);
    }

    let client = Arc::new(builder.build()?);

    for _ in 0..settings.concurrency {
        let c = client.clone();
        let counters = counters.clone();
        let t = template.clone();
        let stop_flag = stop.clone();
        let recorder = {
            let hist = latency_hist
                .lock()
                .expect("latency histogram mutex poisoned");
            hist.recorder()
        };
        join_set.spawn(async move {
            worker_loop(c, t, deadline, interval, stop_flag, counters, recorder).await
        });
    }

    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(_e)) => {}
            Err(_e) => {}
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = reporter.await;
    let elapsed = begin.elapsed().as_secs_f64().max(0.001);
    let (runtime_metrics, final_metrics) = {
        let mut hist = latency_hist
            .lock()
            .expect("latency histogram mutex poisoned");
        hist.refresh();
        let runtime_metrics = build_runtime_metrics(&counters, &hist, elapsed);
        let final_metrics = build_final_metrics(&counters, &hist, elapsed);
        (runtime_metrics, final_metrics)
    };

    let _ = events.send(EngineEvent::Progress(runtime_metrics));

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
    mut latency_recorder: Recorder<u64>,
) -> Result<()> {
    let base_request = build_request(client.as_ref(), &template);

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let started = Instant::now();
        let req = base_request
            .try_clone()
            .unwrap_or_else(|| build_request(client.as_ref(), &template));
        let resp = req.send().await;
        counters.total.fetch_add(1, Ordering::Relaxed);

        match resp {
            Ok(r) => {
                let status = r.status();
                if let Some(code_slot) = status_code_slot(status.as_u16()) {
                    counters.status_codes[code_slot].fetch_add(1, Ordering::Relaxed);
                }
                let body_ok = r.bytes().await.is_ok();
                let latency_us = started.elapsed().as_micros() as u64;
                latency_recorder.saturating_record(latency_us.max(1));
                if !body_ok {
                    counters.transport_error.fetch_add(1, Ordering::Relaxed);
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                } else if status.is_success() {
                    counters.success.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                counters.transport_error.fetch_add(1, Ordering::Relaxed);
                let latency_us = started.elapsed().as_micros() as u64;
                latency_recorder.saturating_record(latency_us.max(1));
                if e.is_timeout() {
                    counters.timeout.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if !interval.is_zero() {
            tokio::time::sleep(interval).await;
        }
    }

    Ok(())
}

fn build_runtime_metrics(
    counters: &SharedCounters,
    latency_hist: &Histogram<u64>,
    elapsed_secs: f64,
) -> RuntimeMetrics {
    let elapsed = elapsed_secs.max(0.001);
    let total = counters.total.load(Ordering::Relaxed);
    let success = counters.success.load(Ordering::Relaxed);
    let failed = counters.failed.load(Ordering::Relaxed);
    let timeout = counters.timeout.load(Ordering::Relaxed);
    let transport_error = counters.transport_error.load(Ordering::Relaxed);
    let (avg_latency_ms, p50_latency_ms, p95_latency_ms, p99_latency_ms, max_latency_ms) =
        summarize_latency(latency_hist);
    let status_code_counts: Vec<(u16, u64)> = counters
        .status_codes
        .iter()
        .enumerate()
        .filter_map(|(code, count)| {
            let value = count.load(Ordering::Relaxed);
            if value > 0 {
                Some((code as u16, value))
            } else {
                None
            }
        })
        .collect();
    RuntimeMetrics {
        elapsed_secs: elapsed,
        total_requests: total,
        success_requests: success,
        failed_requests: failed,
        timeout_requests: timeout,
        qps: total as f64 / elapsed,
        avg_latency_ms,
        p50_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        max_latency_ms,
        status_code_counts,
        transport_error_requests: transport_error,
    }
}

fn build_final_metrics(
    counters: &SharedCounters,
    latency_hist: &Histogram<u64>,
    elapsed_secs: f64,
) -> FinalMetrics {
    let elapsed = elapsed_secs.max(0.001);
    let total = counters.total.load(Ordering::Relaxed);
    let success = counters.success.load(Ordering::Relaxed);
    let failed = counters.failed.load(Ordering::Relaxed);
    let timeout_count = counters.timeout.load(Ordering::Relaxed);
    let (avg_latency_ms, p50_latency_ms, p95_latency_ms, p99_latency_ms, max_latency_ms) =
        summarize_latency(latency_hist);
    let status_code_counts: Vec<(u16, u64)> = counters
        .status_codes
        .iter()
        .enumerate()
        .filter_map(|(code, count)| {
            let value = count.load(Ordering::Relaxed);
            if value > 0 {
                Some((code as u16, value))
            } else {
                None
            }
        })
        .collect();

    FinalMetrics {
        elapsed_secs: elapsed,
        total_requests: total,
        success_requests: success,
        failed_requests: failed,
        timeout_requests: timeout_count,
        qps: total as f64 / elapsed,
        avg_latency_ms,
        p50_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        max_latency_ms,
        status_code_counts,
    }
}

fn summarize_latency(latency_hist: &Histogram<u64>) -> (f64, f64, f64, f64, f64) {
    if latency_hist.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0);
    }

    (
        latency_hist.mean() / 1000.0,
        latency_hist.value_at_quantile(0.50) as f64 / 1000.0,
        latency_hist.value_at_quantile(0.95) as f64 / 1000.0,
        latency_hist.value_at_quantile(0.99) as f64 / 1000.0,
        latency_hist.max() as f64 / 1000.0,
    )
}

fn status_code_slot(code: u16) -> Option<usize> {
    if code <= 599 {
        Some(code as usize)
    } else {
        None
    }
}

fn build_request(client: &Client, template: &RequestTemplate) -> reqwest::RequestBuilder {
    let mut rb = client
        .request(template.method.clone(), &template.url)
        .headers(template.headers.clone());
    if let Some(body) = &template.body {
        rb = rb.body(body.clone());
    }
    rb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_metrics_include_live_latency_summary() {
        let counters = SharedCounters::default();
        counters.total.fetch_add(3, Ordering::Relaxed);
        counters.success.fetch_add(2, Ordering::Relaxed);
        counters.failed.fetch_add(1, Ordering::Relaxed);
        counters.status_codes[200].fetch_add(2, Ordering::Relaxed);
        counters.status_codes[500].fetch_add(1, Ordering::Relaxed);

        let mut hist = Histogram::<u64>::new(3).unwrap();
        hist.record(1_000).unwrap();
        hist.record(2_000).unwrap();
        hist.record(4_000).unwrap();

        let metrics = build_runtime_metrics(&counters, &hist, 2.0);

        assert_eq!(metrics.total_requests, 3);
        assert_eq!(metrics.success_requests, 2);
        assert_eq!(metrics.failed_requests, 1);
        assert_eq!(metrics.qps, 1.5);
        assert_eq!(metrics.status_code_counts, vec![(200, 2), (500, 1)]);
        assert!(metrics.avg_latency_ms > 0.0);
        assert!(metrics.p95_latency_ms >= metrics.p50_latency_ms);
        assert!((metrics.max_latency_ms - 4.0).abs() < 0.2);
    }

    #[test]
    fn final_metrics_include_request_summary() {
        let counters = SharedCounters::default();
        counters.total.fetch_add(4, Ordering::Relaxed);
        counters.success.fetch_add(2, Ordering::Relaxed);
        counters.failed.fetch_add(1, Ordering::Relaxed);
        counters.timeout.fetch_add(1, Ordering::Relaxed);
        counters.status_codes[200].fetch_add(2, Ordering::Relaxed);
        counters.status_codes[503].fetch_add(1, Ordering::Relaxed);
        counters.transport_error.fetch_add(1, Ordering::Relaxed);

        let mut hist = Histogram::<u64>::new(3).unwrap();
        hist.record(2_000).unwrap();
        hist.record(3_000).unwrap();

        let metrics = build_final_metrics(&counters, &hist, 2.0);

        assert_eq!(metrics.total_requests, 4);
        assert_eq!(metrics.success_requests, 2);
        assert_eq!(metrics.failed_requests, 1);
        assert_eq!(metrics.timeout_requests, 1);
        assert_eq!(metrics.qps, 2.0);
        assert_eq!(metrics.status_code_counts, vec![(200, 2), (503, 1)]);
        assert!(metrics.avg_latency_ms > 0.0);
    }
}
