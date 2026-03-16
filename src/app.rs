use crate::curl_parser::{RequestTemplate, parse_curl};
use crate::i18n::{I18nKey, Language, t};
use crate::loadtest::{EngineEvent, FinalMetrics, LoadTestSettings, RuntimeMetrics, run_load_test};
use anyhow::{Result, anyhow};
use eframe::CreationContext;
use eframe::egui::{self, Color32, FontData, FontDefinitions, FontFamily, FontId, RichText, Sense, Stroke, TextEdit};
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

pub struct ApiQpsApp {
    language: Language,
    curl_input: String,
    request_draft: EditableRequest,
    convert_status: Option<ConvertStatus>,
    generic_error: Option<String>,
    settings: LoadTestSettings,
    runtime: Arc<Runtime>,
    run_state: RunState,
    latest_runtime_metrics: RuntimeMetrics,
    final_metrics: Option<FinalMetrics>,
    // response_preview: ResponsePreview, // unused
    qps_history: Vec<f64>,
    latency_history: Vec<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq)] // Added derive
struct EditableRequest {
    api_url: String,
    method: String,
    headers_json: String,
    body: String,
}

#[derive(Clone, Debug)] // Added derive
struct ConvertStatus {
    ok: bool,
    message: String,
}

/*
#[derive(Clone, Debug)] // Added derive
struct ResponsePreview {
    status: String,
    time_ms: String,
    size: String,
    body_preview: String,
}
*/

enum RunState {
    Idle,
    Running {
        stop_flag: Arc<AtomicBool>,
        events: UnboundedReceiver<EngineEvent>,
    },
}

impl ApiQpsApp {
    pub fn new(cc: &CreationContext<'_>) -> Self {
        let mut fonts = FontDefinitions::default();
        fonts.font_data.insert(
            "msyh".to_owned(),
            Arc::new(FontData::from_static(include_bytes!("msyh.ttf"))),
        );
        if let Some(family) = fonts.families.get_mut(&FontFamily::Proportional) {
            family.insert(0, "msyh".to_owned());
        }
        if let Some(family) = fonts.families.get_mut(&FontFamily::Monospace) {
            family.insert(0, "msyh".to_owned());
        }
        cc.egui_ctx.set_fonts(fonts);

        cc.egui_ctx.style_mut(|style| {
            style.visuals.panel_fill = Color32::from_rgb(245, 245, 247);
            style.visuals.window_fill = Color32::from_rgb(245, 245, 247);
            style.visuals.extreme_bg_color = Color32::from_rgb(255, 255, 255);
            style.visuals.override_text_color = Some(Color32::from_rgb(29, 29, 31));
            style.visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(255, 255, 255);
            style.visuals.widgets.inactive.bg_fill = Color32::from_rgb(255, 255, 255);
            style.visuals.widgets.hovered.bg_fill = Color32::from_rgb(244, 245, 248);
            style.visuals.widgets.active.bg_fill = Color32::from_rgb(235, 240, 252);
            style.visuals.widgets.noninteractive.bg_stroke.color = Color32::from_rgb(229, 229, 234);
            style.visuals.widgets.inactive.bg_stroke.color = Color32::from_rgb(229, 229, 234);
            style.visuals.selection.bg_fill = Color32::from_rgb(0, 122, 255);
        });

        let mut app = Self {
            language: Language::ZhCn,
            curl_input: "curl -X POST -H 'Content-Type: application/json' -d '{\"key\":\"value\"}' https://api.example.com/endpoint".to_owned(),
            request_draft: EditableRequest {
                api_url: String::new(),
                method: "GET".to_owned(),
                headers_json: "{}".to_owned(),
                body: String::new(),
            },
            convert_status: None,
            generic_error: None,
            settings: LoadTestSettings {
                concurrency: 100,
                duration_secs: 60,
                interval_ms: 10,
                timeout_secs: 5,
            },
            runtime: Arc::new(Runtime::new().expect("failed to create runtime")),
            run_state: RunState::Idle,
            latest_runtime_metrics: RuntimeMetrics::default(),
            final_metrics: None,
            /*
            response_preview: ResponsePreview {
                status: "-".to_owned(),
                time_ms: "-".to_owned(),
                size: "-".to_owned(),
                body_preview: "压测模式默认不抓取响应体".to_owned(),
            },
            */
            qps_history: Vec::new(),
            latency_history: Vec::new(),
        };
        app.auto_convert_from_curl();
        app
    }

    fn start_test(&mut self) {
        self.generic_error = None;
        let template = match self.build_template_from_draft() {
            Ok(tpl) => tpl,
            Err(e) => {
                self.generic_error = Some(format!("{}: {e}", t(self.language, I18nKey::GenericError)));
                return;
            }
        };

        let settings = self.settings.clone();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_for_task = stop_flag.clone();
        let (tx, rx) = unbounded_channel();
        self.final_metrics = None;
        self.latest_runtime_metrics = RuntimeMetrics::default();
        self.qps_history.clear();
        self.latency_history.clear();
        /*
        self.response_preview.status = "RUNNING".to_owned();
        self.response_preview.time_ms = "-".to_owned();
        self.response_preview.size = "-".to_owned();
        self.response_preview.body_preview = "压测进行中".to_owned();
        */

        let runtime = self.runtime.clone();
        runtime.spawn(async move {
            if let Err(err) = run_load_test(template, settings, tx.clone(), stop_for_task).await {
                let _ = tx.send(EngineEvent::Failed(err.to_string()));
            }
        });

        self.run_state = RunState::Running {
            stop_flag,
            events: rx,
        };
    }

    fn stop_test(&mut self) {
        if let RunState::Running { stop_flag, .. } = &self.run_state {
            stop_flag.store(true, Ordering::Relaxed);
        }
    }

    fn consume_events(&mut self) {
        let mut should_idle = false;
        if let RunState::Running { events, .. } = &mut self.run_state {
            while let Ok(ev) = events.try_recv() {
                match ev {
                    EngineEvent::Progress(m) => {
                        self.latest_runtime_metrics = m;
                        Self::push_history(&mut self.qps_history, self.latest_runtime_metrics.qps);
                        Self::push_history(
                            &mut self.latency_history,
                            self.latest_runtime_metrics.avg_latency_ms,
                        );
                        /*
                        self.response_preview.time_ms =
                            format!("{:.2} ms", self.latest_runtime_metrics.avg_latency_ms);
                        */
                    }
                    EngineEvent::Completed(m) => {
                        /*
                        self.response_preview.status = if m.failed_requests == 0 && m.timeout_requests == 0 {
                            "200".to_owned()
                        } else {
                            "MIXED".to_owned()
                        };
                        self.response_preview.time_ms = format!("{:.2} ms", m.avg_latency_ms);
                        self.response_preview.size = "-".to_owned();
                        self.response_preview.body_preview = m
                            .last_error
                            .clone()
                            .unwrap_or_else(|| "压测完成，默认不采样响应体".to_owned());
                        */
                        self.final_metrics = Some(m);
                        should_idle = true;
                    }
                    EngineEvent::Failed(e) => {
                        self.generic_error =
                            Some(format!("{}: {e}", t(self.language, I18nKey::GenericError)));
                        /*
                        self.response_preview.status = "ERROR".to_owned();
                        self.response_preview.body_preview = e;
                        */
                        should_idle = true;
                    }
                }
            }
        }
        if should_idle {
            self.run_state = RunState::Idle;
        }
    }

    fn is_running(&self) -> bool {
        matches!(self.run_state, RunState::Running { .. })
    }

    fn auto_convert_from_curl(&mut self) {
        match parse_curl(&self.curl_input) {
            Ok(template) => {
                self.request_draft = Self::draft_from_template(template);
                self.convert_status = Some(ConvertStatus {
                    ok: true,
                    message: t(self.language, I18nKey::ConvertSuccess).to_owned(),
                });
            }
            Err(e) => {
                self.convert_status = Some(ConvertStatus {
                    ok: false,
                    message: format!("{}: {e}", t(self.language, I18nKey::ConvertFailed)),
                });
            }
        }
    }

    fn draft_from_template(template: RequestTemplate) -> EditableRequest {
        let mut obj = Map::new();
        for (k, v) in &template.headers {
            if let Ok(val) = v.to_str() {
                obj.insert(k.as_str().to_owned(), Value::String(val.to_owned()));
            }
        }
        let headers_json =
            serde_json::to_string_pretty(&Value::Object(obj)).unwrap_or_else(|_| "{}".to_owned());
        let body = template
            .body
            .map(|b| Self::normalize_possible_json_body(&String::from_utf8_lossy(&b)))
            .unwrap_or_default();
        EditableRequest {
            api_url: template.url,
            method: template.method.as_str().to_owned(),
            headers_json,
            body,
        }
    }

    fn build_template_from_draft(&self) -> Result<RequestTemplate> {
        let method =
            Method::from_str(self.request_draft.method.trim()).map_err(|_| anyhow!("请求类型无效"))?;
        let url = self.request_draft.api_url.trim().to_owned();
        if url.is_empty() {
            return Err(anyhow!("API URL 不能为空"));
        }
        let _ = url::Url::parse(&url).map_err(|_| anyhow!("API URL 非法"))?;

        let headers_text = self.request_draft.headers_json.trim();
        let value: Value = if headers_text.is_empty() {
            Value::Object(Map::new())
        } else {
            serde_json::from_str(headers_text).map_err(|_| anyhow!("Header 不是合法 JSON"))?
        };
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!("Header JSON 必须是对象类型"))?;

        let mut headers = HeaderMap::new();
        for (k, v) in obj {
            let name = HeaderName::from_str(k).map_err(|_| anyhow!("Header 名非法: {k}"))?;
            let val_str = if let Some(s) = v.as_str() {
                s.to_owned()
            } else {
                v.to_string()
            };
            let header_val =
                HeaderValue::from_str(&val_str).map_err(|_| anyhow!("Header 值非法: {k}"))?;
            headers.insert(name, header_val);
        }

        let body = if Self::method_supports_body(&method) && !self.request_draft.body.trim().is_empty()
        {
            let normalized = Self::normalize_possible_json_body(&self.request_draft.body);
            Some(normalized.as_bytes().to_vec())
        } else {
            None
        };

        Ok(RequestTemplate {
            method,
            url,
            headers,
            body,
        })
    }

    fn method_supports_body(method: &Method) -> bool {
        !matches!(*method, Method::GET | Method::HEAD)
    }

    fn normalize_possible_json_body(input: &str) -> String {
        let mut text = input.trim().to_owned();
        if text.starts_with("${") {
            text.remove(0);
        }
        text = text
            .replace("\\r\\n", "\n")
            .replace("\\n", "\n")
            .replace("\\t", "\t");

        match serde_json::from_str::<Value>(&text) {
            Ok(json) => serde_json::to_string_pretty(&json).unwrap_or(text),
            Err(_) => text,
        }
    }

    fn push_history(history: &mut Vec<f64>, v: f64) {
        history.push(v);
        if history.len() > 180 {
            history.remove(0);
        }
    }

    fn reset_state(&mut self) {
        self.curl_input = "curl -X POST -H 'Content-Type: application/json' -d '{\"key\":\"value\"}' https://api.example.com/endpoint".to_owned();
        self.request_draft = EditableRequest {
            api_url: String::new(),
            method: "GET".to_owned(),
            headers_json: "{}".to_owned(),
            body: String::new(),
        };
        self.convert_status = None;
        self.generic_error = None;
        self.settings = LoadTestSettings {
            concurrency: 100,
            duration_secs: 60,
            interval_ms: 10,
            timeout_secs: 5,
        };
        self.run_state = RunState::Idle;
        self.latest_runtime_metrics = RuntimeMetrics::default();
        self.final_metrics = None;
        /*
        self.response_preview = ResponsePreview {
            status: "-".to_owned(),
            time_ms: "-".to_owned(),
            size: "-".to_owned(),
            body_preview: "压测模式默认不抓取响应体".to_owned(),
        };
        */
        self.qps_history.clear();
        self.latency_history.clear();
        self.auto_convert_from_curl();
    }
}

impl eframe::App for ApiQpsApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.consume_events();
        ctx.request_repaint_after(std::time::Duration::from_millis(60));

        egui::TopBottomPanel::top("top_bar")
            .resizable(false)
            .exact_height(74.0)
            .show(ctx, |ui| {
                card(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("API QPS DEVTOOLS").size(20.0).strong());
                        ui.add_space(8.0);
                        if ui.button("↺ 重置").clicked() {
                            self.reset_state();
                        }
                        ui.separator();
                        ui.label(format!(
                            "{}: {}",
                            t(self.language, I18nKey::Target),
                            if self.request_draft.api_url.is_empty() {
                                "http://127.0.0.1/api".to_owned()
                            } else {
                                self.request_draft.api_url.clone()
                            }
                        ));
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            egui::ComboBox::from_id_salt("lang_combo")
                                .selected_text(match self.language {
                                    Language::ZhCn => "中文",
                                    Language::EnUs => "English",
                                })
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.language, Language::ZhCn, "中文");
                                    ui.selectable_value(&mut self.language, Language::EnUs, "English");
                                });
                        });
                    });
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            let viewport_height = ui.available_height();
            egui::ScrollArea::both()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.set_min_height(viewport_height);
                    
                    // Create a 2-column layout
                    ui.columns(2, |columns| {
                        // Left Column: Request Configuration & Load Test Config
                        columns[0].vertical(|ui| {
                            // 1. Request Configuration
                            card(ui, |ui| {
                                ui.with_layout(egui::Layout::top_down(egui::Align::Min), |ui| {
                                    ui.label(RichText::new(t(self.language, I18nKey::RequestBuilder)).size(16.0).strong());
                                    ui.separator();
                                    ui.label(t(self.language, I18nKey::CurlLabel));
                                    egui::ScrollArea::vertical().max_height(100.0).show(ui, |ui| {
                                        let response = ui.add_sized(
                                            [ui.available_width(), 100.0],
                                            TextEdit::multiline(&mut self.curl_input)
                                                .font(FontId::new(13.0, FontFamily::Monospace))
                                                .code_editor()
                                                .desired_width(f32::INFINITY)
                                                .hint_text(t(self.language, I18nKey::InputPlaceholder)),
                                        );
                                        if response.changed() {
                                            self.auto_convert_from_curl();
                                        }
                                    });
                                    if let Some(status) = &self.convert_status {
                                        ui.colored_label(
                                            if status.ok {
                                                Color32::from_rgb(52, 199, 89)
                                            } else {
                                                Color32::from_rgb(255, 59, 48)
                                            },
                                            &status.message,
                                        );
                                    }
                                    ui.add_space(8.0);
                                    ui.horizontal(|ui| {
                                        ui.label(t(self.language, I18nKey::RequestType));
                                        egui::ComboBox::from_id_salt("method_combo")
                                            .selected_text(&self.request_draft.method)
                                            .show_ui(ui, |ui| {
                                                for method in ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] {
                                                    ui.selectable_value(
                                                        &mut self.request_draft.method,
                                                        method.to_owned(),
                                                        method,
                                                    );
                                                }
                                            });
                                        ui.label(t(self.language, I18nKey::ApiUrl));
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut self.request_draft.api_url)
                                                .font(FontId::new(13.0, FontFamily::Monospace))
                                                .code_editor(),
                                        );
                                    });
                                    
                                    ui.add_space(8.0);
                                    ui.columns(2, |cols| {
                                        cols[0].vertical(|ui| {
                                            ui.label(t(self.language, I18nKey::HeaderJson));
                                            egui::ScrollArea::vertical().max_height(150.0).show(ui, |ui| {
                                                ui.add_sized(
                                                    [ui.available_width(), 150.0],
                                                    TextEdit::multiline(&mut self.request_draft.headers_json)
                                                        .font(FontId::new(13.0, FontFamily::Monospace))
                                                        .code_editor()
                                                        .desired_width(f32::INFINITY),
                                                );
                                            });
                                        });
                                        cols[1].vertical(|ui| {
                                            ui.label(t(self.language, I18nKey::RequestBody));
                                            let method = Method::from_str(self.request_draft.method.trim()).unwrap_or(Method::GET);
                                            let supports_body = Self::method_supports_body(&method);
                                            ui.add_enabled_ui(supports_body, |ui| {
                                                egui::ScrollArea::vertical().max_height(150.0).show(ui, |ui| {
                                                    ui.add_sized(
                                                        [ui.available_width(), 150.0],
                                                        TextEdit::multiline(&mut self.request_draft.body)
                                                            .font(FontId::new(13.0, FontFamily::Monospace))
                                                            .code_editor()
                                                            .desired_width(f32::INFINITY),
                                                    );
                                                });
                                            });
                                            if !supports_body {
                                                ui.label(RichText::new(t(self.language, I18nKey::BodyNotRequired)).size(11.0).color(Color32::GRAY));
                                            }
                                        });
                                    });
                                });
                            });
        
                            ui.add_space(8.0);
        
                            // 2. Load Test Configuration & Controls
                            card(ui, |ui| {
                                ui.with_layout(egui::Layout::top_down(egui::Align::Min), |ui| {
                                    ui.horizontal(|ui| {
                                        ui.label(RichText::new(t(self.language, I18nKey::LoadTestConfig)).size(16.0).strong());
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            if ui
                                                .add_enabled(
                                                    self.is_running(),
                                                    egui::Button::new(t(self.language, I18nKey::StopTest))
                                                        .fill(Color32::from_rgb(255, 59, 48))
                                                        .stroke(Stroke::NONE)
                                                        .min_size(egui::vec2(80.0, 24.0)),
                                                )
                                                .clicked()
                                            {
                                                self.stop_test();
                                            }
                                            if ui
                                                .add_enabled(
                                                    !self.is_running(),
                                                    egui::Button::new(t(self.language, I18nKey::StartTest))
                                                        .fill(Color32::from_rgb(0, 122, 255))
                                                        .stroke(Stroke::NONE)
                                                        .min_size(egui::vec2(80.0, 24.0)),
                                                )
                                                .clicked()
                                            {
                                                self.start_test();
                                            }
                                        });
                                    });
                                    ui.separator();
                                    
                                    // Use a grid for settings to keep them aligned
                                    egui::Grid::new("settings_grid").spacing([20.0, 10.0]).show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(t(self.language, I18nKey::Concurrency));
                                            ui.add(egui::DragValue::new(&mut self.settings.concurrency).range(1..=20000));
                                        });
                                        ui.horizontal(|ui| {
                                            ui.label(t(self.language, I18nKey::Duration));
                                            ui.add(egui::DragValue::new(&mut self.settings.duration_secs).range(1..=86400));
                                            ui.label("s");
                                        });
                                        ui.horizontal(|ui| {
                                            ui.label(t(self.language, I18nKey::Timeout));
                                            ui.add(egui::DragValue::new(&mut self.settings.timeout_secs).range(1..=120));
                                            ui.label("s");
                                        });
                                        ui.horizontal(|ui| {
                                            ui.label(t(self.language, I18nKey::Interval));
                                            ui.add(egui::DragValue::new(&mut self.settings.interval_ms).range(0..=60000));
                                            ui.label("ms");
                                        });
                                        ui.end_row();
                                    });
            
                                    if let Some(err) = &self.generic_error {
                                        ui.add_space(4.0);
                                        ui.colored_label(Color32::from_rgb(255, 59, 48), err);
                                    }
                                });
                            });
                        });

                        // Right Column: Metrics & Response Preview
                        columns[1].vertical(|ui| {
                            // 3. Metrics & Charts
                            card(ui, |ui| {
                                ui.with_layout(egui::Layout::top_down(egui::Align::Min), |ui| {
                                    ui.label(RichText::new(t(self.language, I18nKey::RuntimeMetrics)).size(16.0).strong());
                                    ui.separator();
                                    
                                    // Use a grid to ensure all metric cards have consistent size
                                    let errors = self.latest_runtime_metrics.failed_requests + self.latest_runtime_metrics.timeout_requests;
                                    let p95 = self
                                        .final_metrics
                                        .as_ref()
                                        .map(|m| format!("{:.2} ms", m.p95_latency_ms))
                                        .unwrap_or_else(|| "-".to_owned());

                                    egui::Grid::new("metrics_grid")
                                        .spacing([10.0, 10.0])
                                        .min_col_width((ui.available_width() - 20.0) / 3.0) // Distribute width evenly for 3 columns
                                        .show(ui, |ui| {
                                            ui.vertical_centered(|ui| render_metric_card(ui, t(self.language, I18nKey::TotalRequests), format!("{}", self.latest_runtime_metrics.total_requests)));
                                            ui.vertical_centered(|ui| render_metric_card(ui, "Success", format!("{}", self.latest_runtime_metrics.success_requests)));
                                            ui.vertical_centered(|ui| render_metric_card(ui, t(self.language, I18nKey::Qps), format!("{:.1}", self.latest_runtime_metrics.qps)));
                                            ui.end_row();

                                            ui.vertical_centered(|ui| render_metric_card(ui, "Elapsed", format!("{:.2}s", self.latest_runtime_metrics.elapsed_secs)));
                                            ui.vertical_centered(|ui| render_metric_card(ui, t(self.language, I18nKey::Errors), format!("{}", errors)));
                                            ui.vertical_centered(|ui| render_metric_card(ui, t(self.language, I18nKey::P95Latency), p95));
                                            ui.end_row();
                                        });

                                    ui.add_space(8.0);
                                    ui.columns(2, |cols| {
                                        render_sparkline(&mut cols[0], "QPS 实时趋势 (Requests/sec)", &self.qps_history, Color32::from_rgb(0, 122, 255));
                                        render_sparkline(
                                            &mut cols[1],
                                            "响应延迟趋势 (Avg Latency/ms)",
                                            &self.latency_history,
                                            Color32::from_rgb(255, 149, 0),
                                        );
                                    });
                                    if let Some(final_metrics) = &self.final_metrics {
                                        ui.separator();
                                        ui.label(RichText::new("压测最终报告").strong());
                                        ui.label(format!(
                                            "耗时: {:.2}s | 总数: {} | 成功: {} | 失败: {} | 超时: {} | 平均QPS: {:.1}",
                                            final_metrics.elapsed_secs,
                                            final_metrics.total_requests,
                                            final_metrics.success_requests,
                                            final_metrics.failed_requests,
                                            final_metrics.timeout_requests,
                                            final_metrics.qps
                                        ));
                                        ui.label(format!(
                                            "延迟: 平均 {:.2}ms | P50 {:.2}ms | P95 {:.2}ms | P99 {:.2}ms | Max {:.2}ms",
                                            final_metrics.avg_latency_ms,
                                            final_metrics.p50_latency_ms,
                                            final_metrics.p95_latency_ms,
                                            final_metrics.p99_latency_ms,
                                            final_metrics.max_latency_ms
                                        ));
                                        if let Some(err) = &final_metrics.last_error {
                                            ui.colored_label(Color32::from_rgb(255, 59, 48), format!("Last Error: {}", err));
                                        }
                                    }
                                });
                            });
                        });
                    });
                });
        });
    }
}

fn card(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui)) {
    egui::Frame::new()
        .fill(Color32::from_rgb(255, 255, 255))
        .stroke(Stroke::new(1.0, Color32::from_rgb(229, 229, 234)))
        .corner_radius(egui::CornerRadius::same(12))
        .inner_margin(egui::Margin::same(14))
        .show(ui, add_contents);
}

fn render_metric_card(ui: &mut egui::Ui, label: &str, value: String) {
    egui::Frame::new()
        .fill(Color32::from_rgb(255, 255, 255))
        .stroke(Stroke::new(1.0, Color32::from_rgb(229, 229, 234)))
        .corner_radius(egui::CornerRadius::same(12))
        .inner_margin(egui::Margin::same(12))
        .show(ui, |ui| {
            // Remove fixed width to allow flexible layout from parent scope
            // ui.set_width(180.0); 
            ui.label(RichText::new(label).size(12.0).color(Color32::from_rgb(117, 117, 122)));
            ui.label(
                RichText::new(value)
                    .font(FontId::new(28.0, FontFamily::Proportional))
                    .strong(),
            );
        });
}

fn render_sparkline(ui: &mut egui::Ui, title: &str, data: &[f64], color: Color32) {
    ui.label(RichText::new(title).size(13.0).strong());
    let (rect, _) = ui.allocate_exact_size(egui::vec2(ui.available_width(), 150.0), Sense::hover());
    let painter = ui.painter();
    painter.rect_filled(rect, 8.0, Color32::from_rgb(250, 250, 252));
    painter.rect_stroke(
        rect,
        8.0,
        Stroke::new(1.0, Color32::from_rgb(229, 229, 234)),
        egui::StrokeKind::Outside,
    );
    if data.len() < 2 {
        return;
    }
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    for v in data {
        min = min.min(*v);
        max = max.max(*v);
    }
    let span = (max - min).max(0.0001);
    let step_x = rect.width() / (data.len().saturating_sub(1) as f32);
    let mut points = Vec::with_capacity(data.len());
    for (i, v) in data.iter().enumerate() {
        let x = rect.left() + step_x * i as f32;
        let normalized = ((v - min) / span) as f32;
        let y = rect.bottom() - normalized * rect.height();
        points.push(egui::pos2(x, y));
    }
    painter.add(egui::Shape::line(points, Stroke::new(2.0, color)));
}
