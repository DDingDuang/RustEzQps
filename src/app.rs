use crate::curl_parser::{RequestTemplate, parse_curl};
use crate::i18n::{I18nKey, Language, t};
use crate::loadtest::{EngineEvent, FinalMetrics, LoadTestSettings, RuntimeMetrics, run_load_test};
use anyhow::{Result, anyhow};
use bytes::Bytes;
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
    runtime: Option<Arc<Runtime>>,
    run_state: RunState,
    latest_runtime_metrics: RuntimeMetrics,
    final_metrics: Option<FinalMetrics>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EditableRequest {
    api_url: String,
    method: String,
    headers_json: String,
    body: String,
}

#[derive(Clone, Debug)]
struct ConvertStatus {
    ok: bool,
    message: String,
}

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
            // Apple-like styling
            style.visuals.panel_fill = Color32::from_rgb(242, 242, 247); // macOS secondary background
            style.visuals.window_fill = Color32::from_rgb(255, 255, 255);
            style.visuals.extreme_bg_color = Color32::from_rgb(255, 255, 255); // Input fields
            style.visuals.override_text_color = Some(Color32::from_rgb(29, 29, 31)); // SF Text Color
            
            style.visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(255, 255, 255);
            style.visuals.widgets.noninteractive.corner_radius = egui::CornerRadius::same(8);
            
            // Buttons - Inactive
            style.visuals.widgets.inactive.bg_fill = Color32::from_rgb(255, 255, 255);
            style.visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(8);
            style.visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, Color32::from_rgb(209, 209, 214)); // System Gray 4
            
            // Buttons - Hovered
            style.visuals.widgets.hovered.bg_fill = Color32::from_rgb(242, 242, 247);
            style.visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(8);
            style.visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, Color32::from_rgb(209, 209, 214));

            // Buttons - Active
            style.visuals.widgets.active.bg_fill = Color32::from_rgb(229, 229, 234); // System Gray 5
            style.visuals.widgets.active.corner_radius = egui::CornerRadius::same(8);
            style.visuals.widgets.active.bg_stroke = Stroke::new(1.0, Color32::from_rgb(209, 209, 214));

            // Selection
            style.visuals.selection.bg_fill = Color32::from_rgb(0, 122, 255); // System Blue
            style.visuals.selection.stroke = Stroke::new(1.0, Color32::from_rgb(0, 122, 255));

            // Spacing
            style.spacing.item_spacing = egui::vec2(10.0, 10.0);
            style.spacing.window_margin = egui::Margin::same(16);
            style.spacing.button_padding = egui::vec2(12.0, 6.0);
        });

        let default_lang = Language::ZhCn;
        let (runtime, runtime_error) = match Runtime::new() {
            Ok(rt) => (Some(Arc::new(rt)), None),
            Err(e) => (
                None,
                Some(format!(
                    "{}: {e}",
                    t(default_lang, I18nKey::RuntimeInitFailed)
                )),
            ),
        };

        let mut app = Self {
            language: default_lang,
            curl_input: "curl -X POST -H 'Content-Type: application/json' -d '{\"key\":\"value\"}' https://api.example.com/endpoint".to_owned(),
            request_draft: EditableRequest {
                api_url: String::new(),
                method: "GET".to_owned(),
                headers_json: "{}".to_owned(),
                body: String::new(),
            },
            convert_status: None,
            generic_error: runtime_error,
            settings: LoadTestSettings {
                concurrency: 100,
                duration_secs: 60,
                interval_ms: 10,
                timeout_secs: 5,
                keep_alive: true,
            },
            runtime,
            run_state: RunState::Idle,
            latest_runtime_metrics: RuntimeMetrics::default(),
            final_metrics: None,
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
        let language = self.language;
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_for_task = stop_flag.clone();
        let (tx, rx) = unbounded_channel();
        self.final_metrics = None;
        self.latest_runtime_metrics = RuntimeMetrics::default();

        let Some(runtime) = self.runtime.clone() else {
            self.generic_error = Some(format!(
                "{}: {}",
                t(self.language, I18nKey::GenericError),
                t(self.language, I18nKey::RuntimeUnavailable)
            ));
            return;
        };
        runtime.spawn(async move {
            if let Err(err) = run_load_test(template, settings, language, tx.clone(), stop_for_task).await {
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
        let mut processed = 0usize;
        const MAX_EVENTS_PER_FRAME: usize = 512;
        if let RunState::Running { events, .. } = &mut self.run_state {
            while processed < MAX_EVENTS_PER_FRAME {
                let Ok(ev) = events.try_recv() else {
                    break;
                };
                processed += 1;
                match ev {
                    EngineEvent::Progress(m) => {
                        self.latest_runtime_metrics = m;
                    }
                    EngineEvent::Completed(m) => {
                        self.final_metrics = Some(m);
                        should_idle = true;
                    }
                    EngineEvent::Failed(e) => {
                        self.generic_error =
                            Some(format!("{}: {e}", t(self.language, I18nKey::GenericError)));
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
        match parse_curl(&self.curl_input, self.language) {
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
        let method = Method::from_str(self.request_draft.method.trim())
            .map_err(|_| anyhow!(t(self.language, I18nKey::InvalidRequestMethod)))?;
        let url = self.request_draft.api_url.trim().to_owned();
        if url.is_empty() {
            return Err(anyhow!(t(self.language, I18nKey::EmptyApiUrl)));
        }
        let _ = url::Url::parse(&url).map_err(|_| anyhow!(t(self.language, I18nKey::InvalidApiUrl)))?;

        let headers_text = self.request_draft.headers_json.trim();
        let value: Value = if headers_text.is_empty() {
            Value::Object(Map::new())
        } else {
            serde_json::from_str(headers_text).map_err(|_| anyhow!(t(self.language, I18nKey::HeaderNotJson)))?
        };
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!(t(self.language, I18nKey::HeaderMustObject)))?;

        let mut headers = HeaderMap::new();
        for (k, v) in obj {
            let name = HeaderName::from_str(k)
                .map_err(|_| anyhow!("{}: {k}", t(self.language, I18nKey::InvalidHeaderName)))?;
            let val_str = if let Some(s) = v.as_str() {
                s.to_owned()
            } else {
                v.to_string()
            };
            let header_val = HeaderValue::from_str(&val_str)
                .map_err(|_| anyhow!("{}: {k}", t(self.language, I18nKey::InvalidHeaderValue)))?;
            headers.insert(name, header_val);
        }

        let body = if Self::method_supports_body(&method) && !self.request_draft.body.trim().is_empty()
        {
            let normalized = Self::normalize_possible_json_body(&self.request_draft.body);
            Some(Bytes::from(normalized.into_bytes()))
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

    fn method_text_supports_body(method: &str) -> bool {
        !(method.eq_ignore_ascii_case("GET") || method.eq_ignore_ascii_case("HEAD"))
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
            keep_alive: true,
        };
        self.run_state = RunState::Idle;
        self.latest_runtime_metrics = RuntimeMetrics::default();
        self.final_metrics = None;
        self.auto_convert_from_curl();
    }
}

impl eframe::App for ApiQpsApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.consume_events();
        if self.is_running() {
            ctx.request_repaint_after(std::time::Duration::from_millis(33));
        }

        egui::TopBottomPanel::top("top_bar")
            .frame(egui::Frame::new()
                .fill(Color32::from_rgb(255, 255, 255))
                .inner_margin(egui::Margin::symmetric(24, 12))
                .shadow(egui::Shadow {
                    offset: [0, 1],
                    blur: 6,
                    spread: 0,
                    color: Color32::from_black_alpha(10),
                })
            )
            .resizable(false)
            .exact_height(64.0)
            .show(ctx, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.label(RichText::new(t(self.language, I18nKey::AppTitle)).size(18.0).strong().color(Color32::from_rgb(29, 29, 31)));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let zh_label = t(self.language, I18nKey::LanguageZh);
                        let en_label = t(self.language, I18nKey::LanguageEn);
                         egui::ComboBox::from_id_salt("lang_combo")
                            .selected_text(match self.language {
                                Language::ZhCn => zh_label,
                                Language::EnUs => en_label,
                            })
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut self.language, Language::ZhCn, zh_label);
                                ui.selectable_value(&mut self.language, Language::EnUs, en_label);
                            });
                        
                        ui.add_space(16.0);
                        
                        // Styled Reset Button
                        if ui
                            .add(
                                egui::Button::new(t(self.language, I18nKey::Reset))
                                    .min_size(egui::vec2(60.0, 28.0)),
                            )
                            .clicked()
                        {
                            self.reset_state();
                        }

                        ui.add_space(16.0);
                        
                        ui.label(RichText::new(format!(
                            "{}: {}",
                            t(self.language, I18nKey::Target),
                            if self.request_draft.api_url.is_empty() {
                                "http://127.0.0.1/api"
                            } else {
                                self.request_draft.api_url.as_str()
                            }
                        )).color(Color32::from_rgb(142, 142, 147)));
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
                                    ui.add_space(4.0);
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
                                    
                                    ui.add_space(4.0);
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
                                            let supports_body = Self::method_text_supports_body(self.request_draft.method.trim());
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
                                            ui.label(t(self.language, I18nKey::SecondUnit));
                                        });
                                        ui.horizontal(|ui| {
                                            ui.label(t(self.language, I18nKey::Timeout));
                                            ui.add(egui::DragValue::new(&mut self.settings.timeout_secs).range(1..=120));
                                            ui.label(t(self.language, I18nKey::SecondUnit));
                                        });
                                        ui.horizontal(|ui| {
                                            ui.label(t(self.language, I18nKey::Interval));
                                            ui.add(egui::DragValue::new(&mut self.settings.interval_ms).range(0..=60000));
                                            ui.label(t(self.language, I18nKey::MillisecondUnit));
                                        });
                                        ui.end_row();
                                    });
            
                                    ui.add_space(8.0);
                                    ui.checkbox(&mut self.settings.keep_alive, t(self.language, I18nKey::KeepAlive));

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
                                        .spacing([0.0, 0.0]) // Spacing handled by card margins
                                        .min_col_width((ui.available_width()) / 3.0)
                                        .show(ui, |ui| {
                                            render_metric_card(ui, t(self.language, I18nKey::TotalRequests), format!("{}", self.latest_runtime_metrics.total_requests));
                                            render_metric_card(ui, t(self.language, I18nKey::Success), format!("{}", self.latest_runtime_metrics.success_requests));
                                            render_metric_card(ui, t(self.language, I18nKey::Qps), format!("{:.1}", self.latest_runtime_metrics.qps));
                                            ui.end_row();

                                            render_metric_card(ui, t(self.language, I18nKey::Elapsed), format!("{:.2}s", self.latest_runtime_metrics.elapsed_secs));
                                            render_metric_card(ui, t(self.language, I18nKey::Errors), format!("{}", errors));
                                            render_metric_card(ui, t(self.language, I18nKey::P95Latency), p95);
                                            ui.end_row();
                                        });

                                    ui.add_space(6.0);
                                    render_status_code_bars(
                                        ui,
                                        self.language,
                                        &self.latest_runtime_metrics.status_code_counts,
                                        self.latest_runtime_metrics.transport_error_requests,
                                    );
                                    if let Some(final_metrics) = &self.final_metrics {
                                        ui.separator();
                                        ui.label(RichText::new(t(self.language, I18nKey::FinalReport)).strong());
                                        egui::Grid::new("final_report_grid")
                                            .num_columns(2)
                                            .spacing([12.0, 6.0])
                                            .show(ui, |ui| {
                                                ui.label(RichText::new(t(self.language, I18nKey::ElapsedTime)).color(Color32::from_rgb(142, 142, 147)));
                                                ui.label(format!("{:.2}s", final_metrics.elapsed_secs));
                                                ui.end_row();
                                                ui.label(RichText::new(t(self.language, I18nKey::TotalRequestsFinal)).color(Color32::from_rgb(142, 142, 147)));
                                                ui.label(format!("{}", final_metrics.total_requests));
                                                ui.end_row();
                                                ui.label(RichText::new(t(self.language, I18nKey::SuccessFailTimeout)).color(Color32::from_rgb(142, 142, 147)));
                                                ui.label(format!(
                                                    "{} / {} / {}",
                                                    final_metrics.success_requests,
                                                    final_metrics.failed_requests,
                                                    final_metrics.timeout_requests
                                                ));
                                                ui.end_row();
                                                ui.label(RichText::new(t(self.language, I18nKey::AvgQps)).color(Color32::from_rgb(142, 142, 147)));
                                                ui.label(format!("{:.1}", final_metrics.qps));
                                                ui.end_row();
                                                ui.label(RichText::new(t(self.language, I18nKey::LatencyDetail)).color(Color32::from_rgb(142, 142, 147)));
                                                ui.label(format!(
                                                    "{:.2} / {:.2} / {:.2} / {:.2} / {:.2} ms",
                                                    final_metrics.avg_latency_ms,
                                                    final_metrics.p50_latency_ms,
                                                    final_metrics.p95_latency_ms,
                                                    final_metrics.p99_latency_ms,
                                                    final_metrics.max_latency_ms
                                                ));
                                                ui.end_row();
                                            });
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
        .stroke(Stroke::new(0.5, Color32::from_black_alpha(20)))
        .corner_radius(egui::CornerRadius::same(16))
        .shadow(egui::Shadow {
            offset: [0, 4],
            blur: 12,
            spread: 0,
            color: Color32::from_black_alpha(15),
        })
        .inner_margin(egui::Margin::same(20))
        .outer_margin(egui::Margin::same(10)) // Add gap between cards
        .show(ui, add_contents);
}

fn render_metric_card(ui: &mut egui::Ui, label: &str, value: String) {
    egui::Frame::new()
        .fill(Color32::from_rgb(255, 255, 255))
        .stroke(Stroke::new(0.5, Color32::from_black_alpha(20)))
        .corner_radius(egui::CornerRadius::same(12))
        .shadow(egui::Shadow {
            offset: [0, 2],
            blur: 6,
            spread: 0,
            color: Color32::from_black_alpha(10),
        })
        .inner_margin(egui::Margin::same(16))
        .outer_margin(egui::Margin::same(6)) // Add gap between metric cards
        .show(ui, |ui| {
            ui.set_min_width(100.0); // Ensure minimum width
            ui.label(RichText::new(label).size(13.0).color(Color32::from_rgb(142, 142, 147)));
            ui.add_space(4.0);
            ui.label(
                RichText::new(value)
                    .font(FontId::new(24.0, FontFamily::Proportional))
                    .strong()
                    .color(Color32::from_rgb(29, 29, 31)),
            );
        });
}

fn render_status_code_bars(
    ui: &mut egui::Ui,
    language: Language,
    status_counts: &[(u16, u64)],
    transport_error_requests: u64,
) {
    ui.label(RichText::new(t(language, I18nKey::StatusCodeDist)).size(13.0).strong().color(Color32::from_rgb(29, 29, 31)));
    let height = 156.0;
    let (response, painter) = ui.allocate_painter(egui::vec2(ui.available_width(), height), Sense::hover());
    let rect = response.rect;

    painter.rect_filled(rect, egui::CornerRadius::same(12), Color32::from_rgb(255, 255, 255));
    painter.rect_stroke(
        rect,
        egui::CornerRadius::same(12),
        Stroke::new(1.0, Color32::from_rgb(229, 229, 234)),
        egui::StrokeKind::Outside,
    );

    let margin_left = 52.0;
    let margin_right = 20.0;
    let margin_top = 10.0;
    let margin_bottom = 28.0;
    let chart_rect = egui::Rect::from_min_max(
        rect.min + egui::vec2(margin_left, margin_top),
        rect.max - egui::vec2(margin_right, margin_bottom),
    );

    let font_id = FontId::new(10.5, FontFamily::Proportional);
    let text_color = Color32::from_rgb(142, 142, 147);

    if status_counts.is_empty() && transport_error_requests == 0 {
        painter.text(
            chart_rect.center(),
            egui::Align2::CENTER_CENTER,
            t(language, I18nKey::WaitingData),
            FontId::new(12.0, FontFamily::Proportional),
            text_color,
        );
        return;
    }

    let mut bars: Vec<(StatusCodeLabel, u64, Color32)> = Vec::with_capacity(status_counts.len() + 1);
    for (code, count) in status_counts {
        bars.push((StatusCodeLabel::Code(*code), *count, status_color(*code)));
    }
    if transport_error_requests > 0 {
        bars.push((
            StatusCodeLabel::Err,
            transport_error_requests,
            Color32::from_rgb(255, 59, 48),
        ));
    }

    let max_count = bars.iter().map(|(_, count, _)| *count).max().unwrap_or(1).max(1);
    let max_axis = ((max_count as f64) * 1.2).ceil().max(1.0) as u64;

    let grid_steps = 3;
    for i in 0..=grid_steps {
        let t = i as f32 / grid_steps as f32;
        let y = chart_rect.bottom() - t * chart_rect.height();
        let label_value = ((t as f64) * max_axis as f64).round() as u64;
        painter.line_segment(
            [egui::pos2(chart_rect.left(), y), egui::pos2(chart_rect.right(), y)],
            Stroke::new(1.0, Color32::from_rgb(242, 242, 247)),
        );
        painter.text(
            egui::pos2(chart_rect.left() - 6.0, y),
            egui::Align2::RIGHT_CENTER,
            format!("{label_value}"),
            font_id.clone(),
            text_color,
        );
    }

    painter.line_segment(
        [egui::pos2(chart_rect.left(), chart_rect.top()), egui::pos2(chart_rect.left(), chart_rect.bottom())],
        Stroke::new(1.0, Color32::from_rgb(209, 209, 214)),
    );
    painter.line_segment(
        [egui::pos2(chart_rect.left(), chart_rect.bottom()), egui::pos2(chart_rect.right(), chart_rect.bottom())],
        Stroke::new(1.0, Color32::from_rgb(209, 209, 214)),
    );

    let n = bars.len() as f32;
    let slot_w = (chart_rect.width() / n).max(10.0);
    let bar_w = (slot_w * 0.38).max(4.0);
    let bar_painter = painter.with_clip_rect(chart_rect.expand2(egui::vec2(0.0, 2.0)));
    let max_x_labels = ((chart_rect.width() / 34.0).floor() as usize).max(1);
    let label_step = bars.len().div_ceil(max_x_labels).max(1);

    let mut hovered: Option<(egui::Pos2, StatusCodeLabel, u64, Color32)> = None;

    let hover_pos = response.hover_pos();
    let show_bar_value = bars.len() <= 24;
    for (idx, (label, count, color)) in bars.iter().enumerate() {
        let center_x = chart_rect.left() + slot_w * (idx as f32 + 0.5);
        let left = center_x - bar_w * 0.5;
        let right = center_x + bar_w * 0.5;
        let h_ratio = (*count as f64 / max_axis as f64) as f32;
        let top = chart_rect.bottom() - h_ratio * chart_rect.height();
        let bar_rect = egui::Rect::from_min_max(egui::pos2(left, top), egui::pos2(right, chart_rect.bottom()));

        bar_painter.rect_filled(bar_rect, egui::CornerRadius::same(4), *color);
        bar_painter.rect_stroke(
            bar_rect,
            egui::CornerRadius::same(4),
            Stroke::new(1.0, color.linear_multiply(0.8)),
            egui::StrokeKind::Outside,
        );

        if idx % label_step == 0 || idx + 1 == bars.len() {
            painter.text(
                egui::pos2(center_x, chart_rect.bottom() + 6.0),
                egui::Align2::CENTER_TOP,
                label.display_text(language),
                font_id.clone(),
                text_color,
            );
        }
        if show_bar_value && bar_rect.height() >= 14.0 && bar_w >= 10.0 {
            painter.text(
                bar_rect.center(),
                egui::Align2::CENTER_CENTER,
                format!("{count}"),
                FontId::new(9.5, FontFamily::Proportional),
                Color32::WHITE,
            );
        }

        if let Some(pos) = hover_pos {
            if bar_rect.contains(pos) {
                hovered = Some((egui::pos2(center_x, top), *label, *count, *color));
            }
        }
    }

    if let Some((anchor, label, count, color)) = hovered {
        painter.circle_filled(anchor, 4.0, color);
        let tooltip_text = format!("{}: {count}", label.display_text(language));
        let galley = painter.layout_no_wrap(
            tooltip_text,
            FontId::new(13.0, FontFamily::Proportional),
            Color32::WHITE,
        );
        let padding = egui::vec2(10.0, 6.0);
        let tooltip_size = galley.rect.size() + padding * 2.0;
        let mut tooltip_pos = anchor - egui::vec2(tooltip_size.x * 0.5, tooltip_size.y + 10.0);
        if tooltip_pos.x < rect.left() {
            tooltip_pos.x = rect.left();
        }
        if tooltip_pos.x + tooltip_size.x > rect.right() {
            tooltip_pos.x = rect.right() - tooltip_size.x;
        }
        if tooltip_pos.y < rect.top() {
            tooltip_pos.y = anchor.y + 10.0;
        }
        let tooltip_rect = egui::Rect::from_min_size(tooltip_pos, tooltip_size);
        painter.rect_filled(
            tooltip_rect,
            egui::CornerRadius::same(8),
            Color32::from_rgb(29, 29, 31),
        );
        painter.galley(tooltip_rect.min + padding, galley, Color32::WHITE);
    }
}

#[derive(Clone, Copy)]
enum StatusCodeLabel {
    Code(u16),
    Err,
}

impl StatusCodeLabel {
    fn display_text(&self, language: Language) -> String {
        match self {
            Self::Code(code) => code.to_string(),
            Self::Err => t(language, I18nKey::TransportErrShort).to_owned(),
        }
    }
}

fn status_color(code: u16) -> Color32 {
    match code {
        200..=299 => Color32::from_rgb(52, 199, 89),
        300..=399 => Color32::from_rgb(0, 122, 255),
        400..=499 => Color32::from_rgb(255, 149, 0),
        500..=599 => Color32::from_rgb(255, 59, 48),
        _ => Color32::from_rgb(142, 142, 147),
    }
}
