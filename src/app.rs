use crate::curl_parser::{RequestTemplate, parse_curl};
use crate::i18n::{I18nKey, Language, t};
use crate::loadtest::{EngineEvent, FinalMetrics, LoadTestSettings, RuntimeMetrics, run_load_test};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use eframe::CreationContext;
use eframe::egui::{
    self, Color32, FontData, FontDefinitions, FontFamily, FontId, RichText, Sense, Stroke, TextEdit,
};
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

const MAX_TREND_POINTS: usize = 720;

pub struct ApiQpsApp {
    language: Language,
    curl_input: String,
    request_draft: EditableRequest,
    expanded_editor: Option<ExpandedEditor>,
    convert_status: Option<ConvertStatus>,
    generic_error: Option<String>,
    settings: LoadTestSettings,
    runtime: Option<Arc<Runtime>>,
    run_state: RunState,
    latest_runtime_metrics: RuntimeMetrics,
    metric_history: Vec<MetricHistoryPoint>,
    final_metrics: Option<FinalMetrics>,
    final_report_drawer_open: bool,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpandedEditor {
    Curl,
    Headers,
    Body,
}

enum RunState {
    Idle,
    Running {
        stop_flag: Arc<AtomicBool>,
        events: UnboundedReceiver<EngineEvent>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct MetricHistoryPoint {
    elapsed_secs: f64,
    qps: f64,
    avg_latency_ms: f64,
    p95_latency_ms: f64,
}

impl MetricHistoryPoint {
    fn from_runtime_metrics(metrics: &RuntimeMetrics) -> Self {
        Self {
            elapsed_secs: metrics.elapsed_secs,
            qps: metrics.qps,
            avg_latency_ms: metrics.avg_latency_ms,
            p95_latency_ms: metrics.p95_latency_ms,
        }
    }
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
            style.visuals.panel_fill = theme_bg();
            style.visuals.window_fill = theme_surface();
            style.visuals.extreme_bg_color = theme_surface();
            style.visuals.override_text_color = Some(theme_ink());
            style.visuals.faint_bg_color = theme_surface_soft();

            style.visuals.widgets.noninteractive.bg_fill = theme_surface();
            style.visuals.widgets.noninteractive.corner_radius = egui::CornerRadius::same(12);

            style.visuals.widgets.inactive.bg_fill = theme_surface();
            style.visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(12);
            style.visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, theme_line());

            style.visuals.widgets.hovered.bg_fill = theme_surface_soft();
            style.visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(12);
            style.visuals.widgets.hovered.bg_stroke =
                Stroke::new(1.0, theme_line().linear_multiply(1.1));

            style.visuals.widgets.active.bg_fill = Color32::from_rgb(236, 226, 211);
            style.visuals.widgets.active.corner_radius = egui::CornerRadius::same(12);
            style.visuals.widgets.active.bg_stroke = Stroke::new(1.0, theme_line());

            style.visuals.selection.bg_fill = theme_primary();
            style.visuals.selection.stroke = Stroke::new(1.0, theme_primary());

            style.spacing.item_spacing = egui::vec2(12.0, 12.0);
            style.spacing.window_margin = egui::Margin::same(18);
            style.spacing.button_padding = egui::vec2(14.0, 8.0);
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
            expanded_editor: None,
            convert_status: None,
            generic_error: runtime_error,
            settings: LoadTestSettings {
                concurrency: 100,
                duration_secs: 10,
                interval_ms: 1,
                timeout_secs: 5,
                keep_alive: true,
            },
            runtime,
            run_state: RunState::Idle,
            latest_runtime_metrics: RuntimeMetrics::default(),
            metric_history: Vec::new(),
            final_metrics: None,
            final_report_drawer_open: false,
        };
        app.auto_convert_from_curl();
        app
    }

    fn start_test(&mut self) {
        self.generic_error = None;
        let template = match self.build_template_from_draft() {
            Ok(tpl) => tpl,
            Err(e) => {
                self.generic_error =
                    Some(format!("{}: {e}", t(self.language, I18nKey::GenericError)));
                return;
            }
        };

        let settings = self.settings.clone();
        let language = self.language;
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_for_task = stop_flag.clone();
        let (tx, rx) = unbounded_channel();
        self.final_metrics = None;
        self.final_report_drawer_open = false;
        self.latest_runtime_metrics = RuntimeMetrics::default();
        self.metric_history.clear();

        let Some(runtime) = self.runtime.clone() else {
            self.generic_error = Some(format!(
                "{}: {}",
                t(self.language, I18nKey::GenericError),
                t(self.language, I18nKey::RuntimeUnavailable)
            ));
            return;
        };
        runtime.spawn(async move {
            if let Err(err) =
                run_load_test(template, settings, language, tx.clone(), stop_for_task).await
            {
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
                        push_metric_history_sample(
                            &mut self.metric_history,
                            MetricHistoryPoint::from_runtime_metrics(&m),
                        );
                        self.latest_runtime_metrics = m;
                    }
                    EngineEvent::Completed(m) => {
                        self.final_metrics = Some(m);
                        self.final_report_drawer_open = true;
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
        let _ =
            url::Url::parse(&url).map_err(|_| anyhow!(t(self.language, I18nKey::InvalidApiUrl)))?;

        let headers_text = self.request_draft.headers_json.trim();
        let value: Value = if headers_text.is_empty() {
            Value::Object(Map::new())
        } else {
            serde_json::from_str(headers_text)
                .map_err(|_| anyhow!(t(self.language, I18nKey::HeaderNotJson)))?
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

        let body =
            if Self::method_supports_body(&method) && !self.request_draft.body.trim().is_empty() {
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
        self.expanded_editor = None;
        self.convert_status = None;
        self.generic_error = None;
        self.settings = LoadTestSettings {
            concurrency: 100,
            duration_secs: 10,
            interval_ms: 1,
            timeout_secs: 5,
            keep_alive: true,
        };
        self.run_state = RunState::Idle;
        self.latest_runtime_metrics = RuntimeMetrics::default();
        self.metric_history.clear();
        self.final_metrics = None;
        self.final_report_drawer_open = false;
        self.auto_convert_from_curl();
    }

    fn target_display(&self) -> &str {
        if self.request_draft.api_url.is_empty() {
            "http://127.0.0.1/api"
        } else {
            self.request_draft.api_url.as_str()
        }
    }

    fn render_hero_banner(&mut self, ui: &mut egui::Ui) {
        let status_label = if self.is_running() {
            t(self.language, I18nKey::RunningState)
        } else {
            t(self.language, I18nKey::IdleState)
        };
        let status_color = if self.is_running() {
            theme_danger()
        } else {
            theme_primary()
        };
        let frame_width = ui.available_width();

        hero_card_frame().show(ui, |ui| {
            ui.set_min_width((frame_width - 28.0).max(320.0));
            ui.set_min_height(44.0);

            let total_width = ui.available_width();
            let controls_width = if total_width < 920.0 { 168.0 } else { 216.0 };
            let summary_width = (total_width - controls_width - 8.0).max(220.0);

            ui.horizontal(|ui| {
                ui.spacing_mut().item_spacing.x = 8.0;
                ui.allocate_ui_with_layout(
                    egui::vec2(summary_width, 30.0),
                    egui::Layout::left_to_right(egui::Align::Center),
                    |ui| {
                        self.render_hero_summary(ui, status_label, status_color);
                    },
                );
                ui.allocate_ui_with_layout(
                    egui::vec2(controls_width, 30.0),
                    egui::Layout::right_to_left(egui::Align::Center),
                    |ui| {
                        self.render_hero_controls(ui);
                    },
                );
            });
        });
    }

    fn render_hero_controls(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 8.0;
            let zh_label = t(self.language, I18nKey::LanguageZh);
            let en_label = t(self.language, I18nKey::LanguageEn);
            capsule_frame(theme_surface_soft(), theme_line()).show(ui, |ui| {
                egui::ComboBox::from_id_salt("lang_combo")
                    .selected_text(match self.language {
                        Language::ZhCn => zh_label,
                        Language::EnUs => en_label,
                    })
                    .width(64.0)
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.language, Language::ZhCn, zh_label);
                        ui.selectable_value(&mut self.language, Language::EnUs, en_label);
                    });
            });

            if ui
                .add(
                    egui::Button::new(
                        RichText::new(t(self.language, I18nKey::Reset)).color(theme_ink()),
                    )
                    .fill(theme_surface_soft())
                    .stroke(Stroke::new(1.0, theme_line()))
                    .min_size(egui::vec2(84.0, 30.0)),
                )
                .clicked()
            {
                self.reset_state();
            }
        });
    }

    fn render_hero_summary(&self, ui: &mut egui::Ui, status_label: &str, status_color: Color32) {
        let available_width = ui.available_width();
        let show_target = available_width >= 320.0;
        let show_concurrency = available_width >= 560.0;
        let show_duration = available_width >= 740.0;
        let show_keep_alive = available_width >= 900.0 && self.settings.keep_alive;
        let target_max_chars = if available_width >= 980.0 {
            48
        } else if available_width >= 780.0 {
            34
        } else if available_width >= 620.0 {
            24
        } else {
            18
        };

        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 8.0;
            ui.label(
                RichText::new(t(self.language, I18nKey::AppTitle))
                    .size(18.0)
                    .strong()
                    .color(theme_ink()),
            );
            render_pill(
                ui,
                status_label,
                status_color.linear_multiply(0.10),
                status_color,
                status_color.linear_multiply(0.25),
            );
            render_pill(
                ui,
                &self.request_draft.method,
                theme_surface_soft(),
                theme_ink(),
                theme_line(),
            );
            if show_target {
                render_pill(
                    ui,
                    &truncate_middle(self.target_display(), target_max_chars),
                    theme_surface_soft(),
                    theme_muted(),
                    theme_line(),
                );
            }
            if show_concurrency {
                render_pill(
                    ui,
                    &format!(
                        "{} {}",
                        t(self.language, I18nKey::Concurrency),
                        self.settings.concurrency
                    ),
                    theme_surface_soft(),
                    theme_muted(),
                    theme_line(),
                );
            }
            if show_duration {
                render_pill(
                    ui,
                    &format!(
                        "{} {}{}",
                        t(self.language, I18nKey::Duration),
                        self.settings.duration_secs,
                        t(self.language, I18nKey::SecondUnit)
                    ),
                    theme_surface_soft(),
                    theme_muted(),
                    theme_line(),
                );
            }
            if show_keep_alive {
                render_pill(
                    ui,
                    t(self.language, I18nKey::KeepAlive),
                    theme_surface_soft(),
                    theme_muted(),
                    theme_line(),
                );
            }
        });
    }

    fn render_request_panel(&mut self, ui: &mut egui::Ui) {
        card(ui, |ui| {
            panel_header(ui, t(self.language, I18nKey::RequestBuilder), |ui| {
                if let Some(status) = &self.convert_status {
                    let color = if status.ok {
                        theme_green()
                    } else {
                        theme_coral()
                    };
                    render_pill(
                        ui,
                        &status.message,
                        color.linear_multiply(0.14),
                        color,
                        color.linear_multiply(0.35),
                    );
                }
            });

            ui.horizontal(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(140.0, 0.0),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        ui.label(
                            RichText::new(t(self.language, I18nKey::RequestType))
                                .size(12.0)
                                .color(theme_muted()),
                        );
                        capsule_frame(theme_surface_soft(), theme_line()).show(ui, |ui| {
                            egui::ComboBox::from_id_salt("method_combo")
                                .selected_text(&self.request_draft.method)
                                .width(100.0)
                                .show_ui(ui, |ui| {
                                    for method in ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
                                    {
                                        ui.selectable_value(
                                            &mut self.request_draft.method,
                                            method.to_owned(),
                                            method,
                                        );
                                    }
                                });
                        });
                    },
                );
                ui.add_space(8.0);
                ui.vertical(|ui| {
                    ui.label(
                        RichText::new(t(self.language, I18nKey::ApiUrl))
                            .size(12.0)
                            .color(theme_muted()),
                    );
                    inset_frame(theme_surface_soft()).show(ui, |ui| {
                        ui.add_sized(
                            [ui.available_width(), 34.0],
                            TextEdit::singleline(&mut self.request_draft.api_url)
                                .font(FontId::new(13.0, FontFamily::Monospace))
                                .code_editor(),
                        );
                    });
                });
            });

            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(t(self.language, I18nKey::CurlLabel))
                        .size(12.0)
                        .strong()
                        .color(theme_muted()),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .add(
                            egui::Button::new(
                                RichText::new(t(self.language, I18nKey::ExpandEditor))
                                    .size(11.5)
                                    .color(theme_ink()),
                            )
                            .fill(theme_surface_soft())
                            .stroke(Stroke::new(1.0, theme_line()))
                            .min_size(egui::vec2(92.0, 28.0)),
                        )
                        .clicked()
                    {
                        self.expanded_editor = Some(ExpandedEditor::Curl);
                    }
                });
            });
            let response = render_fixed_code_input(
                ui,
                &mut self.curl_input,
                96.0,
                Some(t(self.language, I18nKey::InputPlaceholder)),
            );
            if response.changed() {
                self.auto_convert_from_curl();
            }
            if response.double_clicked() && !self.curl_input.trim().is_empty() {
                self.expanded_editor = Some(ExpandedEditor::Curl);
            }

            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(t(self.language, I18nKey::HeaderJson))
                        .size(12.0)
                        .strong()
                        .color(theme_muted()),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .add(
                            egui::Button::new(
                                RichText::new(t(self.language, I18nKey::ExpandEditor))
                                    .size(11.5)
                                    .color(theme_ink()),
                            )
                            .fill(theme_surface_soft())
                            .stroke(Stroke::new(1.0, theme_line()))
                            .min_size(egui::vec2(92.0, 28.0)),
                        )
                        .clicked()
                    {
                        self.expanded_editor = Some(ExpandedEditor::Headers);
                    }
                });
            });
            let header_response =
                render_fixed_code_input(ui, &mut self.request_draft.headers_json, 112.0, None);
            if header_response.double_clicked()
                && !self.request_draft.headers_json.trim().is_empty()
            {
                self.expanded_editor = Some(ExpandedEditor::Headers);
            }

            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(t(self.language, I18nKey::RequestBody))
                        .size(12.0)
                        .strong()
                        .color(theme_muted()),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .add(
                            egui::Button::new(
                                RichText::new(t(self.language, I18nKey::ExpandEditor))
                                    .size(11.5)
                                    .color(theme_ink()),
                            )
                            .fill(theme_surface_soft())
                            .stroke(Stroke::new(1.0, theme_line()))
                            .min_size(egui::vec2(92.0, 28.0)),
                        )
                        .clicked()
                    {
                        self.expanded_editor = Some(ExpandedEditor::Body);
                    }
                });
            });
            let body_response =
                render_fixed_code_input(ui, &mut self.request_draft.body, 130.0, None);
            if body_response.double_clicked() && !self.request_draft.body.trim().is_empty() {
                self.expanded_editor = Some(ExpandedEditor::Body);
            }
        });
    }

    fn render_expanded_editor_window(&mut self, ctx: &egui::Context) {
        let Some(target) = self.expanded_editor else {
            return;
        };

        let mut keep_open = true;
        let mut close_requested = false;
        let title = match target {
            ExpandedEditor::Curl => t(self.language, I18nKey::CurlLabel),
            ExpandedEditor::Headers => t(self.language, I18nKey::HeaderJson),
            ExpandedEditor::Body => t(self.language, I18nKey::RequestBody),
        };

        egui::Window::new(title)
            .id(egui::Id::new("expanded_editor_window"))
            .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
            .default_size(egui::vec2(920.0, 560.0))
            .collapsible(false)
            .resizable(true)
            .open(&mut keep_open)
            .show(ctx, |ui| {
                let editor_height = (ui.available_height() - 46.0).max(280.0);
                let response = match target {
                    ExpandedEditor::Curl => render_fixed_code_input(
                        ui,
                        &mut self.curl_input,
                        editor_height,
                        Some(t(self.language, I18nKey::InputPlaceholder)),
                    ),
                    ExpandedEditor::Headers => render_fixed_code_input(
                        ui,
                        &mut self.request_draft.headers_json,
                        editor_height,
                        None,
                    ),
                    ExpandedEditor::Body => render_fixed_code_input(
                        ui,
                        &mut self.request_draft.body,
                        editor_height,
                        None,
                    ),
                };

                if response.changed() {
                    if matches!(target, ExpandedEditor::Curl) {
                        self.auto_convert_from_curl();
                    }
                }

                ui.add_space(10.0);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .add(
                            egui::Button::new(
                                RichText::new(t(self.language, I18nKey::Close))
                                    .color(Color32::WHITE)
                                    .strong(),
                            )
                            .fill(theme_primary())
                            .stroke(Stroke::NONE)
                            .min_size(egui::vec2(92.0, 32.0)),
                        )
                        .clicked()
                    {
                        close_requested = true;
                    }
                });
            });

        if !keep_open || close_requested {
            self.expanded_editor = None;
        }
    }

    fn render_settings_panel(&mut self, ui: &mut egui::Ui) {
        card(ui, |ui| {
            panel_header(ui, t(self.language, I18nKey::LoadTestConfig), |ui| {
                render_pill(
                    ui,
                    if self.is_running() {
                        t(self.language, I18nKey::RunningState)
                    } else {
                        t(self.language, I18nKey::IdleState)
                    },
                    if self.is_running() {
                        theme_danger().linear_multiply(0.10)
                    } else {
                        theme_primary().linear_multiply(0.10)
                    },
                    if self.is_running() {
                        theme_danger()
                    } else {
                        theme_primary()
                    },
                    if self.is_running() {
                        theme_danger().linear_multiply(0.24)
                    } else {
                        theme_primary().linear_multiply(0.24)
                    },
                );
            });

            egui::Grid::new("settings_deck_grid")
                .num_columns(2)
                .spacing([10.0, 10.0])
                .show(ui, |ui| {
                    render_setting_tile(
                        ui,
                        t(self.language, I18nKey::Concurrency),
                        theme_navy(),
                        |ui| {
                            ui.add(
                                egui::DragValue::new(&mut self.settings.concurrency)
                                    .range(1..=20000)
                                    .speed(1),
                            );
                        },
                    );
                    render_setting_tile(
                        ui,
                        t(self.language, I18nKey::Duration),
                        theme_amber(),
                        |ui| {
                            ui.horizontal(|ui| {
                                ui.add(
                                    egui::DragValue::new(&mut self.settings.duration_secs)
                                        .range(1..=86400)
                                        .speed(1),
                                );
                                ui.label(t(self.language, I18nKey::SecondUnit));
                            });
                        },
                    );
                    ui.end_row();

                    render_setting_tile(
                        ui,
                        t(self.language, I18nKey::Timeout),
                        theme_teal(),
                        |ui| {
                            ui.horizontal(|ui| {
                                ui.add(
                                    egui::DragValue::new(&mut self.settings.timeout_secs)
                                        .range(1..=120)
                                        .speed(1),
                                );
                                ui.label(t(self.language, I18nKey::SecondUnit));
                            });
                        },
                    );
                    render_setting_tile(
                        ui,
                        t(self.language, I18nKey::Interval),
                        theme_coral(),
                        |ui| {
                            ui.horizontal(|ui| {
                                ui.add(
                                    egui::DragValue::new(&mut self.settings.interval_ms)
                                        .range(0..=60000)
                                        .speed(1),
                                );
                                ui.label(t(self.language, I18nKey::MillisecondUnit));
                            });
                        },
                    );
                    ui.end_row();
                });

            ui.add_space(6.0);
            inset_frame(theme_surface_soft()).show(ui, |ui| {
                ui.checkbox(
                    &mut self.settings.keep_alive,
                    RichText::new(t(self.language, I18nKey::KeepAlive))
                        .size(12.0)
                        .strong()
                        .color(theme_ink()),
                );

                ui.add_space(10.0);
                ui.columns(2, |cols| {
                    if cols[0]
                        .add_enabled(
                            !self.is_running(),
                            egui::Button::new(
                                RichText::new(t(self.language, I18nKey::StartTest))
                                    .color(Color32::WHITE)
                                    .strong(),
                            )
                            .fill(theme_primary())
                            .stroke(Stroke::NONE)
                            .min_size(egui::vec2(cols[0].available_width(), 34.0)),
                        )
                        .clicked()
                    {
                        self.start_test();
                    }

                    if cols[1]
                        .add_enabled(
                            self.is_running(),
                            egui::Button::new(
                                RichText::new(t(self.language, I18nKey::StopTest))
                                    .color(Color32::WHITE)
                                    .strong(),
                            )
                            .fill(theme_danger())
                            .stroke(Stroke::NONE)
                            .min_size(egui::vec2(cols[1].available_width(), 34.0)),
                        )
                        .clicked()
                    {
                        self.stop_test();
                    }
                });
            });

            if let Some(err) = &self.generic_error {
                ui.add_space(8.0);
                render_error_callout(ui, err);
            }
        });
    }

    fn runtime_error_count(&self) -> u64 {
        self.latest_runtime_metrics.failed_requests + self.latest_runtime_metrics.timeout_requests
    }

    fn runtime_has_latency(&self) -> bool {
        self.latest_runtime_metrics.total_requests > 0
    }

    fn runtime_p95_text(&self) -> String {
        if self.runtime_has_latency() {
            format!("{:.2} ms", self.latest_runtime_metrics.p95_latency_ms)
        } else {
            "-".to_owned()
        }
    }

    fn render_runtime_summary_strip(&self, ui: &mut egui::Ui) {
        hero_card_frame().show(ui, |ui| {
            ui.set_min_height(38.0);
            ui.horizontal_wrapped(|ui| {
                ui.spacing_mut().item_spacing = egui::vec2(8.0, 6.0);
                render_summary_strip_metric(
                    ui,
                    t(self.language, I18nKey::TotalRequests),
                    format!("{}", self.latest_runtime_metrics.total_requests),
                    theme_navy(),
                );
                render_summary_strip_metric(
                    ui,
                    t(self.language, I18nKey::Success),
                    format!("{}", self.latest_runtime_metrics.success_requests),
                    theme_green(),
                );
                render_summary_strip_metric(
                    ui,
                    t(self.language, I18nKey::Qps),
                    format!("{:.1}", self.latest_runtime_metrics.qps),
                    theme_teal(),
                );
                render_summary_strip_metric(
                    ui,
                    t(self.language, I18nKey::Elapsed),
                    format!("{:.2}s", self.latest_runtime_metrics.elapsed_secs),
                    theme_amber(),
                );
                render_summary_strip_metric(
                    ui,
                    t(self.language, I18nKey::Errors),
                    format!("{}", self.runtime_error_count()),
                    theme_coral(),
                );
                render_summary_strip_metric(
                    ui,
                    t(self.language, I18nKey::P95Latency),
                    self.runtime_p95_text(),
                    theme_navy(),
                );
            });
        });
    }

    fn render_final_report_drawer(&mut self, ui: &mut egui::Ui, final_metrics: &FinalMetrics) {
        let is_open = self.final_report_drawer_open;
        let close_label = ">";
        let open_label = "<";
        let collapsed_label = t(self.language, I18nKey::FinalReport)
            .chars()
            .next()
            .unwrap_or('R')
            .to_string();

        egui::Frame::new()
            .fill(theme_surface())
            .stroke(Stroke::new(1.0, theme_line().linear_multiply(0.9)))
            .corner_radius(egui::CornerRadius::same(16))
            .shadow(egui::Shadow {
                offset: [0, 4],
                blur: 14,
                spread: 0,
                color: Color32::from_black_alpha(8),
            })
            .inner_margin(egui::Margin::same(12))
            .show(ui, |ui| {
                if is_open {
                    ui.horizontal(|ui| {
                        ui.label(
                            RichText::new(t(self.language, I18nKey::FinalReport))
                                .size(16.0)
                                .strong()
                                .color(theme_ink()),
                        );
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui
                                .add(
                                    egui::Button::new(close_label)
                                        .fill(theme_surface_soft())
                                        .stroke(Stroke::new(1.0, theme_line()))
                                        .min_size(egui::vec2(28.0, 28.0)),
                                )
                                .clicked()
                            {
                                self.final_report_drawer_open = false;
                            }
                        });
                    });
                    ui.separator();
                    render_report_field(
                        ui,
                        t(self.language, I18nKey::ElapsedTime),
                        format!("{:.2}s", final_metrics.elapsed_secs),
                    );
                    render_report_field(
                        ui,
                        t(self.language, I18nKey::TotalRequestsFinal),
                        format!("{}", final_metrics.total_requests),
                    );
                    render_report_field(
                        ui,
                        t(self.language, I18nKey::SuccessFailTimeout),
                        format!(
                            "{} / {} / {}",
                            final_metrics.success_requests,
                            final_metrics.failed_requests,
                            final_metrics.timeout_requests
                        ),
                    );
                    render_report_field(
                        ui,
                        t(self.language, I18nKey::AvgQps),
                        format!("{:.1}", final_metrics.qps),
                    );
                    render_report_field(
                        ui,
                        t(self.language, I18nKey::HttpStatusCodes),
                        format_status_code_counts(&final_metrics.status_code_counts),
                    );
                    render_report_field(
                        ui,
                        t(self.language, I18nKey::LatencyDetail),
                        format!(
                            "{:.2} / {:.2} / {:.2} / {:.2} / {:.2} ms",
                            final_metrics.avg_latency_ms,
                            final_metrics.p50_latency_ms,
                            final_metrics.p95_latency_ms,
                            final_metrics.p99_latency_ms,
                            final_metrics.max_latency_ms
                        ),
                    );
                } else {
                    ui.vertical(|ui| {
                        ui.add_space(2.0);
                        if ui
                            .add(
                                egui::Button::new(open_label)
                                    .fill(theme_surface_soft())
                                    .stroke(Stroke::new(1.0, theme_line()))
                                    .min_size(egui::vec2(20.0, 32.0)),
                            )
                            .clicked()
                        {
                            self.final_report_drawer_open = true;
                        }
                        ui.add_space(10.0);
                        ui.label(
                            RichText::new(collapsed_label)
                                .size(14.0)
                                .strong()
                                .color(theme_muted()),
                        );
                    });
                }
            });
    }

    fn render_redesigned_ui(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default()
            .frame(egui::Frame::new().fill(theme_bg()))
            .show(ctx, |ui| {
                paint_app_backdrop(ui.painter(), ui.max_rect());
                egui::Frame::new()
                    .inner_margin(egui::Margin {
                        left: 12,
                        right: 28,
                        top: 12,
                        bottom: 12,
                    })
                    .show(ui, |ui| {
                        ui.add_space(10.0);
                        self.render_hero_banner(ui);
                        ui.add_space(12.0);
                        self.render_runtime_summary_strip(ui);
                        ui.add_space(12.0);

                        let total_width = ui.available_width();
                        let content_height = ui.available_height();
                        let gap = 14.0;
                        let left_width = 428.0;
                        let has_final_report = self.final_metrics.is_some();
                        let drawer_width = if has_final_report {
                            if self.final_report_drawer_open {
                                304.0
                            } else {
                                40.0
                            }
                        } else {
                            0.0
                        };
                        let drawer_gap = if has_final_report { gap } else { 0.0 };
                        let middle_width =
                            (total_width - left_width - gap - drawer_gap - drawer_width).max(0.0);

                        ui.horizontal_top(|ui| {
                            ui.spacing_mut().item_spacing.x = 0.0;
                            ui.allocate_ui_with_layout(
                                egui::vec2(left_width, content_height),
                                egui::Layout::top_down(egui::Align::Min),
                                |ui| {
                                    self.render_request_panel(ui);
                                },
                            );
                            ui.add_space(gap);
                            ui.allocate_ui_with_layout(
                                egui::vec2(middle_width, content_height),
                                egui::Layout::top_down(egui::Align::Min),
                                |ui| {
                                    self.render_settings_panel(ui);
                                },
                            );
                            if let Some(final_metrics) = self.final_metrics.clone() {
                                ui.add_space(drawer_gap);
                                ui.allocate_ui_with_layout(
                                    egui::vec2(drawer_width, content_height),
                                    egui::Layout::top_down(egui::Align::Min),
                                    |ui| {
                                        self.render_final_report_drawer(ui, &final_metrics);
                                    },
                                );
                            }
                        });
                    });
            });
    }
}

impl eframe::App for ApiQpsApp {
    #[allow(unreachable_code)]
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.consume_events();
        if self.is_running() {
            ctx.request_repaint_after(std::time::Duration::from_millis(33));
        }

        self.render_redesigned_ui(ctx);
        self.render_expanded_editor_window(ctx);
        return;

        egui::TopBottomPanel::top("top_bar")
            .frame(
                egui::Frame::new()
                    .fill(Color32::from_rgb(255, 255, 255))
                    .inner_margin(egui::Margin::symmetric(24, 12))
                    .shadow(egui::Shadow {
                        offset: [0, 1],
                        blur: 6,
                        spread: 0,
                        color: Color32::from_black_alpha(10),
                    }),
            )
            .resizable(false)
            .exact_height(64.0)
            .show(ctx, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.label(
                        RichText::new(t(self.language, I18nKey::AppTitle))
                            .size(18.0)
                            .strong()
                            .color(Color32::from_rgb(29, 29, 31)),
                    );
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

                        ui.label(
                            RichText::new(format!(
                                "{}: {}",
                                t(self.language, I18nKey::Target),
                                if self.request_draft.api_url.is_empty() {
                                    "http://127.0.0.1/api"
                                } else {
                                    self.request_draft.api_url.as_str()
                                }
                            ))
                            .color(Color32::from_rgb(142, 142, 147)),
                        );
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
                                    ui.label(
                                        RichText::new(t(self.language, I18nKey::RequestBuilder))
                                            .size(16.0)
                                            .strong(),
                                    );
                                    ui.separator();
                                    ui.label(t(self.language, I18nKey::CurlLabel));
                                    egui::ScrollArea::vertical()
                                        .max_height(100.0)
                                        .show(ui, |ui| {
                                            let response = ui.add_sized(
                                                [ui.available_width(), 100.0],
                                                TextEdit::multiline(&mut self.curl_input)
                                                    .font(FontId::new(13.0, FontFamily::Monospace))
                                                    .code_editor()
                                                    .desired_width(f32::INFINITY)
                                                    .hint_text(t(
                                                        self.language,
                                                        I18nKey::InputPlaceholder,
                                                    )),
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
                                                for method in [
                                                    "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD",
                                                ] {
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
                                            egui::ScrollArea::vertical().max_height(150.0).show(
                                                ui,
                                                |ui| {
                                                    ui.add_sized(
                                                        [ui.available_width(), 150.0],
                                                        TextEdit::multiline(
                                                            &mut self.request_draft.headers_json,
                                                        )
                                                        .font(FontId::new(
                                                            13.0,
                                                            FontFamily::Monospace,
                                                        ))
                                                        .code_editor()
                                                        .desired_width(f32::INFINITY),
                                                    );
                                                },
                                            );
                                        });
                                        cols[1].vertical(|ui| {
                                            ui.label(t(self.language, I18nKey::RequestBody));
                                            let supports_body = Self::method_text_supports_body(
                                                self.request_draft.method.trim(),
                                            );
                                            ui.add_enabled_ui(supports_body, |ui| {
                                                egui::ScrollArea::vertical()
                                                    .max_height(150.0)
                                                    .show(ui, |ui| {
                                                        ui.add_sized(
                                                            [ui.available_width(), 150.0],
                                                            TextEdit::multiline(
                                                                &mut self.request_draft.body,
                                                            )
                                                            .font(FontId::new(
                                                                13.0,
                                                                FontFamily::Monospace,
                                                            ))
                                                            .code_editor()
                                                            .desired_width(f32::INFINITY),
                                                        );
                                                    });
                                            });
                                            if !supports_body {
                                                ui.label(
                                                    RichText::new(t(
                                                        self.language,
                                                        I18nKey::BodyNotRequired,
                                                    ))
                                                    .size(11.0)
                                                    .color(Color32::GRAY),
                                                );
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
                                        ui.label(
                                            RichText::new(t(
                                                self.language,
                                                I18nKey::LoadTestConfig,
                                            ))
                                            .size(16.0)
                                            .strong(),
                                        );
                                        ui.with_layout(
                                            egui::Layout::right_to_left(egui::Align::Center),
                                            |ui| {
                                                if ui
                                                    .add_enabled(
                                                        self.is_running(),
                                                        egui::Button::new(t(
                                                            self.language,
                                                            I18nKey::StopTest,
                                                        ))
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
                                                        egui::Button::new(t(
                                                            self.language,
                                                            I18nKey::StartTest,
                                                        ))
                                                        .fill(Color32::from_rgb(0, 122, 255))
                                                        .stroke(Stroke::NONE)
                                                        .min_size(egui::vec2(80.0, 24.0)),
                                                    )
                                                    .clicked()
                                                {
                                                    self.start_test();
                                                }
                                            },
                                        );
                                    });
                                    ui.separator();

                                    // Use a grid for settings to keep them aligned
                                    egui::Grid::new("settings_grid").spacing([20.0, 10.0]).show(
                                        ui,
                                        |ui| {
                                            ui.horizontal(|ui| {
                                                ui.label(t(self.language, I18nKey::Concurrency));
                                                ui.add(
                                                    egui::DragValue::new(
                                                        &mut self.settings.concurrency,
                                                    )
                                                    .range(1..=20000),
                                                );
                                            });
                                            ui.horizontal(|ui| {
                                                ui.label(t(self.language, I18nKey::Duration));
                                                ui.add(
                                                    egui::DragValue::new(
                                                        &mut self.settings.duration_secs,
                                                    )
                                                    .range(1..=86400),
                                                );
                                                ui.label(t(self.language, I18nKey::SecondUnit));
                                            });
                                            ui.horizontal(|ui| {
                                                ui.label(t(self.language, I18nKey::Timeout));
                                                ui.add(
                                                    egui::DragValue::new(
                                                        &mut self.settings.timeout_secs,
                                                    )
                                                    .range(1..=120),
                                                );
                                                ui.label(t(self.language, I18nKey::SecondUnit));
                                            });
                                            ui.horizontal(|ui| {
                                                ui.label(t(self.language, I18nKey::Interval));
                                                ui.add(
                                                    egui::DragValue::new(
                                                        &mut self.settings.interval_ms,
                                                    )
                                                    .range(0..=60000),
                                                );
                                                ui.label(t(
                                                    self.language,
                                                    I18nKey::MillisecondUnit,
                                                ));
                                            });
                                            ui.end_row();
                                        },
                                    );

                                    ui.add_space(8.0);
                                    ui.checkbox(
                                        &mut self.settings.keep_alive,
                                        t(self.language, I18nKey::KeepAlive),
                                    );

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
                                    ui.label(
                                        RichText::new(t(self.language, I18nKey::RuntimeMetrics))
                                            .size(16.0)
                                            .strong(),
                                    );
                                    ui.separator();

                                    // Use a grid to ensure all metric cards have consistent size
                                    let errors = self.latest_runtime_metrics.failed_requests
                                        + self.latest_runtime_metrics.timeout_requests;
                                    let has_runtime_latency =
                                        self.latest_runtime_metrics.total_requests > 0;
                                    let p95 = if has_runtime_latency {
                                        format!(
                                            "{:.2} ms",
                                            self.latest_runtime_metrics.p95_latency_ms
                                        )
                                    } else {
                                        "-".to_owned()
                                    };

                                    egui::Grid::new("metrics_grid")
                                        .spacing([0.0, 0.0]) // Spacing handled by card margins
                                        .min_col_width((ui.available_width()) / 3.0)
                                        .show(ui, |ui| {
                                            render_metric_card(
                                                ui,
                                                t(self.language, I18nKey::TotalRequests),
                                                format!(
                                                    "{}",
                                                    self.latest_runtime_metrics.total_requests
                                                ),
                                            );
                                            render_metric_card(
                                                ui,
                                                t(self.language, I18nKey::Success),
                                                format!(
                                                    "{}",
                                                    self.latest_runtime_metrics.success_requests
                                                ),
                                            );
                                            render_metric_card(
                                                ui,
                                                t(self.language, I18nKey::Qps),
                                                format!("{:.1}", self.latest_runtime_metrics.qps),
                                            );
                                            ui.end_row();

                                            render_metric_card(
                                                ui,
                                                t(self.language, I18nKey::Elapsed),
                                                format!(
                                                    "{:.2}s",
                                                    self.latest_runtime_metrics.elapsed_secs
                                                ),
                                            );
                                            render_metric_card(
                                                ui,
                                                t(self.language, I18nKey::Errors),
                                                format!("{}", errors),
                                            );
                                            render_metric_card(
                                                ui,
                                                t(self.language, I18nKey::P95Latency),
                                                p95,
                                            );
                                            ui.end_row();
                                        });

                                    ui.add_space(6.0);
                                    render_qps_trend_chart(ui, self.language, &self.metric_history);
                                    ui.add_space(8.0);
                                    render_latency_trend_chart(
                                        ui,
                                        self.language,
                                        &self.metric_history,
                                    );
                                    ui.add_space(6.0);
                                    ui.label(
                                        RichText::new(t(self.language, I18nKey::LatencyDetail))
                                            .size(13.0)
                                            .strong()
                                            .color(Color32::from_rgb(29, 29, 31)),
                                    );
                                    if has_runtime_latency {
                                        ui.label(format!(
                                            "{:.2} / {:.2} / {:.2} / {:.2} / {:.2} ms",
                                            self.latest_runtime_metrics.avg_latency_ms,
                                            self.latest_runtime_metrics.p50_latency_ms,
                                            self.latest_runtime_metrics.p95_latency_ms,
                                            self.latest_runtime_metrics.p99_latency_ms,
                                            self.latest_runtime_metrics.max_latency_ms
                                        ));
                                    } else {
                                        ui.label(
                                            RichText::new(t(self.language, I18nKey::WaitingData))
                                                .size(12.0)
                                                .color(Color32::from_rgb(142, 142, 147)),
                                        );
                                    }
                                    ui.add_space(6.0);
                                    render_status_code_bars(
                                        ui,
                                        self.language,
                                        &self.latest_runtime_metrics.status_code_counts,
                                        self.latest_runtime_metrics.transport_error_requests,
                                    );
                                    if let Some(final_metrics) = &self.final_metrics {
                                        ui.separator();
                                        ui.label(
                                            RichText::new(t(self.language, I18nKey::FinalReport))
                                                .strong(),
                                        );
                                        egui::Grid::new("final_report_grid")
                                            .num_columns(2)
                                            .spacing([12.0, 6.0])
                                            .show(ui, |ui| {
                                                ui.label(
                                                    RichText::new(t(
                                                        self.language,
                                                        I18nKey::ElapsedTime,
                                                    ))
                                                    .color(Color32::from_rgb(142, 142, 147)),
                                                );
                                                ui.label(format!(
                                                    "{:.2}s",
                                                    final_metrics.elapsed_secs
                                                ));
                                                ui.end_row();
                                                ui.label(
                                                    RichText::new(t(
                                                        self.language,
                                                        I18nKey::TotalRequestsFinal,
                                                    ))
                                                    .color(Color32::from_rgb(142, 142, 147)),
                                                );
                                                ui.label(format!(
                                                    "{}",
                                                    final_metrics.total_requests
                                                ));
                                                ui.end_row();
                                                ui.label(
                                                    RichText::new(t(
                                                        self.language,
                                                        I18nKey::SuccessFailTimeout,
                                                    ))
                                                    .color(Color32::from_rgb(142, 142, 147)),
                                                );
                                                ui.label(format!(
                                                    "{} / {} / {}",
                                                    final_metrics.success_requests,
                                                    final_metrics.failed_requests,
                                                    final_metrics.timeout_requests
                                                ));
                                                ui.end_row();
                                                ui.label(
                                                    RichText::new(t(
                                                        self.language,
                                                        I18nKey::AvgQps,
                                                    ))
                                                    .color(Color32::from_rgb(142, 142, 147)),
                                                );
                                                ui.label(format!("{:.1}", final_metrics.qps));
                                                ui.end_row();
                                                ui.label(
                                                    RichText::new(t(
                                                        self.language,
                                                        I18nKey::LatencyDetail,
                                                    ))
                                                    .color(Color32::from_rgb(142, 142, 147)),
                                                );
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

#[derive(Clone)]
struct TrendSeries<'a> {
    label: &'a str,
    color: Color32,
    points: Vec<(f64, f64)>,
}

#[derive(Clone, Copy)]
enum TrendValueFormat {
    Qps,
    LatencyMs,
}

fn theme_bg() -> Color32 {
    Color32::from_rgb(242, 244, 247)
}

fn theme_surface() -> Color32 {
    Color32::from_rgb(255, 255, 255)
}

fn theme_surface_soft() -> Color32 {
    Color32::from_rgb(247, 249, 252)
}

fn theme_ink() -> Color32 {
    Color32::from_rgb(28, 33, 43)
}

fn theme_muted() -> Color32 {
    Color32::from_rgb(107, 117, 131)
}

fn theme_line() -> Color32 {
    Color32::from_rgb(218, 224, 232)
}

fn theme_navy() -> Color32 {
    Color32::from_rgb(45, 62, 92)
}

fn theme_coral() -> Color32 {
    Color32::from_rgb(193, 87, 72)
}

fn theme_teal() -> Color32 {
    Color32::from_rgb(56, 120, 150)
}

fn theme_amber() -> Color32 {
    Color32::from_rgb(179, 132, 58)
}

fn theme_green() -> Color32 {
    Color32::from_rgb(76, 132, 92)
}

fn theme_primary() -> Color32 {
    Color32::from_rgb(58, 99, 160)
}

fn theme_danger() -> Color32 {
    Color32::from_rgb(177, 72, 72)
}

fn paint_app_backdrop(painter: &egui::Painter, rect: egui::Rect) {
    painter.rect_filled(rect, egui::CornerRadius::ZERO, theme_bg());
}

fn hero_card_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(theme_surface())
        .stroke(Stroke::new(1.0, theme_line()))
        .corner_radius(egui::CornerRadius::same(14))
        .shadow(egui::Shadow {
            offset: [0, 4],
            blur: 12,
            spread: 0,
            color: Color32::from_black_alpha(8),
        })
        .inner_margin(egui::Margin::same(10))
}

fn inset_frame(fill: Color32) -> egui::Frame {
    egui::Frame::new()
        .fill(fill)
        .stroke(Stroke::new(1.0, theme_line().linear_multiply(0.9)))
        .corner_radius(egui::CornerRadius::same(12))
        .inner_margin(egui::Margin::same(12))
}

fn capsule_frame(fill: Color32, stroke: Color32) -> egui::Frame {
    egui::Frame::new()
        .fill(fill)
        .stroke(Stroke::new(1.0, stroke))
        .corner_radius(egui::CornerRadius::same(255))
        .inner_margin(egui::Margin::symmetric(8, 4))
}

fn card(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui)) {
    let frame_width = ui.available_width();
    egui::Frame::new()
        .fill(theme_surface())
        .stroke(Stroke::new(1.0, theme_line().linear_multiply(0.9)))
        .corner_radius(egui::CornerRadius::same(16))
        .shadow(egui::Shadow {
            offset: [0, 4],
            blur: 14,
            spread: 0,
            color: Color32::from_black_alpha(8),
        })
        .inner_margin(egui::Margin::same(16))
        .show(ui, |ui| {
            ui.set_width((frame_width - 32.0).max(0.0));
            add_contents(ui);
        });
}

fn panel_header(ui: &mut egui::Ui, title: &str, add_right: impl FnOnce(&mut egui::Ui)) {
    ui.horizontal(|ui| {
        ui.spacing_mut().item_spacing.x = 0.0;
        ui.label(RichText::new(title).size(17.0).strong().color(theme_ink()));
        let remaining_width = ui.available_width().max(0.0);
        ui.allocate_ui_with_layout(
            egui::vec2(remaining_width, 24.0),
            egui::Layout::right_to_left(egui::Align::Center),
            |ui| {
                add_right(ui);
            },
        );
    });
    ui.add_space(2.0);
}

fn render_report_field(ui: &mut egui::Ui, label: &str, value: String) {
    ui.label(
        RichText::new(label)
            .size(11.5)
            .strong()
            .color(theme_muted()),
    );
    ui.label(RichText::new(value).size(13.0).color(theme_ink()));
    ui.add_space(8.0);
}

fn format_status_code_counts(status_code_counts: &[(u16, u64)]) -> String {
    if status_code_counts.is_empty() {
        return "-".to_owned();
    }

    status_code_counts
        .iter()
        .map(|(code, count)| format!("{code}: {count}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_summary_strip_metric(ui: &mut egui::Ui, label: &str, value: String, accent: Color32) {
    capsule_frame(accent.linear_multiply(0.08), accent.linear_multiply(0.22)).show(ui, |ui| {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 6.0;
            ui.colored_label(accent, "■");
            ui.label(RichText::new(label).size(11.0).color(theme_muted()));
            ui.label(RichText::new(value).size(13.0).strong().color(theme_ink()));
        });
    });
}

fn render_pill(ui: &mut egui::Ui, text: &str, fill: Color32, text_color: Color32, stroke: Color32) {
    capsule_frame(fill, stroke).show(ui, |ui| {
        ui.label(RichText::new(text).size(11.5).color(text_color));
    });
}

fn truncate_middle(text: &str, max_chars: usize) -> String {
    let total_chars = text.chars().count();
    if total_chars <= max_chars || max_chars <= 3 {
        return text.to_owned();
    }

    let head_len = (max_chars.saturating_sub(1) * 2) / 3;
    let tail_len = max_chars.saturating_sub(head_len + 1);
    let head: String = text.chars().take(head_len).collect();
    let tail: String = text
        .chars()
        .rev()
        .take(tail_len)
        .collect::<String>()
        .chars()
        .rev()
        .collect();

    format!("{head}…{tail}")
}

fn render_setting_tile(
    ui: &mut egui::Ui,
    label: &str,
    accent: Color32,
    add_control: impl FnOnce(&mut egui::Ui),
) {
    inset_frame(accent.linear_multiply(0.08)).show(ui, |ui| {
        ui.set_min_height(84.0);
        ui.label(RichText::new(label).size(12.0).strong().color(accent));
        ui.add_space(8.0);
        add_control(ui);
    });
}

fn render_error_callout(ui: &mut egui::Ui, text: &str) {
    inset_frame(theme_coral().linear_multiply(0.10)).show(ui, |ui| {
        ui.label(RichText::new(text).size(12.5).color(theme_coral()));
    });
}

fn render_fixed_code_input(
    ui: &mut egui::Ui,
    text: &mut String,
    height: f32,
    hint_text: Option<&str>,
) -> egui::Response {
    let width = ui.available_width();
    let outer_size = egui::vec2(width, height);
    let (outer_rect, outer_response) = ui.allocate_exact_size(outer_size, Sense::hover());

    ui.painter().rect_filled(
        outer_rect,
        egui::CornerRadius::same(12),
        theme_surface_soft(),
    );
    ui.painter().rect_stroke(
        outer_rect,
        egui::CornerRadius::same(12),
        Stroke::new(1.0, theme_line()),
        egui::StrokeKind::Outside,
    );

    let content_rect = outer_rect.shrink2(egui::vec2(8.0, 8.0));
    let mut content_ui = ui.new_child(
        egui::UiBuilder::new()
            .max_rect(content_rect)
            .layout(egui::Layout::top_down(egui::Align::Min)),
    );

    let mut edit = TextEdit::multiline(text)
        .frame(false)
        .font(FontId::new(13.0, FontFamily::Monospace))
        .code_editor()
        .desired_width(f32::INFINITY)
        .desired_rows(((height - 16.0) / 18.0).round().max(3.0) as usize);
    if let Some(hint_text) = hint_text {
        edit = edit.hint_text(hint_text);
    }

    let edit_response = egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .max_height(content_rect.height())
        .show(&mut content_ui, |ui| {
            ui.set_min_width(content_rect.width());
            ui.add_sized([content_rect.width(), content_rect.height()], edit)
        })
        .inner;

    edit_response | outer_response
}

fn render_metric_card(ui: &mut egui::Ui, label: &str, value: String) {
    egui::Frame::new()
        .fill(theme_surface_soft())
        .stroke(Stroke::new(1.0, theme_line()))
        .corner_radius(egui::CornerRadius::same(18))
        .shadow(egui::Shadow {
            offset: [0, 2],
            blur: 6,
            spread: 0,
            color: Color32::from_black_alpha(10),
        })
        .inner_margin(egui::Margin::same(16))
        .show(ui, |ui| {
            ui.set_min_width(100.0);
            ui.label(RichText::new(label).size(13.0).color(theme_muted()));
            ui.add_space(4.0);
            ui.label(
                RichText::new(value)
                    .font(FontId::new(24.0, FontFamily::Proportional))
                    .strong()
                    .color(theme_ink()),
            );
        });
}

fn render_qps_trend_chart(ui: &mut egui::Ui, language: Language, history: &[MetricHistoryPoint]) {
    let qps_color = theme_teal();

    render_trend_header(ui, t(language, I18nKey::QpsTrend), |ui| {
        if let Some(sample) = history.last() {
            render_trend_badge(
                ui,
                t(language, I18nKey::Qps),
                format!("{:.1}", sample.qps),
                qps_color,
            );
        }
    });

    let series = [TrendSeries {
        label: t(language, I18nKey::Qps),
        color: qps_color,
        points: history
            .iter()
            .map(|point| (point.elapsed_secs, point.qps))
            .collect(),
    }];

    render_trend_chart(ui, language, &series, TrendValueFormat::Qps);
}

fn render_latency_trend_chart(
    ui: &mut egui::Ui,
    language: Language,
    history: &[MetricHistoryPoint],
) {
    let avg_color = theme_amber();
    let p95_color = theme_coral();

    render_trend_header(ui, t(language, I18nKey::LatencyTrend), |ui| {
        if let Some(sample) = history.last() {
            render_trend_badge(
                ui,
                t(language, I18nKey::AvgLatency),
                format!("{:.2} ms", sample.avg_latency_ms),
                avg_color,
            );
            render_trend_badge(
                ui,
                t(language, I18nKey::P95Latency),
                format!("{:.2} ms", sample.p95_latency_ms),
                p95_color,
            );
        }
    });

    let series = [
        TrendSeries {
            label: t(language, I18nKey::AvgLatency),
            color: avg_color,
            points: history
                .iter()
                .map(|point| (point.elapsed_secs, point.avg_latency_ms))
                .collect(),
        },
        TrendSeries {
            label: t(language, I18nKey::P95Latency),
            color: p95_color,
            points: history
                .iter()
                .map(|point| (point.elapsed_secs, point.p95_latency_ms))
                .collect(),
        },
    ];

    render_trend_chart(ui, language, &series, TrendValueFormat::LatencyMs);
}

fn render_trend_header(ui: &mut egui::Ui, title: &str, add_badges: impl FnOnce(&mut egui::Ui)) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(title).size(12.5).strong().color(theme_ink()));
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            add_badges(ui);
        });
    });
    ui.add_space(4.0);
}

fn render_trend_badge(ui: &mut egui::Ui, label: &str, value: String, color: Color32) {
    ui.horizontal(|ui| {
        ui.colored_label(color, "■");
        ui.label(
            RichText::new(format!("{label}: {value}"))
                .size(11.0)
                .color(theme_muted()),
        );
    });
}

fn render_trend_chart(
    ui: &mut egui::Ui,
    language: Language,
    series: &[TrendSeries<'_>],
    value_format: TrendValueFormat,
) {
    let height = 136.0;
    let (response, painter) =
        ui.allocate_painter(egui::vec2(ui.available_width(), height), Sense::hover());
    let rect = response.rect;

    painter.rect_filled(rect, egui::CornerRadius::same(12), theme_surface_soft());
    painter.rect_stroke(
        rect,
        egui::CornerRadius::same(12),
        Stroke::new(1.0, theme_line()),
        egui::StrokeKind::Outside,
    );

    let margin_left = 52.0;
    let margin_right = 14.0;
    let margin_top = 12.0;
    let margin_bottom = 28.0;
    let chart_rect = egui::Rect::from_min_max(
        rect.min + egui::vec2(margin_left, margin_top),
        rect.max - egui::vec2(margin_right, margin_bottom),
    );

    let font_id = FontId::new(10.5, FontFamily::Proportional);
    let text_color = theme_muted();
    if !series.iter().any(|series| !series.points.is_empty()) {
        painter.text(
            chart_rect.center(),
            egui::Align2::CENTER_CENTER,
            t(language, I18nKey::WaitingData),
            FontId::new(12.0, FontFamily::Proportional),
            text_color,
        );
        return;
    }

    let x_min = series
        .iter()
        .filter_map(|series| series.points.first().map(|(elapsed, _)| *elapsed))
        .fold(f64::INFINITY, f64::min);
    let mut x_max = series
        .iter()
        .filter_map(|series| series.points.last().map(|(elapsed, _)| *elapsed))
        .fold(0.0, f64::max);
    if (x_max - x_min).abs() < f64::EPSILON {
        x_max = x_min + 1.0;
    }

    let raw_y_max = series
        .iter()
        .flat_map(|series| series.points.iter().map(|(_, value)| *value))
        .fold(0.0, f64::max);
    let y_max = nice_axis_upper((raw_y_max * 1.1).max(1.0));

    let horizontal_steps = 3;
    for i in 0..=horizontal_steps {
        let t = i as f32 / horizontal_steps as f32;
        let y = chart_rect.bottom() - t * chart_rect.height();
        let label_value = y_max * t as f64;
        painter.line_segment(
            [
                egui::pos2(chart_rect.left(), y),
                egui::pos2(chart_rect.right(), y),
            ],
            Stroke::new(1.0, theme_line().linear_multiply(0.35)),
        );
        painter.text(
            egui::pos2(chart_rect.left() - 6.0, y),
            egui::Align2::RIGHT_CENTER,
            format_trend_axis_value(label_value, value_format),
            font_id.clone(),
            text_color,
        );
    }

    let vertical_steps = 4;
    for i in 0..=vertical_steps {
        let t = i as f32 / vertical_steps as f32;
        let x = chart_rect.left() + t * chart_rect.width();
        let elapsed = x_min + (x_max - x_min) * t as f64;
        painter.line_segment(
            [
                egui::pos2(x, chart_rect.top()),
                egui::pos2(x, chart_rect.bottom()),
            ],
            Stroke::new(1.0, theme_line().linear_multiply(0.22)),
        );
        painter.text(
            egui::pos2(x, chart_rect.bottom() + 6.0),
            egui::Align2::CENTER_TOP,
            format_elapsed_axis_label(elapsed, language),
            font_id.clone(),
            text_color,
        );
    }

    painter.line_segment(
        [
            egui::pos2(chart_rect.left(), chart_rect.top()),
            egui::pos2(chart_rect.left(), chart_rect.bottom()),
        ],
        Stroke::new(1.0, theme_line()),
    );
    painter.line_segment(
        [
            egui::pos2(chart_rect.left(), chart_rect.bottom()),
            egui::pos2(chart_rect.right(), chart_rect.bottom()),
        ],
        Stroke::new(1.0, theme_line()),
    );

    let chart_painter = painter.with_clip_rect(chart_rect.expand2(egui::vec2(2.0, 2.0)));
    let max_plot_points = ((chart_rect.width().max(64.0) as usize) * 2).max(64);
    for series in series {
        let sampled = sample_trend_points(&series.points, max_plot_points);
        let points: Vec<egui::Pos2> = sampled
            .iter()
            .map(|(elapsed, value)| {
                trend_point_to_pos(*elapsed, *value, x_min, x_max, y_max, chart_rect)
            })
            .collect();

        if points.len() >= 2 {
            chart_painter.add(egui::Shape::line(
                points.clone(),
                Stroke::new(2.0, series.color),
            ));
        }
        if let Some(last) = points.last() {
            chart_painter.circle_filled(*last, 3.5, series.color);
        }
    }

    if let Some(pointer) = response.hover_pos().filter(|pos| chart_rect.contains(*pos)) {
        let elapsed =
            x_min + ((pointer.x - chart_rect.left()) / chart_rect.width()) as f64 * (x_max - x_min);
        painter.line_segment(
            [
                egui::pos2(pointer.x, chart_rect.top()),
                egui::pos2(pointer.x, chart_rect.bottom()),
            ],
            Stroke::new(1.0, theme_line()),
        );
        render_trend_tooltip(
            &painter,
            rect,
            pointer,
            elapsed,
            series,
            language,
            value_format,
        );
    }
}

fn render_trend_tooltip(
    painter: &egui::Painter,
    bounds: egui::Rect,
    anchor: egui::Pos2,
    elapsed: f64,
    series: &[TrendSeries<'_>],
    language: Language,
    value_format: TrendValueFormat,
) {
    let mut lines = vec![format_elapsed_axis_label(elapsed, language)];
    let mut colors = vec![Color32::WHITE];

    for series in series {
        if let Some((_, value)) = nearest_trend_point(&series.points, elapsed) {
            lines.push(format!(
                "{}: {}",
                series.label,
                format_trend_tooltip_value(*value, value_format)
            ));
            colors.push(series.color);
        }
    }

    let font_id = FontId::new(12.0, FontFamily::Proportional);
    let line_height = 16.0;
    let padding = egui::vec2(10.0, 8.0);
    let max_width = lines
        .iter()
        .map(|line| {
            painter
                .layout_no_wrap(line.clone(), font_id.clone(), Color32::WHITE)
                .rect
                .width()
        })
        .fold(0.0, f32::max);
    let tooltip_size = egui::vec2(
        max_width + padding.x * 2.0,
        lines.len() as f32 * line_height + padding.y * 2.0,
    );
    let mut tooltip_pos = anchor + egui::vec2(12.0, -tooltip_size.y - 8.0);
    if tooltip_pos.x + tooltip_size.x > bounds.right() {
        tooltip_pos.x = bounds.right() - tooltip_size.x;
    }
    if tooltip_pos.x < bounds.left() {
        tooltip_pos.x = bounds.left();
    }
    if tooltip_pos.y < bounds.top() {
        tooltip_pos.y = anchor.y + 10.0;
    }
    let tooltip_rect = egui::Rect::from_min_size(tooltip_pos, tooltip_size);

    painter.rect_filled(
        tooltip_rect,
        egui::CornerRadius::same(8),
        Color32::from_rgb(29, 29, 31),
    );

    for (idx, (line, color)) in lines.iter().zip(colors.iter()).enumerate() {
        painter.text(
            tooltip_rect.min + egui::vec2(padding.x, padding.y + idx as f32 * line_height),
            egui::Align2::LEFT_TOP,
            line,
            font_id.clone(),
            *color,
        );
    }
}

fn trend_point_to_pos(
    elapsed_secs: f64,
    value: f64,
    min_elapsed: f64,
    max_elapsed: f64,
    max_value: f64,
    chart_rect: egui::Rect,
) -> egui::Pos2 {
    let x_ratio = if max_elapsed <= min_elapsed {
        0.0
    } else {
        ((elapsed_secs - min_elapsed) / (max_elapsed - min_elapsed)).clamp(0.0, 1.0)
    };
    let y_ratio = if max_value <= 0.0 {
        0.0
    } else {
        (value / max_value).clamp(0.0, 1.0)
    };

    egui::pos2(
        chart_rect.left() + chart_rect.width() * x_ratio as f32,
        chart_rect.bottom() - chart_rect.height() * y_ratio as f32,
    )
}

fn sample_trend_points(points: &[(f64, f64)], max_points: usize) -> Vec<(f64, f64)> {
    if points.len() <= max_points || max_points < 2 {
        return points.to_vec();
    }

    let stride = points.len().div_ceil(max_points);
    let mut sampled: Vec<(f64, f64)> = points.iter().step_by(stride).copied().collect();
    if sampled.last() != points.last() {
        if let Some(last) = points.last() {
            sampled.push(*last);
        }
    }
    sampled
}

fn nearest_trend_point(points: &[(f64, f64)], elapsed: f64) -> Option<&(f64, f64)> {
    match points.binary_search_by(|(point_elapsed, _)| point_elapsed.total_cmp(&elapsed)) {
        Ok(idx) => points.get(idx),
        Err(idx) => match (idx.checked_sub(1), points.get(idx)) {
            (Some(left_idx), Some(right)) => {
                let left = &points[left_idx];
                if (elapsed - left.0).abs() <= (right.0 - elapsed).abs() {
                    Some(left)
                } else {
                    Some(right)
                }
            }
            (Some(left_idx), None) => points.get(left_idx),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        },
    }
}

fn push_metric_history_sample(history: &mut Vec<MetricHistoryPoint>, sample: MetricHistoryPoint) {
    if let Some(last) = history.last_mut() {
        if sample.elapsed_secs <= last.elapsed_secs {
            *last = sample;
            return;
        }
    }

    history.push(sample);
    if history.len() > MAX_TREND_POINTS {
        compact_metric_history(history);
    }
}

fn compact_metric_history(history: &mut Vec<MetricHistoryPoint>) {
    if history.len() <= MAX_TREND_POINTS {
        return;
    }

    let last = history.last().copied();
    let mut compacted: Vec<MetricHistoryPoint> = history.iter().step_by(2).copied().collect();
    if compacted.last().copied() != last {
        if let Some(last) = last {
            compacted.push(last);
        }
    }
    *history = compacted;
}

fn nice_axis_upper(max_value: f64) -> f64 {
    if max_value <= 0.0 {
        return 1.0;
    }

    let magnitude = 10f64.powf(max_value.log10().floor());
    for step in [1.0, 2.0, 5.0, 10.0] {
        let candidate = magnitude * step;
        if candidate >= max_value {
            return candidate;
        }
    }
    magnitude * 10.0
}

fn format_trend_axis_value(value: f64, value_format: TrendValueFormat) -> String {
    match value_format {
        TrendValueFormat::Qps => format_compact_number(value),
        TrendValueFormat::LatencyMs => {
            if value >= 100.0 {
                format!("{value:.0}")
            } else if value >= 10.0 {
                format!("{value:.1}")
            } else {
                format!("{value:.2}")
            }
        }
    }
}

fn format_trend_tooltip_value(value: f64, value_format: TrendValueFormat) -> String {
    match value_format {
        TrendValueFormat::Qps => format!("{value:.1}"),
        TrendValueFormat::LatencyMs => format!("{value:.2} ms"),
    }
}

fn format_compact_number(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1}k", value / 1_000.0)
    } else if value >= 100.0 {
        format!("{value:.0}")
    } else if value >= 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.2}")
    }
}

fn format_elapsed_axis_label(elapsed_secs: f64, language: Language) -> String {
    if elapsed_secs >= 60.0 {
        let minutes = (elapsed_secs / 60.0).floor() as u64;
        let seconds = (elapsed_secs % 60.0).round() as u64;
        format!("{minutes}m{seconds:02}{}", t(language, I18nKey::SecondUnit))
    } else if elapsed_secs >= 10.0 {
        format!("{elapsed_secs:.0}{}", t(language, I18nKey::SecondUnit))
    } else {
        format!("{elapsed_secs:.1}{}", t(language, I18nKey::SecondUnit))
    }
}

fn render_status_code_bars(
    ui: &mut egui::Ui,
    language: Language,
    status_counts: &[(u16, u64)],
    transport_error_requests: u64,
) {
    ui.label(
        RichText::new(t(language, I18nKey::StatusCodeDist))
            .size(13.0)
            .strong()
            .color(theme_ink()),
    );
    let height = 156.0;
    let (response, painter) =
        ui.allocate_painter(egui::vec2(ui.available_width(), height), Sense::hover());
    let rect = response.rect;

    painter.rect_filled(rect, egui::CornerRadius::same(12), theme_surface_soft());
    painter.rect_stroke(
        rect,
        egui::CornerRadius::same(12),
        Stroke::new(1.0, theme_line()),
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
    let text_color = theme_muted();

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

    let mut bars: Vec<(StatusCodeLabel, u64, Color32)> =
        Vec::with_capacity(status_counts.len() + 1);
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

    let max_count = bars
        .iter()
        .map(|(_, count, _)| *count)
        .max()
        .unwrap_or(1)
        .max(1);
    let max_axis = ((max_count as f64) * 1.2).ceil().max(1.0) as u64;

    let grid_steps = 3;
    for i in 0..=grid_steps {
        let t = i as f32 / grid_steps as f32;
        let y = chart_rect.bottom() - t * chart_rect.height();
        let label_value = ((t as f64) * max_axis as f64).round() as u64;
        painter.line_segment(
            [
                egui::pos2(chart_rect.left(), y),
                egui::pos2(chart_rect.right(), y),
            ],
            Stroke::new(1.0, theme_line().linear_multiply(0.35)),
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
        [
            egui::pos2(chart_rect.left(), chart_rect.top()),
            egui::pos2(chart_rect.left(), chart_rect.bottom()),
        ],
        Stroke::new(1.0, theme_line()),
    );
    painter.line_segment(
        [
            egui::pos2(chart_rect.left(), chart_rect.bottom()),
            egui::pos2(chart_rect.right(), chart_rect.bottom()),
        ],
        Stroke::new(1.0, theme_line()),
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
        let bar_rect = egui::Rect::from_min_max(
            egui::pos2(left, top),
            egui::pos2(right, chart_rect.bottom()),
        );

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
        200..=299 => theme_green(),
        300..=399 => theme_teal(),
        400..=499 => theme_amber(),
        500..=599 => theme_coral(),
        _ => theme_muted(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_history_replaces_duplicate_timestamp() {
        let mut history = Vec::new();

        push_metric_history_sample(
            &mut history,
            MetricHistoryPoint {
                elapsed_secs: 1.0,
                qps: 10.0,
                avg_latency_ms: 5.0,
                p95_latency_ms: 8.0,
            },
        );
        push_metric_history_sample(
            &mut history,
            MetricHistoryPoint {
                elapsed_secs: 1.0,
                qps: 20.0,
                avg_latency_ms: 6.0,
                p95_latency_ms: 9.0,
            },
        );

        assert_eq!(history.len(), 1);
        assert_eq!(history[0].qps, 20.0);
    }

    #[test]
    fn metric_history_compaction_keeps_latest_point() {
        let mut history = Vec::new();
        for idx in 0..=MAX_TREND_POINTS {
            push_metric_history_sample(
                &mut history,
                MetricHistoryPoint {
                    elapsed_secs: idx as f64,
                    qps: idx as f64,
                    avg_latency_ms: idx as f64,
                    p95_latency_ms: idx as f64,
                },
            );
        }

        assert!(history.len() <= MAX_TREND_POINTS);
        assert_eq!(
            history.last().unwrap().elapsed_secs,
            MAX_TREND_POINTS as f64
        );
    }
}
