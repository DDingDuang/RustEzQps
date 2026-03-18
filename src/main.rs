#![cfg_attr(all(not(debug_assertions), target_os = "windows"), windows_subsystem = "windows")]

mod app;
mod curl_parser;
mod i18n;
mod loadtest;

use eframe::egui;

fn main() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1440.0, 700.0])
            .with_title("API QPS 压测工具"),
        ..Default::default()
    };

    eframe::run_native(
        "API QPS 压测工具",
        options,
        Box::new(|cc| Ok(Box::new(app::ApiQpsApp::new(cc)))),
    )
}
