#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod app;
mod curl_parser;
mod i18n;
mod loadtest;

use eframe::egui;
use i18n::{I18nKey, Language, t};

fn main() -> Result<(), eframe::Error> {
    let title = t(Language::ZhCn, I18nKey::AppTitle);
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1120.0, 821.0])
            .with_min_inner_size([850.0, 821.0])
            .with_title(title),
        ..Default::default()
    };

    eframe::run_native(
        title,
        options,
        Box::new(|cc| Ok(Box::new(app::ApiQpsApp::new(cc)))),
    )
}
