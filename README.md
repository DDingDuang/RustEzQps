# API QPS DevTools (ReqWave)

<div align="center">
  <h3>⚡ High-Performance GUI API Load Testing Tool in Rust ⚡</h3>
  <p>Designed for developers, built with eframe (egui) + Tokio + Reqwest.</p>
  <p>
    <a href="README.md">English</a> | 
    <a href="README_CN.md">简体中文</a>
  </p>
</div>

---

## 🌟 Key Features

- **🚀 Blazing Fast**: Powered by Rust's asynchronous runtime `tokio` and `reqwest` connection pooling, easily achieving tens of thousands of QPS on a single machine.
- **📋 Smart Curl Parsing**: Paste `cURL (bash)` commands directly from your browser devtools. It automatically parses URL, Method, Headers, and Body.
- **📊 Real-time Monitoring**: Shows real-time QPS, latency percentiles, status code distribution, and success/failure counts.
- **📉 Precision Statistics**: Uses `hdrhistogram` to calculate high-precision latency metrics like P50, P95, and P99.
- **🖥️ Cross-Platform GUI**: Built on `eframe` (Immediate Mode GUI), supporting Windows, macOS, and Linux without runtime dependencies.
- **🌐 Multi-language**: Built-in support for switching between English and Chinese.

## 📸 Overview

![Screenshot](demo.png)

## 🛠️ Quick Start

### 1. Run

If you have Rust installed:

```bash
cargo run --release
```

### 2. Usage Guide

1.  **Copy Request**: In Chrome/Edge DevTools Network panel, right-click a request -> `Copy` -> `Copy as cURL (bash)`.
2.  **Paste & Config**: Paste the command into the top-left input box. The tool will auto-fill the API URL, Headers, and Body.
3.  **Tune Settings**: Adjust Concurrency, Duration, Timeout, etc.
4.  **Start Test**: Click the `Start Test` button.
5.  **Analyze**: Watch the real-time metrics and charts on the right. A final report will be generated upon completion.

### 3. Reset

Click the `↺ Reset` button in the top bar to restore the initial state.

## ⚙️ Performance Design

- **Connection Pooling**: Toggleable `HTTP Keep-Alive` to reuse TCP connections and minimize handshake overhead.
- **Lock-free Counting**: Uses atomic operations (`AtomicU64`) for high-performance metrics counting.
- **Async I/O**: Fully asynchronous non-blocking I/O to maximize CPU and network bandwidth utilization.

## 🤖 AI Declaration & Acknowledgments

The core logic, GUI layout, and documentation of this project were primarily **co-created with AI**.

- **Free to Use**: This project is fully open-source. Feel free to use it however you like, without any restrictions.
- **Give a Star**: If you find this tool useful, please give it a **Star ⭐️**. It's the best way to support this project!

## 📦 Build for Release

To build an optimized release binary:

```bash
cargo build --release
```

The executable will be located in `target/release/`.

## 📝 License

MIT License
