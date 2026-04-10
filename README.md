# Zanvexis Quantitative Trading Engine

## Overview
A high-frequency quantitative trading architecture developed for crypto futures markets. The system is designed to identify real-time market anomalies by cross-referencing Volume Surge, Funding Rate distortions, Open Interest velocity, and Volatility Regimes (ATR). 

Operating with a dual-layer architecture, it combines the microsecond precision of Rust for data ingestion and mathematical modeling with the flexibility of Python for order execution and orchestration.

## Architecture
The system is strictly divided into two decoupled layers communicating via high-speed IPC (Inter-Process Communication) to prevent bottlenecks and ensure maximum resilience.

* **Core Engine (Rust):** Connects directly to exchange WebSockets. Responsible for ultra-fast JSON parsing, real-time kline aggregation, multi-factor score calculation, and telemetry generation.
* **Orchestrator Brain (Python):** Listens to the IPC feed. Responsible for state management, order execution, strict risk management tracking, and outputting auditable logs.

## Core Features
* **Multi-Factor Scoring Algorithm:** Calculates an institutional-grade entry signal based on:
  * Dynamic Volume Surge detection.
  * Funding Rate normalization (detecting over-leveraged directional bias).
  * Open Interest (OI) velocity tracking.
  * Median True Range (ATR) baseline comparisons.
* **Resilient Infrastructure:**
  * **IPC Health Checks:** Real-time ping/pong monitoring between Rust and Python. Auto-halts new entries if the execution layer becomes degraded.
  * **Auto-Quarantine Module:** Isolates static or stagnant assets dynamically to optimize memory and processing focus.
* **Advanced Risk Management:**
  * Strict Time Stop mechanisms to avoid capital locking in ranging markets.
  * Trailing Breakeven logic for capital preservation.
  * Dynamic Take Profit and Stop Loss based on continuous market feedback.

## Tech Stack
* **Data Processing & Math Modeling:** Rust, Tokio (Async Runtime), Tungstenite (WebSockets), Serde (JSON parsing).
* **Execution & Orchestration:** Python 3, Pandas, Asyncio.
* **Deployment:** Designed for headless Linux server environments (AWS EC2, Ubuntu) utilizing daemonized processes.

## Disclaimer
This software is built for educational and research purposes within quantitative finance. It includes a Paper Trading mode for strategy validation without financial risk. Cryptocurrency trading carries a high level of risk and may not be suitable for all investors.

---
Developed and maintained by Vinicius Silva Pontual (Zanvexis).
