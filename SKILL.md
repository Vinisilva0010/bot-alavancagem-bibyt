# WORKSPACE DIRECTIVE: PROJECT BINGX APEX V8 (PAPER TRADING & TELEMETRY)

## 1. PROJECT CONTEXT & CONSTRAINTS
- **Exchange:** BingX Futures.
- **Architecture:** Rust (Low-latency WebSocket ingest + Paper Trade Sim) + Python (New Listing Detection + Telemetry Store + Analytics).
- **Capital:** Micro-account ($50 max risk). STRICT PAPER TRADING MODE — real execution is hard-locked at compile time.
- **Current State (V8):** Pivoted from failed Liquidation Cascade Sniper to New Listing Alpha + Telemetry Engine. The bot now detects newly listed futures on BingX, collects massive telemetry data, and simulates trades with fee/slippage modeling.

## 2. THE TDD & MATH-FIRST MANDATE
- **Validate Before Coding:** DO NOT write execution code until you mathematically prove the edge. Present a TDD with Expected Value (EV), the market inefficiency, and break-even win-rate after fees.
- **V8 Alpha:** Micro-momentum on newly listed Futures (high volatility + low efficiency in first 24-72h).
- **Signal Score V8:** OI Velocity (0-30) + Volatility (0-25) + Funding Divergence (0-25) + Volume Surge (0-20). Threshold: 50/100.

## 3. THE "LAPIDATION" ENGINE (TELEMETRY OBSESSION)
- The primary goal is MASSIVE DATA COLLECTION via paper trading.
- **Telemetry:** Rust emits structured JSON events (ticker, paper_entry, paper_exit) to Python via IPC.
- **Storage:** Python Maestro saves all events to `.jsonl` files (one per day) for ML analysis.
- **Paper Trade Sim:** Models taker fees (0.045%), slippage (0.02%), and trailing breakeven stop.

## 4. AUTONOMOUS REFACTORING PROTOCOL
- Full Read/Write access. Read `main.rs` and Python scripts.
- The hard-lock on real capital is at `const PAPER_TRADE_MODE: bool = true;` — NEVER change without review.
- No real order API endpoints exist in V8. All execution is simulated.

## 5. OUTPUT FORMAT
- Report in BRAZILIAN PORTUGUESE.
- Explain the mathematical edge, telemetry structure, and confirm capital lock.