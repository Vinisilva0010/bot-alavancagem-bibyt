// =============================================================================
// APEX PREDATOR V9 — SURGICAL REFACTOR
// Engine: Rust + Tokio | Exchange: BingX Perpetual Futures V2
// Estratégia: Micro-Momentum em Moedas Recém-Listadas em Futuros
// Paper Trade ONLY — Coleta massiva de dados para ML
// Março 2026
//
// CHANGELOG V9.0 — REFATORAÇÃO CIRÚRGICA:
//   [CRÍTICO] Score multiplicativo: somatória linear → produto F_vol×F_fund×F_oi×F_regime
//             Funding rate de -3.9% agora VETA o trade (F_funding ≈ 0)
//   [CRÍTICO] Quarentena inteligente: cooldown fixo 30s → 3 camadas AND
//             (temporal mínimo + ATR regime + OI delta normalizado)
//   [CRÍTICO] Heartbeat IPC: try_send silencioso → máquina de estados
//             SYNCING/OPERATIONAL/DEGRADED/HALT com ações explícitas
//   [ALTO]    Filtro de símbolos: aceitar qualquer string → whitelist
//             com validação de formato (rejeita ações, forex, commodities)
//   [PRESERVADO] WebSocket BingX, autenticação HMAC, alavancagem,
//                margem isolada, loop Tokio principal — INTOCADOS
// =============================================================================

// ── Módulos novos (cirúrgicos) ────────────────────────────────────────────────
mod ipc_health;
mod quarantine;
mod score;
mod symbol_filter;
mod circuit_breaker;

use ipc_health::{IpcHealthAction, IpcHealthMonitor};
use quarantine::QuarantineManager;
use score::{
    compute_atr_from_klines, compute_atr_median_from_klines,
    compute_volume_surge_from_klines, compute_simple_atr,
    median, KlineBar, ScoreInputs,
};
use symbol_filter::filter_valid_symbols;

// ── Imports originais (preservados) ──────────────────────────────────────────
use chrono::Utc;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use log::{error, info, warn};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::Read;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

type HmacSha256 = Hmac<Sha256>;

// =============================================================================
// 🛑 CAPITAL LOCK — preservado intacto
// =============================================================================
const PAPER_TRADE_MODE: bool = true;

// =============================================================================
// CONSTANTES — preservadas do V8, com ajustes marcados
// =============================================================================

const LEVERAGE: u32 = 5;
const RISK_PER_TRADE_PCT: f64 = 0.02;

// V9: COOLDOWN_SEC foi REMOVIDO — substituído pela QuarantineManager
// Mantido aqui apenas como documentação do que foi substituído
// const COOLDOWN_SEC: i64 = 30; // ← SUBSTITUÍDO por quarantine.rs

const WS_PING_INTERVAL_SEC: u64 = 20;
const TICKER_POLL_INTERVAL_SEC: u64 = 3;
const TELEMETRY_LOG_INTERVAL_SEC: u64 = 60;
const IPC_CHANNEL_BUFFER: usize = 256;

const BINGX_REST: &str = "https://open-api.bingx.com";
const BINGX_WS: &str = "wss://open-api-swap.bingx.com/swap-market";

// V9: MIN_SIGNAL_SCORE removido — substituído por score::MIN_SIGNAL_SCORE_F64
// Outros parâmetros de sinal removidos — encapsulados em score.rs

// --- Parâmetros do Paper Trade Simulator (preservados) ---
const TAKER_FEE_PCT: f64 = 0.045;
const SIMULATED_SLIPPAGE_PCT: f64 = 0.02;
const STOP_LOSS_PCT: f64 = 2.5;
const TAKE_PROFIT_PCT: f64 = 4.0;
const TIME_STOP_SEC: i64 = 300;
const TRAILING_BREAKEVEN_PCT: f64 = 1.5;
const ROLLING_WINDOW_SIZE: usize = 300;

/// Número de velas de 1m mantidas no histórico para ATR e volume surge.
/// 60 velas = 1h de histórico de klines.
const KLINE_WINDOW_SIZE: usize = 60;

/// Janela de velas usada para calcular ATR atual (últimas N velas).
const ATR_CURRENT_WINDOW: usize = 5;

/// Número de amostras para calcular a mediana do ATR (benchmark de regime).
const ATR_MEDIAN_SAMPLES: usize = 20;

/// Número de velas anteriores usadas para calcular a média de volume (surge).
const VOLUME_AVG_WINDOW: usize = 10;

// =============================================================================
// AUTENTICAÇÃO BINGX — preservada intacta
// =============================================================================

fn build_bingx_signature(params: &mut Vec<(&str, String)>) -> (String, String) {
    let api_key = env::var("BINGX_API_KEY").expect("BINGX_API_KEY não encontrada no .env");
    let secret_key =
        env::var("BINGX_SECRET_KEY").expect("BINGX_SECRET_KEY não encontrada no .env");

    params.sort_by_key(|(k, _)| *k);

    let query_string = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");

    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes())
        .expect("HMAC aceita qualquer tamanho de chave");
    mac.update(query_string.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    (api_key, format!("{}&signature={}", query_string, signature))
}

fn timestamp_ms() -> String {
    Utc::now().timestamp_millis().to_string()
}

// =============================================================================
// WARM-UP — preservado intacto
// =============================================================================

async fn configure_margin_and_mode(client: &Client, symbol: &str) {
    {
        let mut params = vec![
            ("symbol", symbol.to_string()),
            ("marginType", "ISOLATED".to_string()),
            ("timestamp", timestamp_ms()),
        ];
        let (api_key, signed_query) = build_bingx_signature(&mut params);
        let url = format!(
            "{}/openApi/swap/v2/trade/marginType?{}",
            BINGX_REST, signed_query
        );
        match client.post(&url).header("X-BX-APIKEY", &api_key).send().await {
            Ok(res) => {
                let text = res.text().await.unwrap_or_default();
                let parsed: Value = serde_json::from_str(&text).unwrap_or_default();
                if parsed["code"] == 0 || parsed["code"] == -4046 {
                    info!("⚙️  [{}] Margin Type: ISOLATED ✓", symbol);
                } else {
                    warn!("⚠️  [{}] MarginType: {} (não fatal)", symbol, text);
                }
            }
            Err(e) => warn!("⚠️  [{}] Rede falhou ao setar MarginType: {}", symbol, e),
        }
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    {
        let mut params = vec![
            ("dualSidePosition", "true".to_string()),
            ("timestamp", timestamp_ms()),
        ];
        let (api_key, signed_query) = build_bingx_signature(&mut params);
        let url = format!(
            "{}/openApi/swap/v1/positionSide/dual?{}",
            BINGX_REST, signed_query
        );
        match client.post(&url).header("X-BX-APIKEY", &api_key).send().await {
            Ok(res) => {
                let text = res.text().await.unwrap_or_default();
                let parsed: Value = serde_json::from_str(&text).unwrap_or_default();
                if parsed["code"] == 0 || parsed["code"] == -4059 {
                    info!("⚙️  [{}] Position Mode: HEDGE ✓", symbol);
                } else {
                    warn!("⚠️  [{}] PositionMode: {} (não fatal)", symbol, text);
                }
            }
            Err(e) => warn!("⚠️  [{}] Rede falhou ao setar PositionMode: {}", symbol, e),
        }
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    for side in &["LONG", "SHORT"] {
        let mut params = vec![
            ("leverage", LEVERAGE.to_string()),
            ("side", side.to_string()),
            ("symbol", symbol.to_string()),
            ("timestamp", timestamp_ms()),
        ];
        let (api_key, signed_query) = build_bingx_signature(&mut params);
        let url = format!("{}/openApi/swap/v2/trade/leverage?{}", BINGX_REST, signed_query);
        match client.post(&url).header("X-BX-APIKEY", &api_key).send().await {
            Ok(res) => {
                let parsed: Value = res.json().await.unwrap_or_default();
                if parsed["code"] == 0 {
                    info!("⚙️  [{}] Leverage {}x ({}): ✓", symbol, LEVERAGE, side);
                } else {
                    warn!("⚠️  [{}] Leverage ({}): {} (não fatal)", symbol, side, parsed);
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    }
}

// =============================================================================
// MÓDULO DE BALANÇO — preservado intacto
// =============================================================================

async fn fetch_usdt_balance(
    client: &Client,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let mut params = vec![("timestamp", timestamp_ms())];
    let (api_key, signed_query) = build_bingx_signature(&mut params);
    let url = format!("{}/openApi/swap/v2/user/balance?{}", BINGX_REST, signed_query);
    let res = client.get(&url).header("X-BX-APIKEY", &api_key).send().await?;
    let parsed: Value = res.json().await?;
    if parsed["code"] != 0 {
        error!("💀 [AUTH] Falha BingX: {}", parsed);
        panic!("Credenciais inválidas. Abortando.");
    }
    let balance = parsed["data"]["balance"]["availableMargin"]
        .as_str()
        .unwrap_or("0")
        .parse::<f64>()
        .unwrap_or(0.0);
    info!("🏦 [BingX] Auth OK. Saldo disponível: {:.2} USDT", balance);
    Ok(balance)
}

// =============================================================================
// DESCOMPRESSÃO GZIP — preservada intacta
// =============================================================================

fn decompress_gzip(data: &[u8]) -> Option<String> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).ok()?;
    Some(decompressed)
}

// =============================================================================
// TICKER POLLING — preservado intacto
// =============================================================================

#[derive(Clone, Default, Debug)]
struct TickerSnapshot {
    last_price: f64,
    open_interest: f64,
    funding_rate: f64,
    volume_24h: f64,
}

type TickerMap = Arc<Mutex<HashMap<String, TickerSnapshot>>>;

fn parse_json_f64(val: &Value) -> f64 {
    if let Some(f) = val.as_f64() { return f; }
    if let Some(s) = val.as_str() { return s.parse::<f64>().unwrap_or(0.0); }
    0.0
}

async fn ticker_polling_loop(
    client: Client,
    symbols: Arc<Mutex<Vec<String>>>,
    ticker_map: TickerMap,
) {
    let mut interval =
        tokio::time::interval(tokio::time::Duration::from_secs(TICKER_POLL_INTERVAL_SEC));

    loop {
        interval.tick().await;
        let loop_start = std::time::Instant::now();
        let current_symbols = symbols.lock().await.clone();
        if current_symbols.is_empty() { continue; }

        let mut price_vol_map: HashMap<String, (f64, f64)> = HashMap::new();
        let mut funding_map: HashMap<String, f64> = HashMap::new();
        let mut oi_map: HashMap<String, f64> = HashMap::new();

        {
            let url = format!("{}/openApi/swap/v2/quote/ticker", BINGX_REST);
            match tokio::time::timeout(tokio::time::Duration::from_secs(4), client.get(&url).send()).await {
                Ok(Ok(res)) => {
                    if let Ok(parsed) = res.json::<Value>().await {
                        if let Some(data) = parsed["data"].as_array() {
                            for item in data {
                                let sym = item["symbol"].as_str().unwrap_or("").to_string();
                                if current_symbols.contains(&sym) {
                                    let price = parse_json_f64(&item["lastPrice"]);
                                    let vol = parse_json_f64(&item["quoteVolume"]);
                                    price_vol_map.insert(sym, (price, vol));
                                }
                            }
                        }
                    }
                }
                Ok(Err(e)) => warn!("⚠️ [TICKER] Request falhou: {}", e),
                Err(_) => warn!("⏰ [TIMEOUT] BingX ignorou o Ticker, abortando req."),
            }
        }
        {
            let url = format!("{}/openApi/swap/v2/quote/premiumIndex", BINGX_REST);
            match tokio::time::timeout(tokio::time::Duration::from_secs(4), client.get(&url).send()).await {
                Ok(Ok(res)) => {
                    if let Ok(parsed) = res.json::<Value>().await {
                        if let Some(data) = parsed["data"].as_array() {
                            for item in data {
                                let sym = item["symbol"].as_str().unwrap_or("").to_string();
                                if current_symbols.contains(&sym) {
                                    let fr = parse_json_f64(&item["lastFundingRate"]) * 100.0;
                                    funding_map.insert(sym, fr);
                                }
                            }
                        }
                    }
                }
                Ok(Err(e)) => warn!("⚠️ [FUNDING] Request falhou: {}", e),
                Err(_) => warn!("⏰ [TIMEOUT] BingX ignorou o Funding, abortando req."),
            }
        }

        let fetches = futures_util::stream::iter(
            current_symbols.clone().into_iter().map(|sym| {
                let c = client.clone();
                let url = format!("{}/openApi/swap/v2/quote/openInterest?symbol={}", BINGX_REST, sym);
                async move {
                    match tokio::time::timeout(tokio::time::Duration::from_secs(4), c.get(&url).send()).await {
                        Ok(Ok(res)) => match res.json::<Value>().await {
                            Ok(parsed) => {
                                if parsed["code"].as_i64().unwrap_or(-1) == 0 {
                                    let oi = parse_json_f64(&parsed["data"]["openInterest"]);
                                    if oi > 0.0 { return Some((sym, oi)); }
                                }
                            }
                            Err(e) => warn!("[OI] Parse falhou {}: {}", sym, e),
                        },
                        Ok(Err(e)) => warn!("[OI] Request falhou {}: {}", sym, e),
                        Err(_) => warn!("⏰ [TIMEOUT] BingX ignorou o OI da moeda {}, abortando req.", sym),
                    }
                    None
                }
            }),
        ).buffer_unordered(3).collect::<Vec<_>>().await;

        for op in fetches {
            if let Some((s, oi)) = op { oi_map.insert(s, oi); }
        }

        {
            let mut map = ticker_map.lock().await;
            for symbol in &current_symbols {
                let (price, vol) = price_vol_map.get(symbol).copied().unwrap_or((0.0, 0.0));
                let funding = funding_map.get(symbol).copied().unwrap_or(0.0);
                let oi = oi_map.get(symbol).copied().unwrap_or(
                    map.get(symbol).map(|s| s.open_interest).unwrap_or(0.0),
                );
                if price > 0.0 {
                    map.insert(symbol.clone(), TickerSnapshot {
                        last_price: price,
                        open_interest: oi,
                        funding_rate: funding,
                        volume_24h: vol,
                    });
                }
            }
        }

        let elapsed = loop_start.elapsed();
        if elapsed.as_secs() > 4 {
            warn!("⚠️ [POLLING STALL] Ticker loop demorou {:?}", elapsed);
        }
    }
}

// =============================================================================
// ESTADO DE CADA MOEDA — V9: inclui QuarantineManager + histórico de funding
// para cálculo de mediana (necessário para F_funding do score)
// =============================================================================

struct PaperPosition {
    is_short: bool,
    entry_price: f64,
    entry_time: i64,
    highest_pnl: f64,
}

struct NewListingCoinState {
    listing_detected_at: i64,
    oi_history: VecDeque<(i64, f64)>,
    price_history: VecDeque<(i64, f64)>,
    // volume_history removido: volume_24h é acumulador monotônico, inútil para surge
    // O volume real vem agora de kline_history.volume (campo v do kline_1m)
    funding_history: VecDeque<(i64, f64)>,
    // V9.1: rolling window de velas OHLC reais (do WebSocket kline_1m)
    // Alimenta F_regime (ATR real) e F_volume (volume de vela real)
    kline_history: VecDeque<KlineBar>,
    upper_wick_ratio: f64,
    lower_wick_ratio: f64,
    oi_velocity_pct_min: f64,
    price_volatility_1m: f64,
    // volume_surge_ratio removido do estado — calculado on-demand de kline_history
    paper_position: Option<PaperPosition>,
    total_paper_trades: u32,
    paper_wins: u32,
    paper_losses: u32,
    paper_total_pnl: f64,
    quarantine: QuarantineManager,
}

impl NewListingCoinState {
    fn new(detected_at: i64) -> Self {
        NewListingCoinState {
            listing_detected_at: detected_at,
            oi_history: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            price_history: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            funding_history: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            kline_history: VecDeque::with_capacity(KLINE_WINDOW_SIZE),
            upper_wick_ratio: 0.0,
            lower_wick_ratio: 0.0,
            oi_velocity_pct_min: 0.0,
            price_volatility_1m: 0.0,
            paper_position: None,
            total_paper_trades: 0,
            paper_wins: 0,
            paper_losses: 0,
            paper_total_pnl: 0.0,
            quarantine: QuarantineManager::new(),
        }
    }

    fn ingest_ticker(&mut self, now: i64, snap: &TickerSnapshot) {
        self.oi_history.push_back((now, snap.open_interest));
        self.price_history.push_back((now, snap.last_price));
        // volume_24h NÃO é mais armazenado — é acumulador monotônico
        // O volume real de curto prazo vem de kline_history (alimentado pelo WS)
        self.funding_history.push_back((now, snap.funding_rate));

        while self.oi_history.len() > ROLLING_WINDOW_SIZE { self.oi_history.pop_front(); }
        while self.price_history.len() > ROLLING_WINDOW_SIZE { self.price_history.pop_front(); }
        while self.funding_history.len() > ROLLING_WINDOW_SIZE { self.funding_history.pop_front(); }

        self.compute_oi_velocity(now);
        self.compute_price_volatility(now);
        // compute_volume_surge removido — calculado on-demand de kline_history no eval_iv
    }

    /// V9.1: ingere uma vela de kline_1m recebida pelo WebSocket.
    /// Chamado no braço msg_opt do tokio::select! quando chega um kline_1m.
   /// V9.1 FIX: ingere uma vela de kline_1m recebida pelo WebSocket.
    /// Resolve o bug do WebSocket mandando múltiplos updates para a mesma vela.
    fn ingest_kline(&mut self, bar: KlineBar) {
        if let Some(last_bar) = self.kline_history.back_mut() {
            // Se o timestamp for igual, é a MESMA vela de 1 minuto sendo atualizada.
            // Nós apenas sobrescrevemos a antiga com os dados mais recentes.
            if last_bar.ts == bar.ts {
                *last_bar = bar;
                return;
            }
        }
        
        // Se o timestamp for diferente, é um novo minuto. Adicionamos na lista.
        self.kline_history.push_back(bar);
        while self.kline_history.len() > KLINE_WINDOW_SIZE {
            self.kline_history.pop_front();
        }
    }

    fn compute_oi_velocity(&mut self, now: i64) {
        let one_min_ago = now - 60;
        let current_oi = self.oi_history.back().map(|(_, v)| *v).unwrap_or(0.0);
        let old_oi = self.oi_history.iter().rev()
            .find(|(ts, _)| *ts <= one_min_ago)
            .map(|(_, v)| *v)
            .unwrap_or(current_oi);
        self.oi_velocity_pct_min = if old_oi > 0.0 {
            ((current_oi - old_oi) / old_oi) * 100.0
        } else {
            0.0
        };
    }

    fn compute_price_volatility(&mut self, now: i64) {
        let one_min_ago = now - 60;
        let recent: Vec<f64> = self.price_history.iter()
            .filter(|(ts, _)| *ts >= one_min_ago)
            .map(|(_, v)| *v)
            .collect();
        if recent.len() < 2 { self.price_volatility_1m = 0.0; return; }
        let max = recent.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let min = recent.iter().cloned().fold(f64::INFINITY, f64::min);
        let avg = recent.iter().sum::<f64>() / recent.len() as f64;
        if avg > 0.0 { self.price_volatility_1m = ((max - min) / avg) * 100.0; }
    }

    /// V9: calcula mediana do funding rate histórico para o score
    fn funding_rate_median(&self) -> f64 {
        let values: Vec<f64> = self.funding_history.iter().map(|(_, v)| *v).collect();
        median(&values)
    }

    fn hours_since_listing(&self, now: i64) -> f64 {
        (now - self.listing_detected_at) as f64 / 3600.0
    }
}

// =============================================================================
// PAPER TRADE SIMULATOR — preservado intacto
// =============================================================================

enum PaperExitReason {
    StopLoss,
    TakeProfit,
    TimeStop,
    TrailingBreakeven,
}

impl PaperExitReason {
    fn as_str(&self) -> &'static str {
        match self {
            PaperExitReason::StopLoss => "STOP_LOSS",
            PaperExitReason::TakeProfit => "TAKE_PROFIT",
            PaperExitReason::TimeStop => "TIME_STOP",
            PaperExitReason::TrailingBreakeven => "TRAILING_BREAKEVEN",
        }
    }
    fn is_stop_loss(&self) -> bool {
        matches!(self, PaperExitReason::StopLoss)
    }
}

fn evaluate_paper_exit(pos: &PaperPosition, current_price: f64, now: i64) -> Option<PaperExitReason> {
    let raw_pnl_pct = if pos.is_short {
        ((pos.entry_price - current_price) / pos.entry_price) * 100.0
    } else {
        ((current_price - pos.entry_price) / pos.entry_price) * 100.0
    };
    let net_pnl_pct = raw_pnl_pct - (TAKER_FEE_PCT * 2.0) - (SIMULATED_SLIPPAGE_PCT * 2.0);
    let duration = now - pos.entry_time;

    if net_pnl_pct <= -STOP_LOSS_PCT { return Some(PaperExitReason::StopLoss); }
    if net_pnl_pct >= TAKE_PROFIT_PCT { return Some(PaperExitReason::TakeProfit); }
    if pos.highest_pnl >= TRAILING_BREAKEVEN_PCT && net_pnl_pct <= 0.0 {
        return Some(PaperExitReason::TrailingBreakeven);
    }
    if duration > TIME_STOP_SEC { return Some(PaperExitReason::TimeStop); }
    None
}

fn calculate_net_pnl(entry_price: f64, exit_price: f64, is_short: bool) -> f64 {
    let raw = if is_short {
        ((entry_price - exit_price) / entry_price) * 100.0
    } else {
        ((exit_price - entry_price) / entry_price) * 100.0
    };
    raw - (TAKER_FEE_PCT * 2.0) - (SIMULATED_SLIPPAGE_PCT * 2.0)
}

// =============================================================================
// TELEMETRIA — preservada, com campos extras V9
// =============================================================================

fn build_telemetry_event(
    symbol: &str,
    event_type: &str,
    snap: &TickerSnapshot,
    state: &NewListingCoinState,
    extra: Option<Value>,
) -> String {
    let now_ms = Utc::now().timestamp_millis();
    // V9.1: calcula volume_surge e ATR de kline on-demand para telemetria
    let kline_vol_surge = compute_volume_surge_from_klines(&state.kline_history, VOLUME_AVG_WINDOW);
    let kline_atr_current = compute_atr_from_klines(&state.kline_history, ATR_CURRENT_WINDOW);
    let kline_atr_median = compute_atr_median_from_klines(&state.kline_history, ATR_CURRENT_WINDOW, ATR_MEDIAN_SAMPLES);

    let mut event = json!({
        "ts_ms":              now_ms,
        "symbol":             symbol,
        "event_type":         event_type,
        "price":              snap.last_price,
        "open_interest":      snap.open_interest,
        "funding_rate":       snap.funding_rate,
        "volume_24h":         snap.volume_24h,
        "oi_velocity_pct_m":  state.oi_velocity_pct_min,
        "volatility_1m":      state.price_volatility_1m,
        // V9.1: volume_surge e ATR agora vêm de kline real (não volume_24h)
        "volume_surge":       kline_vol_surge,
        "kline_atr_current":  kline_atr_current,
        "kline_atr_median":   kline_atr_median,
        "kline_count":        state.kline_history.len(),
        "upper_wick":         state.upper_wick_ratio,
        "lower_wick":         state.lower_wick_ratio,
        "hours_listed":       state.hours_since_listing(Utc::now().timestamp()),
        "data_points":        state.price_history.len(),
        "quarantine_state":   format!("{:?}", state.quarantine.state),
    });

    if let Some(ext) = extra {
        if let (Some(base), Some(extra_map)) = (event.as_object_mut(), ext.as_object()) {
            for (k, v) in extra_map { base.insert(k.clone(), v.clone()); }
        }
    }
    format!("{}\n", event)
}

// =============================================================================
// TASK IPC — V9 FIX: opera na WriteHalf do socket (sem Mutex de leitura)
//
// O V8 original usava Arc<Mutex<TcpStream>> compartilhado com a read task.
// Isso causava deadlock: read_line().await segurava o Mutex, impedindo write.
// Agora a write half é separada pelo tokio::io::split() antes de qualquer uso.
// =============================================================================

async fn ipc_writer_task_split(
    write_half: Arc<Mutex<WriteHalf<TcpStream>>>,
    mut rx: mpsc::Receiver<String>,
) {
    while let Some(msg) = rx.recv().await {
        let mut guard = write_half.lock().await;
        if let Err(e) = guard.write_all(msg.as_bytes()).await {
            error!("[IPC] Falha ao escrever no Maestro: {}", e);
            // Não quebra o loop — tenta na próxima mensagem.
            // A task de leitura vai detectar EOF e registrar disconnect.
        }
    }
}

// =============================================================================
// MOTOR PRINCIPAL V9
// =============================================================================

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("╔═══════════════════════════════════════════════════╗");
    info!("║   APEX PREDATOR V9 — SURGICAL REFACTOR            ║");
    info!("║   BingX Perpetual Futures V2 | Rust + Tokio       ║");
    info!("║   Mode: PAPER TRADE + TELEMETRY COLLECTION        ║");
    info!("║   Leverage: {}x | Hedge Mode | ISOLATED             ║", LEVERAGE);
    info!("║   Score: Multiplicativo | Quarentena: 3 camadas   ║");
    info!("╚═══════════════════════════════════════════════════╝");

    if PAPER_TRADE_MODE {
        warn!("🛡️  CAPITAL LOCK ATIVO — Nenhuma ordem real será enviada.");
        warn!("🛡️  Modo: COLETA DE DADOS + PAPER TRADE SIMULADO.");
    } else {
        error!("⚠️  MODO LIVE DETECTADO! Certifique-se de que sabe o que está fazendo!");
    }

    let http_client = Client::builder()
        .user_agent("Mozilla/5.0")
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .expect("Falha ao criar HTTP client");

    let usdt_balance = fetch_usdt_balance(&http_client).await.unwrap_or(0.0);
    if usdt_balance == 0.0 {
        warn!("⚠️  Saldo 0 USDT — paper trades serão simulados com $50 virtual.");
    }
    let sim_balance = if usdt_balance > 0.0 { usdt_balance } else { 50.0 };
    info!("💰 Paper trade sim balance: ${:.2} | Risk/trade: ${:.2}",
        sim_balance, sim_balance * RISK_PER_TRADE_PCT);

    // --- IPC com Python Maestro ---
    // FIX BUG 1: usa tokio::io::split() para separar read/write half do socket.
    // O V8/V9 original compartilhava um Arc<Mutex<TcpStream>> entre a task de
    // leitura e a task de escrita. read_line().await segurava o Mutex enquanto
    // bloqueava, impedindo o ipc_writer_task de escrever o HEARTBEAT no socket.
    // Resultado: deadlock → Python nunca recebia nada → HALT em 15s.
    // Com split(), leitura e escrita operam em halves independentes, sem Mutex.
    let maestro_raw = TcpStream::connect("127.0.0.1:9000")
        .await
        .expect("Python Maestro offline. Inicie o Terminal 1 primeiro.");

    // Divide o socket em metades independentes ANTES de qualquer uso
    let (maestro_read_half, maestro_write_half) = tokio::io::split(maestro_raw);
    // A write half é encapsulada em Arc<Mutex> apenas para o ipc_writer_task
    // (que é a única task que escreve — o Mutex aqui é de exclusão de escritores,
    //  não de bloqueio de leitores)
    let maestro_write = Arc::new(Mutex::new(maestro_write_half));
    // A read half fica em um BufReader próprio, nunca compartilhada
    let mut maestro_reader = BufReader::new(maestro_read_half);

    // FIX BUG 2+3: IpcHealthMonitor inicia em OPERATIONAL, não SYNCING.
    // O Python já provou que está vivo ao aceitar a conexão TCP e enviar
    // SET_TARGETS. Iniciar em SYNCING com timeout de 15s desde o boot
    // + warm-up de ~11s = HALT quase garantido antes do primeiro heartbeat.
    let ipc_health = Arc::new(Mutex::new(IpcHealthMonitor::new()));
    // Registra o primeiro ACK imediatamente: Python está vivo (conectou e vai
    // enviar SET_TARGETS agora). Isso move o estado SYNCING → OPERATIONAL
    // e reseta o relógio de timeout para o momento atual.
    {
        let mut health = ipc_health.lock().await;
        let _ = health.register_ack();
        info!("[IPC HEALTH] Estado inicial: {} (Python conectado)", health.state_label());
    }

    // Lê alvos iniciais do Maestro — usa a read half diretamente
    let initial_targets_raw: Vec<String> = {
        let mut line = String::new();
        maestro_reader.read_line(&mut line).await
            .expect("Falha ao ler alvos do Maestro");
        let cmd: Value = serde_json::from_str(line.trim())
            .expect("JSON inválido do Maestro");
        cmd["symbols"].as_array().unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    };

    // V9: FILTRO DE SÍMBOLOS — rejeita ações, forex, commodities
    let initial_targets = filter_valid_symbols(&initial_targets_raw);
    info!("🎯 {} alvos válidos após filtro ({} recebidos do Maestro).",
        initial_targets.len(), initial_targets_raw.len());
    for t in &initial_targets {
        info!("   📌 {}", t);
    }

    // Confirma prontidão para o Python usando a write half
    {
        let mut w = maestro_write.lock().await;
        let _ = w.write_all(b"V9 Telemetry Engine armado. Modo: PAPER + DATA COLLECTION.\n").await;
    }

    // --- Canais mpsc ---
    let (ipc_tx, ipc_rx) = mpsc::channel::<String>(IPC_CHANNEL_BUFFER);
    // ipc_writer_task agora recebe a write half, não o socket inteiro
    tokio::spawn(ipc_writer_task_split(Arc::clone(&maestro_write), ipc_rx));

    // --- Lista de símbolos compartilhada ---
    let active_symbols: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(initial_targets.clone()));

    // --- Warm-up ---
    info!("🔥 Warm-up: configurando margem e alavancagem...");
    for symbol in &initial_targets {
        configure_margin_and_mode(&http_client, symbol).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    info!("✅ Warm-up completo.");

    // FIX RACE CONDITION: reseta o relógio do IpcHealthMonitor após o warm-up.
    // O warm-up faz ~11s de I/O bloqueante contra a BingX. Sem esse reset,
    // check_health() contabiliza esse tempo como "Python ausente" e
    // dispara HALT imediatamente ao entrar no loop principal.
    {
        let mut health = ipc_health.lock().await;
        health.reset_clock();
    }

    // --- Estado de mercado por moeda ---
    let now = Utc::now().timestamp();
    let mut market_state: HashMap<String, NewListingCoinState> = initial_targets
        .iter()
        .map(|s| (s.clone(), NewListingCoinState::new(now)))
        .collect();

        // --- GENERAL (Circuit Breaker Macro) ---
    let mut macro_breaker = circuit_breaker::MacroCircuitBreaker::new();

    // --- Ticker polling ---
    let ticker_map: TickerMap = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(ticker_polling_loop(
        http_client.clone(),
        Arc::clone(&active_symbols),
        Arc::clone(&ticker_map),
    ));

    // --- Task para receber comandos do Maestro + registrar ACKs ---
    // FIX BUG 1: usa maestro_reader (read half já separada pelo split()),
    // sem nenhum Mutex. Leitura e escrita agora são completamente independentes.
    let maestro_reader_symbols = Arc::clone(&active_symbols);
    let ipc_tx_maestro = ipc_tx.clone();
    let ipc_health_reader = Arc::clone(&ipc_health);
    tokio::spawn(async move {
        // maestro_reader foi movido para esta task — é o único dono da read half
        let mut reader = maestro_reader;
        loop {
            let mut buf = String::new();
            let line = match reader.read_line(&mut buf).await {
                Ok(0) => {
                    warn!("[IPC] Maestro desconectou (EOF).");
                    let mut health = ipc_health_reader.lock().await;
                    let _ = health.register_disconnect();
                    break;
                }
                Ok(_) => buf,
                Err(e) => {
                    error!("[IPC] Erro ao ler do Maestro: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }

            if let Ok(cmd) = serde_json::from_str::<Value>(trimmed) {
                let action = cmd["action"].as_str().unwrap_or("");

                // V9: qualquer mensagem JSON válida do Python conta como ACK de saúde
                // Isso inclui ACK de heartbeat, NEW_LISTING, SET_TARGETS — qualquer coisa.
                {
                    let mut health = ipc_health_reader.lock().await;
                    let health_action = health.register_ack();
                    if health_action == IpcHealthAction::Resume {
                        warn!("[IPC HEALTH] 🔄 Python reconectado — retomando operação normal.");
                    }
                }

                if action == "NEW_LISTING" || action == "SET_TARGETS" {
                    if let Some(syms) = cmd["symbols"].as_array() {
                        let raw_syms: Vec<String> = syms.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect();
                        let new_syms = filter_valid_symbols(&raw_syms);
                        if !new_syms.is_empty() {
                            warn!("🆕 [MAESTRO] {} novos alvos válidos recebidos: {:?}",
                                new_syms.len(), new_syms);
                            let mut guard = maestro_reader_symbols.lock().await;
                            for s in &new_syms {
                                if !guard.contains(s) { guard.push(s.clone()); }
                            }
                        }
                    }
                }
            }
            // Responde ACK genérico para manter o heartbeat vivo do lado Python
            let _ = ipc_tx_maestro.try_send(format!(
                "{{\"action\":\"ACK\",\"ts\":{}}}\n",
                Utc::now().timestamp()
            ));
        }
    });

    // --- Event loop com reconexão automática ---
    'reconnect: loop {
        info!("🔌 Conectando ao WebSocket BingX...");

        let (mut ws, _) = match connect_async(BINGX_WS).await {
            Ok(s) => s,
            Err(e) => {
                error!("❌ WS falhou: {}. Tentando em 5s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue 'reconnect;
            }
        };

        let current_syms = active_symbols.lock().await.clone();
        for symbol in &current_syms {
            let mut all_ws_syms = current_syms.clone();
        if !all_ws_syms.contains(&"BTC-USDT".to_string()) {
            all_ws_syms.push("BTC-USDT".to_string());
        }
        
        for symbol in &all_ws_syms { // <--- MUDE DE current_syms PARA all_ws_syms
            let sub = json!({"id": "1", "reqType": "sub", "dataType": format!("{}@kline_1m", symbol)});
            if ws.send(Message::Text(sub.to_string())).await.is_err() {
                error!("❌ Sub kline {} falhou. Reconectando...", symbol);
                continue 'reconnect;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        info!("📡 Subscrições ativas para {} símbolos. Telemetria rodando.", current_syms.len());

        let mut ping_iv = tokio::time::interval(tokio::time::Duration::from_secs(WS_PING_INTERVAL_SEC));
        let mut tele_iv = tokio::time::interval(tokio::time::Duration::from_secs(TELEMETRY_LOG_INTERVAL_SEC));
        let mut eval_iv = tokio::time::interval(tokio::time::Duration::from_secs(2));
        // V9: heartbeat IPC separado do ping WS
        // FIX RACE CONDITION: MissedTickBehavior::Skip evita rajada de ticks
        // acumulados. Com o padrão Burst, qualquer pausa do loop (reconexão WS,
        // polling stall) dispara check_health() N vezes consecutivas sem dar
        // chance de ACK entre elas, garantindo HALT espúrio.
        let mut hb_iv = {
            let mut iv = tokio::time::interval(
                tokio::time::Duration::from_secs(ipc_health::HEARTBEAT_INTERVAL_SECS)
            );
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            iv
        };

        loop {
            tokio::select! {

                // ── Heartbeat WS (preservado) ──────────────────────────────
                _ = ping_iv.tick() => {
                    if ws.send(Message::Text("Ping".to_string())).await.is_err() {
                        error!("💔 Heartbeat WS falhou. Reconectando...");
                        break;
                    }
                }

                // ── V9: Heartbeat IPC → Python ─────────────────────────────
                // Envia ping para o Python e verifica timeout
                _ = hb_iv.tick() => {
                    let mut health = ipc_health.lock().await;

                    // Verifica estado de saúde baseado no tempo
                    let health_action = health.check_health();
                    match health_action {
                        IpcHealthAction::ExecuteHalt => {
                            error!(
                                "🚨 [IPC HALT] Python não responde — HALT ativado. Estado: {}",
                                health.state_label()
                            );
                            // Em Paper Trade: loga todas as posições abertas que serão zeradas
                            for (sym, state) in &market_state {
                                if state.paper_position.is_some() {
                                    warn!("[HALT] Zerando posição paper em {}", sym);
                                }
                            }
                            // Zera todas as posições paper
                            for state in market_state.values_mut() {
                                state.paper_position = None;
                            }
                            error!("[HALT] Bot suspenso. Aguardando Python reconectar...");
                        }
                        IpcHealthAction::FreezeNewEntries => {
                            warn!("[IPC DEGRADED] Python não responde. Novas entradas congeladas.");
                        }
                        IpcHealthAction::Resume => {
                            info!("[IPC HEALTH] ✅ Operação retomada.");
                        }
                        IpcHealthAction::None => {}
                    }

                    // Envia heartbeat para o Python
                    let _ = ipc_tx.try_send(format!(
                        "{{\"action\":\"HEARTBEAT\",\"ts\":{},\"state\":\"{}\"}}\n",
                        Utc::now().timestamp(),
                        health.state_label()
                    ));
                }

                // ── Telemetria Summary (preservado, com campo de saúde IPC) ──
                _ = tele_iv.tick() => {
                    let total_trades: u32 = market_state.values().map(|s| s.total_paper_trades).sum();
                    let total_wins: u32   = market_state.values().map(|s| s.paper_wins).sum();
                    let total_pnl: f64    = market_state.values().map(|s| s.paper_total_pnl).sum();
                    let active_positions: Vec<&String> = market_state.iter()
                        .filter(|(_, s)| s.paper_position.is_some())
                        .map(|(k, _)| k).collect();

                    let ipc_state = {
                        let h = ipc_health.lock().await;
                        h.state_label().to_string()
                    };

                    info!("═══════════════════ TELEMETRIA V9 ═══════════════════");
                    info!("  📊 Símbolos monitorados: {}", market_state.len());
                    info!("  🔌 IPC Health: {}", ipc_state);
                    info!("  📝 Paper trades totais: {} ({}W/{}L)",
                        total_trades, total_wins, total_trades.saturating_sub(total_wins));
                    info!("  💰 PnL acumulado (paper): {:.4}%", total_pnl);
                    if !active_positions.is_empty() {
                        info!("  🔶 Posições abertas: {:?}", active_positions);
                    } else {
                        info!("  🟢 Sem posições abertas. Scanning...");
                    }

                    if let Ok(snap_map) = ticker_map.try_lock() {
                        for (sym, state) in &market_state {
                            if let Some(snap) = snap_map.get(sym) {
                                let ks = compute_volume_surge_from_klines(&state.kline_history, VOLUME_AVG_WINDOW);
                                let ka = compute_atr_from_klines(&state.kline_history, ATR_CURRENT_WINDOW);
                                let km = compute_atr_median_from_klines(&state.kline_history, ATR_CURRENT_WINDOW, ATR_MEDIAN_SAMPLES);
                                info!(
                                    "  [{:>14}] px={:<10.6} | OI vel={:>+7.2}%/m | fund={:>+7.4}% | vol_surge={:.2}x | atr_now={:.6} atr_med={:.6} | klines={} | quar={:?}",
                                    sym, snap.last_price,
                                    state.oi_velocity_pct_min,
                                    snap.funding_rate,
                                    ks, ka, km,
                                    state.kline_history.len(),
                                    state.quarantine.state
                                );
                            }
                        }
                    }
                    info!("═════════════════════════════════════════════════════");
                }

                // ── Avaliação de Sinal + Gestão de Posição V9 ────────────────
                _ = eval_iv.tick() => {
                    let now = Utc::now().timestamp();

                    // V9: verifica saúde do IPC antes de qualquer avaliação
                    let ipc_can_trade = {
                        let health = ipc_health.lock().await;
                        health.can_enter_trade()
                    };

                    let snap_map = match ticker_map.try_lock() {
                        Ok(g) => g.clone(),
                        Err(_) => continue,
                    };
                    let current_syms = match active_symbols.try_lock() {
                        Ok(g) => g.clone(),
                        Err(_) => continue,
                    };

                    // Adiciona novos símbolos (com filtro V9)
                    for sym in &current_syms {
                        if !market_state.contains_key(sym) {
                            info!("🆕 [NEW TARGET] {} adicionado ao monitoramento.", sym);
                            market_state.insert(sym.clone(), NewListingCoinState::new(now));
                            let sub = json!({
                                "id": "1",
                                "reqType": "sub",
                                "dataType": format!("{}@kline_1m", sym)
                            });
                            let _ = ws.send(Message::Text(sub.to_string())).await;
                        }
                    }

                    // Processa cada símbolo
                    for (sym, state) in market_state.iter_mut() {
                        let snap = match snap_map.get(sym) {
                            Some(s) if s.last_price > 0.0 => s,
                            _ => continue,
                        };

                       // V9: atualiza quarentena com dados atuais de mercado
                        let current_atr = compute_simple_atr(
                            &state.price_history.iter().cloned().collect::<Vec<_>>(),
                            20
                        );
                        state.quarantine.tick(current_atr, snap.open_interest);

                        // Ingest nos rolling windows
                        state.ingest_ticker(now, snap);

                        // Telemetria de ticker
                        let event_line = build_telemetry_event(sym, "ticker", snap, state, None);
                        let _ = ipc_tx.try_send(event_line);

                        // ── Gestão de posição aberta ─────────────────────────
                        let exit_info = if let Some(pos) = &mut state.paper_position {
                            let current_pnl = calculate_net_pnl(pos.entry_price, snap.last_price, pos.is_short);
                            if current_pnl > pos.highest_pnl { pos.highest_pnl = current_pnl; }

                            evaluate_paper_exit(pos, snap.last_price, now).map(|reason| {
                                let is_stop = reason.is_stop_loss();
                                let net_pnl     = calculate_net_pnl(pos.entry_price, snap.last_price, pos.is_short);
                                let is_short    = pos.is_short;
                                let entry_price = pos.entry_price;
                                let entry_time  = pos.entry_time;
                                let highest_pnl = pos.highest_pnl;
                                let duration    = now - entry_time;
                                let reason_str  = reason.as_str().to_string();
                                let side_str    = if is_short { "SHORT" } else { "LONG" }.to_string();
                                (net_pnl, duration, is_short, entry_price, highest_pnl, reason_str, side_str, is_stop)
                            })
                        } else {
                            None
                        };

                        if let Some((net_pnl, duration, _is_short, entry_price, highest_pnl, reason_str, side_str, is_stop)) = exit_info {
                            warn!(
                                "📝 [PAPER EXIT] {} {} | {} | PnL: {:+.4}% | {}s | highest: {:+.4}%",
                                side_str, sym, reason_str, net_pnl, duration, highest_pnl
                            );

                            state.paper_position = None;

                            // V9: atualiza quarentena com resultado do trade
                            if is_stop {
                                state.quarantine.enter_quarantine();
                            } else if net_pnl > 0.0 {
                                state.quarantine.register_win();
                            }

                            let event_line = build_telemetry_event(
                                sym, "paper_exit", snap, state,
                                Some(json!({
                                    "action":        "TRADE_CLOSED",
                                    "side":          side_str,
                                    "entry_price":   entry_price,
                                    "exit_price":    snap.last_price,
                                    "pnl_pct":       format!("{:.4}", net_pnl).parse::<f64>().unwrap_or(0.0),
                                    "duration_sec":  duration,
                                    "exit_reason":   reason_str,
                                    "highest_pnl":   highest_pnl,
                                    "leverage":      LEVERAGE,
                                    "fee_total_pct": TAKER_FEE_PCT * 2.0 + SIMULATED_SLIPPAGE_PCT * 2.0,
                                    // V9: campos de diagnóstico do score
                                    "quarantine_after": format!("{:?}", state.quarantine.state),
                                })),
                            );
                            let _ = ipc_tx.try_send(event_line);

                            state.total_paper_trades += 1;
                            if net_pnl > 0.0 {
                                state.paper_wins += 1;
                            } else {
                                state.paper_losses += 1;
                            }
                            state.paper_total_pnl += net_pnl;

                            continue; // Posição fechada — não avalia entrada neste tick
                        }

                        if state.paper_position.is_some() { continue; }

                        // ── Avaliação de entrada V9 ──────────────────────────

                        // Camada 1: IPC deve estar OPERATIONAL
                        if !ipc_can_trade {
                            continue; // IPC degradado ou em HALT — não entrar
                        }

                        if macro_breaker.is_tripped(now) {
                            continue; // Lockdown Macro Ativo. O Sniper está proibido de atirar.
                        }

                        // Camada 2: quarentena inteligente
                        if !state.quarantine.can_trade() {
                            continue; // Em quarentena — não entrar
                        }

                        // Camada 3: dados suficientes
                        // Exige pelo menos 2 velas de kline para ATR ter sentido
                        if state.price_history.len() < 10 { continue; }
                        if state.kline_history.len() < 2 { continue; }

                        // V9.1: ATR real de kline_1m (h-l por vela)
                        // Substitui o ATR calculado sobre ticks de 3s que dava
                        // atr_current ≈ atr_median sempre → F_regime = 0 em 100% dos casos
                        let atr_current = compute_atr_from_klines(
                            &state.kline_history, ATR_CURRENT_WINDOW
                        );
                        let atr_median_val = compute_atr_median_from_klines(
                            &state.kline_history, ATR_CURRENT_WINDOW, ATR_MEDIAN_SAMPLES
                        );

                        // V9.1: volume surge real de kline_1m
                        // Substitui volume_24h (acumulador monotônico → surge ≈ 1.001 sempre)
                        let volume_surge = compute_volume_surge_from_klines(
                            &state.kline_history, VOLUME_AVG_WINDOW
                        );

                        let score_inputs = ScoreInputs {
                            volume_surge_ratio:   volume_surge,          // kline real
                            funding_rate_pct:     snap.funding_rate,
                            funding_rate_median:  state.funding_rate_median(),
                            oi_velocity_pct_min:  state.oi_velocity_pct_min,
                            atr_current,                                  // kline real
                            atr_median: atr_median_val,                   // kline real
                        };

                        // V9: Score multiplicativo — qualquer fator ruim → sem entrada
                        if let Some(sig) = score::evaluate_signal(&score_inputs) {
                            let simulated_entry = if sig.is_short {
                                snap.last_price * (1.0 - SIMULATED_SLIPPAGE_PCT / 100.0)
                            } else {
                                snap.last_price * (1.0 + SIMULATED_SLIPPAGE_PCT / 100.0)
                            };

                            warn!(
                                "🎯 [PAPER ENTRY] {} {} | Score: {:.4} | px={:.6} | F_vol={:.3} | F_fund={:.3} | F_oi={:.3} | F_reg={:.3}",
                                if sig.is_short { "SHORT" } else { "LONG" },
                                sym, sig.score, simulated_entry,
                                sig.f_volume, sig.f_funding, sig.f_oi, sig.f_regime
                            );

                            let event_line = build_telemetry_event(
                                sym, "paper_entry", snap, state,
                                Some(json!({
                                    "signal_direction":      if sig.is_short { "SHORT" } else { "LONG" },
                                    "signal_score":          sig.score,
                                    "simulated_entry_price": simulated_entry,
                                    "leverage":              LEVERAGE,
                                    // V9: breakdown do score para análise ML
                                    "f_volume":   sig.f_volume,
                                    "f_funding":  sig.f_funding,
                                    "f_oi":       sig.f_oi,
                                    "f_regime":   sig.f_regime,
                                })),
                            );
                            let _ = ipc_tx.try_send(event_line);

                            state.paper_position = Some(PaperPosition {
                                is_short: sig.is_short,
                                entry_price: simulated_entry,
                                entry_time: now,
                                highest_pnl: 0.0,
                            });
                        }
                    }
                }

                // ── Mensagens WebSocket (preservado intacto) ──────────────
                msg_opt = ws.next() => {
                    let message = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            error!("❌ WS erro: {}. Reconectando...", e);
                            break;
                        }
                        None => {
                            warn!("📴 WS encerrado. Reconectando...");
                            break;
                        }
                    };

                    let text = match message {
                        Message::Binary(data) => match decompress_gzip(&data) {
                            Some(t) => t,
                            None => continue,
                        },
                        Message::Text(t) => {
                            if t == "Ping" {
                                let _ = ws.send(Message::Text("Pong".to_string())).await;
                                continue;
                            }
                            t
                        }
                        Message::Ping(p) => {
                            let _ = ws.send(Message::Pong(p)).await;
                            continue;
                        }
                        Message::Pong(_) => continue,
                        _ => continue,
                    };

                    if text == "Ping" {
                        let _ = ws.send(Message::Text("Pong".to_string())).await;
                        continue;
                    }
                    if text.contains("\"event\"") || text.contains("\"result\"") { continue; }

                    let parsed = match serde_json::from_str::<Value>(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let data_type = parsed["dataType"].as_str().unwrap_or("");
                    if !data_type.contains("@kline_1m") { continue; }

                    let symbol = data_type.split('@').next().unwrap_or("").to_string();
                    if symbol.is_empty() { continue; }

                      let data_val = &parsed["data"];
                    let kd_opt = if data_val.is_array() {
                        data_val.as_array().and_then(|arr| arr.last()).and_then(|v| v.as_object())
                    } else {
                        data_val.as_object()
                    };

                    if let Some(kd) = kd_opt { 
                        let parse_f = |key: &str| -> f64 {
                            kd.get(key)
                                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                .unwrap_or(0.0)
                        };
                        let o = parse_f("o");
                        let h = parse_f("h");
                        let l = parse_f("l");
                        let c = parse_f("c");
                        // V9.1: campo v = volume da vela (não volume_24h acumulador)
                        let mut v = parse_f("v");
                        let financeiro_usdt = v * c;
                        
                        if financeiro_usdt < 3000.0 {
                            // Se a vela não girou nem 3 mil dólares, é cidade fantasma.
                            // Forçamos o volume pra ZERO. A nota final no score.rs não vai bater 0.80.
                            v = 0.0; 
                        }
                        let ts_kline = kd.get("T")
                            .and_then(|t| t.as_i64()
                                .or_else(|| t.as_str().and_then(|s| s.parse().ok())))
                            .unwrap_or_else(|| Utc::now().timestamp());
                        if symbol == "BTC-USDT" {
                            macro_breaker.process_kline(&symbol, c, ts_kline);
                            continue; // O BTC não vai pro motor de trade, ele morre aqui.
                        }

                        if let Some(state) = market_state.get_mut(&symbol) {
                            let range = h - l;
                            if range > 0.0 {
                                state.upper_wick_ratio = ((h - o.max(c)) / range) * 100.0;
                                state.lower_wick_ratio = ((o.min(c) - l) / range) * 100.0;
                            }
                            // V9.1: ingere a vela no histórico de klines
                            // range e volume alimentam F_regime e F_volume respectivamente
                            if h > 0.0 && l > 0.0 {
                                state.ingest_kline(KlineBar {
                                    ts:     ts_kline,
                                    range:  (h - l),
                                    volume: v,
                                    close:  c,
                                });
                            }
                        }
                    }
                }
            } // fim tokio::select!
        } // fim loop interno

        warn!("🔄 Reconectando em 3s...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    } // fim 'reconnect

