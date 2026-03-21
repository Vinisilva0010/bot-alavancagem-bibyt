// =============================================================================
// APEX PREDATOR V8 — NEW LISTING ALPHA + TELEMETRY ENGINE
// Engine: Rust + Tokio | Exchange: BingX Perpetual Futures V2
// Estratégia: Micro-Momentum em Moedas Recém-Listadas em Futuros
// Paper Trade ONLY — Coleta massiva de dados para ML
// Março 2026
// =============================================================================

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

type HmacSha256 = Hmac<Sha256>;

// =============================================================================
// 🛑 CAPITAL LOCK — HARD-CODED, NUNCA CONTROLADO POR ENV
// Para habilitar trading real:
//   1. Mudar para false
//   2. Recompilar
//   3. Review de código obrigatório
// =============================================================================
const PAPER_TRADE_MODE: bool = true;

// =============================================================================
// CONSTANTES DE CONFIGURAÇÃO — V8 NEW LISTING ALPHA
// =============================================================================

/// Alavancagem para paper trades (simulação)
const LEVERAGE: u32 = 5;

/// % do saldo arriscado por paper trade
const RISK_PER_TRADE_PCT: f64 = 0.02;

/// Cooldown mínimo entre trades no mesmo símbolo (segundos)
const COOLDOWN_SEC: i64 = 30;

/// Intervalo de heartbeat WS (segundos)
const WS_PING_INTERVAL_SEC: u64 = 20;

/// Intervalo de ticker polling — preço, volume, funding, OI (segundos)
/// OI é buscado por endpoint dedicado per-symbol dentro do mesmo loop
const TICKER_POLL_INTERVAL_SEC: u64 = 3;

/// Intervalo de telemetria summary no log (segundos)
const TELEMETRY_LOG_INTERVAL_SEC: u64 = 60;

/// Buffer do canal mpsc para IPC
const IPC_CHANNEL_BUFFER: usize = 256;

/// Base URL REST BingX
const BINGX_REST: &str = "https://open-api.bingx.com";

/// WebSocket BingX Perpetual Futures
const BINGX_WS: &str = "wss://open-api-swap.bingx.com/swap-market";

// --- Parâmetros do Score de Sinal (V8.1 - Calibrado para Mid-Caps) ---

/// Score mínimo para disparar paper trade
const MIN_SIGNAL_SCORE: u32 = 35;

/// OI velocity: crescimento mínimo % por minuto para pontuar
/// DOGE/PEPE movem ~0.3-1.0%/min em momentum
const OI_VELOCITY_MIN_PCT: f64 = 0.3;

/// Volatilidade 1m mínima (%) para pontuar
/// Mid-caps fazem 0.15-0.5% em 1 minuto normalmente
const VOLATILITY_1M_MIN_PCT: f64 = 0.15;

/// Funding divergence mínima (%) para pontuar
/// Mid-caps têm funding 0.01-0.05% frequentemente
const FUNDING_DIVERGENCE_MIN: f64 = 0.01;

/// Volume surge ratio mínimo (atual / média 5m)
/// Spikes menores mas frequentes em voláteis
const VOLUME_SURGE_MIN_RATIO: f64 = 1.3;

// --- Parâmetros do Paper Trade Simulator ---

/// Taker fee BingX por leg (%)
const TAKER_FEE_PCT: f64 = 0.045;

/// Slippage simulado por leg (%)
const SIMULATED_SLIPPAGE_PCT: f64 = 0.02;

/// Stop loss (%)
const STOP_LOSS_PCT: f64 = 1.5;

/// Take profit (%)
const TAKE_PROFIT_PCT: f64 = 3.0;

/// Time stop: segundos máximos em posição
const TIME_STOP_SEC: i64 = 900; // 15 minutos

/// Trailing stop: se PnL > este %, move stop para breakeven
const TRAILING_BREAKEVEN_PCT: f64 = 1.0;

/// Tamanho máximo do histórico rolling (data points)
const ROLLING_WINDOW_SIZE: usize = 300;

// =============================================================================
// AUTENTICAÇÃO BINGX — HMAC-SHA256
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
// WARM-UP — MARGIN TYPE + POSITION MODE + LEVERAGE
// =============================================================================

async fn configure_margin_and_mode(client: &Client, symbol: &str) {
    // 1. Margin Type: ISOLATED
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

        match client
            .post(&url)
            .header("X-BX-APIKEY", &api_key)
            .send()
            .await
        {
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

    // 2. Position Mode: Hedge (Dual-Side)
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

        match client
            .post(&url)
            .header("X-BX-APIKEY", &api_key)
            .send()
            .await
        {
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

    // 3. Leverage para LONG e SHORT
    for side in &["LONG", "SHORT"] {
        let mut params = vec![
            ("leverage", LEVERAGE.to_string()),
            ("side", side.to_string()),
            ("symbol", symbol.to_string()),
            ("timestamp", timestamp_ms()),
        ];
        let (api_key, signed_query) = build_bingx_signature(&mut params);
        let url = format!(
            "{}/openApi/swap/v2/trade/leverage?{}",
            BINGX_REST, signed_query
        );

        match client
            .post(&url)
            .header("X-BX-APIKEY", &api_key)
            .send()
            .await
        {
            Ok(res) => {
                let parsed: Value = res.json().await.unwrap_or_default();
                if parsed["code"] == 0 {
                    info!("⚙️  [{}] Leverage {}x ({}): ✓", symbol, LEVERAGE, side);
                } else {
                    warn!(
                        "⚠️  [{}] Leverage ({}): {} (não fatal)",
                        symbol, side, parsed
                    );
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    }
}

// =============================================================================
// MÓDULO DE BALANÇO — BingX /openApi/swap/v2/user/balance
// =============================================================================

async fn fetch_usdt_balance(
    client: &Client,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let mut params = vec![("timestamp", timestamp_ms())];
    let (api_key, signed_query) = build_bingx_signature(&mut params);
    let url = format!(
        "{}/openApi/swap/v2/user/balance?{}",
        BINGX_REST, signed_query
    );

    let res = client
        .get(&url)
        .header("X-BX-APIKEY", &api_key)
        .send()
        .await?;

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
// DESCOMPRESSÃO GZIP — BingX comprime TODAS as mensagens WS
// =============================================================================

fn decompress_gzip(data: &[u8]) -> Option<String> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).ok()?;
    Some(decompressed)
}

// =============================================================================
// TICKER POLLING — Preço + Volume + Funding + Open Interest
//
// FLUXO (3 passos sequenciais a cada TICKER_POLL_INTERVAL_SEC):
//   1. GET /quote/ticker           → lastPrice + quoteVolume  (em lote)
//   2. GET /quote/premiumIndex     → lastFundingRate           (em lote)
//   3. GET /quote/openInterest?symbol=X → openInterest         (per-symbol)
//
// NOTA CRÍTICA: BingX /quote/ticker NÃO retorna openInterest!
// O campo OI deve ser buscado no endpoint dedicado (passo 3).
// =============================================================================

#[derive(Clone, Default, Debug)]
struct TickerSnapshot {
    last_price: f64,
    open_interest: f64,
    funding_rate: f64,
    volume_24h: f64,
}

type TickerMap = Arc<Mutex<HashMap<String, TickerSnapshot>>>;

/// Parse seguro de um campo JSON que pode vir como String ou f64.
/// Evita criar temporários que quebram o borrow checker.
fn parse_json_f64(val: &Value) -> f64 {
    if let Some(f) = val.as_f64() {
        return f;
    }
    if let Some(s) = val.as_str() {
        return s.parse::<f64>().unwrap_or(0.0);
    }
    0.0
}

async fn ticker_polling_loop(client: Client, symbols: Arc<Mutex<Vec<String>>>, ticker_map: TickerMap) {
    let mut interval =
        tokio::time::interval(tokio::time::Duration::from_secs(TICKER_POLL_INTERVAL_SEC));

    loop {
        interval.tick().await;
        let loop_start = std::time::Instant::now();

        let current_symbols = symbols.lock().await.clone();
        if current_symbols.is_empty() {
            continue;
        }

        let mut price_vol_map: HashMap<String, (f64, f64)> = HashMap::new();
        let mut funding_map: HashMap<String, f64> = HashMap::new();
        let mut oi_map: HashMap<String, f64> = HashMap::new();

        // ─── PASSO 1: Ticker em lote (preço + volume) ────────────────────
        // GET /openApi/swap/v2/quote/ticker → retorna TODAS as moedas
        // Campos usados: lastPrice, quoteVolume
        // O campo openInterest NÃO existe neste endpoint.
        {
            let url = format!("{}/openApi/swap/v2/quote/ticker", BINGX_REST);
            if let Ok(res) = client.get(&url).send().await {
                if let Ok(parsed) = res.json::<Value>().await {
                    if let Some(data) = parsed["data"].as_array() {
                        for item in data {
                            let sym = item["symbol"].as_str().unwrap_or("").to_string();
                            if current_symbols.contains(&sym) {
                                let price = parse_json_f64(&item["lastPrice"]);
                                let vol   = parse_json_f64(&item["quoteVolume"]);
                                price_vol_map.insert(sym, (price, vol));
                            }
                        }
                    }
                }
            }
        }

        // ─── PASSO 2: Funding Rate em lote ───────────────────────────────
        // GET /openApi/swap/v2/quote/premiumIndex → retorna TODAS as moedas
        // Campo usado: lastFundingRate (vem como decimal, ex: 0.0001 = 0.01%)
        {
            let url = format!("{}/openApi/swap/v2/quote/premiumIndex", BINGX_REST);
            if let Ok(res) = client.get(&url).send().await {
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
        }

        // ─── PASSO 3: Open Interest per-symbol (endpoint dedicado) ───────
        let fetches = futures_util::stream::iter(current_symbols.clone().into_iter().map(|sym| {
            let c = client.clone();
            let url = format!("{}/openApi/swap/v2/quote/openInterest?symbol={}", BINGX_REST, sym);
            async move {
                match c.get(&url).send().await {
                    Ok(res) => {
                        match res.json::<Value>().await {
                            Ok(parsed) => {
                                if parsed["code"].as_i64().unwrap_or(-1) == 0 {
                                    let oi = parse_json_f64(&parsed["data"]["openInterest"]);
                                    if oi > 0.0 {
                                        return Some((sym, oi));
                                    }
                                }
                            }
                            Err(e) => warn!("[OI] Parse falhou {}: {}", sym, e),
                        }
                    }
                    Err(e) => warn!("[OI] Request falhou {}: {}", sym, e),
                }
                None
            }
        }))
        .buffer_unordered(10) // 10 reqs simultaneas
        .collect::<Vec<_>>()
        .await;

        for op in fetches {
            if let Some((s, oi)) = op {
                oi_map.insert(s, oi);
            }
        }

        // ─── PASSO 4: Consolida tudo no TickerSnapshot ───────────────────
        {
            let mut map = ticker_map.lock().await;
            for symbol in &current_symbols {
                let (price, vol) = price_vol_map.get(symbol).copied().unwrap_or((0.0, 0.0));
                let funding = funding_map.get(symbol).copied().unwrap_or(0.0);
                let oi = oi_map.get(symbol).copied().unwrap_or(
                    // Se o OI não veio neste ciclo, preserva o valor anterior
                    map.get(symbol).map(|s| s.open_interest).unwrap_or(0.0)
                );

                if price > 0.0 {
                    map.insert(
                        symbol.clone(),
                        TickerSnapshot {
                            last_price: price,
                            open_interest: oi,
                            funding_rate: funding,
                            volume_24h: vol,
                        },
                    );
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
// ESTADO DE CADA MOEDA — V8 COM ROLLING WINDOWS
// =============================================================================

struct PaperPosition {
    is_short: bool,
    entry_price: f64,
    entry_time: i64,
    highest_pnl: f64, // para trailing stop
}

struct NewListingCoinState {
    /// Quando o Python avisou desta moeda
    listing_detected_at: i64,

    /// Rolling window de OI: (timestamp, valor)
    oi_history: VecDeque<(i64, f64)>,

    /// Rolling window de preços: (timestamp, valor)
    price_history: VecDeque<(i64, f64)>,

    /// Rolling window de volume: (timestamp, valor)
    volume_history: VecDeque<(i64, f64)>,

    /// Rolling window de funding: (timestamp, valor)
    funding_history: VecDeque<(i64, f64)>,

    // Wick ratios do candle mais recente
    upper_wick_ratio: f64,
    lower_wick_ratio: f64,

    // Métricas derivadas (calculadas a cada tick)
    oi_velocity_pct_min: f64,
    price_volatility_1m: f64,
    volume_surge_ratio: f64,

    // Paper trade
    paper_position: Option<PaperPosition>,
    last_signal_ts: i64,

    // Contadores de telemetria
    total_paper_trades: u32,
    paper_wins: u32,
    paper_losses: u32,
    paper_total_pnl: f64,
}

impl NewListingCoinState {
    fn new(detected_at: i64) -> Self {
        NewListingCoinState {
            listing_detected_at: detected_at,
            oi_history: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            price_history: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            volume_history: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            funding_history: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            upper_wick_ratio: 0.0,
            lower_wick_ratio: 0.0,
            oi_velocity_pct_min: 0.0,
            price_volatility_1m: 0.0,
            volume_surge_ratio: 0.0,
            paper_position: None,
            last_signal_ts: 0,
            total_paper_trades: 0,
            paper_wins: 0,
            paper_losses: 0,
            paper_total_pnl: 0.0,
        }
    }

    /// Atualiza rolling windows com dados do ticker snapshot
    fn ingest_ticker(&mut self, now: i64, snap: &TickerSnapshot) {
        // Push novos valores
        self.oi_history.push_back((now, snap.open_interest));
        self.price_history.push_back((now, snap.last_price));
        self.volume_history.push_back((now, snap.volume_24h));
        self.funding_history.push_back((now, snap.funding_rate));

        // Trunca para manter o tamanho do rolling window
        while self.oi_history.len() > ROLLING_WINDOW_SIZE {
            self.oi_history.pop_front();
        }
        while self.price_history.len() > ROLLING_WINDOW_SIZE {
            self.price_history.pop_front();
        }
        while self.volume_history.len() > ROLLING_WINDOW_SIZE {
            self.volume_history.pop_front();
        }
        while self.funding_history.len() > ROLLING_WINDOW_SIZE {
            self.funding_history.pop_front();
        }

        // Calcula métricas derivadas
        self.compute_oi_velocity(now);
        self.compute_price_volatility(now);
        self.compute_volume_surge(now);
    }

    /// OI velocity: % de mudança do OI no último minuto
    fn compute_oi_velocity(&mut self, now: i64) {
        let one_min_ago = now - 60;
        let current_oi = self.oi_history.back().map(|(_, v)| *v).unwrap_or(0.0);

        // Encontra OI de ~1 minuto atrás
        let old_oi = self
            .oi_history
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= one_min_ago)
            .map(|(_, v)| *v)
            .unwrap_or(current_oi);

        if old_oi > 0.0 {
            self.oi_velocity_pct_min = ((current_oi - old_oi) / old_oi) * 100.0;
        } else {
            self.oi_velocity_pct_min = 0.0;
        }
    }

    /// Volatilidade: (max - min) / média no último minuto
    fn compute_price_volatility(&mut self, now: i64) {
        let one_min_ago = now - 60;
        let recent: Vec<f64> = self
            .price_history
            .iter()
            .filter(|(ts, _)| *ts >= one_min_ago)
            .map(|(_, v)| *v)
            .collect();

        if recent.len() < 2 {
            self.price_volatility_1m = 0.0;
            return;
        }

        let max = recent.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let min = recent.iter().cloned().fold(f64::INFINITY, f64::min);
        let avg = recent.iter().sum::<f64>() / recent.len() as f64;

        if avg > 0.0 {
            self.price_volatility_1m = ((max - min) / avg) * 100.0;
        }
    }

    /// Volume surge: volume atual vs média dos últimos 5 minutos
    fn compute_volume_surge(&mut self, now: i64) {
        let five_min_ago = now - 300;
        let recent_vols: Vec<f64> = self
            .volume_history
            .iter()
            .filter(|(ts, _)| *ts >= five_min_ago)
            .map(|(_, v)| *v)
            .collect();

        if recent_vols.len() < 2 {
            self.volume_surge_ratio = 1.0;
            return;
        }

        let avg = recent_vols.iter().sum::<f64>() / recent_vols.len() as f64;
        let current = *recent_vols.last().unwrap_or(&0.0);

        if avg > 0.0 {
            self.volume_surge_ratio = current / avg;
        } else {
            self.volume_surge_ratio = 1.0;
        }
    }

    /// Horas desde a detecção da listagem
    fn hours_since_listing(&self, now: i64) -> f64 {
        (now - self.listing_detected_at) as f64 / 3600.0
    }
}

// =============================================================================
// SISTEMA DE SCORE V8 — ADAPTATIVO PARA NOVAS LISTAGENS
//
// OI Velocity       (0-30 pts): OI crescendo rápido = liquidez entrando
// Volatility 1m     (0-25 pts): Range alto = oportunidade de momentum
// Funding Divergence (0-25 pts): Funding extremo = crowded trade, mean reversion
// Volume Surge      (0-20 pts): Volume spike = interesse institucional
//
// Threshold: 50/100
// =============================================================================

struct SignalResult {
    score: u32,
    is_short: bool,
    oi_velocity: f64,
    volatility: f64,
    funding: f64,
    volume_surge: f64,
}

fn evaluate_new_listing_signal(
    state: &NewListingCoinState,
    snap: &TickerSnapshot,
    now: i64,
) -> Option<SignalResult> {
    if snap.last_price == 0.0 {
        return None;
    }

    // Cooldown
    if now - state.last_signal_ts < COOLDOWN_SEC {
        return None;
    }

    // Já em posição?
    if state.paper_position.is_some() {
        return None;
    }

    // Precisa de pelo menos 30 segundos de dados
    if state.price_history.len() < 10 {
        return None;
    }

    // --- Score SHORT: funding muito positivo → mercado over-leveraged long → short ---
    {
        let mut score: u32 = 0;

        // OI crescendo rápido
        if state.oi_velocity_pct_min.abs() >= OI_VELOCITY_MIN_PCT {
            score += 30;
        } else if state.oi_velocity_pct_min.abs() >= OI_VELOCITY_MIN_PCT * 0.5 {
            score += 15;
        }

        // Volatilidade alta
        if state.price_volatility_1m >= VOLATILITY_1M_MIN_PCT {
            score += 25;
        } else if state.price_volatility_1m >= VOLATILITY_1M_MIN_PCT * 0.5 {
            score += 12;
        }

        // Funding positivo extremo → short (mean reversion)
        if snap.funding_rate >= FUNDING_DIVERGENCE_MIN {
            score += 25;
        } else if snap.funding_rate >= FUNDING_DIVERGENCE_MIN * 0.5 {
            score += 12;
        }

        // Volume surge
        if state.volume_surge_ratio >= VOLUME_SURGE_MIN_RATIO {
            score += 20;
        } else if state.volume_surge_ratio >= VOLUME_SURGE_MIN_RATIO * 0.6 {
            score += 10;
        }

        if score >= MIN_SIGNAL_SCORE {
            return Some(SignalResult {
                score,
                is_short: true,
                oi_velocity: state.oi_velocity_pct_min,
                volatility: state.price_volatility_1m,
                funding: snap.funding_rate,
                volume_surge: state.volume_surge_ratio,
            });
        }
    }

    // --- Score LONG: funding muito negativo → mercado over-leveraged short → long ---
    {
        let mut score: u32 = 0;

        if state.oi_velocity_pct_min.abs() >= OI_VELOCITY_MIN_PCT {
            score += 30;
        } else if state.oi_velocity_pct_min.abs() >= OI_VELOCITY_MIN_PCT * 0.5 {
            score += 15;
        }

        if state.price_volatility_1m >= VOLATILITY_1M_MIN_PCT {
            score += 25;
        } else if state.price_volatility_1m >= VOLATILITY_1M_MIN_PCT * 0.5 {
            score += 12;
        }

        // Funding negativo extremo → long (mean reversion)
        if snap.funding_rate <= -FUNDING_DIVERGENCE_MIN {
            score += 25;
        } else if snap.funding_rate <= -FUNDING_DIVERGENCE_MIN * 0.5 {
            score += 12;
        }

        if state.volume_surge_ratio >= VOLUME_SURGE_MIN_RATIO {
            score += 20;
        } else if state.volume_surge_ratio >= VOLUME_SURGE_MIN_RATIO * 0.6 {
            score += 10;
        }

        if score >= MIN_SIGNAL_SCORE {
            return Some(SignalResult {
                score,
                is_short: false,
                oi_velocity: state.oi_velocity_pct_min,
                volatility: state.price_volatility_1m,
                funding: snap.funding_rate,
                volume_surge: state.volume_surge_ratio,
            });
        }
    }

    None
}

// =============================================================================
// PAPER TRADE SIMULATOR — Simula entrada/saída com fee + slippage
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
}

fn evaluate_paper_exit(
    pos: &PaperPosition,
    current_price: f64,
    now: i64,
) -> Option<PaperExitReason> {
    let raw_pnl_pct = if pos.is_short {
        ((pos.entry_price - current_price) / pos.entry_price) * 100.0
    } else {
        ((current_price - pos.entry_price) / pos.entry_price) * 100.0
    };

    // Desconta fees (entrada + saída)
    let net_pnl_pct = raw_pnl_pct - (TAKER_FEE_PCT * 2.0) - (SIMULATED_SLIPPAGE_PCT * 2.0);
    let duration = now - pos.entry_time;

    // Stop loss
    if net_pnl_pct <= -STOP_LOSS_PCT {
        return Some(PaperExitReason::StopLoss);
    }

    // Take profit
    if net_pnl_pct >= TAKE_PROFIT_PCT {
        return Some(PaperExitReason::TakeProfit);
    }

    // Trailing breakeven: se já esteve acima de TRAILING_BREAKEVEN_PCT mas agora voltou a 0
    if pos.highest_pnl >= TRAILING_BREAKEVEN_PCT && net_pnl_pct <= 0.0 {
        return Some(PaperExitReason::TrailingBreakeven);
    }

    // Time stop
    if duration > TIME_STOP_SEC {
        return Some(PaperExitReason::TimeStop);
    }

    None
}

/// Calcula PnL líquido do paper trade (com fees e slippage)
fn calculate_net_pnl(entry_price: f64, exit_price: f64, is_short: bool) -> f64 {
    let raw = if is_short {
        ((entry_price - exit_price) / entry_price) * 100.0
    } else {
        ((exit_price - entry_price) / entry_price) * 100.0
    };
    raw - (TAKER_FEE_PCT * 2.0) - (SIMULATED_SLIPPAGE_PCT * 2.0)
}

// =============================================================================
// TELEMETRIA — Emite um evento JSON estruturado para o IPC
// =============================================================================

fn build_telemetry_event(
    symbol: &str,
    event_type: &str,
    snap: &TickerSnapshot,
    state: &NewListingCoinState,
    extra: Option<Value>,
) -> String {
    let now_ms = Utc::now().timestamp_millis();
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
        "volume_surge":       state.volume_surge_ratio,
        "upper_wick":         state.upper_wick_ratio,
        "lower_wick":         state.lower_wick_ratio,
        "hours_listed":       state.hours_since_listing(Utc::now().timestamp()),
        "data_points":        state.price_history.len(),
    });

    if let Some(ext) = extra {
        if let (Some(base), Some(extra_map)) = (event.as_object_mut(), ext.as_object()) {
            for (k, v) in extra_map {
                base.insert(k.clone(), v.clone());
            }
        }
    }

    format!("{}\n", event)
}

// =============================================================================
// TASK IPC — Escreve no socket do Python Maestro
// =============================================================================

async fn ipc_writer_task(stream: Arc<Mutex<TcpStream>>, mut rx: mpsc::Receiver<String>) {
    while let Some(msg) = rx.recv().await {
        let mut guard = stream.lock().await;
        if let Err(e) = guard.write_all(msg.as_bytes()).await {
            error!("[IPC] Falha ao escrever no Maestro: {}", e);
        }
    }
}

// =============================================================================
// MOTOR PRINCIPAL — V8 NEW LISTING ALPHA + TELEMETRY ENGINE
// =============================================================================

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("╔═══════════════════════════════════════════════════╗");
    info!("║   APEX PREDATOR V8 — NEW LISTING ALPHA ENGINE     ║");
    info!("║   BingX Perpetual Futures V2 | Rust + Tokio       ║");
    info!("║   Mode: PAPER TRADE + TELEMETRY COLLECTION        ║");
    info!("║   Leverage: {}x | Hedge Mode | ISOLATED             ║", LEVERAGE);
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

    // --- 1. Autenticação e saldo ---
    let usdt_balance = fetch_usdt_balance(&http_client).await.unwrap_or(0.0);
    if usdt_balance == 0.0 {
        warn!("⚠️  Saldo 0 USDT — paper trades serão simulados com $50 virtual.");
    }
    let sim_balance = if usdt_balance > 0.0 {
        usdt_balance
    } else {
        50.0
    };
    info!(
        "💰 Paper trade sim balance: ${:.2} | Risk/trade: ${:.2}",
        sim_balance,
        sim_balance * RISK_PER_TRADE_PCT
    );

    // --- 2. Conexão IPC com Python Maestro ---
    let maestro_raw = TcpStream::connect("127.0.0.1:9000")
        .await
        .expect("Python Maestro offline. Inicie o Terminal 1 primeiro.");

    let maestro_stream = Arc::new(Mutex::new(maestro_raw));

    // Lê alvos iniciais do Maestro
    let initial_targets: Vec<String> = {
        let mut guard = maestro_stream.lock().await;
        let mut reader = BufReader::new(&mut *guard);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .expect("Falha ao ler alvos do Maestro");
        let cmd: Value = serde_json::from_str(line.trim()).expect("JSON inválido do Maestro");
        cmd["symbols"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    };

    info!("🎯 {} alvos iniciais recebidos do Maestro.", initial_targets.len());
    for t in &initial_targets {
        info!("   📌 {}", t);
    }

    {
        let mut g = maestro_stream.lock().await;
        let _ = g
            .write_all(b"V8 Telemetry Engine armado. Modo: PAPER + DATA COLLECTION.\n")
            .await;
    }

    // --- 3. Canais mpsc ---
    let (ipc_tx, ipc_rx) = mpsc::channel::<String>(IPC_CHANNEL_BUFFER);
    tokio::spawn(ipc_writer_task(Arc::clone(&maestro_stream), ipc_rx));

    // --- 4. Lista de símbolos compartilhada (atualizável pelo Maestro) ---
    let active_symbols: Arc<Mutex<Vec<String>>> =
        Arc::new(Mutex::new(initial_targets.clone()));

    // --- 5. Warm-up: Margin + Leverage ---
    info!("🔥 Warm-up: configurando margem e alavancagem...");
    for symbol in &initial_targets {
        configure_margin_and_mode(&http_client, symbol).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    info!("✅ Warm-up completo.");

    // --- 6. Estado de mercado por moeda ---
    let now = Utc::now().timestamp();
    let mut market_state: HashMap<String, NewListingCoinState> = initial_targets
        .iter()
        .map(|s| (s.clone(), NewListingCoinState::new(now)))
        .collect();

    // --- 7. Ticker polling (preço + volume + funding) ---
    let ticker_map: TickerMap = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(ticker_polling_loop(
        http_client.clone(),
        Arc::clone(&active_symbols),
        Arc::clone(&ticker_map),
    ));

    // --- 8. Task para receber comandos do Maestro (novas listagens, etc.) ---
    let maestro_reader_symbols = Arc::clone(&active_symbols);
    let maestro_reader_stream = Arc::clone(&maestro_stream);
    let ipc_tx_maestro = ipc_tx.clone();
    tokio::spawn(async move {
        // Lê linhas do Maestro continuamente
        loop {
            let line = {
                let mut guard = maestro_reader_stream.lock().await;
                let mut reader = BufReader::new(&mut *guard);
                let mut buf = String::new();
                match reader.read_line(&mut buf).await {
                    Ok(0) => {
                        warn!("[IPC] Maestro desconectou.");
                        break;
                    }
                    Ok(_) => buf,
                    Err(e) => {
                        error!("[IPC] Erro ao ler do Maestro: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            if let Ok(cmd) = serde_json::from_str::<Value>(trimmed) {
                let action = cmd["action"].as_str().unwrap_or("");

                if action == "NEW_LISTING" || action == "SET_TARGETS" {
                    if let Some(syms) = cmd["symbols"].as_array() {
                        let new_syms: Vec<String> = syms
                            .iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect();

                        if !new_syms.is_empty() {
                            warn!(
                                "🆕 [MAESTRO] {} novos alvos recebidos: {:?}",
                                new_syms.len(),
                                new_syms
                            );
                            let mut guard = maestro_reader_symbols.lock().await;
                            for s in &new_syms {
                                if !guard.contains(s) {
                                    guard.push(s.clone());
                                }
                            }
                        }
                    }
                }
            }

            let _ = ipc_tx_maestro
                .send(format!(
                    "{{\"action\":\"ACK\",\"ts\":{}}}\n",
                    Utc::now().timestamp()
                ))
                .await;
        }
    });

    // --- 9. Event loop com reconexão automática ---
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

        // Subscreve candles 1m para wick ratio
        let current_syms = active_symbols.lock().await.clone();
        for symbol in &current_syms {
            let sub = json!({
                "id":       "1",
                "reqType":  "sub",
                "dataType": format!("{}@kline_1m", symbol)
            });
            if ws.send(Message::Text(sub.to_string())).await.is_err() {
                error!("❌ Sub kline {} falhou. Reconectando...", symbol);
                continue 'reconnect;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        info!("📡 Subscrições ativas para {} símbolos. Telemetria rodando.", current_syms.len());

        let mut ping_iv =
            tokio::time::interval(tokio::time::Duration::from_secs(WS_PING_INTERVAL_SEC));
        let mut tele_iv =
            tokio::time::interval(tokio::time::Duration::from_secs(TELEMETRY_LOG_INTERVAL_SEC));

        // Ciclo de avaliação de sinal a cada 2s (independente do WS)
        let mut eval_iv =
            tokio::time::interval(tokio::time::Duration::from_secs(2));

        loop {
            tokio::select! {
                // --- Heartbeat ---
                _ = ping_iv.tick() => {
                    if ws.send(Message::Text("Ping".to_string())).await.is_err() {
                        error!("💔 Heartbeat falhou. Reconectando...");
                        break;
                    }
                }

                // --- Telemetria Summary (log) ---
                _ = tele_iv.tick() => {
                    let total_trades: u32 = market_state.values().map(|s| s.total_paper_trades).sum();
                    let total_wins: u32 = market_state.values().map(|s| s.paper_wins).sum();
                    let total_pnl: f64 = market_state.values().map(|s| s.paper_total_pnl).sum();
                    let active_positions: Vec<&String> = market_state
                        .iter()
                        .filter(|(_, s)| s.paper_position.is_some())
                        .map(|(k, _)| k)
                        .collect();

                    info!("═══════════════════ TELEMETRIA V8 ═══════════════════");
                    info!("  📊 Símbolos monitorados: {}", market_state.len());
                    info!("  📝 Paper trades totais: {} ({}W/{}L)",
                        total_trades, total_wins, total_trades - total_wins);
                    info!("  💰 PnL acumulado (paper): {:.4}%", total_pnl);
                    if !active_positions.is_empty() {
                        info!("  🔶 Posições abertas: {:?}", active_positions);
                    } else {
                        info!("  🟢 Sem posições abertas. Scanning...");
                    }

                    // Log status de cada moeda
                    let snap_map = ticker_map.lock().await;
                    for (sym, state) in &market_state {
                        if let Some(snap) = snap_map.get(sym) {
                            info!(
                                "  [{:>12}] px={:<10.6} | OI={:<12.1} | OI vel={:>+7.2}%/m | vol_1m={:>5.2}% | fund={:>+7.4}% | surge={:.1}x | pts={}",
                                sym, snap.last_price,
                                snap.open_interest,
                                state.oi_velocity_pct_min,
                                state.price_volatility_1m,
                                snap.funding_rate,
                                state.volume_surge_ratio,
                                state.price_history.len()
                            );
                        }
                    }
                    info!("═════════════════════════════════════════════════════");
                }

                // --- Avaliação de Sinal + Gestão de Posição (a cada 2s) ---
                _ = eval_iv.tick() => {
                    let now = Utc::now().timestamp();
                    let snap_map = ticker_map.lock().await.clone();

                    // Adiciona novos símbolos ao market_state se necessário
                    let current_syms = active_symbols.lock().await.clone();
                    for sym in &current_syms {
                        if !market_state.contains_key(sym) {
                            info!("🆕 [NEW TARGET] {} adicionado ao monitoramento.", sym);
                            market_state.insert(sym.clone(), NewListingCoinState::new(now));

                            // Subscreve kline para o novo símbolo
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

                        // Ingest ticker data into rolling windows
                        state.ingest_ticker(now, snap);

                        // Emit telemetry event
                        let tele = build_telemetry_event(sym, "ticker", snap, state, None);
                        let _ = ipc_tx.send(tele).await;

                        // --- Gestão de posição aberta ---
                        // Extraímos os dados para variáveis locais ANTES de chamar
                        // build_telemetry_event, evitando borrow simultâneo &mut e & de state.
                        let exit_info = if let Some(pos) = &mut state.paper_position {
                            let current_pnl = calculate_net_pnl(
                                pos.entry_price,
                                snap.last_price,
                                pos.is_short,
                            );
                            if current_pnl > pos.highest_pnl {
                                pos.highest_pnl = current_pnl;
                            }

                            if let Some(reason) = evaluate_paper_exit(pos, snap.last_price, now) {
                                let net_pnl = calculate_net_pnl(
                                    pos.entry_price,
                                    snap.last_price,
                                    pos.is_short,
                                );
                                // Copia tudo do pos para locals antes de dropar o borrow
                                let is_short = pos.is_short;
                                let entry_price = pos.entry_price;
                                let entry_time = pos.entry_time;
                                let highest_pnl = pos.highest_pnl;
                                let duration = now - entry_time;
                                let reason_str = reason.as_str().to_string();
                                let side_str = if is_short { "SHORT" } else { "LONG" };

                                Some((net_pnl, duration, is_short, entry_price, highest_pnl, reason_str, side_str.to_string()))
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        if let Some((net_pnl, duration, _is_short, entry_price, highest_pnl, reason_str, side_str)) = exit_info {
                            warn!(
                                "📝 [PAPER EXIT] {} {} | {} | PnL: {:+.4}% | {}s | highest: {:+.4}%",
                                side_str, sym, reason_str, net_pnl, duration, highest_pnl
                            );

                            // Fecha posição ANTES de chamar build_telemetry_event
                            // para liberar o &mut borrow de state.paper_position
                            state.paper_position = None;

                            // Agora state está livre — podemos passar &state
                            let report = build_telemetry_event(
                                sym,
                                "paper_exit",
                                snap,
                                state,
                                Some(json!({
                                    "action": "TRADE_CLOSED",
                                    "side": side_str,
                                    "entry_price": entry_price,
                                    "exit_price": snap.last_price,
                                    "pnl_pct": format!("{:.4}", net_pnl).parse::<f64>().unwrap_or(0.0),
                                    "duration_sec": duration,
                                    "exit_reason": reason_str,
                                    "highest_pnl": highest_pnl,
                                    "leverage": LEVERAGE,
                                    "fee_total_pct": TAKER_FEE_PCT * 2.0 + SIMULATED_SLIPPAGE_PCT * 2.0,
                                })),
                            );
                            let _ = ipc_tx.send(report).await;

                            // Atualiza contadores
                            state.total_paper_trades += 1;
                            if net_pnl > 0.0 {
                                state.paper_wins += 1;
                            } else {
                                state.paper_losses += 1;
                            }
                            state.paper_total_pnl += net_pnl;
                            state.last_signal_ts = now;

                            continue; // Posição fechada, não avalia entrada neste tick
                        }

                        // Se ainda em posição (sem exit), pula avaliação de entrada
                        if state.paper_position.is_some() {
                            continue;
                        }

                        // --- Avaliação de entrada ---
                        if let Some(sig) = evaluate_new_listing_signal(state, snap, now) {
                            let simulated_entry = if sig.is_short {
                                snap.last_price * (1.0 - SIMULATED_SLIPPAGE_PCT / 100.0)
                            } else {
                                snap.last_price * (1.0 + SIMULATED_SLIPPAGE_PCT / 100.0)
                            };

                            warn!(
                                "🎯 [PAPER ENTRY] {} {} | Score: {}/100 | px={:.6} | OI vel={:+.2}%/m | vol={:.2}% | fund={:+.4}% | surge={:.1}x",
                                if sig.is_short { "SHORT" } else { "LONG" },
                                sym,
                                sig.score,
                                simulated_entry,
                                sig.oi_velocity,
                                sig.volatility,
                                sig.funding,
                                sig.volume_surge
                            );

                            // Telemetria do sinal
                            let tele = build_telemetry_event(
                                sym,
                                "paper_entry",
                                snap,
                                state,
                                Some(json!({
                                    "signal_direction": if sig.is_short { "SHORT" } else { "LONG" },
                                    "signal_score": sig.score,
                                    "simulated_entry_price": simulated_entry,
                                    "leverage": LEVERAGE,
                                })),
                            );
                            let _ = ipc_tx.send(tele).await;

                            // Abre paper position
                            state.paper_position = Some(PaperPosition {
                                is_short: sig.is_short,
                                entry_price: simulated_entry,
                                entry_time: now,
                                highest_pnl: 0.0,
                            });
                            state.last_signal_ts = now;
                        }
                    }
                }

                // --- Mensagens WebSocket (GZIP) ---
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
                        Message::Binary(data) => {
                            match decompress_gzip(&data) {
                                Some(t) => t,
                                None => continue,
                            }
                        }
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

                    // Ignora mensagens de controle
                    if text.contains("\"event\"") || text.contains("\"result\"") {
                        continue;
                    }

                    let parsed = match serde_json::from_str::<Value>(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    // BingX kline: {"dataType":"SYM@kline_1m","data":{...}}
                    let data_type = parsed["dataType"].as_str().unwrap_or("");
                    if !data_type.contains("@kline_1m") {
                        continue;
                    }

                    let symbol = data_type
                        .split('@')
                        .next()
                        .unwrap_or("")
                        .to_string();

                    if symbol.is_empty() {
                        continue;
                    }

                    // Atualiza wick ratio do candle
                    if let Some(kd) = parsed["data"].as_object() {
                        let parse_f = |key: &str| -> f64 {
                            kd.get(key)
                                .and_then(|v| {
                                    v.as_f64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                                .unwrap_or(0.0)
                        };

                        let o = parse_f("o");
                        let h = parse_f("h");
                        let l = parse_f("l");
                        let c = parse_f("c");

                        if let Some(state) = market_state.get_mut(&symbol) {
                            let range = h - l;
                            if range > 0.0 {
                                state.upper_wick_ratio =
                                    ((h - o.max(c)) / range) * 100.0;
                                state.lower_wick_ratio =
                                    ((o.min(c) - l) / range) * 100.0;
                            }
                        }
                    }
                }
            }
        }

        warn!("🔄 Reconectando em 3s...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    }
}