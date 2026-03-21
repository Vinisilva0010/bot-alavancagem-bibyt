// =============================================================================
// APEX PREDATOR V7 — LIQUIDATION CASCADE SNIPER
// Engine: Rust + Tokio | Exchange: MEXC Futures V1
// Estratégia: OI Exhaustion + Wick Rejection + Funding Extremo
// Migrado de OKX → MEXC | Março 2026
// =============================================================================
//
// Cargo.toml:
// [dependencies]
// tokio            = { version = "1", features = ["full"] }
// tokio-tungstenite = { version = "0.21", features = ["native-tls"] }
// futures-util     = "0.3"
// reqwest          = { version = "0.11", features = ["json"] }
// serde_json       = "1"
// hmac             = "0.12"
// sha2             = "0.10"
// hex              = "0.4"
// chrono           = "0.4"
// log              = "0.4"
// env_logger       = "0.11"
// dotenv           = "0.15"
// =============================================================================

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use log::{error, info, warn};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

type HmacSha256 = Hmac<Sha256>;

// =============================================================================
// CONSTANTES DE CONFIGURAÇÃO
// =============================================================================

/// Alavancagem aplicada no warm-up para todos os símbolos
const LEVERAGE: u32 = 20;

/// % mínimo de queda do OI na janela para acionar análise
const OI_DROP_ALARM_PCT: f64 = 1.5;

/// Janela de referência do OI em segundos
const OI_WINDOW_SEC: i64 = 300;

/// % mínimo do pavio em relação ao candle total
const MIN_WICK_RATIO: f64 = 35.0;

/// Funding rate mínimo (%) para considerar euforia/pânico
const FUNDING_THRESHOLD: f64 = 0.01;

/// % de perda máxima antes do stop loss hard
const STOP_LOSS_PCT: f64 = 1.5;

/// % de lucro alvo
const TAKE_PROFIT_PCT: f64 = 2.5;

/// Segundos máximos aguardando snap-back
const TIME_STOP_SEC: i64 = 90;

/// % do saldo arriscado por trade
const RISK_PER_TRADE_PCT: f64 = 0.01;

/// Cooldown mínimo entre trades no mesmo símbolo (segundos)
const COOLDOWN_SEC: i64 = 120;

/// Intervalo do heartbeat WS (segundos)
const WS_PING_INTERVAL_SEC: u64 = 20;

/// Intervalo de polling do OI (segundos) — 10 símbolos * 1s = 10 req/s < limite de 20
const OI_POLL_INTERVAL_SEC: u64 = 1;

/// Intervalo do log de telemetria (segundos)
const TELEMETRY_INTERVAL_SEC: u64 = 300;

/// Buffer do canal mpsc para relatórios IPC (mensagens)
const IPC_CHANNEL_BUFFER: usize = 128;

// =============================================================================
// AUTENTICAÇÃO MEXC — HMAC-SHA256
// Payload: api_key + timestamp + param_string
// Diferença crítica da OKX: sem Passphrase. Headers: ApiKey, Request-Time, Signature.
// =============================================================================

fn generate_mexc_signature(param_string: &str) -> (String, String, String) {
    let api_key = env::var("MEXC_API_KEY").expect("MEXC_API_KEY não encontrada no .env");
    let secret_key = env::var("MEXC_SECRET_KEY").expect("MEXC_SECRET_KEY não encontrada no .env");

    let timestamp = Utc::now().timestamp_millis().to_string();

    // Payload MEXC: api_key + timestamp + param_string
    let payload = format!("{}{}{}", api_key, timestamp, param_string);

    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes())
        .expect("HMAC aceita qualquer tamanho de chave");
    mac.update(payload.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    (api_key, timestamp, signature)
}

// =============================================================================
// MÓDULO DE INSTRUMENTOS — MEXC /api/v1/contract/detail
// =============================================================================

#[derive(Clone, Debug)]
struct InstrumentMeta {
    /// Tamanho mínimo de variação de preço
    price_unit: f64,
    /// Valor em USDT por contrato
    contract_size: f64,
    /// Casas decimais do preço
    price_decimals: usize,
}

async fn fetch_instrument_meta(
    client: &Client,
    symbol: &str,
) -> Result<InstrumentMeta, Box<dyn std::error::Error + Send + Sync>> {
    let url = "https://contract.mexc.com/api/v1/contract/detail";
    let res = client
        .get(url)
        .query(&[("symbol", symbol)])
        .send()
        .await?;

    let parsed: Value = res.json().await?;

    if parsed["success"] != true {
        return Err(format!("MEXC detail falhou para {}: {}", symbol, parsed).into());
    }

    let data = &parsed["data"];
    let price_unit: f64 = data["priceUnit"].as_f64().unwrap_or(0.01);
    let contract_size: f64 = data["contractSize"].as_f64().unwrap_or(1.0);

    let price_decimals = if price_unit < 1.0 {
        (-price_unit.log10().floor()) as usize
    } else {
        0
    };

    Ok(InstrumentMeta {
        price_unit,
        contract_size,
        price_decimals,
    })
}

fn round_to_tick(price: f64, price_unit: f64, decimals: usize) -> String {
    let factor = 10f64.powi(decimals as i32);
    let rounded = (price / price_unit).round() * price_unit;
    format!("{:.prec$}", (rounded * factor).round() / factor, prec = decimals)
}

// =============================================================================
// WARM-UP — ALAVANCAGEM
// Chamado durante o boot para todos os símbolos. NUNCA no momento do gatilho.
// MEXC: POST /api/v1/private/position/change_leverage
// =============================================================================

async fn set_leverage(
    client: &Client,
    symbol: &str,
    leverage: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = "/api/v1/private/position/change_leverage";
    let base_url = "https://contract.mexc.com";

    // MEXC: parâmetros como query string para GET autenticado, ou body para POST
    // change_leverage usa POST com body JSON
    let body = json!({
        "symbol": symbol,
        "leverage": leverage,
        "openType": 1  // 1 = isolated, 2 = cross
    })
    .to_string();

    let param_string = body.clone();
    let (api_key, timestamp, signature) = generate_mexc_signature(&param_string);

    let res = client
        .post(format!("{}{}", base_url, path))
        .header("ApiKey", &api_key)
        .header("Request-Time", &timestamp)
        .header("Signature", &signature)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;

    let parsed: Value = res.json().await?;

    if parsed["success"] == true {
        info!("⚙️  [{}] Alavancagem {}x configurada (isolated).", symbol, leverage);
        Ok(())
    } else {
        Err(format!("Falha ao setar leverage para {}: {}", symbol, parsed).into())
    }
}

// =============================================================================
// MÓDULO DE BALANÇO — MEXC /api/v1/private/account/assets
// =============================================================================

async fn fetch_usdt_balance(client: &Client) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let path = "/api/v1/private/account/assets";
    let base_url = "https://contract.mexc.com";

    // GET autenticado — param_string vazio para endpoints sem query params
    let (api_key, timestamp, signature) = generate_mexc_signature("");

    let res = client
        .get(format!("{}{}", base_url, path))
        .header("ApiKey", &api_key)
        .header("Request-Time", &timestamp)
        .header("Signature", &signature)
        .send()
        .await?;

    let parsed: Value = res.json().await?;

    if parsed["success"] != true {
        error!("💀 [AUTH] Falha na autenticação MEXC: {}", parsed);
        panic!("Credenciais inválidas. Abortando.");
    }

    if let Some(assets) = parsed["data"].as_array() {
        for asset in assets {
            if asset["currency"] == "USDT" {
                let balance = asset["availableBalance"]
                    .as_f64()
                    .unwrap_or(0.0);
                info!("🏦 [MEXC] Auth OK. Saldo disponível: {:.2} USDT", balance);
                return Ok(balance);
            }
        }
    }

    warn!("⚠️ [MEXC] USDT não encontrado. Verifique se é conta de futuros.");
    Ok(0.0)
}

// =============================================================================
// MÓDULO DE EXECUÇÃO — MEXC /api/v1/private/order/submit
//
// MEXC side encoding (diferente da OKX):
//   1 = open long
//   2 = close short  
//   3 = open short
//   4 = close long
//
// orderType: 1 = limit (post-only via priceType=2), 5 = market
// openType:  1 = isolated, 2 = cross
// =============================================================================

async fn execute_entry_order(
    client: &Client,
    symbol: &str,
    is_short: bool,
    price: f64,
    balance: f64,
    meta: &InstrumentMeta,
    result_tx: mpsc::Sender<(String, bool, f64, bool)>, // (symbol, success, price, is_short)
) {
    let path = "/api/v1/private/order/submit";
    let base_url = "https://contract.mexc.com";

    // MEXC side: 1=open long, 3=open short
    let side: u8 = if is_short { 3 } else { 1 };

    let notional = balance * RISK_PER_TRADE_PCT;
    // vol em MEXC = número de contratos
    let vol = (notional / (price * meta.contract_size))
        .floor()
        .max(1.0) as u64;

    let px_str = round_to_tick(price, meta.price_unit, meta.price_decimals);

    let body = json!({
        "symbol":    symbol,
        "price":     px_str,
        "vol":       vol,
        "side":      side,
        "type":      1,        // limit
        "openType":  1,        // isolated
        "leverage":  LEVERAGE
    })
    .to_string();

    let (api_key, timestamp, signature) = generate_mexc_signature(&body);

    match client
        .post(format!("{}{}", base_url, path))
        .header("ApiKey", &api_key)
        .header("Request-Time", &timestamp)
        .header("Signature", &signature)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(res) => {
            let text = res.text().await.unwrap_or_default();
            let parsed: Value = serde_json::from_str(&text).unwrap_or_default();
            let success = parsed["success"] == true;

            if success {
                warn!(
                    "💥 [ENTRADA] {} {} | vol={} | px={} | Aceito pela MEXC",
                    if is_short { "SHORT" } else { "LONG" },
                    symbol,
                    vol,
                    price
                );
            } else {
                error!("❌ [ENTRADA REJEITADA] {} | Resposta: {}", symbol, text);
            }

            // Fire-and-forget: envia resultado pelo canal sem bloquear o caller
            let _ = result_tx.send((symbol.to_string(), success, price, is_short)).await;
        }
        Err(e) => {
            error!("❌ [REDE] Falha ao enviar entrada para {}: {}", symbol, e);
            let _ = result_tx.send((symbol.to_string(), false, price, is_short)).await;
        }
    }
}

// =============================================================================
// MÓDULO DE SAÍDA — MEXC market order para garantir execução
// MEXC side: 2=close short, 4=close long
// =============================================================================

async fn execute_exit_order(
    client: &Client,
    symbol: &str,
    is_short: bool,
    vol: u64,
) {
    let path = "/api/v1/private/order/submit";
    let base_url = "https://contract.mexc.com";

    // Para fechar: 2=close short, 4=close long
    let side: u8 = if is_short { 2 } else { 4 };

    let body = json!({
        "symbol":   symbol,
        "vol":      vol,
        "side":     side,
        "type":     5,   // market — garante execução na saída
        "openType": 1
    })
    .to_string();

    let (api_key, timestamp, signature) = generate_mexc_signature(&body);

    match client
        .post(format!("{}{}", base_url, path))
        .header("ApiKey", &api_key)
        .header("Request-Time", &timestamp)
        .header("Signature", &signature)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(res) => {
            let text = res.text().await.unwrap_or_default();
            warn!(
                "🛡️ [SAÍDA] {} {} | vol={} | Resposta: {}",
                if is_short { "SHORT" } else { "LONG" },
                symbol,
                vol,
                text
            );
        }
        Err(e) => {
            error!(
                "❌ [SAÍDA CRÍTICA] Falha ao fechar {} para {}: {}",
                symbol, if is_short { "SHORT" } else { "LONG" }, e
            );
            // TODO V8: implementar retry com exponential backoff aqui
        }
    }
}

// =============================================================================
// POLLING DE OPEN INTEREST — MEXC /api/v1/contract/openInterest
// MEXC NÃO tem stream de OI. Polling REST a cada OI_POLL_INTERVAL_SEC.
// 10 símbolos * 1 req/s = 10 req/s < limite de 20 req/s da MEXC.
//
// Arquitetura: task dedicado escreve em Arc<Mutex<HashMap>>.
// Event loop lê do HashMap sem fazer I/O — nunca bloqueia o select!.
// =============================================================================

type OiMap = Arc<Mutex<HashMap<String, f64>>>;

async fn oi_polling_loop(client: Client, symbols: Vec<String>, oi_map: OiMap) {
    let mut interval =
        tokio::time::interval(tokio::time::Duration::from_secs(OI_POLL_INTERVAL_SEC));

    loop {
        interval.tick().await;

        for symbol in &symbols {
            let url = format!(
                "https://contract.mexc.com/api/v1/contract/openInterest?symbol={}",
                symbol
            );

            match client.get(&url).send().await {
                Ok(res) => match res.json::<Value>().await {
                    Ok(parsed) => {
                        if let Some(oi) = parsed["data"]["openInterest"].as_f64() {
                            let mut map = oi_map.lock().await;
                            map.insert(symbol.clone(), oi);
                        }
                    }
                    Err(e) => {
                        warn!("[OI-POLL] Parse falhou para {}: {}", symbol, e);
                    }
                },
                Err(e) => {
                    warn!("[OI-POLL] Request falhou para {}: {}", symbol, e);
                }
            }

            // Micro-sleep entre requests do mesmo batch para não saturar
            // 10 símbolos em 1 segundo = 100ms por símbolo
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}

// =============================================================================
// ESTADO DE CADA MOEDA MONITORADA
// =============================================================================

struct CoinState {
    // Dados de mercado
    oi_peak: f64,
    oi_peak_ts: i64,
    current_funding: f64,
    last_close: f64,
    upper_wick_ratio: f64,
    lower_wick_ratio: f64,

    // Controle de trades
    last_shot_ts: i64,

    // Estado da posição aberta
    position_active: bool,
    position_is_short: bool,
    entry_price: f64,
    entry_time: i64,
    position_vol: u64,

    // Flag para evitar duplo gatilho enquanto ordem está em voo
    order_in_flight: bool,
}

impl CoinState {
    fn new(initial_price: f64) -> Self {
        CoinState {
            oi_peak: 0.0,
            oi_peak_ts: 0,
            current_funding: 0.0,
            last_close: initial_price,
            upper_wick_ratio: 0.0,
            lower_wick_ratio: 0.0,
            last_shot_ts: 0,
            position_active: false,
            position_is_short: false,
            entry_price: 0.0,
            entry_time: 0,
            position_vol: 0,
            order_in_flight: false,
        }
    }
}

// =============================================================================
// LÓGICA DE SINAL
// =============================================================================

enum TradeSignal {
    Short,
    Long,
    None,
}

fn evaluate_signal(state: &CoinState, current_oi: f64, current_time: i64) -> TradeSignal {
    if state.last_close == 0.0 || state.oi_peak == 0.0 || current_oi == 0.0 {
        return TradeSignal::None;
    }
    if current_time - state.last_shot_ts < COOLDOWN_SEC {
        return TradeSignal::None;
    }
    if state.order_in_flight {
        return TradeSignal::None;
    }

    let drop_pct = ((state.oi_peak - current_oi) / state.oi_peak) * 100.0;
    if drop_pct < OI_DROP_ALARM_PCT {
        return TradeSignal::None;
    }

    let short_ok = state.current_funding > FUNDING_THRESHOLD
        && state.upper_wick_ratio > MIN_WICK_RATIO;

    let long_ok = state.current_funding < -FUNDING_THRESHOLD
        && state.lower_wick_ratio > MIN_WICK_RATIO;

    if short_ok {
        TradeSignal::Short
    } else if long_ok {
        TradeSignal::Long
    } else {
        TradeSignal::None
    }
}

// =============================================================================
// LÓGICA DE SAÍDA
// =============================================================================

enum ExitReason {
    StopLoss,
    TakeProfit,
    TimeStopProfit,
    TimeStopLoss,
}

fn evaluate_exit(state: &CoinState, current_time: i64) -> Option<ExitReason> {
    if !state.position_active || state.entry_price == 0.0 {
        return None;
    }

    let pnl_pct = if state.position_is_short {
        ((state.entry_price - state.last_close) / state.entry_price) * 100.0
    } else {
        ((state.last_close - state.entry_price) / state.entry_price) * 100.0
    };

    let duration = current_time - state.entry_time;

    if pnl_pct <= -STOP_LOSS_PCT {
        return Some(ExitReason::StopLoss);
    }
    if pnl_pct >= TAKE_PROFIT_PCT {
        return Some(ExitReason::TakeProfit);
    }
    if duration > TIME_STOP_SEC {
        return Some(if pnl_pct > 0.0 {
            ExitReason::TimeStopProfit
        } else {
            ExitReason::TimeStopLoss
        });
    }

    None
}

// =============================================================================
// TASK IPC — Consome canal mpsc e escreve no socket do Python Maestro
// Desacoplado do event loop. Se o Python estiver lento, o buffer do canal
// absorve — o event loop NUNCA bloqueia esperando o socket.
// =============================================================================

async fn ipc_writer_task(
    maestro_stream: Arc<Mutex<TcpStream>>,
    mut rx: mpsc::Receiver<String>,
) {
    while let Some(msg) = rx.recv().await {
        let mut guard = maestro_stream.lock().await;
        if let Err(e) = guard.write_all(msg.as_bytes()).await {
            error!("[IPC] Falha ao escrever no socket do Maestro: {}", e);
            // Não mata o task — o próximo msg tentará novamente
        }
    }
}

// =============================================================================
// MOTOR PRINCIPAL
// =============================================================================

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("╔══════════════════════════════════════════════╗");
    info!("║   APEX PREDATOR V7 — LIQUIDATION SNIPER      ║");
    info!("║   MEXC Futures V1 | Rust + Tokio             ║");
    info!("╚══════════════════════════════════════════════╝");

    let http_client = Client::new();

    // --- 1. Autenticação e saldo ---
    let usdt_balance = fetch_usdt_balance(&http_client).await.unwrap_or(0.0);
    if usdt_balance == 0.0 {
        warn!("⚠️  [PAPER TRADE] Saldo 0 USDT — ordens logadas mas NÃO enviadas.");
    }

    // --- 2. Recebe alvos do Python Maestro via IPC ---
    let maestro_raw = TcpStream::connect("127.0.0.1:9000")
        .await
        .expect("Python Maestro offline. Inicie o Terminal 1 primeiro.");

    let maestro_stream: Arc<Mutex<TcpStream>> = Arc::new(Mutex::new(maestro_raw));

    let alvos: Vec<String> = {
        let mut guard = maestro_stream.lock().await;
        let mut reader = BufReader::new(&mut *guard);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .expect("Falha ao ler alvos do Maestro");

        let cmd: Value =
            serde_json::from_str(line.trim()).expect("JSON inválido do Maestro");
        let symbols: Vec<String> = cmd["symbols"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        info!("🎯 {} alvos recebidos do Maestro.", symbols.len());
        symbols
    };

    {
        let mut guard = maestro_stream.lock().await;
        let _ = guard
            .write_all(b"Sniper armado. Aguardando liquidacoes.\n")
            .await;
    }

    // --- 3. Canal mpsc para IPC desacoplado ---
    // O event loop envia strings para o canal. O ipc_writer_task consome
    // e escreve no socket. Buffer de 128 mensagens — nunca bloqueia o select!.
    let (ipc_tx, ipc_rx) = mpsc::channel::<String>(IPC_CHANNEL_BUFFER);
    tokio::spawn(ipc_writer_task(Arc::clone(&maestro_stream), ipc_rx));

    // --- 4. Canal mpsc para confirmação de ordens (fire-and-forget) ---
    // execute_entry_order roda em task separado e envia resultado aqui.
    // O event loop processa a confirmação no próximo tick — sem bloqueio.
    let (order_tx, mut order_rx) =
        mpsc::channel::<(String, bool, f64, bool)>(32);

    // --- 5. Warm-up: meta de instrumentos + alavancagem + preços iniciais ---
    info!("🔥 Warm-up: buscando metadados e configurando alavancagem...");

    let mut instrument_map: HashMap<String, InstrumentMeta> = HashMap::new();

    for symbol in &alvos {
        // Meta do instrumento
        match fetch_instrument_meta(&http_client, symbol).await {
            Ok(meta) => {
                info!(
                    "📐 [{}] priceUnit={} | contractSize={}",
                    symbol, meta.price_unit, meta.contract_size
                );
                instrument_map.insert(symbol.clone(), meta);
            }
            Err(e) => {
                // Falha no warm-up é fatal — não operamos com meta incorreta
                panic!(
                    "💀 Falha ao buscar meta de {}. Abortando para evitar ordens malformadas. Erro: {}",
                    symbol, e
                );
            }
        }

        // Alavancagem — configurada uma vez no boot, nunca no gatilho
        match set_leverage(&http_client, symbol, LEVERAGE).await {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "⚠️ [{}] Falha ao setar leverage: {}. Continuando com leverage existente.",
                    symbol, e
                );
            }
        }

        // Micro-sleep entre requests do warm-up para respeitar rate limit
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    // Preços iniciais via REST — elimina cold start do candle WS
    let mut market_state: HashMap<String, CoinState> = HashMap::new();
    {
        let url = "https://contract.mexc.com/api/v1/contract/ticker";
        if let Ok(res) = http_client.get(url).send().await {
            if let Ok(parsed) = res.json::<Value>().await {
                if let Some(data) = parsed["data"].as_array() {
                    for item in data {
                        let sym = item["symbol"].as_str().unwrap_or("").to_string();
                        if alvos.contains(&sym) {
                            let price = item["lastPrice"]
                            .as_f64()
                            .unwrap_or(0.0);
                            market_state.insert(sym.clone(), CoinState::new(price));
                            info!("🌡️  [{}] Preço de largada: {}", sym, price);
                        }
                    }
                }
            }
        }
    }
    // Garante entrada para todos os alvos mesmo sem preço inicial
    for symbol in &alvos {
        market_state.entry(symbol.clone()).or_insert_with(|| CoinState::new(0.0));
    }

    info!("✅ Warm-up completo. Sniper operacional.");

    // --- 6. OI Polling — task dedicado, nunca toca o event loop ---
    let oi_map: OiMap = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(oi_polling_loop(
        http_client.clone(),
        alvos.clone(),
        Arc::clone(&oi_map),
    ));

    // --- 7. Loop principal com reconexão automática ---
    let ws_url = "wss://contract.mexc.com/edge";

    'reconnect: loop {
        info!("🔌 Conectando ao WebSocket MEXC Futures...");

        let (mut ws, _) = match connect_async(ws_url).await {
            Ok(s) => s,
            Err(e) => {
                error!("❌ Falha na conexão WS: {}. Tentando em 5s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue 'reconnect;
            }
        };

        // Subscreve candles + funding para todos os alvos
        // MEXC WS: {"method":"sub.kline","param":{"symbol":"BTC_USDT","interval":"Min1"}}
        for symbol in &alvos {
            let sub_candle = json!({
                "method": "sub.kline",
                "param": {
                    "symbol": symbol,
                    "interval": "Min1"
                }
            });
            if ws.send(Message::Text(sub_candle.to_string())).await.is_err() {
                error!("❌ Falha ao subscrever candle de {}. Reconectando...", symbol);
                continue 'reconnect;
            }

            let sub_funding = json!({
                "method": "sub.funding.rate",
                "param": { "symbol": symbol }
            });
            if ws.send(Message::Text(sub_funding.to_string())).await.is_err() {
                error!("❌ Falha ao subscrever funding de {}. Reconectando...", symbol);
                continue 'reconnect;
            }
        }

        info!("📡 Subscrições ativas. Aguardando sinais de liquidação...");

        let mut ping_interval =
            tokio::time::interval(tokio::time::Duration::from_secs(WS_PING_INTERVAL_SEC));
        let mut telemetry_interval =
            tokio::time::interval(tokio::time::Duration::from_secs(TELEMETRY_INTERVAL_SEC));

        // --- 8. Event loop ---
        loop {
            tokio::select! {

                // --- Heartbeat ---
                _ = ping_interval.tick() => {
                    // MEXC WS usa ping via mensagem texto, não frame Ping
                    let ping_msg = json!({"method": "ping"});
                    if ws.send(Message::Text(ping_msg.to_string())).await.is_err() {
                        error!("💔 Heartbeat falhou. Reconectando...");
                        break;
                    }
                }

                // --- Telemetria ---
                _ = telemetry_interval.tick() => {
                    let active: Vec<&String> = market_state
                        .iter()
                        .filter(|(_, s)| s.position_active)
                        .map(|(k, _)| k)
                        .collect();

                    if active.is_empty() {
                        info!("🟢 [TELEMETRIA] Nenhuma posição aberta. Aguardando setup.");
                    } else {
                        warn!("🔶 [TELEMETRIA] Posições ativas: {:?}", active);
                    }
                }

                // --- Confirmações de ordem (fire-and-forget) ---
                // Processa resultados de execute_entry_order sem ter bloqueado o loop
                Some((symbol, success, price, is_short)) = order_rx.recv() => {
                    if let Some(state) = market_state.get_mut(&symbol) {
                        state.order_in_flight = false;

                        if success {
                            state.position_active   = true;
                            state.position_is_short = is_short;
                            state.entry_price       = price;
                            state.entry_time        = Utc::now().timestamp();
                            // vol é estimado — em produção buscar via GET /order após fill
                            let meta = instrument_map.get(&symbol).unwrap();
                            state.position_vol = (
                                (usdt_balance * RISK_PER_TRADE_PCT) /
                                (price * meta.contract_size)
                            ).floor().max(1.0) as u64;

                            // Reset do pico de OI após entrada confirmada
                            let oi_snap = oi_map.lock().await;
                            if let Some(&oi) = oi_snap.get(&symbol) {
                                state.oi_peak    = oi;
                                state.oi_peak_ts = Utc::now().timestamp();
                            }
                        }
                    }
                }

                // --- Mensagens do WebSocket ---
                msg_opt = ws.next() => {
                    let message = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            error!("❌ Erro no stream WS: {}. Reconectando...", e);
                            break;
                        }
                        None => {
                            warn!("📴 Stream WS encerrado. Reconectando...");
                            break;
                        }
                    };

                    let text = match message {
                        Message::Text(t) => t,
                        Message::Ping(p) => {
                            let _ = ws.send(Message::Pong(p)).await;
                            continue;
                        }
                        Message::Pong(_) => continue,
                        _ => continue,
                    };

                    // Ignora mensagens de controle
                    if text.contains("\"channel\":\"pong\"") || text.contains("\"channel\":\"rs.") {
                        continue;
                    }

                    let parsed = match serde_json::from_str::<Value>(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let channel = parsed["channel"].as_str().unwrap_or("");
                    let symbol  = parsed["symbol"].as_str().unwrap_or("").to_string();

                    if symbol.is_empty() || !alvos.contains(&symbol) {
                        continue;
                    }

                    let now = Utc::now().timestamp();

                    // -----------------------------------------------------------
                    // CANAL: Candle 1m
                    // MEXC envia updates do candle em andamento — não rejeitar por ts
                    // -----------------------------------------------------------
                    if channel == "push.kline" {
                        if let Some(kd) = parsed["data"].as_object() {
                            let parse = |key: &str| -> f64 {
                                kd.get(key)
                                  .and_then(|v| v.as_str())
                                  .and_then(|s| s.parse().ok())
                                  .unwrap_or(0.0)
                            };

                            let o = parse("o");
                            let h = parse("h");
                            let l = parse("l");
                            let c = parse("c");

                            if let Some(state) = market_state.get_mut(&symbol) {
                                state.last_close = c;
                                let range = h - l;
                                if range > 0.0 {
                                    state.upper_wick_ratio = ((h - o.max(c)) / range) * 100.0;
                                    state.lower_wick_ratio = ((o.min(c) - l) / range) * 100.0;
                                }
                            }
                        }
                    }

                    // -----------------------------------------------------------
                    // CANAL: Funding Rate
                    // -----------------------------------------------------------
                    else if channel == "push.funding.rate" {
                        if let Some(fr_str) = parsed["data"]["rate"].as_str() {
                            if let Ok(fr) = fr_str.parse::<f64>() {
                                if let Some(state) = market_state.get_mut(&symbol) {
                                    // MEXC retorna como decimal: 0.0001 = 0.01%
                                    state.current_funding = fr * 100.0;
                                }
                            }
                        }
                    }

                    // -----------------------------------------------------------
                    // AVALIAÇÃO DE SINAL (lê OI do HashMap sem I/O)
                    // -----------------------------------------------------------
                    let current_oi = {
                        let oi_snap = oi_map.lock().await;
                        *oi_snap.get(&symbol).unwrap_or(&0.0)
                    };

                    if let Some(state) = market_state.get_mut(&symbol) {

                        // Atualiza janela deslizante do OI
                        if state.oi_peak == 0.0 && current_oi > 0.0 {
                            state.oi_peak    = current_oi;
                            state.oi_peak_ts = now;
                        } else if now - state.oi_peak_ts > OI_WINDOW_SEC {
                            state.oi_peak    = current_oi;
                            state.oi_peak_ts = now;
                        } else if current_oi > state.oi_peak {
                            state.oi_peak    = current_oi;
                            state.oi_peak_ts = now;
                        }

                        // Log de status por símbolo
                        if current_oi > 0.0 {
                            let drop_pct = ((state.oi_peak - current_oi) / state.oi_peak) * 100.0;
                            info!(
                                "[STATUS] {} | close={:.8} | oi_drop={:.3}% | funding={:.4}% | upper_wick={:.1}% | lower_wick={:.1}%",
                                symbol, state.last_close, drop_pct,
                                state.current_funding, state.upper_wick_ratio, state.lower_wick_ratio
                            );
                        }

                        // --- GESTÃO DE POSIÇÃO ABERTA ---
                        if state.position_active {
                            if let Some(reason) = evaluate_exit(state, now) {
                                let pnl_pct = if state.position_is_short {
                                    ((state.entry_price - state.last_close) / state.entry_price) * 100.0
                                } else {
                                    ((state.last_close - state.entry_price) / state.entry_price) * 100.0
                                };

                                let reason_str = match reason {
                                    ExitReason::StopLoss       => "STOP_LOSS",
                                    ExitReason::TakeProfit     => "TAKE_PROFIT",
                                    ExitReason::TimeStopProfit => "TIME_STOP_LUCRO",
                                    ExitReason::TimeStopLoss   => "TIME_STOP_PREJUIZO",
                                };

                                error!(
                                    "🛡️ [SAÍDA] {} | {} | PnL: {:.2}% | Duração: {}s",
                                    symbol, reason_str, pnl_pct, now - state.entry_time
                                );

                                // Fire-and-forget: saída não bloqueia o loop
                                let sym_exit   = symbol.clone();
                                let is_short   = state.position_is_short;
                                let vol        = state.position_vol;
                                let client_ref = http_client.clone();
                                tokio::spawn(async move {
                                    execute_exit_order(&client_ref, &sym_exit, is_short, vol).await;
                                });

                                // Relatório para o Python via canal mpsc (não bloqueia)
                                let report = json!({
                                    "action":       "TRADE_CLOSED",
                                    "symbol":       symbol,
                                    "side":         if state.position_is_short { "short" } else { "long" },
                                    "entry_price":  state.entry_price,
                                    "exit_price":   state.last_close,
                                    "pnl_pct":      format!("{:.4}", pnl_pct).parse::<f64>().unwrap_or(0.0),
                                    "duration_sec": now - state.entry_time,
                                    "exit_reason":  reason_str,
                                    "timestamp":    now
                                });
                                let _ = ipc_tx.send(format!("{}\n", report)).await;

                                // Reseta estado
                                state.position_active   = false;
                                state.position_is_short = false;
                                state.entry_price       = 0.0;
                                state.entry_time        = 0;
                                state.position_vol      = 0;
                                state.last_shot_ts      = now;
                            }

                            continue; // Enquanto em posição, não analisa entrada
                        }

                        // --- ANÁLISE DE ENTRADA ---
                        match evaluate_signal(state, current_oi, now) {
                            TradeSignal::None => {}

                            signal => {
                                let is_short = matches!(signal, TradeSignal::Short);
                                let drop_pct = ((state.oi_peak - current_oi) / state.oi_peak) * 100.0;

                                warn!(
                                    "🎯 [GATILHO] {} {} | OI drop: {:.2}% | Funding: {:.4}% | Wick: {:.1}%",
                                    if is_short { "SHORT" } else { "LONG" },
                                    symbol,
                                    drop_pct,
                                    state.current_funding,
                                    if is_short { state.upper_wick_ratio } else { state.lower_wick_ratio }
                                );

                                // Marca ordem em voo — impede duplo gatilho
                                state.order_in_flight = true;

                                let meta = instrument_map.get(&symbol).unwrap().clone();
                                let sym_order  = symbol.clone();
                                let price      = state.last_close;
                                let bal        = usdt_balance;
                                let client_ref = http_client.clone();
                                let tx_clone   = order_tx.clone();

                                // Fire-and-forget: ordem sai em task separado
                                // O event loop continua processando outros símbolos imediatamente
                                tokio::spawn(async move {
                                    execute_entry_order(
                                        &client_ref,
                                        &sym_order,
                                        is_short,
                                        price,
                                        bal,
                                        &meta,
                                        tx_clone,
                                    )
                                    .await;
                                });
                            }
                        }
                    }
                } // fim msg_opt
            } // fim tokio::select!
        } // fim loop interno

        warn!("🔄 Reconectando em 3 segundos...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    } // fim 'reconnect
}