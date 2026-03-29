import asyncio
import aiohttp
import json
import logging
import csv
import os
import re
import signal
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [MAESTRO V9] - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

BINGX_REST_URL = "https://open-api.bingx.com"
IPC_HOST = "127.0.0.1"
IPC_PORT = 9000
CSV_FILE = "auditoria_trades.csv"
TELEMETRY_DIR = "telemetry_data"
NEW_LISTING_POLL_SEC = 60
MIN_VOLUME_USD_FALLBACK = 1_000_000
TOP_N_FALLBACK = 10
TARGET_REFRESH_SEC = 600
FETCH_RETRY_COUNT = 3
FETCH_RETRY_DELAY = 5

IPC_RECONNECT_INITIAL_BACKOFF_SEC = 2.0
IPC_RECONNECT_MAX_BACKOFF_SEC = 30.0
IPC_RECONNECT_BACKOFF_FACTOR = 2.0

_NON_CRYPTO_PREFIXES = ("NCSK", "JPM", "AAPL", "TSLA", "SPX", "NDX", "GOLD", "SILVER", "OIL", "NATGAS", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", "XAU", "XAG", "WTI", "BRENT", "NCCO")
_RE_BASE_ALPHANUM = re.compile(r'^[A-Za-z0-9]+$')
_RE_LEVERAGED_STOCK = re.compile(r'^[A-Za-z]+\d$')

def validate_crypto_perp_symbol(symbol: str) -> tuple[bool, str]:
    if not symbol.endswith("-USDT"): return False, "não termina com -USDT"
    base = symbol[:-5]
    if not base: return False, "base asset vazio"
    if not _RE_BASE_ALPHANUM.match(base): return False, f"base asset contém caracteres inválidos: '{base}'"
    if len(base) < 2: return False, "base asset muito curto (< 2 chars)"
    if len(base) > 10: return False, f"base asset muito longo"
    base_upper = base.upper()
    for prefix in _NON_CRYPTO_PREFIXES:
        if base_upper.startswith(prefix): return False, f"prefixo na blacklist: '{prefix}'"
    if len(base) <= 6 and _RE_LEVERAGED_STOCK.match(base): return False, f"padrão de ticker de ação alavancada: '{base}'"
    return True, ""

def filter_valid_symbols(symbols: list[str]) -> list[str]:
    valid = []
    for sym in symbols:
        ok, reason = validate_crypto_perp_symbol(sym)
        if ok: valid.append(sym)
        else: logging.warning("⛔ [SYMBOL FILTER] Rejeitado '%s': %s", sym, reason)
    return valid

class MaestroState:
    def __init__(self):
        self.rust_connected: bool = False
        self.known_symbols: set[str] = set()
        self.new_listings: list[str] = []
        self.current_targets: list[str] = []
        self.targets_fetched_at: datetime | None = None
        self.total_trades: int = 0
        self.winning_trades: int = 0
        self.total_pnl: float = 0.0
        self.by_symbol: dict = {}
        self.by_reason: dict = {}
        self.telemetry_file = None
        self.telemetry_count: int = 0
        self.last_heartbeat_received_at: float = 0.0
        self.heartbeat_ack_count: int = 0
    def record_trade(self, dados: dict):
        pnl = float(dados.get("pnl_pct", 0.0))
        symbol = dados.get("symbol", "UNKNOWN")
        reason = dados.get("exit_reason", "UNKNOWN")
        self.total_trades += 1
        self.total_pnl += pnl
        if pnl > 0: self.winning_trades += 1
        if symbol not in self.by_symbol: self.by_symbol[symbol] = {"wins": 0, "losses": 0, "pnl": 0.0}
        self.by_symbol[symbol]["pnl"] += pnl
        if pnl > 0: self.by_symbol[symbol]["wins"] += 1
        else: self.by_symbol[symbol]["losses"] += 1
        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1
    def print_stats(self):
        if self.total_trades == 0: return
        win_rate = (self.winning_trades / self.total_trades) * 100
        avg_pnl = self.total_pnl / self.total_trades
        logging.info("=" * 60)
        logging.info("📊 ESTATÍSTICAS PAPER TRADE (%d trades)", self.total_trades)
        logging.info("   Win Rate : %.1f%%  (%dW / %dL)", win_rate, self.winning_trades, self.total_trades - self.winning_trades)
        logging.info("   PnL Total: %+.2f%%", self.total_pnl)
        logging.info("   PnL Médio: %+.4f%% por trade", avg_pnl)
        logging.info("   Motivos  : %s", self.by_reason)
        logging.info("   Heartbeat ACKs enviados: %d", self.heartbeat_ack_count)
        for sym, stats in self.by_symbol.items():
            total = stats["wins"] + stats["losses"]
            wr = (stats["wins"] / total * 100) if total > 0 else 0
            logging.info("   [%s] %d trades | WR=%.0f%% | PnL=%+.4f%%", sym, total, wr, stats["pnl"])
        logging.info("=" * 60)

state = MaestroState()

def inicializar_telemetria():
    os.makedirs(TELEMETRY_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(TELEMETRY_DIR, f"telemetry_{date_str}.jsonl")
    state.telemetry_file = open(filepath, mode="a", encoding="utf-8")
    logging.info("📡 Telemetria JSONL: %s", filepath)

def gravar_telemetria(raw_line: str):
    if state.telemetry_file:
        try:
            state.telemetry_file.write(raw_line if raw_line.endswith("\n") else raw_line + "\n")
            state.telemetry_count += 1
            if state.telemetry_count % 100 == 0: state.telemetry_file.flush()
        except Exception as e: logging.error("❌ Erro ao gravar telemetria: %s", e)

def inicializar_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "symbol", "side", "entry_price", "exit_price", "pnl_pct", "duration_sec", "exit_reason", "highest_pnl", "leverage", "fee_total_pct"])
        logging.info("📁 CSV de auditoria criado: %s", CSV_FILE)
    else: logging.info("📁 CSV de auditoria existente: %s", CSV_FILE)

def gravar_trade_csv(dados: dict):
    try:
        with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), dados.get("symbol", "UNKNOWN"), dados.get("side", "UNKNOWN"), dados.get("entry_price", 0.0), dados.get("exit_price", 0.0), dados.get("pnl_pct", 0.0), dados.get("duration_sec", 0), dados.get("exit_reason", "UNKNOWN"), dados.get("highest_pnl", 0.0), dados.get("leverage", 5), dados.get("fee_total_pct", 0.0)])
    except Exception as e: logging.error("❌ Erro ao gravar CSV: %s", e)

async def fetch_all_contracts() -> list[dict]:
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{BINGX_REST_URL}/openApi/swap/v2/quote/contracts"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status != 200: raise ValueError(f"HTTP {response.status}")
                data = await response.json()
                return data.get("data", [])
    except Exception as e:
        logging.warning("⚠️  Falha ao buscar contratos: %s", e)
        return []

async def detect_new_listings() -> list[str]:
    contracts = await fetch_all_contracts()
    if not contracts: return []
    current_symbols: set[str] = set()
    for c in contracts:
        sym = c.get("symbol", "")
        if sym and sym.endswith("-USDT"): current_symbols.add(sym)
    if not state.known_symbols:
        state.known_symbols = current_symbols
        logging.info("📋 Cache inicial de contratos: %d símbolos.", len(current_symbols))
        return []
    new_symbols = current_symbols - state.known_symbols
    state.known_symbols = current_symbols
    if new_symbols:
        raw_list = sorted(new_symbols)
        valid_list = filter_valid_symbols(raw_list)
        if valid_list:
            logging.warning("🆕🆕🆕 NOVAS LISTAGENS VÁLIDAS: %s", valid_list)
            state.new_listings.extend(valid_list)
            return valid_list
    return []

async def fetch_top_by_volatility() -> list[str]:
    for attempt in range(1, FETCH_RETRY_COUNT + 1):
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{BINGX_REST_URL}/openApi/swap/v2/quote/ticker"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200: raise ValueError(f"HTTP {response.status}")
                    data = await response.json()
                    tickers = data.get("data", [])
                    candidates = []
                    for t in tickers:
                        symbol = t.get("symbol", "")
                        ok, _ = validate_crypto_perp_symbol(symbol)
                        if not ok: continue
                        vol_usd = float(t.get("quoteVolume", 0) or 0)
                        if vol_usd < MIN_VOLUME_USD_FALLBACK: continue
                        price_change = abs(float(t.get("priceChangePercent", 0) or 0))
                        candidates.append((symbol, price_change, vol_usd))
                    if not candidates: raise ValueError("Nenhum ticker válido após filtro.")
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    targets = candidates[:TOP_N_FALLBACK]
                    logging.info("🔥 Top %d por VOLATILIDADE 24h:", len(targets))
                    for sym, pct, vol in targets: logging.info("   📈 %-20s | Δ24h: %+.2f%% | Vol: $%.0f", sym, pct, vol)
                    return [t[0] for t in targets]
        except Exception as e:
            logging.warning("⚠️  Tentativa %d falhou: %s", attempt, e)
            if attempt < FETCH_RETRY_COUNT: await asyncio.sleep(FETCH_RETRY_DELAY)
    return ["DOGE-USDT", "PEPE-USDT", "WIF-USDT"]

async def build_initial_targets() -> list[str]:
    await detect_new_listings()
    targets = list(state.new_listings)
    volatile = await fetch_top_by_volatility()
    for sym in volatile:
        if sym not in targets: targets.append(sym)
    return targets

async def new_listing_detection_loop(writer: asyncio.StreamWriter):
    while True:
        await asyncio.sleep(NEW_LISTING_POLL_SEC)
        try:
            new = await detect_new_listings()
            if new:
                cmd = {"action": "NEW_LISTING", "symbols": new, "detected_at": int(datetime.now().timestamp())}
                writer.write((json.dumps(cmd) + "\n").encode("utf-8"))
                await writer.drain()
        except Exception as e: logging.error("❌ Erro no loop de detecção: %s", e)

async def targets_refresh_loop(writer: asyncio.StreamWriter):
    await asyncio.sleep(TARGET_REFRESH_SEC)
    while True:
        try:
            new_targets = await fetch_top_by_volatility()
            combined = list(state.new_listings) + new_targets
            seen: set[str] = set()
            unique: list[str] = []
            for s in combined:
                if s not in seen:
                    seen.add(s)
                    unique.append(s)
            if set(unique) != set(state.current_targets):
                state.current_targets = unique
                state.targets_fetched_at = datetime.now()
                cmd = {"action": "SET_TARGETS", "symbols": unique}
                writer.write((json.dumps(cmd) + "\n").encode("utf-8"))
                await writer.drain()
        except Exception as e: logging.error("❌ Erro no loop de rotação: %s", e)
        await asyncio.sleep(TARGET_REFRESH_SEC)

async def heartbeat_responder_task(writer: asyncio.StreamWriter, hb_queue: asyncio.Queue):
    while True:
        try:
            hb_ts: int = await hb_queue.get()
            ack = {"action": "ACK", "ts": int(time.time()), "ack_to": hb_ts}
            writer.write((json.dumps(ack) + "\n").encode("utf-8"))
            await writer.drain()
            state.heartbeat_ack_count += 1
            state.last_heartbeat_received_at = time.monotonic()
            hb_queue.task_done()
        except asyncio.CancelledError: break
        except Exception as e: logging.error("❌ [HB RESPONDER] Falha: %s", e)

async def handle_rust_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    if state.rust_connected:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception: pass
        return
    state.rust_connected = True
    listing_task = None
    refresh_task = None
    hb_task = None
    hb_queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    try:
        targets = await build_initial_targets()
        state.current_targets = targets
        state.targets_fetched_at = datetime.now()
        cmd = {"action": "SET_TARGETS", "symbols": targets}
        writer.write((json.dumps(cmd) + "\n").encode("utf-8"))
        await writer.drain()
        listing_task = asyncio.create_task(new_listing_detection_loop(writer))
        refresh_task = asyncio.create_task(targets_refresh_loop(writer))
        hb_task = asyncio.create_task(heartbeat_responder_task(writer, hb_queue))
        while True:
            data = await reader.readline()
            if not data: break
            mensagem = data.decode("utf-8", errors="replace").strip()
            if not mensagem: continue
            gravar_telemetria(mensagem)
            try:
                pacote = json.loads(mensagem)
                event_type = pacote.get("event_type", "")
                action = pacote.get("action", "")
                if action == "HEARTBEAT":
                    hb_ts = pacote.get("ts", int(time.time()))
                    try: hb_queue.put_nowait(hb_ts)
                    except asyncio.QueueFull: pass
                    continue
                if action == "ACK": continue
                if event_type == "paper_exit" or action == "TRADE_CLOSED":
                    gravar_trade_csv(pacote)
                    state.record_trade(pacote)
                    state.print_stats()
                elif event_type == "paper_entry":
                    sym = pacote.get("symbol", "?")
                    direction = pacote.get("signal_direction", "?")
                    score = pacote.get("signal_score", 0)
                    logging.info("🎯 [PAPER ENTRY] %s %s | Score: %.4f", direction, sym, float(score) if score != "?" else 0)
            except json.JSONDecodeError: pass
    except asyncio.CancelledError: pass
    except ConnectionResetError: pass
    except Exception as e: logging.error("❌ Erro no handler: %s", e)
    finally:
        for task in [listing_task, refresh_task, hb_task]:
            if task and not task.done():
                task.cancel()
                try: await task
                except asyncio.CancelledError: pass
        state.rust_connected = False
        try:
            writer.close()
            await writer.wait_closed()
        except Exception: pass

async def run_ipc_server_with_resilience():
    backoff = IPC_RECONNECT_INITIAL_BACKOFF_SEC
    while True:
        server = None
        try:
            server = await asyncio.start_server(handle_rust_connection, IPC_HOST, IPC_PORT)
            backoff = IPC_RECONNECT_INITIAL_BACKOFF_SEC
            async with server: await server.serve_forever()
        except OSError as e: logging.error("❌ [IPC] Erro na porta: %s", e)
        except asyncio.CancelledError: break
        except Exception as e: logging.error("❌ [IPC] Erro: %s", e)
        finally:
            if server:
                server.close()
                try: await server.wait_closed()
                except Exception: pass
        await asyncio.sleep(backoff)
        backoff = min(backoff * IPC_RECONNECT_BACKOFF_FACTOR, IPC_RECONNECT_MAX_BACKOFF_SEC)

def handle_shutdown(loop: asyncio.AbstractEventLoop):
    state.print_stats()
    if state.telemetry_file:
        state.telemetry_file.flush()
        state.telemetry_file.close()
    loop.stop()

async def main():
    inicializar_csv()
    inicializar_telemetria()
    await run_ipc_server_with_resilience()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, handle_shutdown, loop)
        except NotImplementedError: pass
    try: loop.run_until_complete(main())
    except (KeyboardInterrupt, RuntimeError): pass
    finally:
        state.print_stats()
        if state.telemetry_file:
            state.telemetry_file.flush()
            state.telemetry_file.close()
        loop.close()
