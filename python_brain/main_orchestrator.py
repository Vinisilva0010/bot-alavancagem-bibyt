# =============================================================================
# APEX PREDATOR V8 — PYTHON MAESTRO (Cérebro Central)
# Responsabilidades:
#   1. Detectar NOVAS LISTAGENS de futuros na BingX (alpha principal)
#   2. Servir como IPC server para o motor Rust
#   3. Gravar telemetria completa em .jsonl (Machine Learning ready)
#   4. Gravar auditoria de paper trades em CSV
#   5. Exibir estatísticas em tempo real no terminal
# =============================================================================

import asyncio
import aiohttp
import json
import logging
import csv
import os
import signal
from datetime import datetime

# =============================================================================
# CONFIGURAÇÃO DE LOG
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [MAESTRO V8] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# =============================================================================
# CONSTANTES
# =============================================================================
BINGX_REST_URL      = "https://open-api.bingx.com"
IPC_HOST            = "127.0.0.1"
IPC_PORT            = 9000
CSV_FILE            = "auditoria_trades.csv"
TELEMETRY_DIR       = "telemetry_data"

# Detecção de novas listagens: intervalo de polling (segundos)
NEW_LISTING_POLL_SEC    = 60

# Volume mínimo para alvos de fallback (quando não há novas listagens)
# Volume mínimo para filtrar shitcoins sem liquidez
MIN_VOLUME_USD_FALLBACK = 1_000_000
TOP_N_FALLBACK          = 10

# Intervalo de rotação dos alvos (segundos) — 10min, mercado muda rápido
TARGET_REFRESH_SEC      = 600

# Retry
FETCH_RETRY_COUNT   = 3
FETCH_RETRY_DELAY   = 5

# =============================================================================
# ESTADO GLOBAL
# =============================================================================
class MaestroState:
    def __init__(self):
        self.rust_connected: bool = False
        self.known_symbols: set[str] = set()  # Cache de todos os contratos conhecidos
        self.new_listings: list[str] = []       # Moedas recém-detectadas
        self.current_targets: list[str] = []
        self.targets_fetched_at: datetime | None = None
        self.total_trades: int = 0
        self.winning_trades: int = 0
        self.total_pnl: float = 0.0
        self.by_symbol: dict = {}
        self.by_reason: dict = {}
        self.telemetry_file = None
        self.telemetry_count: int = 0

    def record_trade(self, dados: dict):
        pnl = float(dados.get("pnl_pct", 0.0))
        symbol = dados.get("symbol", "UNKNOWN")
        reason = dados.get("exit_reason", "UNKNOWN")

        self.total_trades += 1
        self.total_pnl += pnl
        if pnl > 0:
            self.winning_trades += 1

        if symbol not in self.by_symbol:
            self.by_symbol[symbol] = {"wins": 0, "losses": 0, "pnl": 0.0}
        self.by_symbol[symbol]["pnl"] += pnl
        if pnl > 0:
            self.by_symbol[symbol]["wins"] += 1
        else:
            self.by_symbol[symbol]["losses"] += 1

        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1

    def print_stats(self):
        if self.total_trades == 0:
            return

        win_rate = (self.winning_trades / self.total_trades) * 100
        avg_pnl  = self.total_pnl / self.total_trades

        logging.info("=" * 60)
        logging.info(f"📊 ESTATÍSTICAS PAPER TRADE ({self.total_trades} trades)")
        logging.info(f"   Win Rate : {win_rate:.1f}%  ({self.winning_trades}W / {self.total_trades - self.winning_trades}L)")
        logging.info(f"   PnL Total: {self.total_pnl:+.2f}%")
        logging.info(f"   PnL Médio: {avg_pnl:+.4f}% por trade")
        logging.info(f"   Motivos  : {self.by_reason}")

        for sym, stats in self.by_symbol.items():
            total = stats["wins"] + stats["losses"]
            wr = (stats["wins"] / total * 100) if total > 0 else 0
            logging.info(f"   [{sym}] {total} trades | WR={wr:.0f}% | PnL={stats['pnl']:+.4f}%")

        if self.total_trades >= 20 and win_rate < 40.0:
            logging.warning(
                "⚠️  WIN RATE ABAIXO DE 40%% após %d trades. "
                "Reavalie os parâmetros.",
                self.total_trades,
            )
        logging.info("=" * 60)

state = MaestroState()

# =============================================================================
# MÓDULO DE TELEMETRIA — JSONL (Machine Learning ready)
# =============================================================================
def inicializar_telemetria():
    os.makedirs(TELEMETRY_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(TELEMETRY_DIR, f"telemetry_{date_str}.jsonl")
    state.telemetry_file = open(filepath, mode="a", encoding="utf-8")
    logging.info("📡 Telemetria JSONL: %s", filepath)

def gravar_telemetria(raw_line: str):
    """Recebe uma linha JSON do Rust e grava no .jsonl"""
    if state.telemetry_file:
        try:
            state.telemetry_file.write(raw_line if raw_line.endswith("\n") else raw_line + "\n")
            state.telemetry_count += 1
            # Flush a cada 100 eventos para não perder dados em crash
            if state.telemetry_count % 100 == 0:
                state.telemetry_file.flush()
        except Exception as e:
            logging.error("❌ Erro ao gravar telemetria: %s", e)

# =============================================================================
# MÓDULO DE AUDITORIA — CSV
# =============================================================================
def inicializar_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "symbol", "side", "entry_price", "exit_price",
                "pnl_pct", "duration_sec", "exit_reason", "highest_pnl",
                "leverage", "fee_total_pct",
            ])
        logging.info("📁 CSV de auditoria criado: %s", CSV_FILE)
    else:
        logging.info("📁 CSV de auditoria existente: %s", CSV_FILE)

def gravar_trade_csv(dados: dict):
    try:
        with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                dados.get("symbol",        "UNKNOWN"),
                dados.get("side",          "UNKNOWN"),
                dados.get("entry_price",   0.0),
                dados.get("exit_price",    0.0),
                dados.get("pnl_pct",       0.0),
                dados.get("duration_sec",  0),
                dados.get("exit_reason",   "UNKNOWN"),
                dados.get("highest_pnl",   0.0),
                dados.get("leverage",      5),
                dados.get("fee_total_pct", 0.0),
            ])
        logging.info(
            "💾 TRADE GRAVADO | %s | %s | PnL: %s%% | %s",
            dados.get("symbol"),
            dados.get("side", "?"),
            dados.get("pnl_pct"),
            dados.get("exit_reason"),
        )
    except Exception as e:
        logging.error("❌ Erro ao gravar CSV: %s", e)

# =============================================================================
# MÓDULO DE INTELIGÊNCIA — DETECÇÃO DE NOVAS LISTAGENS
#
# Estratégia:
#   1. Busca TODOS os contratos futuros da BingX
#   2. Compara com cache local (known_symbols)
#   3. Novos = alerta de listagem → enviados ao Rust como prioridade
#   4. Fallback: top 5 por volume para manter o bot sempre ativo
# =============================================================================

async def fetch_all_contracts() -> list[dict]:
    """Busca todos os contratos futuros da BingX"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{BINGX_REST_URL}/openApi/swap/v2/quote/contracts"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status != 200:
                    raise ValueError(f"HTTP {response.status}")
                data = await response.json()
                contracts = data.get("data", [])
                return contracts
    except Exception as e:
        logging.warning("⚠️  Falha ao buscar contratos: %s", e)
        return []

async def detect_new_listings() -> list[str]:
    """Detecta moedas recém-listadas comparando com cache"""
    contracts = await fetch_all_contracts()
    if not contracts:
        return []

    current_symbols = set()
    for c in contracts:
        sym = c.get("symbol", "")
        if sym and sym.endswith("-USDT"):
            current_symbols.add(sym)

    if not state.known_symbols:
        # Primeira execução: popula o cache sem gerar alertas
        state.known_symbols = current_symbols
        logging.info("📋 Cache inicial de contratos: %d símbolos.", len(current_symbols))
        return []

    # Detecta novos
    new_symbols = current_symbols - state.known_symbols
    if new_symbols:
        state.known_symbols = current_symbols
        new_list = sorted(new_symbols)
        logging.warning("🆕🆕🆕 NOVAS LISTAGENS DETECTADAS: %s", new_list)
        state.new_listings.extend(new_list)
        return new_list

    # Atualiza cache (pode ter removido algum)
    state.known_symbols = current_symbols
    return []

async def fetch_top_by_volatility() -> list[str]:
    """Seleciona top N moedas por VOLATILIDADE (|priceChangePercent| 24h).
    
    Lógica:
      - Busca todos os tickers da BingX
      - Filtra: volume > $1M (liquidez mínima) + sufixo -USDT
      - Exclui: NCCO* (commodities: ouro, petróleo — não cripto)
      - Ordena por abs(priceChangePercent) desc
      - Retorna top 10 mais voláteis
    
    Resultado: DOGE, PEPE, WIF, SHIB etc — moedas que se MOVEM.
    """
    for attempt in range(1, FETCH_RETRY_COUNT + 1):
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{BINGX_REST_URL}/openApi/swap/v2/quote/ticker"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        raise ValueError(f"HTTP {response.status}")

                    data = await response.json()
                    tickers = data.get("data", [])

                    valid = []
                    for t in tickers:
                        symbol = t.get("symbol", "")
                        if not symbol.endswith("-USDT"):
                            continue
                        # Exclui commodities (NCCOGOLD, NCCOOILWTI etc)
                        if symbol.startswith("NCCO"):
                            continue

                        vol_usd = float(t.get("quoteVolume", 0) or 0)
                        if vol_usd < MIN_VOLUME_USD_FALLBACK:
                            continue

                        price_change = abs(float(t.get("priceChangePercent", 0) or 0))
                        valid.append((symbol, price_change, vol_usd))

                    if not valid:
                        raise ValueError("Nenhum ticker válido.")

                    # Ordena por volatilidade (priceChangePercent) desc
                    valid.sort(key=lambda x: x[1], reverse=True)
                    targets = valid[:TOP_N_FALLBACK]

                    logging.info("🔥 Top %d por VOLATILIDADE 24h:", len(targets))
                    for sym, pct, vol in targets:
                        logging.info("   📈 %-20s | Δ24h: %+.2f%% | Vol: $%.0f", sym, pct, vol)

                    return [t[0] for t in targets]

        except Exception as e:
            logging.warning("⚠️  Tentativa %d/%d falhou: %s", attempt, FETCH_RETRY_COUNT, e)
            if attempt < FETCH_RETRY_COUNT:
                await asyncio.sleep(FETCH_RETRY_DELAY)

    return ["DOGE-USDT", "PEPE-USDT", "WIF-USDT"]  # Fallback seguro de voláteis

async def build_initial_targets() -> list[str]:
    """Constrói lista de alvos: novas listagens + top voláteis."""
    await detect_new_listings()

    # Novas listagens têm prioridade máxima
    targets = list(state.new_listings)

    # Completa com top 10 mais voláteis
    volatiles = await fetch_top_by_volatility()
    for sym in volatiles:
        if sym not in targets:
            targets.append(sym)

    logging.info("🎯 Alvos iniciais (%d): %s", len(targets), targets)
    return targets

# =============================================================================
# LOOP DE DETECÇÃO DE NOVAS LISTAGENS
# =============================================================================

async def new_listing_detection_loop(writer: asyncio.StreamWriter):
    """Polls BingX a cada NEW_LISTING_POLL_SEC procurando novas listagens"""
    while True:
        await asyncio.sleep(NEW_LISTING_POLL_SEC)
        try:
            new = await detect_new_listings()
            if new:
                logging.warning("🚀 Enviando %d novas listagens ao motor Rust!", len(new))
                comando = {"action": "NEW_LISTING", "symbols": new, "detected_at": int(datetime.now().timestamp())}
                writer.write((json.dumps(comando) + "\n").encode("utf-8"))
                await writer.drain()
        except Exception as e:
            logging.error("❌ Erro no loop de detecção: %s", e)

# =============================================================================
# LOOP DE ROTAÇÃO DE ALVOS (FALLBACK)
# =============================================================================

async def targets_refresh_loop(writer: asyncio.StreamWriter):
    """A cada TARGET_REFRESH_SEC, atualiza os alvos de fallback"""
    await asyncio.sleep(TARGET_REFRESH_SEC)
    while True:
        try:
            new_targets = await fetch_top_by_volatility()
            # Adiciona novas listagens pendentes
            combined = list(state.new_listings) + new_targets
            # Remove duplicatas mantendo ordem
            seen = set()
            unique = []
            for s in combined:
                if s not in seen:
                    seen.add(s)
                    unique.append(s)

            if set(unique) != set(state.current_targets):
                logging.info("🔄 Alvos atualizados: %s", unique)
                state.current_targets = unique
                state.targets_fetched_at = datetime.now()
                comando = {"action": "SET_TARGETS", "symbols": unique}
                writer.write((json.dumps(comando) + "\n").encode("utf-8"))
                await writer.drain()
            else:
                logging.info("♻️  Alvos sem mudança.")
        except Exception as e:
            logging.error("❌ Erro no loop de rotação: %s", e)
        await asyncio.sleep(TARGET_REFRESH_SEC)

# =============================================================================
# HANDLER IPC — CONEXÃO COM O RUST
# =============================================================================

async def handle_rust_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
):
    global state

    if state.rust_connected:
        addr = writer.get_extra_info("peername")
        logging.warning("🚨 Segunda conexão rejeitada de %s.", addr)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return

    state.rust_connected = True
    addr = writer.get_extra_info("peername")
    logging.info("🟢 Motor Rust V8 conectado em %s", addr)

    listing_task = None
    refresh_task = None

    try:
        targets = await build_initial_targets()
        state.current_targets = targets
        state.targets_fetched_at = datetime.now()

        comando = {"action": "SET_TARGETS", "symbols": targets}
        writer.write((json.dumps(comando) + "\n").encode("utf-8"))
        await writer.drain()
        logging.info("📤 %d alvos enviados. Motor Rust coletando telemetria.", len(targets))

        # Inicia loops de detecção
        listing_task = asyncio.create_task(new_listing_detection_loop(writer))
        refresh_task = asyncio.create_task(targets_refresh_loop(writer))

        while True:
            data = await reader.readline()

            if not data:
                logging.warning("📴 Rust encerrou a conexão.")
                break

            mensagem = data.decode("utf-8", errors="replace").strip()
            if not mensagem:
                continue

            # Grava TUDO no telemetry JSONL (raw)
            gravar_telemetria(mensagem)

            try:
                pacote = json.loads(mensagem)
                event_type = pacote.get("event_type", "")
                action = pacote.get("action", "")

                if event_type == "paper_exit" or action == "TRADE_CLOSED":
                    gravar_trade_csv(pacote)
                    state.record_trade(pacote)
                    state.print_stats()
                elif event_type == "paper_entry":
                    sym = pacote.get("symbol", "?")
                    direction = pacote.get("signal_direction", "?")
                    score = pacote.get("signal_score", 0)
                    logging.info(
                        "🎯 [PAPER ENTRY] %s %s | Score: %s/100",
                        direction, sym, score
                    )
                elif event_type == "ticker":
                    pass  # Gravado no JSONL, não precisa de log
                else:
                    logging.debug("[RUST] %s", mensagem[:200])

            except json.JSONDecodeError:
                logging.debug("[RUST RAW] %s", mensagem[:200])

    except asyncio.CancelledError:
        logging.info("🛑 Handler cancelado.")
    except ConnectionResetError:
        logging.warning("📴 Rust desconectou abruptamente.")
    except Exception as e:
        logging.error("❌ Erro no handler: %s", e)
    finally:
        for task in [listing_task, refresh_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        state.rust_connected = False
        logging.warning("🛑 Conexão Rust encerrada. Aguardando reconexão.")

        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

# =============================================================================
# SHUTDOWN GRACIOSO
# =============================================================================
def handle_shutdown(loop: asyncio.AbstractEventLoop):
    logging.info("🛑 Shutdown recebido.")
    state.print_stats()
    if state.telemetry_file:
        state.telemetry_file.flush()
        state.telemetry_file.close()
        logging.info("📡 Telemetria JSONL fechada. %d eventos gravados.", state.telemetry_count)
    logging.info("📊 Estatísticas finais impressas.")
    loop.stop()

# =============================================================================
# ENTRYPOINT
# =============================================================================
async def main():
    inicializar_csv()
    inicializar_telemetria()

    logging.info("╔═══════════════════════════════════════════════════╗")
    logging.info("║   APEX PREDATOR V8 — PYTHON MAESTRO              ║")
    logging.info("║   New Listing Detection + Telemetry Store         ║")
    logging.info("║   BingX Futures | IPC + JSONL + CSV               ║")
    logging.info("╚═══════════════════════════════════════════════════╝")

    server = await asyncio.start_server(
        handle_rust_connection,
        IPC_HOST,
        IPC_PORT,
    )

    addr = server.sockets[0].getsockname()
    logging.info("🧠 Servidor IPC em %s:%s. Aguardando motor Rust V8...", addr[0], addr[1])

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown, loop)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(main())
    except (KeyboardInterrupt, RuntimeError):
        pass
    finally:
        state.print_stats()
        if state.telemetry_file:
            state.telemetry_file.flush()
            state.telemetry_file.close()
        logging.info("Maestro V8 desligado.")
        loop.close()