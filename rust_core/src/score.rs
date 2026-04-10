// =============================================================================
// score.rs — Motor de Score Multiplicativo V9.1
// =============================================================================
//
// CHANGELOG V9.1:
//   [CRÍTICO] F_regime: ATR calculado sobre ranges reais de kline_1m (h-l)
//             Antes: ATR sobre ticks de preço a cada 3s → atr_current ≈ atr_median
//             sempre → F_regime = 0 em 100% dos casos → zero trades
//             Agora: ATR sobre ranges de velas OHLC de 1m → variação real
//   [CRÍTICO] F_volume: volume_surge calculado sobre volume de vela (kline.v)
//             Antes: volume_24h (acumulador monotônico) → surge ratio ≈ 1.001
//             sempre → F_volume travado em 0.58
//             Agora: volume da última vela vs média das últimas N velas → real
//
// ARQUITETURA DO SCORE:
//   Score = F_volume × F_funding × F_oi × F_regime   ∈ [0.0, 1.0]
//
//   Produto multiplicativo: se QUALQUER fator for próximo de zero,
//   o Score inteiro colapsa. Não há compensação entre fatores ruins e bons.
//
// GATILHO DE ENTRADA: Score > MIN_SIGNAL_SCORE_F64
// =============================================================================

/// Score mínimo para disparar entrada (produto ∈ [0.0, 1.0])
pub const MIN_SIGNAL_SCORE_F64: f64 = 0.80;

/// Desvio padrão "esperado" do funding rate para normalização.
/// Funding rate de cripto fica normalmente entre ±0.01% a ±0.05%.
/// Acima de 0.5% em módulo → F_funding ≈ 0 → Score colapsa.
const FUNDING_SIGMA: f64 = 0.15;

/// Limiar de volume surge para F_volume atingir saturação (~0.96).
/// volume_surge = 3.0x → tanh(3.0/1.5) = tanh(2.0) ≈ 0.96
const VOLUME_SURGE_SATURATION: f64 = 1.5;

/// Limiar de OI velocity (%/min) para F_oi atingir saturação.
/// 1.0%/min → tanh(1.0/0.5) = tanh(2.0) ≈ 0.96
const OI_VELOCITY_SATURATION: f64 = 0.5;

/// Ratio ATR_atual / ATR_mediana acima do qual consideramos expansão de volatilidade.
/// 1.3 = ATR atual 30% acima da mediana → mercado saindo do chop.
const ATR_REGIME_THRESHOLD: f64 = 1.3;

// =============================================================================
// ESTRUTURA DE UMA VELA — alimenta ATR real e volume surge
// =============================================================================

/// Uma vela de 1m recebida pelo WebSocket kline_1m.
/// Armazenada no rolling window do estado de cada moeda.
#[derive(Debug, Clone, Default)]
pub struct KlineBar {
    /// Timestamp de abertura da vela (unix seconds)
    pub ts: i64,
    /// Range da vela: high - low (em unidades de preço)
    pub range: f64,
    /// Volume da vela (em unidades base, ex: BTC, ETH)
    pub volume: f64,
    /// Preço de fechamento
    pub close: f64,
}

// =============================================================================
// INPUTS DO SCORE
// =============================================================================

/// Dados necessários para calcular o Score V9.1.
/// Separados do estado principal para facilitar testes unitários.
#[derive(Debug, Clone)]
pub struct ScoreInputs {
    /// Ratio volume_ultima_vela / volume_medio_das_ultimas_N_velas.
    /// Ex: 3.0 = volume desta vela é 3x a média → spike real.
    pub volume_surge_ratio: f64,

    /// Funding rate atual em % (ex: -3.9, +0.02, +0.15)
    pub funding_rate_pct: f64,

    /// Mediana do funding rate histórico (janela rolling)
    pub funding_rate_median: f64,

    /// Velocidade do OI: delta percentual por minuto
    /// Positivo = acumulação, Negativo = liquidação
    pub oi_velocity_pct_min: f64,

    /// ATR atual: média dos ranges das últimas N velas de 1m
    pub atr_current: f64,

    /// ATR mediano: mediana dos ATRs calculados sobre janelas anteriores
    pub atr_median: f64,
}

// =============================================================================
// FATORES INDIVIDUAIS
// =============================================================================

/// F_volume ∈ [0.0, 1.0]
///
/// volume_surge = 1.0 (neutro) → F_volume = tanh(1.0/1.5) ≈ 0.58
/// volume_surge = 2.0          → F_volume = tanh(2.0/1.5) ≈ 0.81
/// volume_surge = 4.0          → F_volume = tanh(4.0/1.5) ≈ 0.94
///
/// Com volume de kline real (não volume_24h), este fator vai variar
/// entre 0.3 e 0.95 dependendo da atividade real da vela.
pub fn factor_volume(volume_surge_ratio: f64) -> f64 {
    if volume_surge_ratio <= 0.0 {
        return 0.0;
    }
    (volume_surge_ratio / VOLUME_SURGE_SATURATION).tanh()
}

/// F_funding ∈ [0.0, 1.0]
///
/// VETO PRINCIPAL: funding rate anômalo destrói o Score.
/// Quanto mais o FR se afasta da mediana histórica, menor o fator.
///
/// anomalia = 0    → F_funding = 0.50
/// anomalia = 0.5% → F_funding ≈ 0.07
/// anomalia = 3.9% → F_funding ≈ 0.00 (veto total)
pub fn factor_funding(funding_rate_pct: f64, funding_rate_median: f64) -> f64 {
    let anomaly = (funding_rate_pct - funding_rate_median).abs();
    let normalized = anomaly / FUNDING_SIGMA;
    2.0 / (1.0 + normalized.exp())
}

/// F_oi ∈ [0.0, 1.0]
///
/// Usa valor absoluto: tanto acumulação quanto liquidação são sinais.
///
/// oi_velocity = 0.0%/min → F_oi = 0.0 (mercado dormindo)
/// oi_velocity = 0.3%/min → F_oi ≈ 0.54
/// oi_velocity = 1.0%/min → F_oi ≈ 0.96
pub fn factor_oi(oi_velocity_pct_min: f64) -> f64 {
    (oi_velocity_pct_min.abs() / OI_VELOCITY_SATURATION).tanh()
}

/// F_regime ∈ [0.0, 1.0]
///
/// Filtro de regime baseado em ATR REAL de kline_1m (range h-l por vela).
///
/// Se atr_current / atr_median < ATR_REGIME_THRESHOLD → chop → F_regime baixo
/// Se atr_current / atr_median >= ATR_REGIME_THRESHOLD → expansão → F_regime alto
///
/// Com ATR de velas de 1m (não de ticks de 3s), a variação entre
/// atr_current e atr_median é real e discriminativa.
pub fn factor_regime(atr_current: f64, atr_median: f64) -> f64 {
    if atr_median <= 0.0 || atr_current <= 0.0 {
        return 0.0;
    }
    
    // --- INÍCIO DO FILTRO DE FERIADO ---
    // Cria um piso artificial para o denominador. 
    // Se a média do mercado for menor que 0.05% de volatilidade por minuto,
    // usamos o piso para impedir o "Efeito Estilingue".
    let piso_de_seguranca = 0.0005; 
    let atr_median_blindado = atr_median.max(piso_de_seguranca);
    
    let ratio = atr_current / atr_median_blindado;
    // --- FIM DO FILTRO DE FERIADO ---

    // Normaliza: ratio == ATR_REGIME_THRESHOLD → output = 0.5

    // Normaliza: ratio == ATR_REGIME_THRESHOLD → output = 0.5
    // ratio < 1.0 → 0, ratio == threshold → 0.5, ratio > 2×threshold → ~1.0
    let normalized = (ratio - 1.0) / (ATR_REGIME_THRESHOLD - 1.0);
    (normalized.max(0.0)).tanh().min(1.0)
}

// =============================================================================
// HELPERS — ATR real de kline_1m
// =============================================================================

/// Calcula ATR simples (média dos ranges) sobre as últimas `window` velas.
/// Retorna 0.0 se dados insuficientes.
///
/// Diferença crítica em relação ao compute_simple_atr anterior:
/// este opera sobre `KlineBar.range` (h-l real de velas de 1m),
/// não sobre deltas de preço entre ticks de 3s.
pub fn compute_atr_from_klines(klines: &std::collections::VecDeque<KlineBar>, window: usize) -> f64 {
    if klines.len() < 2 {
        return 0.0;
    }
    let bars: Vec<f64> = klines.iter().rev().take(window).map(|k| k.range).collect();
    if bars.is_empty() {
        return 0.0;
    }
    let sum: f64 = bars.iter().sum();
    sum / bars.len() as f64
}

/// Calcula o volume surge ratio: volume da última vela / média das anteriores.
///
/// window_for_avg: quantas velas anteriores usar para calcular a média.
/// Retorna 1.0 se dados insuficientes (neutro, não colapsa o score).
pub fn compute_volume_surge_from_klines(
    klines: &std::collections::VecDeque<KlineBar>,
    window_for_avg: usize,
) -> f64 {
    if klines.len() < 2 {
        return 1.0;
    }
    let last_vol = match klines.back() {
        Some(k) => k.volume,
        None => return 1.0,
    };
    if last_vol <= 0.0 {
        return 1.0;
    }
    // Média das velas ANTERIORES à última (exclui a última para não contaminar)
    let prev: Vec<f64> = klines
        .iter()
        .rev()
        .skip(1) // pula a última
        .take(window_for_avg)
        .map(|k| k.volume)
        .filter(|&v| v > 0.0)
        .collect();

    if prev.is_empty() {
        return 1.0;
    }
    let avg = prev.iter().sum::<f64>() / prev.len() as f64;
    if avg <= 0.0 {
        return 1.0;
    }
    // Clamp superior em 20x para evitar que um spike absurdo domine
    (last_vol / avg).min(20.0)
}

/// Calcula a mediana do ATR sobre janelas deslizantes do histórico de klines.
///
/// Para cada posição no histórico, calcula ATR de `atr_window` velas.
/// Retorna a mediana desses ATRs como benchmark de regime "normal".
pub fn compute_atr_median_from_klines(
    klines: &std::collections::VecDeque<KlineBar>,
    atr_window: usize,
    sample_count: usize,
) -> f64 {
    if klines.len() < atr_window + 1 {
        return 0.0;
    }
    let klines_vec: Vec<&KlineBar> = klines.iter().collect();
    let mut atrs: Vec<f64> = Vec::with_capacity(sample_count);

    // Desliza uma janela de `atr_window` velas pelo histórico
    let total = klines_vec.len();
    let step = if total > sample_count { total / sample_count } else { 1 };

    let mut idx = atr_window;
    while idx <= total && atrs.len() < sample_count {
        let window_ranges: Vec<f64> = klines_vec[idx - atr_window..idx]
            .iter()
            .map(|k| k.range)
            .collect();
        let atr = window_ranges.iter().sum::<f64>() / window_ranges.len() as f64;
        if atr > 0.0 {
            atrs.push(atr);
        }
        idx += step;
    }

    median(&atrs)
}

/// Calcula a mediana de uma slice de f64.
pub fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

// Mantido para compatibilidade — usado pela quarantine.rs
pub fn compute_simple_atr(prices: &[(i64, f64)], window_size: usize) -> f64 {
    if prices.len() < 2 {
        return 0.0;
    }
    let recent: Vec<f64> = prices.iter().rev().take(window_size).map(|(_, p)| *p).collect();
    if recent.len() < 2 {
        return 0.0;
    }
    let ranges: Vec<f64> = recent.windows(2).map(|w| (w[0] - w[1]).abs()).collect();
    let sum: f64 = ranges.iter().sum();
    sum / ranges.len() as f64
}

// =============================================================================
// SCORE FINAL
// =============================================================================

/// Resultado completo do cálculo de Score V9.1.
#[derive(Debug, Clone)]
pub struct ScoreResult {
    /// Score final ∈ [0.0, 1.0]. Entrada se > MIN_SIGNAL_SCORE_F64.
    pub score: f64,
    pub f_volume: f64,
    pub f_funding: f64,
    pub f_oi: f64,
    pub f_regime: f64,
    /// Direção sugerida: true = SHORT, false = LONG
    pub is_short: bool,
}

/// Calcula o Score multiplicativo V9.1 e determina a direção.
///
/// Direção: definida pelo SINAL do funding rate (árbitro primário).
///   - FR > 0: longs pagando → mercado overextended para cima → SHORT
///   - FR < 0: shorts pagando → mercado overextended para baixo → LONG
///   - FR ≈ 0: OI velocity decide
pub fn calculate_score(inputs: &ScoreInputs) -> ScoreResult {
    let f_volume  = factor_volume(inputs.volume_surge_ratio);
    let f_funding = factor_funding(inputs.funding_rate_pct, inputs.funding_rate_median);
    let f_oi      = factor_oi(inputs.oi_velocity_pct_min);
    let f_regime  = factor_regime(inputs.atr_current, inputs.atr_median);

    let score = f_volume * f_funding * f_oi * f_regime;

    let is_short = if inputs.funding_rate_pct.abs() >= 0.005 {
        inputs.funding_rate_pct > 0.0
    } else {
        inputs.oi_velocity_pct_min < 0.0
    };

    ScoreResult { score, f_volume, f_funding, f_oi, f_regime, is_short }
}

/// Retorna `Some(ScoreResult)` se score > MIN_SIGNAL_SCORE_F64, `None` caso contrário.
pub fn evaluate_signal(inputs: &ScoreInputs) -> Option<ScoreResult> {
    let result = calculate_score(inputs);
    if result.score > MIN_SIGNAL_SCORE_F64 { Some(result) } else { None }
}

// =============================================================================
// TESTES
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    fn make_klines(ranges: &[f64], volumes: &[f64]) -> VecDeque<KlineBar> {
        ranges.iter().zip(volumes.iter()).enumerate().map(|(i, (&r, &v))| {
            KlineBar { ts: i as i64 * 60, range: r, volume: v, close: 100.0 }
        }).collect()
    }

    // ── Fatores individuais ──────────────────────────────────────────────────

    #[test]
    fn test_factor_volume_neutro() {
        let f = factor_volume(1.0);
        assert!(f > 0.3 && f < 0.7, "volume neutro deve ser ~0.58, got {}", f);
    }

    #[test]
    fn test_factor_volume_explosivo() {
        let f = factor_volume(4.0);
        assert!(f > 0.9, "volume explosivo deve ser > 0.9, got {}", f);
    }

    #[test]
    fn test_factor_volume_zero() {
        assert_eq!(factor_volume(0.0), 0.0);
    }

    #[test]
    fn test_factor_funding_normal() {
        let f = factor_funding(0.01, 0.01);
        assert!(f > 0.4, "FR idêntico à mediana deve ter fator > 0.4, got {}", f);
    }

    #[test]
    fn test_factor_funding_bizarro_veto() {
        let f = factor_funding(-3.9, 0.01);
        assert!(f < 0.01, "FR bizarro deve resultar em veto, got {}", f);
    }

    #[test]
    fn test_factor_oi_zero() {
        assert_eq!(factor_oi(0.0), 0.0);
    }

    #[test]
    fn test_factor_oi_forte() {
        let f = factor_oi(1.0);
        assert!(f > 0.9, "OI velocity forte deve ser > 0.9, got {}", f);
    }

    #[test]
    fn test_factor_oi_simetrico() {
        let pos = factor_oi(0.5);
        let neg = factor_oi(-0.5);
        assert!((pos - neg).abs() < 1e-10, "deve ser simétrico");
    }

    #[test]
    fn test_factor_regime_chop_kline() {
        // ATR atual = ATR mediana → ratio = 1.0, abaixo do threshold 1.3 → chop
        let f = factor_regime(0.01, 0.01);
        assert!(f < 0.5, "ATR idêntico = chop, got {}", f);
    }

    #[test]
    fn test_factor_regime_expansao_kline() {
        // ATR atual = 2x mediana → ratio = 2.0, acima do threshold → expansão
        let f = factor_regime(0.02, 0.01);
        assert!(f > 0.6, "ATR 2x mediana = expansão, got {}", f);
    }

    #[test]
    fn test_factor_regime_zero_inputs() {
        assert_eq!(factor_regime(0.0, 0.0), 0.0);
    }

    // ── Helpers de kline ─────────────────────────────────────────────────────

    #[test]
    fn test_compute_atr_from_klines_vazio() {
        let klines: VecDeque<KlineBar> = VecDeque::new();
        assert_eq!(compute_atr_from_klines(&klines, 5), 0.0);
    }

    #[test]
    fn test_compute_atr_from_klines_valores_iguais() {
        let klines = make_klines(&[0.5; 10], &[100.0; 10]);
        let atr = compute_atr_from_klines(&klines, 5);
        assert!((atr - 0.5).abs() < 1e-10, "ATR deve ser 0.5, got {}", atr);
    }

    #[test]
    fn test_compute_atr_from_klines_variavel() {
        // Ranges alternando 0.2 e 0.8 → média = 0.5
        let ranges: Vec<f64> = (0..10).map(|i| if i % 2 == 0 { 0.2 } else { 0.8 }).collect();
        let klines = make_klines(&ranges, &[100.0; 10]);
        let atr = compute_atr_from_klines(&klines, 10);
        assert!((atr - 0.5).abs() < 1e-6, "ATR deve ser ~0.5, got {}", atr);
    }

    #[test]
    fn test_volume_surge_spike_real() {
        // Últimas 9 velas com volume 100, última com 300 → surge = 3.0x
        let mut vols = vec![100.0f64; 9];
        vols.push(300.0);
        let klines = make_klines(&vec![0.5; 10], &vols);
        let surge = compute_volume_surge_from_klines(&klines, 8);
        assert!(surge > 2.5, "spike de volume deve detectar surge > 2.5x, got {}", surge);
    }

    #[test]
    fn test_volume_surge_neutro() {
        // Todos os volumes iguais → surge ≈ 1.0
        let klines = make_klines(&[0.5; 10], &[100.0; 10]);
        let surge = compute_volume_surge_from_klines(&klines, 8);
        assert!((surge - 1.0).abs() < 0.01, "volume neutro deve ter surge ≈ 1.0, got {}", surge);
    }

    #[test]
    fn test_volume_surge_cap_20x() {
        // Volume da última vela = 10000x a média → deve ser limitado a 20.0
        let mut vols = vec![1.0f64; 9];
        vols.push(100_000.0);
        let klines = make_klines(&vec![0.5; 10], &vols);
        let surge = compute_volume_surge_from_klines(&klines, 8);
        assert!(surge <= 20.0, "surge deve ser limitado a 20x, got {}", surge);
    }

    #[test]
    fn test_volume_surge_dados_insuficientes() {
        let klines: VecDeque<KlineBar> = VecDeque::new();
        assert_eq!(compute_volume_surge_from_klines(&klines, 8), 1.0);
    }

    // ── Cenários de mercado extremos ─────────────────────────────────────────

    #[test]
    fn test_score_explosao_institucional_com_kline() {
        // Volume spike 3x, OI acelerando, ATR expandindo, FR normal
        let inputs = ScoreInputs {
            volume_surge_ratio:  3.0,
            funding_rate_pct:    0.02,
            funding_rate_median: 0.015,
            oi_velocity_pct_min: 0.8,
            atr_current:         0.025, // kline ATR atual
            atr_median:          0.012, // ratio = 2.08 > threshold 1.3 → trending
        };
        let result = calculate_score(&inputs);
        assert!(
            result.score > MIN_SIGNAL_SCORE_F64,
            "explosão institucional deve passar. score={:.4} f_vol={:.3} f_fund={:.3} f_oi={:.3} f_reg={:.3}",
            result.score, result.f_volume, result.f_funding, result.f_oi, result.f_regime
        );
    }

    #[test]
    fn test_score_funding_bizarro_mata_trade() {
        let inputs = ScoreInputs {
            volume_surge_ratio:  4.0,
            funding_rate_pct:    -3.9,
            funding_rate_median: 0.01,
            oi_velocity_pct_min: 1.2,
            atr_current:         0.03,
            atr_median:          0.01,
        };
        let result = calculate_score(&inputs);
        assert!(result.score < MIN_SIGNAL_SCORE_F64,
            "FR bizarro deve matar o score, got {:.6}", result.score);
        assert!(result.f_funding < 0.01,
            "F_funding deve ser quase zero, got {:.6}", result.f_funding);
    }

    #[test]
    fn test_score_chop_sem_volume_spike() {
        // Exatamente o cenário dos dados reais: vol_surge ≈ 1.001
        let inputs = ScoreInputs {
            volume_surge_ratio:  1.001,
            funding_rate_pct:    0.005,
            funding_rate_median: 0.005,
            oi_velocity_pct_min: 0.05,
            atr_current:         0.010,
            atr_median:          0.010, // ratio = 1.0 = chop
        };
        let result = calculate_score(&inputs);
        assert!(result.score < MIN_SIGNAL_SCORE_F64,
            "chop com vol neutro não deve entrar, got {:.4}", result.score);
    }

    #[test]
    fn test_score_prl_cenario_real() {
        // PRL-USDT dos dados reais: oi_vel=7.16 (alto!), vol_surge≈1.001 (travado)
        // Com kline volume real, vol_surge seria muito maior durante o spike de OI
        // Este teste simula o cenário COM volume de kline correto
        let inputs = ScoreInputs {
            volume_surge_ratio:  4.5,  // volume kline real durante spike de OI
            funding_rate_pct:    0.0087,
            funding_rate_median: 0.005,
            oi_velocity_pct_min: 7.16,
            atr_current:         0.020, // ATR kline durante o movimento
            atr_median:          0.010, // ratio = 2.0 → expansão clara
        };
        let result = calculate_score(&inputs);
        assert!(
            result.score > MIN_SIGNAL_SCORE_F64,
            "PRL com OI explodindo + volume real deve entrar. score={:.4}",
            result.score
        );
    }

    #[test]
    fn test_score_bounded() {
        let cases = vec![
            ScoreInputs { volume_surge_ratio: 100.0, funding_rate_pct: 0.001, funding_rate_median: 0.001, oi_velocity_pct_min: 100.0, atr_current: 100.0, atr_median: 1.0 },
            ScoreInputs { volume_surge_ratio: 0.0,   funding_rate_pct: -100.0, funding_rate_median: 0.0, oi_velocity_pct_min: 0.0, atr_current: 0.0, atr_median: 0.0 },
            ScoreInputs { volume_surge_ratio: f64::MAX, funding_rate_pct: f64::MIN, funding_rate_median: 0.0, oi_velocity_pct_min: f64::MAX, atr_current: 1.0, atr_median: 1.0 },
        ];
        for (i, inp) in cases.iter().enumerate() {
            let r = calculate_score(inp);
            assert!(!r.score.is_nan(), "caso {}: score é NaN", i);
            assert!(r.score >= 0.0 && r.score <= 1.0, "caso {}: score fora de [0,1]: {}", i, r.score);
        }
    }

    #[test]
    fn test_median_par()   { assert_eq!(median(&[1.0, 3.0, 5.0, 7.0]), 4.0); }
    #[test]
    fn test_median_impar() { assert_eq!(median(&[1.0, 3.0, 5.0]), 3.0); }
    #[test]
    fn test_median_vazio() { assert_eq!(median(&[]), 0.0); }
}
