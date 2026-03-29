use std::collections::VecDeque;
pub const MIN_COOLDOWN_TICKS: u32 = 30;
const ATR_RELEASE_PERCENTILE: f64 = 0.40;
const ATR_HISTORY_SIZE: usize = 50;
const CONSECUTIVE_STOP_MULTIPLIER: f64 = 0.5;
#[derive(Debug, Clone, PartialEq)]
pub enum QuarantineState { Active, Quarantine { since_tick: u64, consecutive_stops: u32 }, Released }
pub struct QuarantineManager { pub state: QuarantineState, pub tick_count: u64, pub atr_history: VecDeque<f64>, pub oi_history: VecDeque<f64> }
impl QuarantineManager {
    pub fn new() -> Self { QuarantineManager { state: QuarantineState::Active, tick_count: 0, atr_history: VecDeque::with_capacity(ATR_HISTORY_SIZE), oi_history: VecDeque::with_capacity(10) } }
    pub fn tick(&mut self, current_atr: f64, current_oi: f64) { self.tick_count += 1; if current_atr > 0.0 { self.atr_history.push_back(current_atr); if self.atr_history.len() > ATR_HISTORY_SIZE { self.atr_history.pop_front(); } } if current_oi > 0.0 { self.oi_history.push_back(current_oi); if self.oi_history.len() > 5 { self.oi_history.pop_front(); } } if matches!(self.state, QuarantineState::Quarantine { .. }) { self.try_release(); } }
    pub fn enter_quarantine(&mut self) { let consecutive = match &self.state { QuarantineState::Quarantine { consecutive_stops, .. } => *consecutive_stops + 1, _ => 1 }; self.state = QuarantineState::Quarantine { since_tick: self.tick_count, consecutive_stops: consecutive }; }
    pub fn register_win(&mut self) { self.state = QuarantineState::Active; }
    pub fn can_trade(&self) -> bool { matches!(self.state, QuarantineState::Active | QuarantineState::Released) }
    fn try_release(&mut self) { let (since_tick, consecutive) = match &self.state { QuarantineState::Quarantine { since_tick, consecutive_stops } => (*since_tick, *consecutive_stops), _ => return }; let required_ticks = self.required_cooldown_ticks(consecutive); let ticks_elapsed = self.tick_count.saturating_sub(since_tick); if ticks_elapsed < required_ticks as u64 { return; } if !self.check_atr_regime() { return; } if self.oi_history.len() >= 3 && !self.check_oi_stability() { return; } self.state = QuarantineState::Released; }
    fn required_cooldown_ticks(&self, consecutive_stops: u32) -> u32 { let multiplier = 1.0 + (consecutive_stops.saturating_sub(1) as f64 * CONSECUTIVE_STOP_MULTIPLIER); (MIN_COOLDOWN_TICKS as f64 * multiplier).ceil() as u32 }
    fn check_atr_regime(&self) -> bool { if self.atr_history.len() < 5 { return true; } let current_atr = match self.atr_history.back() { Some(&v) => v, None => return true }; let mut sorted: Vec<f64> = self.atr_history.iter().cloned().collect(); sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)); let percentile_idx = (sorted.len() as f64 * ATR_RELEASE_PERCENTILE) as usize; let percentile_value = sorted.get(percentile_idx).copied().unwrap_or(0.0); current_atr >= percentile_value }
    fn check_oi_stability(&self) -> bool { let oi_vals: Vec<f64> = self.oi_history.iter().cloned().collect(); if oi_vals.len() < 3 { return true; } let deltas: Vec<f64> = oi_vals.windows(2).map(|w| (w[1] - w[0]).abs()).collect(); if deltas.is_empty() { return true; } let mean = deltas.iter().sum::<f64>() / deltas.len() as f64; let variance = deltas.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / deltas.len() as f64; let std_dev = variance.sqrt(); let latest_delta = *deltas.last().unwrap_or(&0.0); latest_delta <= mean + std_dev }
}
