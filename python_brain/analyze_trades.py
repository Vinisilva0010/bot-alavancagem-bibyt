import csv
import sys

trades = []
with open(r'\\wsl.localhost\Ubuntu\home\vnspo\Apex_V7_Predator\python_brain\auditoria_trades.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        trades.append(row)

total = len(trades)
wins = [t for t in trades if float(t['pnl_percentual']) > 0]
losses = [t for t in trades if float(t['pnl_percentual']) <= 0]

print(f'=== ESTATISTICAS GERAIS ===')
print(f'Total trades: {total}')
print(f'Wins: {len(wins)} ({len(wins)/total*100:.1f}%)')
print(f'Losses: {len(losses)} ({len(losses)/total*100:.1f}%)')

total_pnl = sum(float(t['pnl_percentual']) for t in trades)
avg_win = sum(float(t['pnl_percentual']) for t in wins) / len(wins) if wins else 0
avg_loss = sum(float(t['pnl_percentual']) for t in losses) / len(losses) if losses else 0
print(f'PnL total: {total_pnl:.4f}%')
print(f'Avg win: +{avg_win:.4f}%')
print(f'Avg loss: {avg_loss:.4f}%')
if avg_loss != 0:
    print(f'R:R ratio: {abs(avg_win/avg_loss):.2f}')

print()
print('=== POR MOTIVO DE SAIDA ===')
reasons = {}
for t in trades:
    r = t['motivo_saida']
    if r not in reasons:
        reasons[r] = {'count': 0, 'pnl': 0, 'wins': 0}
    reasons[r]['count'] += 1
    reasons[r]['pnl'] += float(t['pnl_percentual'])
    if float(t['pnl_percentual']) > 0:
        reasons[r]['wins'] += 1

for r, data in sorted(reasons.items()):
    wr = data['wins']/data['count']*100
    print(f'{r:25s} | {data["count"]:3d} trades | WR: {wr:.1f}% | PnL: {data["pnl"]:+.4f}%')

print()
print('=== POR DIRECAO ===')
for d in ['SHORT', 'LONG']:
    dt = [t for t in trades if t['direcao'] == d]
    if not dt: continue
    dw = [t for t in dt if float(t['pnl_percentual']) > 0]
    dpnl = sum(float(t['pnl_percentual']) for t in dt)
    print(f'{d:6s} | {len(dt):3d} trades | WR: {len(dw)/len(dt)*100:.1f}% | PnL: {dpnl:+.4f}%')

print()
print('=== POR ATIVO (top perdedores e ganhadores) ===')
symbols = {}
for t in trades:
    s = t['ativo']
    if s not in symbols:
        symbols[s] = {'count': 0, 'pnl': 0, 'wins': 0}
    symbols[s]['count'] += 1
    symbols[s]['pnl'] += float(t['pnl_percentual'])
    if float(t['pnl_percentual']) > 0:
        symbols[s]['wins'] += 1

sorted_syms = sorted(symbols.items(), key=lambda x: x[1]['pnl'])
print('--- TOP 5 PIORES ---')
for s, d in sorted_syms[:5]:
    wr = d['wins']/d['count']*100 if d['count'] > 0 else 0
    print(f'{s:18s} | {d["count"]:2d} trades | WR: {wr:.1f}% | PnL: {d["pnl"]:+.4f}%')

print('--- TOP 5 MELHORES ---')
for s, d in sorted_syms[-5:]:
    wr = d['wins']/d['count']*100 if d['count'] > 0 else 0
    print(f'{s:18s} | {d["count"]:2d} trades | WR: {wr:.1f}% | PnL: {d["pnl"]:+.4f}%')

print()
print('=== TEMPO MEDIO EM POSICAO ===')
avg_dur = sum(int(t['tempo_posicionado_seg']) for t in trades) / total
avg_dur_win = sum(int(t['tempo_posicionado_seg']) for t in wins) / len(wins) if wins else 0
avg_dur_loss = sum(int(t['tempo_posicionado_seg']) for t in losses) / len(losses) if losses else 0
print(f'Geral: {avg_dur:.0f}s ({avg_dur/60:.1f}min)')
print(f'Wins: {avg_dur_win:.0f}s ({avg_dur_win/60:.1f}min)')
print(f'Losses: {avg_dur_loss:.0f}s ({avg_dur_loss/60:.1f}min)')

print()
print('=== DISTRIBUICAO DE PnL ===')
ranges = [(-15,-5), (-5,-2), (-2,-1), (-1,0), (0,0.5), (0.5,1), (1,2), (2,5), (5,15)]
for lo, hi in ranges:
    count = len([t for t in trades if lo <= float(t['pnl_percentual']) < hi])
    bar = '#' * count
    print(f'[{lo:+.0f}% a {hi:+.0f}%): {count:3d} {bar}')
