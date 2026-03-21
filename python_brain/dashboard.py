import streamlit as st
import pandas as pd
import os
import time

# Configuração da página para ocupar a tela toda e usar tema escuro (padrão do Streamlit)
st.set_page_config(page_title="Apex Predator V7", layout="wide", page_icon="🎯")

CSV_FILE = "auditoria_trades.csv"

st.title("🎯 APEX PREDATOR V7 - Centro de Comando")
st.markdown("Monitoramento Quantitativo de Alta Frequência e Gestão de Risco em Tempo Real.")

def carregar_dados():
    # Se o arquivo não existe ou tá vazio, retorna um DataFrame zerado
    if not os.path.exists(CSV_FILE):
        return pd.DataFrame()
    try:
        # O Pandas é o motor que lê o CSV e transforma numa tabela inteligente
        df = pd.read_csv(CSV_FILE)
        return df
    except Exception as e:
        st.error(f"Erro ao ler banco de dados: {e}")
        return pd.DataFrame()

df = carregar_dados()

# O Estado de Espera (Quando o bot ainda não operou)
if df.empty or len(df) == 0:
    st.info("🐺 O Sniper está na espreita no mato. Nenhum trade foi finalizado ainda. O painel será montado automaticamente assim que o primeiro disparo ocorrer.")
else:
    # --- MATEMÁTICA INSTITUCIONAL (KPIs) ---
    total_trades = len(df)
    wins = len(df[df['pnl_pct'] > 0])
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
    total_pnl = df['pnl_pct'].sum()
    avg_pnl = df['pnl_pct'].mean()

    # --- O PAINEL SUPERIOR (Métricas) ---
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric(label="Trades Executados", value=total_trades)
    col2.metric(label="Win Rate (Taxa de Acerto)", value=f"{win_rate:.1f}%")
    
    # Colore o PnL de verde se positivo, vermelho se negativo
    pnl_color = "normal" if total_pnl >= 0 else "inverse"
    col3.metric(label="PnL Acumulado", value=f"{total_pnl:.2f}%", delta=f"{total_pnl:.2f}%", delta_color=pnl_color)
    col4.metric(label="PnL Médio por Trade", value=f"{avg_pnl:.2f}%")

    st.divider()

    # --- GRÁFICOS E TABELAS ---
    col_grafico, col_tabela = st.columns([2, 1])

    with col_grafico:
        st.subheader("📈 Curva de Capital (PnL Acumulado)")
        # Cria uma nova coluna somando o lucro/prejuízo ao longo do tempo
        df['pnl_acumulado'] = df['pnl_pct'].cumsum()
        
        # Plota o gráfico de linha nativo
        st.line_chart(df['pnl_acumulado'], use_container_width=True)

    with col_tabela:
        st.subheader("Distribuição de Saídas")
        # Gráfico de barras mostrando quantos Stop Loss vs Take Profit
        motivos = df['exit_reason'].value_counts()
        st.bar_chart(motivos)

    st.divider()
    
    st.subheader("📋 Auditoria Bruta (Log de Operações)")
    
    # Exibe a tabela completa invertida (trades mais novos no topo)
    df_reverso = df.iloc[::-1].reset_index(drop=True)
    
    # Função rápida para pintar o PnL de verde ou vermelho na tabela
    def colorir_pnl(val):
        if isinstance(val, (int, float)):
            color = '#00FF00' if val > 0 else '#FF0000' if val < 0 else 'white'
            return f'color: {color}'
        return ''

    st.dataframe(df_reverso.style.applymap(colorir_pnl, subset=['pnl_pct']), use_container_width=True)

# --- O MOTOR DE TEMPO REAL ---
# Isso faz a página do seu navegador recarregar os dados do CSV sozinha a cada 10 segundos
time.sleep(10)
st.rerun()