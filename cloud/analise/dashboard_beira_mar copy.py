"""
🏥 DASHBOARD BEIRA-MAR ANALYTICS V2.3
Dashboard interativo para análise de no-show em clínica de estética

Autor: Beira-Mar Analytics Team
Data: 2024 - Versão 2.3 (Personalização de Cores)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from collections import Counter
import warnings
import re
import base64
from io import BytesIO

warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================

st.set_page_config(
    page_title="Beira-Mar Analytics Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #2c3e50;
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #40E0D0 0%, #FF69B4 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #40E0D0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .kpi-good {
        color: #2ecc71;
        font-size: 2rem;
        font-weight: bold;
    }
    .kpi-bad {
        color: #e74c3c;
        font-size: 2rem;
        font-weight: bold;
    }
    .kpi-neutral {
        color: #3498db;
        font-size: 2rem;
        font-weight: bold;
    }
    .tab-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #40E0D0;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Cores da clínica (turquesa e rosa) e paleta geral
COLORS = {
    'primary': '#40E0D0',      # Turquesa
    'secondary': '#FF69B4',     # Rosa
    'accent': '#20B2AA',        # Turquesa escuro
    'positive': '#2ecc71',      # Verde
    'negative': '#e74c3c',      # Vermelho
    'neutral': '#95a5a6',       # Cinza (backup)
    'yellow': '#f1c40f',        # Amarelo (para neutro)
    'gradient': ['#40E0D0', '#48D1CC', '#20B2AA', '#008B8B', '#FF69B4']
}

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

@st.cache_data
def load_data(file_path):
    """Carrega e processa os dados"""
    df = pd.read_csv(file_path)
    original_len = len(df)
    
    # Tentar converter datas com múltiplos formatos possíveis
    df['APPOINTMENTDAY'] = pd.to_datetime(df['APPOINTMENTDAY'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    
    if df['APPOINTMENTDAY'].isna().any():
        mask = df['APPOINTMENTDAY'].isna()
        df.loc[mask, 'APPOINTMENTDAY'] = pd.to_datetime(df.loc[mask, 'APPOINTMENTDAY'], format='%d/%m/%Y', errors='coerce')
    
    if df['APPOINTMENTDAY'].isna().any():
        mask = df['APPOINTMENTDAY'].isna()
        df.loc[mask, 'APPOINTMENTDAY'] = pd.to_datetime(df.loc[mask, 'APPOINTMENTDAY'], errors='coerce')
    
    # ScheduledDay
    df['SCHEDULEDDAY'] = pd.to_datetime(df['SCHEDULEDDAY'], errors='coerce')
    
    # Remover linhas com datas inválidas
    df = df.dropna(subset=['APPOINTMENTDAY'])
    removed = original_len - len(df)
    
    if removed > 0:
        st.sidebar.warning(f"⚠️ {removed:,} registros removidos (datas inválidas)")
    
    if len(df) == 0:
        st.error("❌ Nenhum registro válido após processamento de datas!")
        st.stop()
    
    # Criar coluna de data apenas (sem hora)
    df['APPOINTMENT_DATE'] = df['APPOINTMENTDAY'].dt.date
    
    # Criar faixas etárias
    bins = [0, 18, 30, 40, 50, 60, 100]
    labels = ['0-18', '19-30', '31-40', '41-50', '51-60', '60+']
    df['FAIXA_ETARIA'] = pd.cut(df['AGE'], bins=bins, labels=labels)
    
    # Criar faixas de preço
    price_bins = [0, 50, 100, 150, 200, float('inf')]
    price_labels = ['R$0-50', 'R$51-100', 'R$101-150', 'R$151-200', 'R$200+']
    df['FAIXA_PRECO'] = pd.cut(df['PRICE'], bins=price_bins, labels=price_labels)
    
    # Dia da semana (em português)
    dias_pt = {
        'Monday': 'Segunda',
        'Tuesday': 'Terça',
        'Wednesday': 'Quarta',
        'Thursday': 'Quinta',
        'Friday': 'Sexta',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    df['DIA_SEMANA'] = df['APPOINTMENTDAY'].dt.day_name().map(dias_pt)
    
    # Mês
    df['MES'] = df['APPOINTMENTDAY'].dt.month_name()
    
    # Calcular dias de antecedência do agendamento
    df['DIAS_ANTECEDENCIA'] = (df['APPOINTMENTDAY'] - df['SCHEDULEDDAY']).dt.days
    df['DIAS_ANTECEDENCIA'] = df['DIAS_ANTECEDENCIA'].clip(lower=0)  # Garantir valores não negativos
    
    # Classificar temperatura se não existir
    if 'CLASSIFICACAO_TEMP' not in df.columns and 'TEMP_AR_C' in df.columns:
        df['CLASSIFICACAO_TEMP'] = pd.cut(
            df['TEMP_AR_C'],
            bins=[-float('inf'), 17, 24, 30, float('inf')],
            labels=['Frio (<17°)', 'Agradável (17-24°)', 'Quente (24-30°)', 'Muito Quente (>30°)']
        )
    
    # Classificar intensidade de chuva
    if 'PRECIPITACAO_MM' in df.columns:
        df['INTENSIDADE_CHUVA'] = pd.cut(
            df['PRECIPITACAO_MM'],
            bins=[-float('inf'), 0.1, 2.5, 10, float('inf')],
            labels=['Sem Chuva', 'Chuva Fraca', 'Chuva Moderada', 'Chuva Forte']
        )
    
    return df


def filter_dataframe(df, filters):
    """Aplica filtros ao DataFrame"""
    filtered_df = df.copy()
    
    # Filtro de data
    if filters['date_range']:
        start_date, end_date = filters['date_range']
        filtered_df = filtered_df[
            (filtered_df['APPOINTMENT_DATE'] >= start_date) & 
            (filtered_df['APPOINTMENT_DATE'] <= end_date)
        ]
    
    # Filtro de faixa etária
    if filters['age_range'] and 'Todas' not in filters['age_range']:
        filtered_df = filtered_df[filtered_df['FAIXA_ETARIA'].isin(filters['age_range'])]
    
    # Filtro de serviços
    if filters['services'] and 'Todos' not in filters['services']:
        filtered_df = filtered_df[filtered_df['SERVICENAME'].isin(filters['services'])]
    
    # Filtro de dia da semana
    if filters['day_of_week'] and 'Todos' not in filters['day_of_week']:
        filtered_df = filtered_df[filtered_df['DIA_SEMANA'].isin(filters['day_of_week'])]
    
    # Filtro de faixa de preço
    if filters['price_range'] and 'Todas' not in filters['price_range']:
        filtered_df = filtered_df[filtered_df['FAIXA_PRECO'].isin(filters['price_range'])]
    
    return filtered_df


@st.cache_data
def load_instagram_data(file_path):
    """Carrega dados do Instagram"""
    try:
        df = pd.read_csv(file_path)
        return df
    except:
        return None


def extrair_emojis(texto):
    """Extrai emojis de um texto"""
    import emoji
    return [c for c in str(texto) if c in emoji.EMOJI_DATA]


def analisar_sentimento_simples(texto):
    """Análise de sentimento simplificada baseada em palavras-chave"""
    texto_lower = str(texto).lower()
    
    palavras_positivas = [
        'amei', 'amo', 'lindo', 'linda', 'maravilhos', 'perfeito', 'incrível', 'incrivel',
        'parabéns', 'parabens', 'sucesso', 'arras', 'demais', 'melhor', 'top', 'show',
        'feliz', 'orgulho', 'abençoe', 'abencoe', 'bom', 'boa', 'ótimo', 'otimo',
        '😍', '❤️', '🔥', '👏', '💕', '💖', '😊', '🥰', '💗', '✨'
    ]
    
    palavras_negativas = [
        'ruim', 'péssimo', 'pessimo', 'horrível', 'horrivel', 'feio', 'feia',
        'não gostei', 'nao gostei', 'odiei', 'odeio', 'decepcion', 'triste',
        '😢', '😭', '😡', '👎', '💔'
    ]
    
    score_positivo = sum(1 for p in palavras_positivas if p in texto_lower)
    score_negativo = sum(1 for p in palavras_negativas if p in texto_lower)
    
    if score_positivo > score_negativo:
        return 'positivo'
    elif score_negativo > score_positivo:
        return 'negativo'
    else:
        return 'neutro'


# ============================================================
# SIDEBAR - FILTROS
# ============================================================

st.sidebar.header("🎛️ FILTROS")

# Caminhos dos arquivos
FILE_PATH = '../../dados/refined/cancelamentos_com_clima.csv'
INSTAGRAM_PATH = '../../dados/refined/dataset_comentarios_com_posts.csv'

# Carregar Dados Principais
try:
    df = load_data(FILE_PATH)
    st.sidebar.success(f"✅ {len(df):,} registros carregados")
except FileNotFoundError:
    st.sidebar.error("❌ Arquivo principal não encontrado!")
    st.sidebar.info(f"O sistema buscou em: {FILE_PATH}")
    st.stop()
except Exception as e:
    st.sidebar.error(f"❌ Erro ao carregar dados: {str(e)}")
    st.stop()

# Filtros
st.sidebar.markdown("---")
st.sidebar.subheader("📅 Período")

valid_dates = df['APPOINTMENT_DATE'].dropna()

if len(valid_dates) > 0:
    min_date = valid_dates.min()
    max_date = valid_dates.max()
    
    date_range = st.sidebar.date_input(
        "Selecione o período",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
else:
    st.error("❌ Não há datas válidas no dataset!")
    st.stop()
    date_range = None

st.sidebar.markdown("---")
st.sidebar.subheader("📆 Dia da Semana")

dias_ordem = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
day_options = ['Todos'] + [d for d in dias_ordem if d in df['DIA_SEMANA'].unique()]
day_filter = st.sidebar.multiselect(
    "Dia da Semana",
    options=day_options,
    default=['Todos']
)

st.sidebar.markdown("---")
st.sidebar.subheader("👥 Demografia")

age_options = ['Todas'] + list(df['FAIXA_ETARIA'].dropna().unique())
age_filter = st.sidebar.multiselect(
    "Faixa Etária",
    options=age_options,
    default=['Todas']
)

st.sidebar.markdown("---")
st.sidebar.subheader("💆 Serviços")

service_options = ['Todos'] + sorted(df['SERVICENAME'].unique().tolist())
service_filter = st.sidebar.multiselect(
    "Serviços",
    options=service_options,
    default=['Todos']
)

price_options = ['Todas'] + list(df['FAIXA_PRECO'].dropna().unique())
price_filter = st.sidebar.multiselect(
    "Faixa de Preço",
    options=price_options,
    default=['Todas']
)

# Aplicar filtros
filters = {
    'date_range': date_range if len(date_range) == 2 else None,
    'age_range': age_filter,
    'services': service_filter,
    'day_of_week': day_filter,
    'price_range': price_filter
}

df_filtered = filter_dataframe(df, filters)

st.sidebar.markdown("---")
st.sidebar.info(f"📊 **{len(df_filtered):,}** registros após filtros")

# ============================================================
# DASHBOARD PRINCIPAL
# ============================================================

# Header
st.markdown('<h1 class="main-header">🏥 BEIRA-MAR ANALYTICS</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center; color: #7f8c8d; font-size: 1.2rem;">Dashboard de Análise de No-Show em Clínica de Estética</p>', unsafe_allow_html=True)
st.markdown("---")

# ============================================================
# SEÇÃO 1: KPIs PRINCIPAIS
# ============================================================

st.header("📊 KPIs Principais")

# Calcular métricas
total_appointments = len(df_filtered)
no_show_rate = df_filtered['NO-SHOW'].mean() * 100
avg_age = df_filtered['AGE'].mean()
avg_price = df_filtered['PRICE'].mean()
unique_patients = df_filtered['PATIENTID'].nunique()

# Comparação com dataset completo
no_show_rate_total = df['NO-SHOW'].mean() * 100
no_show_delta = no_show_rate - no_show_rate_total

# Exibir KPIs
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        "📅 Total de Agendamentos",
        f"{total_appointments:,}",
        f"{(total_appointments/len(df)*100):.1f}% do total"
    )

with col2:
    st.metric(
        "❌ Taxa de No-Show",
        f"{no_show_rate:.1f}%",
        f"{no_show_delta:+.1f}%",
        delta_color="inverse"
    )

with col3:
    st.metric(
        "👥 Pacientes Únicos",
        f"{unique_patients:,}"
    )

with col4:
    st.metric(
        "🎂 Idade Média",
        f"{avg_age:.1f} anos"
    )

with col5:
    st.metric(
        "💰 Ticket Médio",
        f"R$ {avg_price:.2f}"
    )

st.markdown("---")

# ============================================================
# SEÇÃO 2: ANÁLISE POR ABAS
# ============================================================

st.header("🔍 Análise Detalhada")

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Principal",
    "💆 Serviços",
    "📱 Instagram",
    "🌡️ Clima"
])

# ============================================================
# TAB 1: PRINCIPAL (SMS, Idade, Dia da Semana)
# ============================================================
with tab1:
    st.markdown("### 📊 Análise Principal - SMS, Idade e Dia da Semana")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de Rosca - Impacto do SMS na Taxa de No-Show
        sms_noshow = df_filtered.groupby('SMS_RECEIVED')['NO-SHOW'].agg(['mean', 'count']).reset_index()
        sms_noshow['mean'] = sms_noshow['mean'] * 100
        sms_labels = ['Sem SMS', 'Com SMS']
        
        fig_sms = go.Figure(data=[go.Pie(
            labels=sms_labels,
            values=sms_noshow['mean'],
            hole=0.5,
            marker=dict(colors=[COLORS['negative'], COLORS['positive']]),
            textinfo='label+percent',
            textfont=dict(size=14),
            hovertemplate='<b>%{label}</b><br>Taxa de No-Show: %{value:.1f}%<br>Total: %{customdata:,}<extra></extra>',
            customdata=sms_noshow['count']
        )])
        fig_sms.update_layout(
            title=dict(text="Impacto do SMS na Taxa de No-Show", font=dict(size=16)),
            height=400,
            annotations=[dict(text='No-Show<br>Rate', x=0.5, y=0.5, font_size=14, showarrow=False)]
        )
        st.plotly_chart(fig_sms, use_container_width=True)
        
        # Mostrar redução percentual
        if len(sms_noshow) == 2:
            reducao = sms_noshow.iloc[0]['mean'] - sms_noshow.iloc[1]['mean']
            if reducao > 0:
                st.success(f"✅ **Redução de {reducao:.1f} pontos percentuais** com o envio de SMS!")
    
    with col2:
        # Gráfico de Barras - Taxa de No-Show por Dia da Semana
        dias_ordem = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        df_filtered['DIA_SEMANA'] = pd.Categorical(df_filtered['DIA_SEMANA'], categories=dias_ordem, ordered=True)
        
        day_noshow = df_filtered.groupby('DIA_SEMANA')['NO-SHOW'].agg(['mean', 'count']).reset_index()
        day_noshow['mean'] = day_noshow['mean'] * 100
        
        fig_day = go.Figure(data=[
            go.Bar(
                x=day_noshow['DIA_SEMANA'],
                y=day_noshow['mean'],
                marker=dict(color=COLORS['primary']),
                text=day_noshow['mean'].round(1),
                textposition='auto',
                texttemplate='%{text}%',
                hovertemplate='<b>%{x}</b><br>Taxa: %{y:.1f}%<br>Total: %{customdata:,}<extra></extra>',
                customdata=day_noshow['count']
            )
        ])
        fig_day.update_layout(
            title=dict(text="Taxa de No-Show por Dia da Semana", font=dict(size=16)),
            xaxis_title="Dia da Semana",
            yaxis_title="Taxa de No-Show (%)",
            height=400
        )
        st.plotly_chart(fig_day, use_container_width=True)
    
    # Segunda linha de gráficos
    col3, col4 = st.columns(2)
    
    with col3:
        # Histograma - Distribuição de Idade
        fig_hist = px.histogram(
            df_filtered,
            x='AGE',
            nbins=30,
            title="Distribuição de Idade dos Pacientes",
            labels={'AGE': 'Idade', 'count': 'Frequência'},
            color_discrete_sequence=[COLORS['secondary']]
        )
        fig_hist.add_vline(
            x=df_filtered['AGE'].mean(),
            line_dash="dash",
            line_color="red",
            annotation_text=f"Média: {df_filtered['AGE'].mean():.1f}",
            annotation_position="top"
        )
        fig_hist.update_layout(height=400)
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col4:
        # Gráfico de Barras - Taxa de No-Show por Faixa Etária
        age_noshow = df_filtered.groupby('FAIXA_ETARIA')['NO-SHOW'].agg(['mean', 'count']).reset_index()
        age_noshow['mean'] = age_noshow['mean'] * 100
        
        fig_age = go.Figure(data=[
            go.Bar(
                x=age_noshow['FAIXA_ETARIA'].astype(str),
                y=age_noshow['mean'],
                marker=dict(color=COLORS['accent']),
                text=age_noshow['mean'].round(1),
                textposition='auto',
                texttemplate='%{text}%',
                hovertemplate='<b>%{x}</b><br>Taxa: %{y:.1f}%<br>Total: %{customdata:,}<extra></extra>',
                customdata=age_noshow['count']
            )
        ])
        fig_age.update_layout(
            title=dict(text="Taxa de No-Show por Faixa Etária", font=dict(size=16)),
            xaxis_title="Faixa Etária",
            yaxis_title="Taxa de No-Show (%)",
            height=400
        )
        st.plotly_chart(fig_age, use_container_width=True)
    
    # Terceira linha - Taxa por dias de antecedência
    st.markdown("### 📅 Taxa de No-Show por Dias de Antecedência")
    
    # Criar faixas de dias de antecedência
    df_antec = df_filtered.copy()
    df_antec['FAIXA_ANTECEDENCIA'] = pd.cut(
        df_antec['DIAS_ANTECEDENCIA'],
        bins=[-1, 0, 1, 3, 7, 14, 30, float('inf')],
        labels=['Mesmo dia', '1 dia', '2-3 dias', '4-7 dias', '8-14 dias', '15-30 dias', '+30 dias']
    )
    
    antec_noshow = df_antec.groupby('FAIXA_ANTECEDENCIA')['NO-SHOW'].agg(['mean', 'count']).reset_index()
    antec_noshow['mean'] = antec_noshow['mean'] * 100
    
    # Configuração de Cores: Gradiente padrão + Vermelho nas duas últimas barras
    # COLORS['gradient'] tem 5 cores. O gráfico tem 7 barras.
    # As barras são: 0, 1, 2, 3, 4, 5, 6
    # 0-4: Usam o gradiente (Teal -> Pink)
    # 5-6: Usam Vermelho
    
    # Lista base (5 cores)
    base_colors = COLORS['gradient']
    
    # Adicionando vermelho para as duas últimas barras
    # Usando um vermelho agradável (#ff5252) que combina bem com o rosa (#FF69B4)
    custom_colors = base_colors + ['#ff5252', '#d32f2f'] 
    
    # Garantir que temos cores suficientes mesmo se faltarem barras nos dados
    final_colors = custom_colors[:len(antec_noshow)]
    
    fig_antec = go.Figure(data=[
        go.Bar(
            x=antec_noshow['FAIXA_ANTECEDENCIA'].astype(str),
            y=antec_noshow['mean'],
            marker=dict(color=final_colors),
            text=antec_noshow['mean'].round(1),
            textposition='auto',
            texttemplate='%{text}%',
            hovertemplate='<b>%{x}</b><br>Taxa: %{y:.1f}%<br>Total: %{customdata:,}<extra></extra>',
            customdata=antec_noshow['count']
        )
    ])
    fig_antec.update_layout(
        title=dict(text="Taxa de No-Show por Dias de Antecedência do Agendamento", font=dict(size=16)),
        xaxis_title="Dias de Antecedência",
        yaxis_title="Taxa de No-Show (%)",
        height=400
    )
    st.plotly_chart(fig_antec, use_container_width=True)


# ============================================================
# TAB 2: SERVIÇOS
# ============================================================
with tab2:
    st.markdown("### 💆 Análise de Serviços")
    
    # 1. Taxa de No-Show por Serviço (Top 15)
    service_noshow = df_filtered.groupby('SERVICENAME')['NO-SHOW'].agg(['mean', 'count']).reset_index()
    service_noshow = service_noshow.sort_values('mean', ascending=True)
    service_noshow['mean'] = service_noshow['mean'] * 100
    
    # Filtrar serviços com pelo menos 10 agendamentos
    service_noshow_filtered = service_noshow[service_noshow['count'] >= 10].tail(15)
    
    fig_service = go.Figure(data=[
        go.Bar(
            y=service_noshow_filtered['SERVICENAME'],
            x=service_noshow_filtered['mean'],
            orientation='h',
            # Gradiente de Verde (baixo no-show) para Vermelho (alto no-show)
            # RdYlGn_r: Red (High) -> Green (Low) invertido
            marker=dict(
                color=service_noshow_filtered['mean'],
                colorscale='RdYlGn_r', 
                showscale=False
            ),
            text=service_noshow_filtered['mean'].round(1),
            textposition='auto',
            texttemplate='%{text}%',
            hovertemplate='<b>%{y}</b><br>Taxa: %{x:.1f}%<br>Total: %{customdata:,}<extra></extra>',
            customdata=service_noshow_filtered['count']
        )
    ])
    fig_service.update_layout(
        title=dict(text="Taxa de No-Show por Serviço", font=dict(size=16)),
        xaxis_title="Taxa de No-Show (%)",
        yaxis_title="Serviço",
        height=600
    )
    st.plotly_chart(fig_service, use_container_width=True)
    
    st.markdown("---")
    
    # 2. Gráfico XY: Valor do Serviço X Taxa de No-Show
    service_price_noshow = df_filtered.groupby('SERVICENAME').agg({
        'PRICE': 'mean',
        'NO-SHOW': ['mean', 'count']
    }).reset_index()
    service_price_noshow.columns = ['SERVICENAME', 'PRICE', 'NOSHOW_RATE', 'COUNT']
    service_price_noshow['NOSHOW_RATE'] = service_price_noshow['NOSHOW_RATE'] * 100
    
    # Filtrar serviços com pelo menos 10 agendamentos
    service_price_noshow = service_price_noshow[service_price_noshow['COUNT'] >= 10]
    
    # Ordenar por preço
    service_price_noshow = service_price_noshow.sort_values('PRICE')
    
    fig_price_noshow = go.Figure()
    
    # Linha conectando os pontos
    fig_price_noshow.add_trace(go.Scatter(
        x=service_price_noshow['PRICE'],
        y=service_price_noshow['NOSHOW_RATE'],
        mode='lines',
        line=dict(color=COLORS['neutral'], width=1, dash='dash'),
        showlegend=False
    ))
    
    # Pontos (bolhas)
    fig_price_noshow.add_trace(go.Scatter(
        x=service_price_noshow['PRICE'],
        y=service_price_noshow['NOSHOW_RATE'],
        mode='markers+text',
        marker=dict(
            size=service_price_noshow['COUNT'] / service_price_noshow['COUNT'].max() * 20 + 5,
            color=service_price_noshow['NOSHOW_RATE'],
            colorscale='RdYlGn_r',
            showscale=True,
            colorbar=dict(title="Taxa %")
        ),
        text=service_price_noshow['SERVICENAME'],
        textposition='top center',
        textfont=dict(size=10),
        hovertemplate='<b>%{text}</b><br>Preço: R$ %{x:.2f}<br>Taxa No-Show: %{y:.1f}%<extra></extra>',
        showlegend=False
    ))
    
    fig_price_noshow.update_layout(
        title=dict(text="Valor do Serviço X Taxa de No-Show", font=dict(size=16)),
        xaxis_title="Preço Médio (R$)",
        yaxis_title="Taxa de No-Show (%)",
        height=700
    )
    st.plotly_chart(fig_price_noshow, use_container_width=True)
    
    # Insight
    st.info("""
    💡 **Insight:** Observa-se uma tendência de que serviços mais caros apresentam menor taxa de no-show. 
    Isso pode indicar que clientes investem mais atenção em procedimentos de maior valor.
    """)


# ============================================================
# TAB 3: INSTAGRAM (Dados não estruturados)
# ============================================================
with tab3:
    st.markdown("### 📱 Análise de Dados do Instagram")
    
    # Carregamento direto do arquivo
    try:
        df_insta = pd.read_csv(INSTAGRAM_PATH)
        file_loaded = True
    except FileNotFoundError:
        st.error(f"❌ Arquivo não encontrado no caminho: {INSTAGRAM_PATH}")
        file_loaded = False
    except Exception as e:
        st.error(f"❌ Erro ao ler arquivo do Instagram: {str(e)}")
        file_loaded = False
    
    if file_loaded:
        if 'comment_text' in df_insta.columns:
            st.success(f"✅ {len(df_insta)} comentários carregados automaticamente")
            
            # Processar emojis
            try:
                import emoji
                has_emoji = True
            except ImportError:
                has_emoji = False
                st.warning("⚠️ Biblioteca 'emoji' não instalada. Análise de emojis limitada.")
            
            if has_emoji:
                # Extrair emojis
                all_emojis = []
                emojis_por_comentario = []
                
                for texto in df_insta['comment_text'].dropna():
                    emojis = [c for c in str(texto) if c in emoji.EMOJI_DATA]
                    all_emojis.extend(emojis)
                    emojis_por_comentario.append(len(emojis))
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Distribuição de emojis por comentário
                    fig_emoji_dist = px.histogram(
                        x=emojis_por_comentario,
                        nbins=20,
                        title="Distribuição de Emojis por Comentário",
                        labels={'x': 'Número de Emojis', 'y': 'Frequência'},
                        color_discrete_sequence=[COLORS['primary']]
                    )
                    media_emojis = np.mean(emojis_por_comentario)
                    fig_emoji_dist.add_vline(
                        x=media_emojis,
                        line_dash="dash",
                        line_color="red",
                        annotation_text=f"Média: {media_emojis:.2f}",
                        annotation_position="top"
                    )
                    fig_emoji_dist.update_layout(height=400)
                    st.plotly_chart(fig_emoji_dist, use_container_width=True)
                
                with col2:
                    # Top 10 emojis
                    emoji_counter = Counter(all_emojis)
                    top_emojis = emoji_counter.most_common(10)
                    
                    if top_emojis:
                        df_top_emojis = pd.DataFrame(top_emojis, columns=['Emoji', 'Contagem'])
                        
                        fig_top_emoji = px.bar(
                            df_top_emojis,
                            x='Contagem',
                            y='Emoji',
                            orientation='h',
                            title='Top 10 Emojis Mais Usados',
                            text='Contagem',
                            color='Contagem',
                            color_continuous_scale='Blues'
                        )
                        fig_top_emoji.update_layout(
                            yaxis={'categoryorder': 'total ascending'},
                            font=dict(size=14),
                            height=400
                        )
                        fig_top_emoji.update_traces(textposition='outside')
                        st.plotly_chart(fig_top_emoji, use_container_width=True)
            
            # Análise de sentimento
            st.markdown("### 💬 Análise de Sentimento")
            
            sentimentos = [analisar_sentimento_simples(t) for t in df_insta['comment_text'].dropna()]
            sent_counter = Counter(sentimentos)
            
            col3, col4 = st.columns(2)
            
            with col3:
                # Distribuição de sentimento
                # Mapeamento de cores: Positivo=Verde, Neutro=Amarelo, Negativo=Vermelho
                color_map = {
                    'positivo': COLORS['positive'],
                    'neutro': COLORS['yellow'],
                    'negativo': COLORS['negative']
                }
                
                labels = list(sent_counter.keys())
                values = list(sent_counter.values())
                colors = [color_map.get(l, COLORS['neutral']) for l in labels]
                
                fig_sent = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.4,
                    marker=dict(colors=colors),
                    textinfo='label+percent'
                )])
                fig_sent.update_layout(
                    title=dict(text="Distribuição de Sentimento", font=dict(size=16)),
                    height=400
                )
                st.plotly_chart(fig_sent, use_container_width=True)
            
            with col4:
                # Estatísticas de sentimento
                total_sent = len(sentimentos)
                st.markdown("#### 📊 Estatísticas de Sentimento")
                st.markdown(f"""
                - 😊 **Positivo:** {sent_counter.get('positivo', 0)} ({sent_counter.get('positivo', 0)/total_sent*100:.1f}%)
                - 😐 **Neutro:** {sent_counter.get('neutro', 0)} ({sent_counter.get('neutro', 0)/total_sent*100:.1f}%)
                - 😞 **Negativo:** {sent_counter.get('negativo', 0)} ({sent_counter.get('negativo', 0)/total_sent*100:.1f}%)
                """)
                
                # Top palavras (simplificado)
                all_words = []
                stopwords_pt = {'de', 'a', 'o', 'que', 'e', 'do', 'da', 'em', 'um', 'para', 'é', 'com', 'não', 'uma', 'os', 'no', 'se', 'na', 'por', 'mais', 'as', 'dos', 'como', 'mas', 'foi', 'ao', 'ele', 'das', 'tem', 'à', 'seu', 'sua', 'ou', 'ser', 'quando', 'muito', 'há', 'nos', 'já', 'está', 'eu', 'também', 'só', 'pelo', 'pela', 'até', 'isso', 'ela', 'entre', 'era', 'depois', 'sem', 'mesmo', 'aos', 'ter', 'seus', 'quem', 'nas', 'me', 'esse', 'eles', 'estão', 'você', 'tinha', 'foram', 'essa', 'num', 'nem', 'suas', 'meu', 'às', 'minha', 'têm', 'numa', 'pelos', 'elas', 'havia', 'seja', 'qual', 'será', 'nós', 'tenho', 'lhe', 'deles', 'essas', 'esses', 'pelas', 'este', 'fosse', 'dele', 'tu', 'te', 'vocês', 'vos', 'lhes', 'meus', 'minhas', 'teu', 'tua', 'teus', 'tuas', 'nosso', 'nossa', 'nossos', 'nossas', 'dela', 'delas', 'esta', 'estes', 'estas', 'aquele', 'aquela', 'aqueles', 'aquelas', 'isto', 'aquilo', 'estou', 'está', 'estamos', 'estão', 'estive', 'esteve', 'estivemos', 'estiveram', 'estava', 'estávamos', 'estavam', 'estivera', 'estivéramos', 'esteja', 'estejamos', 'estejam', 'estivesse', 'estivéssemos', 'estivessem', 'estiver', 'estivermos', 'estiverem', 'hei', 'há', 'havemos', 'hão', 'houve', 'houvemos', 'houveram', 'houvera', 'houvéramos', 'haja', 'hajamos', 'hajam', 'houvesse', 'houvéssemos', 'houvessem', 'houver', 'houvermos', 'houverem', 'houverei', 'houverá', 'houveremos', 'houverão', 'houveria', 'houveríamos', 'houveriam', 'sou', 'somos', 'são', 'era', 'éramos', 'eram', 'fui', 'foi', 'fomos', 'foram', 'fora', 'fôramos', 'seja', 'sejamos', 'sejam', 'fosse', 'fôssemos', 'fossem', 'for', 'formos', 'forem', 'serei', 'será', 'seremos', 'serão', 'seria', 'seríamos', 'seriam', 'tenho', 'tem', 'temos', 'tém', 'tinha', 'tínhamos', 'tinham', 'tive', 'teve', 'tivemos', 'tiveram', 'tivera', 'tivéramos', 'tenha', 'tenhamos', 'tenham', 'tivesse', 'tivéssemos', 'tivessem', 'tiver', 'tivermos', 'tiverem', 'terei', 'terá', 'teremos', 'terão', 'teria', 'teríamos', 'teriam'}
                
                for texto in df_insta['comment_text'].dropna():
                    texto_limpo = re.sub(r'[^\w\s]', '', str(texto).lower())
                    palavras = [p for p in texto_limpo.split() if p not in stopwords_pt and len(p) > 2]
                    all_words.extend(palavras)
                
                word_counter = Counter(all_words)
                top_words = word_counter.most_common(15)
                
                st.markdown("#### 🔤 Top 15 Termos Mais Importantes")
                df_words = pd.DataFrame(top_words, columns=['Termo', 'Frequência'])
                st.dataframe(df_words, use_container_width=True)
            
            # Word Cloud (usando imagem simples se não tiver a biblioteca)
            st.markdown("### ☁️ Nuvem de Palavras")
            try:
                from wordcloud import WordCloud
                import matplotlib.pyplot as plt
                
                texto_completo = ' '.join(all_words)
                wordcloud = WordCloud(
                    width=1200,
                    height=600,
                    background_color='white',
                    colormap='viridis',
                    max_words=100
                ).generate(texto_completo)
                
                fig_wc, ax = plt.subplots(figsize=(16, 8))
                ax.imshow(wordcloud, interpolation='bilinear')
                ax.axis('off')
                st.pyplot(fig_wc)
                
            except ImportError:
                st.info("📦 Para ver a nuvem de palavras, instale: `pip install wordcloud`")
        else:
            st.error("❌ O arquivo deve conter uma coluna 'comment_text'")


# ============================================================
# TAB 4: CLIMA
# ============================================================
with tab4:
    st.markdown("### 🌡️ Impacto do Clima nos No-Shows")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Impacto da Temperatura
        if 'CLASSIFICACAO_TEMP' in df_filtered.columns or 'TEMP_AR_C' in df_filtered.columns:
            # Criar classificação se não existir
            if 'CLASSIFICACAO_TEMP' not in df_filtered.columns:
                df_filtered['CLASSIFICACAO_TEMP'] = pd.cut(
                    df_filtered['TEMP_AR_C'],
                    bins=[-float('inf'), 17, 24, 30, float('inf')],
                    labels=['Frio (<17°)', 'Agradável (17-24°)', 'Quente (24-30°)', 'Muito Quente (>30°)']
                )
            
            temp_noshow = df_filtered.groupby('CLASSIFICACAO_TEMP')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            temp_noshow['mean'] = temp_noshow['mean'] * 100
            
            # Ordenar por valor (decrescente)
            temp_noshow = temp_noshow.sort_values('mean', ascending=False)
            
            # Mapeamento de Cores por Temperatura (Mais Quente = Mais Vermelho)
            # Definindo cores hexadecimais para garantir o gradiente solicitado
            color_temp_map = {
                'Muito Quente (>30°)': '#ff0000',  # Vermelho Puro
                'Quente (24-30°)': '#ff6b6b',      # Vermelho Claro/Salmão
                'Agradável (17-24°)': '#48dbfb',   # Azul Claro
                'Frio (<17°)': '#0097e6'           # Azul Escuro
            }
            
            # Criar lista de cores baseada nas categorias atuais
            bar_colors = [color_temp_map.get(cat, '#bdc3c7') for cat in temp_noshow['CLASSIFICACAO_TEMP']]
            
            fig_temp = go.Figure(data=[
                go.Bar(
                    x=temp_noshow['CLASSIFICACAO_TEMP'].astype(str),
                    y=temp_noshow['mean'],
                    marker=dict(
                        color=bar_colors,  # Aplica o mapeamento de cores
                        line=dict(color='black', width=1)
                    ),
                    text=temp_noshow['mean'].round(1).astype(str) + '%',
                    textposition='outside',
                    hovertemplate='<b>%{x}</b><br>Taxa: %{y:.1f}%<br>Total: %{customdata:,}<extra></extra>',
                    customdata=temp_noshow['count']
                )
            ])
            fig_temp.update_layout(
                title=dict(
                    text="Taxa de No-Show por Classificação de Temperatura",
                    font=dict(size=14, color='white')  # Título em branco solicitado
                ),
                xaxis_title="",
                yaxis_title="Taxa de No-Show (%)",
                height=450,
                xaxis=dict(tickangle=-45)
            )
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.warning("⚠️ Dados de temperatura não disponíveis")
    
    with col2:
        # Taxa de No-Show por Intensidade de Chuva
        if 'INTENSIDADE_CHUVA' in df_filtered.columns or 'PRECIPITACAO_MM' in df_filtered.columns:
            # Criar classificação se não existir
            if 'INTENSIDADE_CHUVA' not in df_filtered.columns:
                df_filtered['INTENSIDADE_CHUVA'] = pd.cut(
                    df_filtered['PRECIPITACAO_MM'],
                    bins=[-float('inf'), 0.1, 2.5, 10, float('inf')],
                    labels=['Sem Chuva', 'Chuva Fraca', 'Chuva Moderada', 'Chuva Forte']
                )
            
            chuva_ordem = ['Sem Chuva', 'Chuva Fraca', 'Chuva Moderada', 'Chuva Forte']
            
            chuva_noshow = df_filtered.groupby('INTENSIDADE_CHUVA')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            chuva_noshow['mean'] = chuva_noshow['mean'] * 100
            
            # Ordenar
            chuva_noshow['INTENSIDADE_CHUVA'] = pd.Categorical(
                chuva_noshow['INTENSIDADE_CHUVA'],
                categories=chuva_ordem,
                ordered=True
            )
            chuva_noshow = chuva_noshow.sort_values('INTENSIDADE_CHUVA')
            
            colors_chuva = ['#87CEEB', '#4682B4', '#4169E1', '#00008B']
            
            fig_chuva = go.Figure(data=[
                go.Bar(
                    x=chuva_noshow['INTENSIDADE_CHUVA'].astype(str),
                    y=chuva_noshow['mean'],
                    marker=dict(color=colors_chuva[:len(chuva_noshow)]),
                    text=chuva_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%',
                    hovertemplate='<b>%{x}</b><br>Taxa: %{y:.1f}%<br>Total: %{customdata:,}<extra></extra>',
                    customdata=chuva_noshow['count']
                )
            ])
            fig_chuva.update_layout(
                title=dict(text="Taxa de No-Show por Intensidade de Chuva", font=dict(size=16)),
                xaxis_title="Intensidade de Chuva",
                yaxis_title="Taxa de No-Show (%)",
                height=450
            )
            st.plotly_chart(fig_chuva, use_container_width=True)
        else:
            st.warning("⚠️ Dados de precipitação não disponíveis")
    
    # Insights sobre clima
    st.markdown("---")
    st.markdown("### 💡 Insights Climáticos")
    
    if 'CLASSIFICACAO_TEMP' in df_filtered.columns and 'INTENSIDADE_CHUVA' in df_filtered.columns:
        # Melhor e pior condição de temperatura
        temp_stats = df_filtered.groupby('CLASSIFICACAO_TEMP')['NO-SHOW'].mean() * 100
        if not temp_stats.empty:
            melhor_temp = temp_stats.idxmin()
            
            col1_insight, col2_insight = st.columns(2)
            with col1_insight:
                st.success(f"""
                ☀️ **Melhor condição de temperatura:** {melhor_temp}  
                Taxa de no-show: {temp_stats[melhor_temp]:.1f}%
                """)


# ============================================================
# SEÇÃO FINAL: INSIGHTS E RECOMENDAÇÕES
# ============================================================

st.markdown("---")
st.header("💡 Insights e Recomendações")

# Calcular insights
insights = []

# SMS
if len(df_filtered[df_filtered['SMS_RECEIVED'] == 1]) > 0:
    sms_yes = df_filtered[df_filtered['SMS_RECEIVED'] == 1]['NO-SHOW'].mean() * 100
    sms_no = df_filtered[df_filtered['SMS_RECEIVED'] == 0]['NO-SHOW'].mean() * 100
    reducao_sms = sms_no - sms_yes
    if reducao_sms > 0:
        insights.append(f"📱 O envio de SMS reduz a taxa de no-show em **{reducao_sms:.1f} pontos percentuais**")

# Faixa etária mais problemática
if 'FAIXA_ETARIA' in df_filtered.columns:
    age_noshow_insight = df_filtered.groupby('FAIXA_ETARIA')['NO-SHOW'].mean().sort_values(ascending=False)
    if len(age_noshow_insight) > 0:
        insights.append(f"👶 A faixa etária **{age_noshow_insight.index[0]}** apresenta a maior taxa de no-show ({age_noshow_insight.values[0]*100:.1f}%)")

# Serviço mais problemático
service_noshow_insight = df_filtered.groupby('SERVICENAME')['NO-SHOW'].agg(['mean', 'count'])
service_noshow_insight = service_noshow_insight[service_noshow_insight['count'] > 10]
if len(service_noshow_insight) > 0:
    service_noshow_insight = service_noshow_insight.sort_values('mean', ascending=False)
    insights.append(f"💆 O serviço **{service_noshow_insight.index[0]}** tem a maior taxa de no-show ({service_noshow_insight['mean'].values[0]*100:.1f}%)")

# Dia da semana
if 'DIA_SEMANA' in df_filtered.columns:
    day_noshow_insight = df_filtered.groupby('DIA_SEMANA')['NO-SHOW'].mean().sort_values(ascending=False)
    if len(day_noshow_insight) > 0:
        insights.append(f"📅 **{day_noshow_insight.index[0]}** é o dia com maior taxa de no-show ({day_noshow_insight.values[0]*100:.1f}%)")

# Exibir insights
if insights:
    for insight in insights:
        st.info(insight)

# Recomendações
st.subheader("🎯 Recomendações Estratégicas")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **🚀 Ações Imediatas:**
    1. ✅ Implementar envio automático de SMS para 100% dos agendamentos
    2. 📞 Criar protocolo de confirmação para agendamentos com mais de 7 dias de antecedência
    3. 🎯 Focar estratégias de retenção em faixas etárias problemáticas
    4. 💰 Revisar política de cancelamento para serviços de baixo valor
    """)

with col2:
    st.markdown("""
    **📊 Análises Futuras:**
    1. 🤖 Desenvolver modelo preditivo de no-show
    2. 📱 Testar diferentes formatos de lembretes (WhatsApp, Email)
    3. 🌡️ Considerar clima ao sugerir reagendamentos
    4. 📈 Implementar sistema de pontuação de risco
    """)

# ============================================================
# RODAPÉ
# ============================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7f8c8d; padding: 2rem 0;'>
    <p>🏥 <b>Beira-Mar Analytics Dashboard</b></p>
    <p>📊 Última atualização: {}</p>
</div>
""".format(datetime.now().strftime('%d/%m/%Y %H:%M')), unsafe_allow_html=True)
