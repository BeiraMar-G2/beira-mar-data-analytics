"""
🏥 DASHBOARD BEIRA-MAR ANALYTICS
Dashboard interativo para análise de no-show em clínica de estética

Autor: Beira-Mar Analytics Team
Data: 2024
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings

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
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #667eea;
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
</style>
""", unsafe_allow_html=True)

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

@st.cache_data
def load_data(file_path):
    """Carrega e processa os dados"""
    df = pd.read_csv(file_path)
    original_len = len(df)
    
    # Tentar converter datas com múltiplos formatos possíveis
    # Formato 1: DD/MM/YYYY HH:MM:SS (formato do exemplo fornecido)
    df['APPOINTMENTDAY'] = pd.to_datetime(df['APPOINTMENTDAY'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    
    # Se ainda houver NaT, tentar formato DD/MM/YYYY (sem hora)
    if df['APPOINTMENTDAY'].isna().any():
        mask = df['APPOINTMENTDAY'].isna()
        df.loc[mask, 'APPOINTMENTDAY'] = pd.to_datetime(df.loc[mask, 'APPOINTMENTDAY'], format='%d/%m/%Y', errors='coerce')
    
    # Se ainda houver NaT, tentar formato ISO (YYYY-MM-DD)
    if df['APPOINTMENTDAY'].isna().any():
        mask = df['APPOINTMENTDAY'].isna()
        df.loc[mask, 'APPOINTMENTDAY'] = pd.to_datetime(df.loc[mask, 'APPOINTMENTDAY'], errors='coerce')
    
    # ScheduledDay
    df['SCHEDULEDDAY'] = pd.to_datetime(df['SCHEDULEDDAY'], errors='coerce')
    
    # Remover linhas com datas inválidas (NaT)
    df = df.dropna(subset=['APPOINTMENTDAY'])
    removed = original_len - len(df)
    
    if removed > 0:
        st.sidebar.warning(f"⚠️ {removed:,} registros removidos (datas inválidas)")
    
    # Se não sobrou nenhum registro válido, mostrar erro detalhado
    if len(df) == 0:
        st.error(f"""
        ❌ **ERRO: Nenhum registro válido após processamento de datas!**
        
        **Problema detectado:**
        - Total de registros no CSV: {original_len:,}
        - Todos foram removidos por datas inválidas
        
        **Formatos de data esperados (APPOINTMENTDAY):**
        1. `DD/MM/YYYY HH:MM:SS` → Exemplo: `29/04/2016 00:00:00`
        2. `DD/MM/YYYY` → Exemplo: `29/04/2016`
        3. `YYYY-MM-DD` → Exemplo: `2016-04-29`
        
        **🔍 SOLUÇÃO:**
        Execute o script de diagnóstico para ver o formato real das suas datas:
        ```bash
        python diagnostico_dados.py
        ```
        
        Ou abra o CSV e verifique o formato da coluna APPOINTMENTDAY.
        """)
        st.stop()
    
    # Criar coluna de data apenas (sem hora)
    df['APPOINTMENT_DATE'] = df['APPOINTMENTDAY'].dt.date
    
    # Criar faixas etárias
    bins = [0, 18, 30, 40, 50, 60, 100]
    labels = ['0-18', '19-30', '31-40', '41-50', '51-60', '60+']
    df['FAIXA_ETARIA'] = pd.cut(df['AGE'], bins=bins, labels=labels)
    
    # Criar faixas de preço
    price_bins = [0, 50, 100, 150, 200]
    price_labels = ['R$0-50', 'R$51-100', 'R$101-150', 'R$151-200']
    df['FAIXA_PRECO'] = pd.cut(df['PRICE'], bins=price_bins, labels=price_labels)
    
    # Dia da semana
    df['DIA_SEMANA'] = df['APPOINTMENTDAY'].dt.day_name()
    
    # Mês
    df['MES'] = df['APPOINTMENTDAY'].dt.month_name()
    
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
    
    # Filtro de gênero
    if filters['gender'] and 'Todos' not in filters['gender']:
        filtered_df = filtered_df[filtered_df['GENDER'].isin(filters['gender'])]
    
    # Filtro de faixa etária
    if filters['age_range'] and 'Todas' not in filters['age_range']:
        filtered_df = filtered_df[filtered_df['FAIXA_ETARIA'].isin(filters['age_range'])]
    
    # Filtro de serviços
    if filters['services'] and 'Todos' not in filters['services']:
        filtered_df = filtered_df[filtered_df['SERVICENAME'].isin(filters['services'])]
    
    # Filtro de SMS
    if filters['sms'] != 'Todos':
        sms_value = 1 if filters['sms'] == 'Com SMS' else 0
        filtered_df = filtered_df[filtered_df['SMS_RECEIVED'] == sms_value]
    
    # Filtro de bolsa
    if filters['scholarship'] != 'Todos':
        scholarship_value = 1 if filters['scholarship'] == 'Com Bolsa' else 0
        filtered_df = filtered_df[filtered_df['SCHOLARSHIP'] == scholarship_value]
    
    # Filtro de faixa de preço
    if filters['price_range'] and 'Todas' not in filters['price_range']:
        filtered_df = filtered_df[filtered_df['FAIXA_PRECO'].isin(filters['price_range'])]
    
    return filtered_df

def create_kpi_card(title, value, delta=None, delta_color="normal"):
    """Cria um card de KPI"""
    col1, col2 = st.columns([3, 1])
    with col1:
        st.metric(title, value, delta=delta, delta_color=delta_color)

# ============================================================
# SIDEBAR - FILTROS
# ============================================================

st.sidebar.header("🎛️ FILTROS")

# Caminho do arquivo
FILE_PATH = '../../dados/refined/cancelamentos_com_clima.csv'

# Tentar carregar os dados
try:
    df = load_data(FILE_PATH)
    st.sidebar.success(f"✅ {len(df):,} registros carregados")
except FileNotFoundError:
    st.error(f"""
    ❌ **Arquivo não encontrado!**
    
    O arquivo esperado não foi encontrado em:  
    `{FILE_PATH}`
    
    **Certifique-se de que:**
    1. O arquivo existe no caminho correto
    2. Você está executando o script da pasta correta
    3. O caminho relativo está correto
    
    **Caminho esperado a partir da localização do script:**
    ```
    dashboard_beira_mar.py (aqui você está)
    └── ../../dados/refined/cancelamentos_com_clima.csv
    ```
    """)
    st.stop()
except Exception as e:
    st.error(f"""
    ❌ **Erro ao carregar o arquivo!**
    
    Erro: {str(e)}
    """)
    st.stop()

# Filtros
st.sidebar.markdown("---")
st.sidebar.subheader("📅 Período")

# Remover NaT e calcular min/max
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
st.sidebar.subheader("👥 Demografia")

gender_options = ['Todos'] + list(df['GENDER'].unique())
gender_filter = st.sidebar.multiselect(
    "Gênero",
    options=gender_options,
    default=['Todos']
)

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

st.sidebar.markdown("---")
st.sidebar.subheader("📱 Comunicação")

sms_filter = st.sidebar.radio(
    "SMS",
    options=['Todos', 'Com SMS', 'Sem SMS'],
    index=0
)

scholarship_filter = st.sidebar.radio(
    "Bolsa de Estudos",
    options=['Todos', 'Com Bolsa', 'Sem Bolsa'],
    index=0
)

# Aplicar filtros
filters = {
    'date_range': date_range if len(date_range) == 2 else None,
    'gender': gender_filter,
    'age_range': age_filter,
    'services': service_filter,
    'sms': sms_filter,
    'scholarship': scholarship_filter,
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
show_rate = (1 - df_filtered['NO-SHOW'].mean()) * 100
avg_age = df_filtered['AGE'].mean()
avg_price = df_filtered['PRICE'].mean()
unique_patients = df_filtered['PATIENTID'].nunique()

# Comparação com dataset completo
no_show_rate_total = df['NO-SHOW'].mean() * 100
no_show_delta = no_show_rate - no_show_rate_total

# Exibir KPIs
col1, col2, col3, col4, col5, col6 = st.columns(6)

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
            "✅ Taxa de Comparecimento",
            f"{show_rate:.1f}%",
            f"{-no_show_delta:+.1f}%",
            delta_color="normal"
        )

with col4:
        st.metric(
            "👥 Pacientes Únicos",
            f"{unique_patients:,}"
        )

with col5:
        st.metric(
            "🎂 Idade Média",
            f"{avg_age:.1f} anos"
        )

with col6:
        st.metric(
            "💰 Ticket Médio",
            f"R$ {avg_price:.2f}"
        )

st.markdown("---")

# ============================================================
# SEÇÃO 2: ANÁLISE DE NO-SHOW
# ============================================================

st.header("📈 Análise de No-Show")

col1, col2 = st.columns(2)

with col1:
        # Pizza de No-Show
        no_show_counts = df_filtered['NO-SHOW'].value_counts()
        fig = go.Figure(data=[go.Pie(
            labels=['Compareceu', 'No-Show'],
            values=no_show_counts.values,
            hole=0.4,
            marker=dict(colors=['#2ecc71', '#e74c3c']),
            textinfo='label+percent',
            textfont=dict(size=14)
        )])
        fig.update_layout(
            title="Distribuição de No-Show",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

with col2:
        # Barras de No-Show
        fig = go.Figure(data=[
            go.Bar(
                x=['Compareceu', 'No-Show'],
                y=no_show_counts.values,
                marker=dict(color=['#2ecc71', '#e74c3c']),
                text=no_show_counts.values,
                textposition='auto',
                texttemplate='%{text:,}'
            )
        ])
        fig.update_layout(
            title="Quantidade de Agendamentos por Status",
            xaxis_title="Status",
            yaxis_title="Quantidade",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# SEÇÃO 3: ANÁLISE POR DIMENSÕES
# ============================================================

st.header("🔍 Análise por Dimensões")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "👥 Gênero e Idade",
        "💆 Serviços e Preços",
        "📱 SMS e Comunicação",
        "🌡️ Fatores Climáticos",
        "🏥 Condições de Saúde"
])

# TAB 1: GÊNERO E IDADE
with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # No-Show por Gênero
            gender_noshow = df_filtered.groupby('GENDER')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            gender_noshow['mean'] = gender_noshow['mean'] * 100
            
            fig = go.Figure(data=[
                go.Bar(
                    x=gender_noshow['GENDER'],
                    y=gender_noshow['mean'],
                    marker=dict(color=['#3498db', '#e91e63']),
                    text=gender_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%',
                    hovertemplate='<b>%{x}</b><br>Taxa de No-Show: %{y:.1f}%<br>Total: %{customdata:,}<extra></extra>',
                    customdata=gender_noshow['count']
                )
            ])
            fig.update_layout(
                title="Taxa de No-Show por Gênero",
                xaxis_title="Gênero",
                yaxis_title="Taxa de No-Show (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # No-Show por Faixa Etária
            age_noshow = df_filtered.groupby('FAIXA_ETARIA')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            age_noshow['mean'] = age_noshow['mean'] * 100
            
            fig = go.Figure(data=[
                go.Bar(
                    x=age_noshow['FAIXA_ETARIA'],
                    y=age_noshow['mean'],
                    marker=dict(color='#e74c3c'),
                    text=age_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%',
                    hovertemplate='<b>%{x}</b><br>Taxa de No-Show: %{y:.1f}%<br>Total: %{customdata:,}<extra></extra>',
                    customdata=age_noshow['count']
                )
            ])
            fig.update_layout(
                title="Taxa de No-Show por Faixa Etária",
                xaxis_title="Faixa Etária",
                yaxis_title="Taxa de No-Show (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Distribuição de Idade
        fig = px.histogram(
            df_filtered,
            x='AGE',
            nbins=30,
            title="Distribuição de Idade dos Pacientes",
            labels={'AGE': 'Idade', 'count': 'Frequência'},
            color_discrete_sequence=['#3498db']
        )
        fig.add_vline(
            x=df_filtered['AGE'].mean(),
            line_dash="dash",
            line_color="red",
            annotation_text=f"Média: {df_filtered['AGE'].mean():.1f}",
            annotation_position="top"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

# TAB 2: SERVIÇOS E PREÇOS
with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # Top 10 Serviços
            top_services = df_filtered['SERVICENAME'].value_counts().head(10)
            
            fig = go.Figure(data=[
                go.Bar(
                    y=top_services.index,
                    x=top_services.values,
                    orientation='h',
                    marker=dict(color='#9b59b6'),
                    text=top_services.values,
                    textposition='auto',
                    texttemplate='%{text:,}'
                )
            ])
            fig.update_layout(
                title="Top 10 Serviços Mais Agendados",
                xaxis_title="Quantidade",
                yaxis_title="Serviço",
                height=500
            )
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Taxa de No-Show por Serviço (Top 10)
            service_noshow = df_filtered.groupby('SERVICENAME')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            service_noshow = service_noshow.sort_values('count', ascending=False).head(10)
            service_noshow['mean'] = service_noshow['mean'] * 100
            
            fig = go.Figure(data=[
                go.Bar(
                    y=service_noshow['SERVICENAME'],
                    x=service_noshow['mean'],
                    orientation='h',
                    marker=dict(color='#e74c3c'),
                    text=service_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%'
                )
            ])
            fig.update_layout(
                title="Taxa de No-Show por Serviço (Top 10)",
                xaxis_title="Taxa de No-Show (%)",
                yaxis_title="Serviço",
                height=500
            )
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Distribuição de Preços
            fig = px.histogram(
                df_filtered,
                x='PRICE',
                nbins=20,
                title="Distribuição de Preços dos Serviços",
                labels={'PRICE': 'Preço (R$)', 'count': 'Frequência'},
                color_discrete_sequence=['#27ae60']
            )
            fig.add_vline(
                x=df_filtered['PRICE'].mean(),
                line_dash="dash",
                line_color="red",
                annotation_text=f"Média: R$ {df_filtered['PRICE'].mean():.2f}",
                annotation_position="top"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            # No-Show por Faixa de Preço
            price_noshow = df_filtered.groupby('FAIXA_PRECO')['NO-SHOW'].mean().reset_index()
            price_noshow['NO-SHOW'] = price_noshow['NO-SHOW'] * 100
            
            fig = go.Figure(data=[
                go.Bar(
                    x=price_noshow['FAIXA_PRECO'],
                    y=price_noshow['NO-SHOW'],
                    marker=dict(color='#e67e22'),
                    text=price_noshow['NO-SHOW'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%'
                )
            ])
            fig.update_layout(
                title="Taxa de No-Show por Faixa de Preço",
                xaxis_title="Faixa de Preço",
                yaxis_title="Taxa de No-Show (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)

# TAB 3: SMS E COMUNICAÇÃO
with tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            # Impacto do SMS
            sms_noshow = df_filtered.groupby('SMS_RECEIVED')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            sms_noshow['mean'] = sms_noshow['mean'] * 100
            sms_labels = ['Sem SMS', 'Com SMS']
            
            fig = go.Figure(data=[
                go.Bar(
                    x=sms_labels,
                    y=sms_noshow['mean'],
                    marker=dict(color=['#e74c3c', '#2ecc71']),
                    text=sms_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%<br>(%{customdata:,} casos)',
                    customdata=sms_noshow['count']
                )
            ])
            fig.update_layout(
                title="Impacto do SMS na Taxa de No-Show",
                xaxis_title="Status SMS",
                yaxis_title="Taxa de No-Show (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Calcular redução
            if len(sms_noshow) == 2:
                reducao = sms_noshow.iloc[0]['mean'] - sms_noshow.iloc[1]['mean']
                st.success(f"✅ **Redução de {reducao:.1f} pontos percentuais** com o envio de SMS!")
        
        with col2:
            # Distribuição de SMS
            sms_counts = df_filtered['SMS_RECEIVED'].value_counts()
            
            fig = go.Figure(data=[go.Pie(
                labels=sms_labels,
                values=sms_counts.values,
                hole=0.4,
                marker=dict(colors=['#e74c3c', '#2ecc71']),
                textinfo='label+percent',
                textfont=dict(size=14)
            )])
            fig.update_layout(
                title="Distribuição de Envio de SMS",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Bolsa de Estudos
        col3, col4 = st.columns(2)
        
        with col3:
            scholarship_noshow = df_filtered.groupby('SCHOLARSHIP')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            scholarship_noshow['mean'] = scholarship_noshow['mean'] * 100
            scholarship_labels = ['Sem Bolsa', 'Com Bolsa']
            
            fig = go.Figure(data=[
                go.Bar(
                    x=scholarship_labels,
                    y=scholarship_noshow['mean'],
                    marker=dict(color=['#3498db', '#9b59b6']),
                    text=scholarship_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%'
                )
            ])
            fig.update_layout(
                title="Impacto da Bolsa de Estudos no No-Show",
                xaxis_title="Status Bolsa",
                yaxis_title="Taxa de No-Show (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            scholarship_counts = df_filtered['SCHOLARSHIP'].value_counts()
            
            fig = go.Figure(data=[go.Pie(
                labels=scholarship_labels,
                values=scholarship_counts.values,
                hole=0.4,
                marker=dict(colors=['#3498db', '#9b59b6']),
                textinfo='label+percent',
                textfont=dict(size=14)
            )])
            fig.update_layout(
                title="Distribuição de Pacientes com Bolsa",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)

# TAB 4: FATORES CLIMÁTICOS
with tab4:
        col1, col2 = st.columns(2)
        
        with col1:
            # No-Show por Classificação de Temperatura
            if 'CLASSIFICACAO_TEMP' in df_filtered.columns:
                temp_noshow = df_filtered.groupby('CLASSIFICACAO_TEMP')['NO-SHOW'].agg(['mean', 'count']).reset_index()
                temp_noshow['mean'] = temp_noshow['mean'] * 100
                temp_noshow = temp_noshow.sort_values('mean', ascending=False)
                
                fig = go.Figure(data=[
                    go.Bar(
                        x=temp_noshow['CLASSIFICACAO_TEMP'],
                        y=temp_noshow['mean'],
                        marker=dict(color='#3498db'),
                        text=temp_noshow['mean'].round(1),
                        textposition='auto',
                        texttemplate='%{text}%'
                    )
                ])
                fig.update_layout(
                    title="Taxa de No-Show por Classificação de Temperatura",
                    xaxis_title="Classificação",
                    yaxis_title="Taxa de No-Show (%)",
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # No-Show por Estação
            if 'ESTACAO_ANO' in df_filtered.columns:
                season_noshow = df_filtered.groupby('ESTACAO_ANO')['NO-SHOW'].agg(['mean', 'count']).reset_index()
                season_noshow['mean'] = season_noshow['mean'] * 100
                
                fig = go.Figure(data=[
                    go.Bar(
                        x=season_noshow['ESTACAO_ANO'],
                        y=season_noshow['mean'],
                        marker=dict(color='#e67e22'),
                        text=season_noshow['mean'].round(1),
                        textposition='auto',
                        texttemplate='%{text}%'
                    )
                ])
                fig.update_layout(
                    title="Taxa de No-Show por Estação do Ano",
                    xaxis_title="Estação",
                    yaxis_title="Taxa de No-Show (%)",
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Scatter plots
        col3, col4 = st.columns(2)
        
        with col3:
            fig = px.scatter(
                df_filtered,
                x='TEMP_AR_C',
                y='NO-SHOW',
                opacity=0.3,
                title="Relação entre Temperatura e No-Show",
                labels={'TEMP_AR_C': 'Temperatura do Ar (°C)', 'NO-SHOW': 'No-Show'},
                color_discrete_sequence=['#e74c3c']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            fig = px.scatter(
                df_filtered,
                x='PRECIPITACAO_MM',
                y='NO-SHOW',
                opacity=0.3,
                title="Relação entre Precipitação e No-Show",
                labels={'PRECIPITACAO_MM': 'Precipitação (mm)', 'NO-SHOW': 'No-Show'},
                color_discrete_sequence=['#3498db']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

# TAB 5: CONDIÇÕES DE SAÚDE
with tab5:
        health_conditions = ['HIPERTENSION', 'DIABETES', 'HANDCAP']
        
        # Criar dados para comparação
        comparison_data = []
        for condition in health_conditions:
            if condition in df_filtered.columns:
                # Binarizar: 0 vs >0
                df_temp = df_filtered.copy()
                df_temp[f'{condition}_binary'] = (df_temp[condition] > 0).astype(int)
                
                with_condition = df_temp[df_temp[f'{condition}_binary'] == 1]['NO-SHOW'].mean() * 100
                without_condition = df_temp[df_temp[f'{condition}_binary'] == 0]['NO-SHOW'].mean() * 100
                
                comparison_data.append({
                    'Condição': condition,
                    'Sem Condição': without_condition,
                    'Com Condição': with_condition
                })
        
        if comparison_data:
            comp_df = pd.DataFrame(comparison_data)
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Sem Condição',
                x=comp_df['Condição'],
                y=comp_df['Sem Condição'],
                marker=dict(color='#3498db'),
                text=comp_df['Sem Condição'].round(1),
                textposition='auto',
                texttemplate='%{text}%'
            ))
            
            fig.add_trace(go.Bar(
                name='Com Condição',
                x=comp_df['Condição'],
                y=comp_df['Com Condição'],
                marker=dict(color='#e74c3c'),
                text=comp_df['Com Condição'].round(1),
                textposition='auto',
                texttemplate='%{text}%'
            ))
            
            fig.update_layout(
                title="Comparação de Condições de Saúde",
                xaxis_title="Condição",
                yaxis_title="Taxa de No-Show (%)",
                barmode='group',
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)

# ============================================================
# SEÇÃO 4: ANÁLISE TEMPORAL
# ============================================================

st.header("📅 Análise Temporal")

# No-Show ao longo do tempo
if 'APPOINTMENTDAY' in df_filtered.columns:
        temporal_data = df_filtered.groupby(df_filtered['APPOINTMENTDAY'].dt.to_period('M'))['NO-SHOW'].agg(['mean', 'count']).reset_index()
        temporal_data['APPOINTMENTDAY'] = temporal_data['APPOINTMENTDAY'].astype(str)
        temporal_data['mean'] = temporal_data['mean'] * 100
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Scatter(
                x=temporal_data['APPOINTMENTDAY'],
                y=temporal_data['mean'],
                name="Taxa de No-Show (%)",
                line=dict(color='#e74c3c', width=3),
                mode='lines+markers'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Bar(
                x=temporal_data['APPOINTMENTDAY'],
                y=temporal_data['count'],
                name="Quantidade de Agendamentos",
                marker=dict(color='#3498db', opacity=0.5)
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            title="Evolução Temporal de No-Show e Volume de Agendamentos",
            xaxis_title="Mês",
            height=500
        )
        fig.update_yaxes(title_text="Taxa de No-Show (%)", secondary_y=False)
        fig.update_yaxes(title_text="Quantidade de Agendamentos", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)

# No-Show por dia da semana
col1, col2 = st.columns(2)

with col1:
        if 'DIA_SEMANA' in df_filtered.columns:
            # Ordenar dias da semana
            dias_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            df_filtered['DIA_SEMANA'] = pd.Categorical(df_filtered['DIA_SEMANA'], categories=dias_ordem, ordered=True)
            
            day_noshow = df_filtered.groupby('DIA_SEMANA')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            day_noshow['mean'] = day_noshow['mean'] * 100
            
            fig = go.Figure(data=[
                go.Bar(
                    x=day_noshow['DIA_SEMANA'],
                    y=day_noshow['mean'],
                    marker=dict(color='#9b59b6'),
                    text=day_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%'
                )
            ])
            fig.update_layout(
                title="Taxa de No-Show por Dia da Semana",
                xaxis_title="Dia da Semana",
                yaxis_title="Taxa de No-Show (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)

with col2:
        if 'MES' in df_filtered.columns:
            month_noshow = df_filtered.groupby('MES')['NO-SHOW'].agg(['mean', 'count']).reset_index()
            month_noshow['mean'] = month_noshow['mean'] * 100
            
            fig = go.Figure(data=[
                go.Bar(
                    x=month_noshow['MES'],
                    y=month_noshow['mean'],
                    marker=dict(color='#e67e22'),
                    text=month_noshow['mean'].round(1),
                    textposition='auto',
                    texttemplate='%{text}%'
                )
            ])
            fig.update_layout(
                title="Taxa de No-Show por Mês",
                xaxis_title="Mês",
                yaxis_title="Taxa de No-Show (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)

# ============================================================
# SEÇÃO 5: INSIGHTS E RECOMENDAÇÕES
# ============================================================

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
        2. 📞 Criar protocolo de confirmação telefônica para serviços de alto valor
        3. 🎯 Focar estratégias de retenção em faixas etárias problemáticas
        4. 💰 Revisar política de cancelamento e taxas de no-show
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
        <p>🏥 <b>Beira-Mar Analytics Dashboard</b> | Desenvolvido com ❤️ usando Streamlit</p>
        <p>📊 Última atualização: {}</p>
</div>
""".format(datetime.now().strftime('%d/%m/%Y %H:%M')), unsafe_allow_html=True)