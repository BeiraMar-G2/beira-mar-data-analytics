#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime, timedelta
from calendar import monthrange  # <--- ESTA LINHA ESTAVA FALTANDO
import textwrap
import locale
import os
import warnings
import random # <--- Movido para o topo para garantir que funcione

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURAÇÕES DE CORES E ESTILO
# ============================================================================

CORES = {
    'turquesa': '#40E0D0',
    'turquesa_escuro': '#20B2AA',
    'rosa': '#FF69B4',
    'rosa_escuro': '#FF1493',
    'verde': '#2ECC71',
    'vermelho': '#E74C3C',
    'amarelo': '#F39C12',
    'cinza': '#95A5A6',
    'cinza_escuro': '#34495E',
    'branco': '#FFFFFF',
    'preto': '#2C3E50',
    'fundo_card': '#F8F9FA', 
    'grid': '#E6E6E6'
}

plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = 'white'
plt.rcParams['axes.edgecolor'] = '#DDDDDD'
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.5
plt.rcParams['grid.color'] = CORES['grid']
plt.rcParams['grid.linestyle'] = '--'

A4_SIZE = (11.69, 8.27)

# Tentar configurar locale
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except:
        pass

# ============================================================================
# CONFIGURAÇÕES DO BANCO DE DADOS
# ============================================================================

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Lqsym@2020',
    'database': 'BeiraMar'
}

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def formatar_moeda(valor):
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def formatar_percentual(valor):
    return f"{valor:.1f}%"

def get_nome_mes(mes):
    meses = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    return meses.get(mes, '')

def get_dia_semana(dia):
    dias = {
        0: 'Segunda', 1: 'Terça', 2: 'Quarta', 3: 'Quinta',
        4: 'Sexta', 5: 'Sábado', 6: 'Domingo'
    }
    return dias.get(dia, '')

def limpar_estilo_grafico(ax):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#DDDDDD')
    ax.spines['bottom'].set_color('#DDDDDD')
    ax.tick_params(colors='#666666')
    ax.yaxis.label.set_color('#444444')
    ax.xaxis.label.set_color('#444444')
    ax.title.set_color('#333333')

def criar_kpi_card(ax, titulo, valor, subtitulo="", cor=CORES['turquesa']):
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    card = mpatches.FancyBboxPatch(
        (0.05, 0.05), 0.90, 0.90,
        boxstyle="round,pad=0.02,rounding_size=0.05",
        facecolor=CORES['fundo_card'],
        edgecolor='#EEEEEE',
        linewidth=1
    )
    ax.add_patch(card)
    
    barra = mpatches.Rectangle(
        (0.05, 0.05), 0.02, 0.90,
        facecolor=cor,
        edgecolor='none'
    )
    ax.add_patch(barra)
    
    ax.text(0.12, 0.80, titulo.upper(), ha='left', va='center', fontsize=9, 
            fontweight='bold', color=CORES['cinza'])
    
    ax.text(0.12, 0.50, str(valor), ha='left', va='center', fontsize=20, 
            fontweight='bold', color=CORES['cinza_escuro'])
            
    if subtitulo:
        ax.text(0.12, 0.25, subtitulo, ha='left', va='center', fontsize=8, 
                color=CORES['cinza'], style='italic')

# ============================================================================
# CLASSE PRINCIPAL DO RELATÓRIO
# ============================================================================

class RelatorioBIBeiraMar:
    
    def __init__(self, ano=None, mes=None, usar_dados_mock=True):
        if ano is None or mes is None:
            hoje = datetime.now()
            if mes is None:
                mes = hoje.month - 1 if hoje.month > 1 else 12
            if ano is None:
                ano = hoje.year if hoje.month > 1 else hoje.year - 1
        
        self.ano = ano
        self.mes = mes
        self.usar_dados_mock = usar_dados_mock
        self.data_geracao = datetime.now()
        
        self.df_agendamentos = None
        self.df_servicos = None
        self.kpis = {}
    
    def conectar_banco(self):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            print(f"⚠️ Erro ao conectar ao banco: {e}")
            return None
    
    def carregar_dados(self):
        if self.usar_dados_mock:
            print("📊 Carregando dados mockados para demonstração...")
            self._carregar_dados_mock()
        else:
            print("🔌 Conectando ao banco de dados...")
            conn = self.conectar_banco()
            
            if conn is None:
                print("⚠️ Usando dados mockados como fallback...")
                self._carregar_dados_mock()
                return
            
            try:
                query_agendamentos = f"""
                SELECT 
                    a.id_agendamento, a.dt_hora, a.valor_pago, a.status,
                    s.id_servico, s.nome as servico_nome, s.preco as servico_preco, s.duracao as servico_duracao,
                    u.id_usuario as cliente_id, u.nome as cliente_nome
                FROM agendamento a
                JOIN servico s ON a.fk_servico = s.id_servico
                JOIN usuario u ON a.fk_cliente = u.id_usuario
                WHERE YEAR(a.dt_hora) = {self.ano} 
                    AND MONTH(a.dt_hora) = {self.mes}
                ORDER BY a.dt_hora
                """
                
                self.df_agendamentos = pd.read_sql(query_agendamentos, conn)
                query_servicos = "SELECT * FROM servico"
                self.df_servicos = pd.read_sql(query_servicos, conn)
                
                conn.close()
                print(f"✅ Dados carregados: {len(self.df_agendamentos)} agendamentos")
                
            except Exception as e:
                print(f"⚠️ Erro ao carregar dados: {e}")
                print("⚠️ Usando dados mockados como fallback...")
                self._carregar_dados_mock()
    
    def _carregar_dados_mock(self):
        self.df_servicos = pd.DataFrame({
            'id_servico': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            'nome': [
                'Massagem Modeladora', 'Drenagem Linfática', 'Hidrolipo NA',
                'Massagem Relaxante', 'Aplicação de Enzimas', 'Limpeza de Pele',
                'Design de Sobrancelhas com Henna', 'Design Simples de Sobrancelhas',
                'Depilação Facial', 'Detox Corporal', 'Pump Up Glúteos + Eletroestimulação'
            ],
            'preco': [90.00, 100.00, 180.00, 100.00, 180.00, 150.00, 45.00, 30.00, 35.00, 150.00, 90.00],
            'duracao': [40, 60, 150, 60, 60, 120, 90, 30, 30, 120, 60]
        })
        
        # Gerando dados fictícios consistentes
        agendamentos_data = []
        import random
        random.seed(42)
        start_date = datetime(self.ano, self.mes, 1)
        days_in_month = monthrange(self.ano, self.mes)[1]
        
        for _ in range(150):
            dia = random.randint(1, days_in_month)
            hora = random.randint(8, 19)
            dt = start_date.replace(day=dia, hour=hora)
            if dt.weekday() == 6: continue # Pula domingos
            
            servico = self.df_servicos.sample(1).iloc[0]
            status = random.choices(['Concluido', 'Cancelado'], weights=[0.85, 0.15])[0]
            
            agendamentos_data.append({
                'id_agendamento': len(agendamentos_data)+1,
                'dt_hora': dt,
                'valor_pago': servico['preco'] if status == 'Concluido' else 0,
                'status': status,
                'id_servico': servico['id_servico'],
                'servico_nome': servico['nome'],
                'servico_preco': servico['preco'],
                'servico_duracao': servico['duracao']
            })
        
        self.df_agendamentos = pd.DataFrame(agendamentos_data)
        print(f"✅ Dados mockados carregados: {len(self.df_agendamentos)} agendamentos")
    
    def calcular_kpis(self):
        df = self.df_agendamentos.copy()
        
        self.kpis['total_agendamentos'] = len(df)
        self.kpis['concluidos'] = len(df[df['status'] == 'Concluido'])
        self.kpis['cancelados'] = len(df[df['status'] == 'Cancelado'])
        
        if self.kpis['total_agendamentos'] > 0:
            self.kpis['taxa_comparecimento'] = (self.kpis['concluidos'] / self.kpis['total_agendamentos']) * 100
            self.kpis['taxa_cancelamento'] = (self.kpis['cancelados'] / self.kpis['total_agendamentos']) * 100
        else:
            self.kpis['taxa_comparecimento'] = 0
            self.kpis['taxa_cancelamento'] = 0
        
        self.kpis['faturamento'] = df[df['status'] == 'Concluido']['valor_pago'].sum()
        self.kpis['ticket_medio'] = self.kpis['faturamento'] / self.kpis['concluidos'] if self.kpis['concluidos'] > 0 else 0
        
        # Cálculo de perda estimada (usando preço do serviço para cancelados)
        # Assumindo que valor_pago é 0 para cancelados, pegamos o preço da tabela
        mask_cancel = df['status'] == 'Cancelado'
        self.kpis['perda_cancelamentos'] = df[mask_cancel]['servico_preco'].sum() if 'servico_preco' in df.columns else 0
        
        df['dia_semana'] = df['dt_hora'].dt.dayofweek
        dias_unicos = df['dt_hora'].dt.date.nunique()
        self.kpis['dias_trabalhados'] = dias_unicos
        self.kpis['media_dia'] = self.kpis['faturamento'] / dias_unicos if dias_unicos > 0 else 0
        self.kpis['atendimentos_por_dia'] = self.kpis['concluidos'] / dias_unicos if dias_unicos > 0 else 0
        
        self.kpis['por_dia_semana'] = df.groupby('dia_semana').agg({
            'id_agendamento': 'count',
            'status': lambda x: (x == 'Cancelado').sum()
        }).rename(columns={'id_agendamento': 'total', 'status': 'cancelados'})
        
        self.kpis['por_dia_semana']['taxa_cancelamento'] = (
            self.kpis['por_dia_semana']['cancelados'] / self.kpis['por_dia_semana']['total'] * 100
        )
        
        self.kpis['por_servico'] = df.groupby('servico_nome').agg({
            'id_agendamento': 'count',
            'status': lambda x: (x == 'Cancelado').sum(),
            'valor_pago': lambda x: x[df.loc[x.index, 'status'] == 'Concluido'].sum()
        }).rename(columns={'id_agendamento': 'total', 'status': 'cancelados', 'valor_pago': 'faturamento'})
        
        self.kpis['por_servico']['taxa_cancelamento'] = (
            self.kpis['por_servico']['cancelados'] / self.kpis['por_servico']['total'] * 100
        ).fillna(0)
        
        df['semana'] = df['dt_hora'].dt.isocalendar().week
        self.kpis['por_semana'] = df.groupby('semana').agg({
            'id_agendamento': 'count',
            'valor_pago': lambda x: x[df.loc[x.index, 'status'] == 'Concluido'].sum()
        }).rename(columns={'id_agendamento': 'total', 'valor_pago': 'faturamento'})
        
        df['hora'] = df['dt_hora'].dt.hour
        self.kpis['por_hora'] = df.groupby('hora')['id_agendamento'].count()
    
    def gerar_pagina_capa(self, pdf):
        fig = plt.figure(figsize=A4_SIZE)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.axis('off')
        
        faixa_lat = mpatches.Rectangle((0, 0), 0.03, 1, facecolor=CORES['turquesa'], edgecolor='none')
        ax.add_patch(faixa_lat)
        
        ax.text(0.1, 0.70, 'Relatório Mensal\nde Performance', ha='left', va='center', fontsize=40, fontweight='bold', color=CORES['preto'])
        ax.text(0.1, 0.82, 'CLÍNICA BEIRA-MAR', ha='left', va='center', fontsize=14, color=CORES['cinza'])
        
        nome_mes = get_nome_mes(self.mes)
        ax.text(0.1, 0.55, f'{nome_mes} {self.ano}', ha='left', va='center', fontsize=24, color=CORES['turquesa_escuro'])
        
        resumo_texto = f"Neste mês registramos um faturamento de {formatar_moeda(self.kpis['faturamento'])} " \
                       f"com {self.kpis['total_agendamentos']} agendamentos.\n" \
                       f"A taxa de comparecimento foi de {formatar_percentual(self.kpis['taxa_comparecimento'])}."
        ax.text(0.1, 0.40, resumo_texto, ha='left', va='top', fontsize=12, color=CORES['cinza_escuro'], wrap=True)

        ax.text(0.95, 0.05, f"Gerado em {self.data_geracao.strftime('%d/%m/%Y')}", ha='right', va='center', fontsize=9, color=CORES['cinza'])
        
        pdf.savefig(fig)
        plt.close(fig)
    
    def gerar_pagina_kpis(self, pdf):
        fig = plt.figure(figsize=A4_SIZE)
        fig.suptitle('Indicadores Principais', fontsize=20, fontweight='bold', color=CORES['cinza_escuro'], x=0.05, ha='left', y=0.95)
        
        gs = fig.add_gridspec(2, 4, hspace=0.3, wspace=0.2, left=0.05, right=0.95, top=0.85, bottom=0.1)
        
        kpi_data = [
            ('Total Agendamentos', str(self.kpis['total_agendamentos']), '', CORES['turquesa']),
            ('Atendimentos', str(self.kpis['concluidos']), f"{formatar_percentual(self.kpis['taxa_comparecimento'])} realizado", CORES['verde']),
            ('Cancelamentos', str(self.kpis['cancelados']), f"{formatar_percentual(self.kpis['taxa_cancelamento'])} taxa", CORES['vermelho']),
            ('Dias Úteis', str(self.kpis['dias_trabalhados']), '', CORES['rosa']),
            ('Faturamento', formatar_moeda(self.kpis['faturamento']), '', CORES['turquesa_escuro']),
            ('Ticket Médio', formatar_moeda(self.kpis['ticket_medio']), 'por cliente', CORES['turquesa']),
            ('Média Diária', formatar_moeda(self.kpis['media_dia']), 'receita/dia', CORES['rosa']),
            ('Perda Estimada', formatar_moeda(self.kpis['perda_cancelamentos']), 'por faltas', CORES['vermelho']),
        ]
        
        for idx, (titulo, valor, subtitulo, cor) in enumerate(kpi_data):
            row = idx // 4
            col = idx % 4
            ax = fig.add_subplot(gs[row, col])
            criar_kpi_card(ax, titulo, valor, subtitulo, cor)
        
        pdf.savefig(fig)
        plt.close(fig)
    
    def gerar_pagina_servicos(self, pdf):
        fig = plt.figure(figsize=A4_SIZE)
        fig.suptitle('Performance por Serviços', fontsize=20, fontweight='bold', color=CORES['cinza_escuro'], x=0.05, ha='left', y=0.95)
        
        # --- CORREÇÃO 1: Ajustei o 'left' de 0.08 para 0.28 para caber os labels ---
        gs = fig.add_gridspec(2, 2, hspace=0.4, wspace=0.3, left=0.28, right=0.95, top=0.85, bottom=0.08)
        
        df_servicos = self.kpis['por_servico'].sort_values('total', ascending=True)
        
        # Gráfico 1 - Volume
        ax1 = fig.add_subplot(gs[0, 0])
        limpar_estilo_grafico(ax1)
        top_servicos = df_servicos.tail(10)
        cores_barras = [CORES['turquesa'] if i < 7 else CORES['turquesa_escuro'] for i in range(len(top_servicos))]
        bars = ax1.barh(range(len(top_servicos)), top_servicos['total'], color=cores_barras, alpha=0.8)
        ax1.set_yticks(range(len(top_servicos)))
        # Labels completos (sem truncar muito pois aumentamos a margem)
        ax1.set_yticklabels(top_servicos.index, fontsize=8) 
        ax1.set_title('Volume de Agendamentos', fontweight='bold', pad=10, loc='left')
        for bar, val in zip(bars, top_servicos['total']):
            ax1.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2, str(int(val)), va='center', fontsize=8)
        
        # Gráfico 2 - Receita
        ax2 = fig.add_subplot(gs[0, 1])
        limpar_estilo_grafico(ax2)
        top_fat = df_servicos.sort_values('faturamento', ascending=True).tail(10)
        cores_fat = [CORES['verde'] for _ in range(len(top_fat))]
        bars2 = ax2.barh(range(len(top_fat)), top_fat['faturamento'], color=cores_fat, alpha=0.7)
        ax2.set_yticks(range(len(top_fat)))
        # Labels ocultos no segundo gráfico se quiser economizar espaço, ou manter
        ax2.set_yticklabels([]) 
        ax2.set_title('Receita Gerada (Top 10)', fontweight='bold', pad=10, loc='left')
        for bar, val in zip(bars2, top_fat['faturamento']):
            ax2.text(bar.get_width() * 1.05, bar.get_y() + bar.get_height()/2, formatar_moeda(val), va='center', fontsize=7)
        
        # Gráfico 3 - Cancelamento (CORREÇÃO 2: Labels agora cabem na margem ajustada do GridSpec)
        ax3 = fig.add_subplot(gs[1, 0])
        limpar_estilo_grafico(ax3)
        df_cancel = df_servicos[df_servicos['cancelados'] > 0].sort_values('taxa_cancelamento', ascending=True)
        # Pegar apenas os top 10 para não lotar se houver muitos
        if len(df_cancel) > 10: df_cancel = df_cancel.tail(10)
            
        cores_cancel = [CORES['amarelo'] if v < 20 else CORES['vermelho'] for v in df_cancel['taxa_cancelamento']]
        bars3 = ax3.barh(range(len(df_cancel)), df_cancel['taxa_cancelamento'], color=cores_cancel, alpha=0.7)
        ax3.set_yticks(range(len(df_cancel)))
        ax3.set_yticklabels(df_cancel.index, fontsize=8)
        ax3.set_title('Taxa de Cancelamento (%)', fontweight='bold', pad=10, loc='left')
        for bar, val in zip(bars3, df_cancel['taxa_cancelamento']):
            ax3.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, f'{val:.1f}%', va='center', fontsize=8)
        
        # Gráfico 4 - Share
        ax4 = fig.add_subplot(gs[1, 1])
        top5_fat = df_servicos.nlargest(5, 'faturamento')
        outros = df_servicos['faturamento'].sum() - top5_fat['faturamento'].sum()
        if outros < 0:
            outros = 0
        labels = list(top5_fat.index) + ['Outros']
        sizes = list(top5_fat['faturamento']) + [outros]

        if sum(sizes) == 0:
            sizes = [1]

        cores_pizza = [CORES['turquesa'], CORES['rosa'], CORES['verde'], CORES['amarelo'], CORES['turquesa_escuro'], CORES['cinza']]
        wedges, texts, autotexts = ax4.pie(sizes, labels=None, autopct='%1.1f%%', colors=cores_pizza[:len(labels)], startangle=90, pctdistance=0.85)
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        ax4.add_artist(centre_circle)
        ax4.set_title('Share de Receita', fontweight='bold', pad=10, loc='left')

        ax4.legend([textwrap.fill(l, width=12) for l in labels], loc='center left', bbox_to_anchor=(1.0, 0.0, 0.5, 1), fontsize=6, frameon=False, labelspacing=1.2)
        plt.setp(autotexts, size=6, weight='bold', color='white')
        
        pdf.savefig(fig)
        plt.close(fig)
    
    def gerar_pagina_temporal(self, pdf):
        fig = plt.figure(figsize=A4_SIZE)
        fig.suptitle('Análise Temporal', fontsize=20, fontweight='bold', color=CORES['cinza_escuro'], x=0.05, ha='left', y=0.95)
        
        gs = fig.add_gridspec(2, 2, hspace=0.4, wspace=0.3, left=0.08, right=0.95, top=0.85, bottom=0.08)
        
        # Gráfico 1
        ax1 = fig.add_subplot(gs[0, 0])
        limpar_estilo_grafico(ax1)
        dias_nomes = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb']
        df_dias = self.kpis['por_dia_semana'].reindex(range(6), fill_value=0)
        x = range(len(dias_nomes))
        width = 0.35
        # Tratar NaN
        df_dias = df_dias.fillna(0)
        
        ax1.bar([i - width/2 for i in x], df_dias['total'] - df_dias['cancelados'], width, label='Realizados', color=CORES['verde'], alpha=0.8)
        ax1.bar([i + width/2 for i in x], df_dias['cancelados'], width, label='Cancelados', color=CORES['vermelho'], alpha=0.8)
        ax1.set_xticks(x)
        ax1.set_xticklabels(dias_nomes)
        ax1.set_title('Volume Semanal', fontweight='bold', pad=10, loc='left')
        ax1.legend(loc='upper right', fontsize=8, frameon=False)
        
        # Gráfico 2
        ax2 = fig.add_subplot(gs[0, 1])
        limpar_estilo_grafico(ax2)
        taxas = df_dias['taxa_cancelamento'].values
        cores_taxa = [CORES['verde'] if t < 15 else (CORES['amarelo'] if t < 25 else CORES['vermelho']) for t in taxas]
        ax2.bar(dias_nomes, taxas, color=cores_taxa, alpha=0.8)
        ax2.set_title('Cancelamento por Dia (%)', fontweight='bold', pad=10, loc='left')
        
        # Gráfico 3
        ax3 = fig.add_subplot(gs[1, 0])
        limpar_estilo_grafico(ax3)
        df_semana = self.kpis['por_semana']
        semanas = [f'S{i}' for i in df_semana.index]
        ax3.fill_between(semanas, df_semana['faturamento'], alpha=0.2, color=CORES['turquesa'])
        ax3.plot(semanas, df_semana['faturamento'], 'o-', color=CORES['turquesa'], linewidth=2)
        ax3.set_title('Evolução do Faturamento', fontweight='bold', pad=10, loc='left')
        
        # Gráfico 4
        ax4 = fig.add_subplot(gs[1, 1])
        limpar_estilo_grafico(ax4)
        horas = self.kpis['por_hora']
        if not horas.empty:
            horas_labels = [f'{h}h' for h in horas.index]
            ax4.bar(horas_labels, horas.values, color=CORES['rosa'], alpha=0.8)
        ax4.set_title('Horários de Pico', fontweight='bold', pad=10, loc='left')
        
        pdf.savefig(fig)
        plt.close(fig)
    
    def gerar_pagina_comparativo(self, pdf):
        fig = plt.figure(figsize=A4_SIZE)
        fig.suptitle('Dashboard de Metas', fontsize=20, fontweight='bold', color=CORES['cinza_escuro'], x=0.05, ha='left', y=0.95)
        
        gs = fig.add_gridspec(2, 3, hspace=0.35, wspace=0.3, left=0.08, right=0.95, top=0.85, bottom=0.08)
        
        # CORREÇÃO 3: Títulos dos gauges
        ax1 = fig.add_subplot(gs[0, 0], projection='polar')
        self._criar_gauge(ax1, self.kpis['taxa_comparecimento'], 'Taxa de Comparecimento', meta=85)
        
        ax2 = fig.add_subplot(gs[0, 1], projection='polar')
        ocupacao = min((self.kpis['atendimentos_por_dia'] / 8) * 100, 100)
        self._criar_gauge(ax2, ocupacao, 'Taxa de Ocupação', meta=75)
        
        ax3 = fig.add_subplot(gs[0, 2], projection='polar')
        meta_faturamento = 12000 # Exemplo
        perc_meta = (self.kpis['faturamento'] / meta_faturamento) * 100
        self._criar_gauge(ax3, min(perc_meta, 150), 'Meta de Faturamento', meta=100)
        
        # Acumulado
        ax4 = fig.add_subplot(gs[1, :2])
        limpar_estilo_grafico(ax4)
        df = self.df_agendamentos.copy()
        df['data'] = df['dt_hora'].dt.date
        evolucao = df[df['status'] == 'Concluido'].groupby('data')['valor_pago'].sum()
        if not evolucao.empty:
            evolucao_acum = evolucao.cumsum()
            ax4.fill_between(range(len(evolucao_acum)), evolucao_acum.values, alpha=0.2, color=CORES['turquesa'])
            ax4.plot(range(len(evolucao_acum)), evolucao_acum.values, 'o-', color=CORES['turquesa'], linewidth=2)
            ax4.set_title('Acumulado do Mês', fontweight='bold', pad=10, loc='left')
        
        # Tabela
        ax5 = fig.add_subplot(gs[1, 2])
        ax5.axis('off')
        tabela_data = [
            ['Métrica', 'Valor'],
            ['Total Agendamentos', str(self.kpis['total_agendamentos'])],
            ['Realizados', str(self.kpis['concluidos'])],
            ['Cancelados', str(self.kpis['cancelados'])],
            ['Faturamento', formatar_moeda(self.kpis['faturamento'])],
            ['Ticket Médio', formatar_moeda(self.kpis['ticket_medio'])],
            ['Dias Trabalhados', str(self.kpis['dias_trabalhados'])],
        ]
        table = ax5.table(cellText=tabela_data[1:], colLabels=tabela_data[0], loc='center', cellLoc='center', colColours=[CORES['fundo_card'], CORES['fundo_card']])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.2, 1.8)
        ax5.set_title('Resumo Numérico', fontweight='bold', pad=20)
        
        pdf.savefig(fig)
        plt.close(fig)
    
    def _criar_gauge(self, ax, valor, titulo, meta=80):
        """Cria um gráfico de gauge - CORRIGIDO PARA MOSTRAR TÍTULO"""
        ax.set_theta_zero_location('N')
        ax.set_theta_direction(-1)
        ax.set_thetamin(0)
        ax.set_thetamax(180)
        theta_bg = np.linspace(0, np.pi, 100)
        ax.fill_between(theta_bg, 0.6, 1, color='#F0F0F0')
        valor_rad = np.pi * (min(valor, 100)/100)
        zona_valor = np.linspace(0, valor_rad, 50)
        
        cor_barra = CORES['turquesa']
        if valor < meta * 0.8: cor_barra = CORES['vermelho']
        elif valor < meta: cor_barra = CORES['amarelo']
        else: cor_barra = CORES['verde']
        
        ax.fill_between(zona_valor, 0.6, 1, color=cor_barra, alpha=0.8)
        
        # Texto central
        ax.text(np.pi/2, 0.2, f'{valor:.1f}%', ha='center', va='center', fontsize=16, fontweight='bold', color=CORES['cinza_escuro'])
        
        # CORREÇÃO 3: Título posicionado relativo aos eixos (transAxes) e não polar
        # 0.5 é o centro horizontal, -0.15 é abaixo do gráfico
        ax.text(0.5, -0.15, titulo, transform=ax.transAxes, ha='center', va='center', fontsize=10, fontweight='bold', color=CORES['cinza'])
        
        ax.set_rticks([])
        ax.set_xticks([])
        ax.spines['polar'].set_visible(False)
    
    def gerar_pagina_insights(self, pdf):
        """Gera a página de insights - CORRIGIDO PROBLEMAS DE LAYOUT E EMOJIS"""
        
        fig = plt.figure(figsize=A4_SIZE)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.axis('off')
        
        # Título
        ax.text(0.1, 0.90, 'Insights e Recomendações', 
                ha='left', va='center', fontsize=24, fontweight='bold', 
                color=CORES['cinza_escuro'])
        
        df_dias = self.kpis['por_dia_semana']
        melhor_dia = df_dias['taxa_cancelamento'].idxmin() if not df_dias.empty else 0
        pior_dia = df_dias['taxa_cancelamento'].idxmax() if not df_dias.empty else 0
        
        df_servicos = self.kpis['por_servico']
        if not df_servicos.empty:
            servico_mais_cancel = df_servicos['taxa_cancelamento'].idxmax()
            servico_menos_cancel = df_servicos['taxa_cancelamento'].idxmin()
            servico_mais_fat = df_servicos['faturamento'].idxmax()
        else:
            servico_mais_cancel = "N/A"
            servico_menos_cancel = "N/A"
            servico_mais_fat = "N/A"

        # CORREÇÃO 4: Substituindo Emojis por Cores para evitar quadrado branco
        insights = [
            {
                'cor_icone': CORES['verde'],
                'titulo': 'Melhor Dia para Agendamentos',
                'texto': f'{get_dia_semana(melhor_dia)} é o dia com menor taxa de cancelamento. Priorize agendamentos importantes neste dia.'
            },
            {
                'cor_icone': CORES['vermelho'],
                'titulo': 'Atenção: Dias de Alta Evasão',
                'texto': f'{get_dia_semana(pior_dia)} apresenta a maior taxa de cancelamento. Considere estratégias de confirmação reforçada.'
            },
            {
                'cor_icone': CORES['turquesa'],
                'titulo': 'Serviço Mais Rentável',
                'texto': f'{servico_mais_fat} foi o serviço que mais faturou no mês. Considere pacotes promocionais.'
            },
            {
                'cor_icone': CORES['amarelo'],
                'titulo': 'Oportunidade de Melhoria',
                'texto': f'{servico_mais_cancel} tem a maior taxa de cancelamento. Avalie política de confirmação específica.'
            },
            {
                'cor_icone': CORES['turquesa_escuro'],
                'titulo': 'Destaque Positivo',
                'texto': f'{servico_menos_cancel} tem a menor taxa de cancelamento. Clientes deste serviço são mais comprometidos.'
            },
            {
                'cor_icone': CORES['rosa'],
                'titulo': 'Análise Financeira',
                'texto': f'O faturamento médio diário foi de {formatar_moeda(self.kpis["media_dia"])}. Potencial perdido com cancelamentos: {formatar_moeda(self.kpis["perda_cancelamentos"])}.'
            },
        ]
        
        y_pos = 0.75
        
        for insight in insights:
            # Desenhar "Bullet point" como círculo (substitui o emoji quadrado)
            circle = mpatches.Circle((0.11, y_pos), 0.008, color=insight['cor_icone'], transform=ax.transData)
            ax.add_patch(circle)
            
            # Título
            ax.text(0.14, y_pos, insight['titulo'], fontsize=11, 
                    fontweight='bold', color=CORES['cinza_escuro'], va='center')
            
            # Texto com quebra de linha manual (textwrap) para evitar que suba ou desalinhe
            texto_formatado = textwrap.fill(insight['texto'], width=90)
            
            # Ajuste fino da posição do texto abaixo do título
            ax.text(0.14, y_pos - 0.03, texto_formatado, fontsize=10, 
                    color=CORES['cinza'], va='top')
            
            # Linha divisória
            ax.plot([0.1, 0.9], [y_pos - 0.09, y_pos - 0.09], color='#EEEEEE', lw=1)
            
            y_pos -= 0.12 # Espaçamento fixo
        
        ax.text(0.5, 0.05, f"Relatório de BI - {get_nome_mes(self.mes)} {self.ano} | Clínica Beira-Mar", 
                ha='center', va='center', fontsize=9, color=CORES['cinza'])
        
        pdf.savefig(fig)
        plt.close(fig)

    def gerar_relatorio(self, caminho_saida=None):
        print("\n" + "="*60)
        print("      GERAÇÃO DO RELATÓRIO DE BI - BEIRA-MAR")
        print("="*60)
        self.carregar_dados()
        self.calcular_kpis()
        if caminho_saida is None:
            nome_mes = get_nome_mes(self.mes).lower()
            caminho_saida = f"relatorio_bi_{nome_mes}_{self.ano}.pdf"
        print(f"\n📄 Gerando PDF: {caminho_saida}")
        with PdfPages(caminho_saida) as pdf:
            print("  → Gerando capa...")
            self.gerar_pagina_capa(pdf)
            print("  → Gerando página de KPIs...")
            self.gerar_pagina_kpis(pdf)
            print("  → Gerando análise de serviços...")
            self.gerar_pagina_servicos(pdf)
            print("  → Gerando análise temporal...")
            self.gerar_pagina_temporal(pdf)
            print("  → Gerando dashboard de performance...")
            self.gerar_pagina_comparativo(pdf)
            print("  → Gerando insights e recomendações...")
            self.gerar_pagina_insights(pdf)
        print(f"\n✅ Relatório gerado com sucesso!")
        print(f"📍 Arquivo: {os.path.abspath(caminho_saida)}")
        print("="*60 + "\n")
        return caminho_saida

if __name__ == "__main__":
    relatorio = RelatorioBIBeiraMar(ano=2025, mes=11, usar_dados_mock=True)
    caminho = relatorio.gerar_relatorio()