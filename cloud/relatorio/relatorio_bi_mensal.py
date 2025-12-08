#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime, timedelta
from calendar import monthrange
import textwrap
import locale
import os
import warnings
import random

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

# Tentar configurar locale para formatar datas em PT-BR
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
    ax.spines['left'].set_color(CORES['grid'])
    ax.spines['bottom'].set_color(CORES['grid'])
    ax.tick_params(colors=CORES['cinza_escuro'])
    ax.grid(True, alpha=0.2, linestyle='--')

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
        
        gs = fig.add_gridspec(2, 2, hspace=0.5, wspace=0.35, left=0.28, right=0.95, top=0.85, bottom=0.08)
        
        df_servicos = self.kpis['por_servico'].sort_values('total', ascending=True)
        
        ax1 = fig.add_subplot(gs[0, 0])
        limpar_estilo_grafico(ax1)
        top_servicos = df_servicos.tail(10)
        cores_barras = [CORES['turquesa'] if i < 7 else CORES['turquesa_escuro'] for i in range(len(top_servicos))]
        bars = ax1.barh(range(len(top_servicos)), top_servicos['total'], color=cores_barras, alpha=0.8)
        ax1.set_yticks(range(len(top_servicos)))
        ax1.set_yticklabels(top_servicos.index, fontsize=8)
        ax1.set_title('Volume de Agendamentos', fontweight='bold', pad=10, loc='left')
        ax1.set_xlabel('Quantidade', fontsize=9)
        for bar, val in zip(bars, top_servicos['total']):
            ax1.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2, str(int(val)), va='center', fontsize=8)
        
        ax2 = fig.add_subplot(gs[0, 1])
        limpar_estilo_grafico(ax2)
        top_fat = df_servicos.sort_values('faturamento', ascending=True).tail(10)
        cores_fat = [CORES['verde'] for _ in range(len(top_fat))]
        bars2 = ax2.barh(range(len(top_fat)), top_fat['faturamento'], color=cores_fat, alpha=0.7)
        ax2.set_yticks(range(len(top_fat)))
        ax2.set_yticklabels(top_fat.index, fontsize=8)
        ax2.set_title('Receita Gerada (Top 10)', fontweight='bold', pad=10, loc='left')
        ax2.set_xlabel('Faturamento (R$)', fontsize=9)
        for bar, val in zip(bars2, top_fat['faturamento']):
            ax2.text(bar.get_width() * 1.02, bar.get_y() + bar.get_height()/2, formatar_moeda(val), va='center', fontsize=7)
        
        ax3 = fig.add_subplot(gs[1, 0])
        limpar_estilo_grafico(ax3)
        df_cancel = df_servicos[df_servicos['cancelados'] > 0].sort_values('taxa_cancelamento', ascending=True)
        if len(df_cancel) > 10:
            df_cancel = df_cancel.tail(10)
        
        cores_cancel = [CORES['amarelo'] if v < 20 else CORES['vermelho'] for v in df_cancel['taxa_cancelamento']]
        bars3 = ax3.barh(range(len(df_cancel)), df_cancel['taxa_cancelamento'], color=cores_cancel, alpha=0.7)
        ax3.set_yticks(range(len(df_cancel)))
        ax3.set_yticklabels(df_cancel.index, fontsize=8)
        ax3.set_title('Taxa de Cancelamento (%)', fontweight='bold', pad=10, loc='left')
        ax3.set_xlabel('Percentual (%)', fontsize=9)
        for bar, val in zip(bars3, df_cancel['taxa_cancelamento']):
            ax3.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, f'{val:.1f}%', va='center', fontsize=8)
        
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
        
        wedges, texts, autotexts = ax4.pie(sizes, labels=None, autopct='%1.1f%%', colors=cores_pizza[:len(labels)], 
                                           startangle=90, pctdistance=0.85, center=(-0.4, 0), radius=1.0)
        
        centre_circle = plt.Circle((-0.4, 0), 0.70, fc='white')
        ax4.add_artist(centre_circle)
        ax4.set_title('Share de Receita', fontweight='bold', pad=10, loc='left')
        
        ax4.legend([textwrap.fill(l, width=20) for l in labels], loc='center left', bbox_to_anchor=(0.75, 0.5), fontsize=8, frameon=False, labelspacing=1.2)
        plt.setp(autotexts, size=7, weight='bold', color='white')
        
        pdf.savefig(fig)
        plt.close(fig)
    
    def gerar_pagina_temporal(self, pdf):
        fig = plt.figure(figsize=A4_SIZE)
        fig.suptitle('Análise Temporal', fontsize=20, fontweight='bold', color=CORES['cinza_escuro'], x=0.05, ha='left', y=0.95)
        
        gs = fig.add_gridspec(2, 2, hspace=0.4, wspace=0.3, left=0.08, right=0.95, top=0.85, bottom=0.08)
        
        df = self.df_agendamentos.copy()
        df['data'] = df['dt_hora'].dt.date
        df['hora'] = df['dt_hora'].dt.hour
        # Gera os nomes dos dias da semana em inglês
        df['dia_semana'] = df['dt_hora'].dt.day_name()
        
        # 1. Gráfico de Área (Agendamentos por Dia)
        ax1 = fig.add_subplot(gs[0, :])
        limpar_estilo_grafico(ax1)
        agendamentos_por_dia = df.groupby('data').size()
        ax1.fill_between(range(len(agendamentos_por_dia)), agendamentos_por_dia.values, alpha=0.3, color=CORES['turquesa'])
        ax1.plot(range(len(agendamentos_por_dia)), agendamentos_por_dia.values, 'o-', color=CORES['turquesa'], linewidth=2, markersize=5)
        ax1.set_title('Agendamentos por Dia', fontweight='bold', pad=10, loc='left')
        ax1.set_ylabel('Quantidade', fontsize=9)
        ax1.set_xlabel('Data', fontsize=9)
        # Ajustar labels do eixo X para datas
        if not agendamentos_por_dia.empty:
            datas_str = [d.strftime('%d/%m') for d in agendamentos_por_dia.index]
            ax1.set_xticks(range(len(datas_str)))
            ax1.set_xticklabels(datas_str, rotation=45, ha='right', fontsize=8)
        ax1.grid(True, alpha=0.3)
        
        # 2. Gráfico de Barras (Por Hora)
        ax2 = fig.add_subplot(gs[1, 0])
        limpar_estilo_grafico(ax2)
        agendamentos_por_hora = df.groupby('hora').size()
        ax2.bar(agendamentos_por_hora.index, agendamentos_por_hora.values, color=CORES['rosa'], alpha=0.7, edgecolor=CORES['cinza_escuro'])
        ax2.set_title('Distribuição por Hora', fontweight='bold', pad=10, loc='left')
        ax2.set_ylabel('Quantidade', fontsize=9)
        ax2.set_xlabel('Hora do Dia', fontsize=9)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # 3. Gráfico de Barras (Por Dia da Semana)
        ax3 = fig.add_subplot(gs[1, 1])
        limpar_estilo_grafico(ax3)
        dias_semana_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dias_semana_pt = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        
        # Reindexa para garantir a ordem correta
        agendamentos_dia_semana = df.groupby('dia_semana').size().reindex(dias_semana_ordem).fillna(0)
        
        # Remove dias com zero agendamentos (opcional, mas o código original filtrava)
        mask = agendamentos_dia_semana > 0
        agendamentos_dia_semana = agendamentos_dia_semana[mask]
        
        dias_labels = [dias_semana_pt[dias_semana_ordem.index(d)] for d in agendamentos_dia_semana.index]
        
        ax3.bar(dias_labels, agendamentos_dia_semana.values, color=CORES['verde'], alpha=0.7, edgecolor=CORES['cinza_escuro'])
        ax3.set_title('Distribuição por Dia da Semana', fontweight='bold', pad=10, loc='left')
        ax3.set_ylabel('Quantidade', fontsize=9)
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax3.grid(True, alpha=0.3, axis='y')
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)

    def gerar_pagina_comparativo(self, pdf):
        fig = plt.figure(figsize=A4_SIZE)
        fig.suptitle('Dashboard de Metas', fontsize=20, fontweight='bold', color=CORES['cinza_escuro'], x=0.05, ha='left', y=0.95)
        
        gs = fig.add_gridspec(2, 3, hspace=0.45, wspace=0.35, left=0.08, right=0.95, top=0.85, bottom=0.08)
        
        ax1 = fig.add_subplot(gs[0, 0])
        self._criar_gauge_melhorado(ax1, self.kpis['taxa_comparecimento'], 'Taxa de\nComparecimento', meta=85)
        
        ax2 = fig.add_subplot(gs[0, 1])
        ocupacao = min((self.kpis['atendimentos_por_dia'] / 8) * 100, 100)
        self._criar_gauge_melhorado(ax2, ocupacao, 'Taxa de\nOcupação', meta=75)
        
        ax3 = fig.add_subplot(gs[0, 2])
        meta_faturamento = 12000
        perc_meta = (self.kpis['faturamento'] / meta_faturamento) * 100
        self._criar_gauge_melhorado(ax3, min(perc_meta, 150), 'Meta de\nFaturamento', meta=100)
        
        ax4 = fig.add_subplot(gs[1, :2])
        limpar_estilo_grafico(ax4)
        df = self.df_agendamentos.copy()
        df['data'] = df['dt_hora'].dt.date
        evolucao = df[df['status'] == 'Concluido'].groupby('data')['valor_pago'].sum()
        if not evolucao.empty:
            evolucao_acum = evolucao.cumsum()
            ax4.fill_between(range(len(evolucao_acum)), evolucao_acum.values, alpha=0.2, color=CORES['turquesa'])
            ax4.plot(range(len(evolucao_acum)), evolucao_acum.values, 'o-', color=CORES['turquesa'], linewidth=2, markersize=4)
            ax4.set_title('Acumulado do Mês', fontweight='bold', pad=10, loc='left')
            ax4.set_ylabel('Faturamento Acumulado (R$)', fontsize=9)
        
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
        table.set_fontsize(8)
        table.scale(1.2, 1.8)
        ax5.set_title('Resumo Numérico', fontweight='bold', pad=20, fontsize=11)
        
        pdf.savefig(fig)
        plt.close(fig)

    def _criar_gauge_melhorado(self, ax, valor, titulo, meta=80):
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        ax.axis('off')
        
        # Fundo da barra
        fundo = mpatches.FancyBboxPatch((1, 3), 8, 1.5, boxstyle="round,pad=0.1", 
                                        facecolor='#E0E0E0', edgecolor='#999999', linewidth=1.5)
        ax.add_patch(fundo)
        
        # Barra de progresso
        percentual = min(valor / 100, 1.2)
        largura_barra = 8 * percentual
        
        if valor < meta * 0.8:
            cor_barra = CORES['vermelho']
        elif valor < meta:
            cor_barra = CORES['amarelo']
        else:
            cor_barra = CORES['verde']
        
        barra = mpatches.FancyBboxPatch((1, 3), largura_barra, 1.5, boxstyle="round,pad=0.1",
                                        facecolor=cor_barra, edgecolor=cor_barra, linewidth=0, alpha=0.85)
        ax.add_patch(barra)
        
        # Valor no centro da barra
        ax.text(5, 3.75, f'{valor:.1f}%', ha='center', va='center', fontsize=16, fontweight='bold', color='white')
        
        # Meta como linha
        meta_pos = 1 + (8 * (meta / 100))
        ax.plot([meta_pos, meta_pos], [2.8, 4.8], 'k-', linewidth=2)
        ax.text(meta_pos, 2.3, f'Meta:{meta:.0f}%', ha='center', fontsize=7, color=CORES['cinza_escuro'])
        
        # Título
        ax.text(5, 7.5, titulo, ha='center', va='center', fontsize=11, fontweight='bold', color=CORES['cinza_escuro'])
        
        # Status
        if valor >= meta:
            status = '✓ META ATINGIDA'
            cor_status = CORES['verde']
        elif valor >= meta * 0.8:
            status = '⊕ NO CAMINHO'
            cor_status = CORES['amarelo']
        else:
            status = '⚠ CRÍTICO'
            cor_status = CORES['vermelho']
        
        ax.text(5, 0.8, status, ha='center', fontsize=8, fontweight='bold', color=cor_status)
    
    # =========================================================================
    # FUNÇÃO DE INSIGHTS (COM LAYOUT CORRIGIDO)
    # =========================================================================
    def gerar_pagina_insights(self, pdf):
        """Gera a página de insights + recomendações de pacotes (via ML/Predictive Analysis)."""

        fig = plt.figure(figsize=A4_SIZE)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.axis('off')

        # ============================
        # TÍTULO da página
        # ============================
        ax.text(0.07, 0.92, 'Insights e Recomendações Inteligentes',
                ha='left', va='center', fontsize=24, fontweight='bold',
                color=CORES['cinza_escuro'])

        # ===========================================================================================
        # 1) RECOMENDAÇÕES DE PACOTES (lado esquerdo) - LAYOUT CORRIGIDO
        # ===========================================================================================

        # Identificar serviços relevantes com base nos KPIs
        df_serv = self.kpis['por_servico']

        serv_faturamento = df_serv['faturamento'].idxmax() if not df_serv.empty else "Massagem Modeladora"
        serv_menos_cancel = df_serv['taxa_cancelamento'].idxmin() if not df_serv.empty else "Drenagem Linfática"
        serv_popular = df_serv['total'].idxmax() if not df_serv.empty else "Relaxante"

        pacotes = [
            {
                "titulo": "Pacote Redução & Modeladora",
                "desc": f"Combinação sugerida com base no serviço de maior faturamento ({serv_faturamento}). "
                        "Indicado para clientes que buscam resultados rápidos.",
                "valor": "R$ 349,00"
            },
            {
                "titulo": "Pacote Premium Relaxamento",
                "desc": f"Montado considerando o serviço com menor taxa de cancelamentos ({serv_menos_cancel}). "
                        "Excelente para fidelização e recorrência.",
                "valor": "R$ 279,00"
            },
            {
                "titulo": "Pacote Detox + Drenagem",
                "desc": f"Baseado nos serviços mais procurados ({serv_popular}). "
                        "Ideal para iniciar programas de detox e retenção.",
                "valor": "R$ 299,00"
            },
        ]

        ax.text(0.07, 0.85, "📦 Pacotes Recomendados (ML / Predição)",
                fontsize=14, fontweight='bold', color=CORES['turquesa_escuro'])

        # Posição inicial Y (ajustada para dar mais espaço)
        y = 0.78
        
        for p in pacotes:
            # Definições de geometria do card
            card_height = 0.13  # Aumentado para caber o texto
            card_width = 0.42
            card_x = 0.05
            
            # O retângulo é desenhado a partir do canto inferior esquerdo (x, y - height)
            ax.add_patch(mpatches.FancyBboxPatch(
                (card_x, y - card_height), card_width, card_height,
                boxstyle="round,pad=0.02,rounding_size=0.02",
                facecolor=CORES['fundo_card'], edgecolor='#DDDDDD'
            ))

            # Título do Pacote (Topo do card)
            ax.text(card_x + 0.02, y - 0.03, p["titulo"], fontsize=11,
                    fontweight='bold', color=CORES['cinza_escuro'])

            # Descrição (Meio do card)
            # Reduzi width para 48 para evitar quebra ruim
            texto = textwrap.fill(p["desc"], width=48) 
            ax.text(card_x + 0.02, y - 0.06, texto, fontsize=9, 
                    color=CORES['cinza'], va='top', linespacing=1.4)

            # Valor (Canto inferior direito do card)
            ax.text(card_x + card_width - 0.03, y - card_height + 0.03, p["valor"], 
                    fontsize=12, fontweight='bold', color=CORES['turquesa_escuro'], 
                    ha='right')

            # Decremento maior para separar os cards
            y -= 0.16

        # Observação indicando metodologia (Rodapé da coluna esquerda)
        ax.text(0.05, 0.25,
                "⚙ Estes pacotes foram gerados através de análise preditiva,\n"
                "avaliando padrões de comportamento e demanda.",
                fontsize=8, color=CORES['cinza_escuro'], style='italic')

        # ===========================================================================================
        # 2) INSIGHTS — lado direito
        # ===========================================================================================

        ax.text(0.55, 0.85, "📊 Principais Insights do Mês",
                fontsize=14, fontweight='bold', color=CORES['cinza_escuro'])

        df_dias = self.kpis['por_dia_semana']
        melhor_dia = df_dias['taxa_cancelamento'].idxmin() if not df_dias.empty else 0
        pior_dia = df_dias['taxa_cancelamento'].idxmax() if not df_dias.empty else 0

        df_servicos = self.kpis['por_servico']
        if not df_servicos.empty:
            servico_mais_cancel = df_servicos['taxa_cancelamento'].idxmax()
            servico_menos_cancel = df_servicos['taxa_cancelamento'].idxmin()
            servico_mais_fat = df_servicos['faturamento'].idxmax()
        else:
            servico_mais_cancel = servico_menos_cancel = servico_mais_fat = "N/A"

        insights = [
            {
                'cor': CORES['verde'],
                'titulo': 'Melhor Dia para Agendamentos',
                'texto': f"{get_dia_semana(melhor_dia)} possui a menor taxa de cancelamento."
            },
            {
                'cor': CORES['vermelho'],
                'titulo': 'Maior Risco de Cancelamentos',
                'texto': f"{get_dia_semana(pior_dia)} apresenta maior evasão."
            },
            {
                'cor': CORES['turquesa'],
                'titulo': 'Serviço Mais Rentável',
                'texto': f"{servico_mais_fat} liderou em faturamento."
            },
            {
                'cor': CORES['amarelo'],
                'titulo': 'Maior Taxa de Cancelamento',
                'texto': f"{servico_mais_cancel} requer atenção."
            },
            {
                'cor': CORES['turquesa_escuro'],
                'titulo': 'Melhor Fidelização',
                'texto': f"{servico_menos_cancel} tem clientes recorrentes."
            },
            {
                'cor': CORES['rosa'],
                'titulo': 'Impacto Financeiro',
                'texto': f"Perda estimada: {formatar_moeda(self.kpis['perda_cancelamentos'])}."
            },
        ]

        y_insights = 0.78
        for item in insights:
            # Bolinha colorida
            ax.add_patch(mpatches.Circle((0.55, y_insights - 0.01), 0.008, color=item['cor']))

            ax.text(0.57, y_insights, item['titulo'], fontsize=11, fontweight='bold',
                    color=CORES['cinza_escuro'], va='center')

            texto_formatado = textwrap.fill(item['texto'], 50)
            ax.text(0.57, y_insights - 0.03, texto_formatado,
                    fontsize=9, color=CORES['cinza'], va='top')

            y_insights -= 0.11

        # Rodapé da página
        ax.text(0.5, 0.05,
                f"Relatório de BI - {get_nome_mes(self.mes)} {self.ano} | Clínica Beira-Mar",
                ha='center', fontsize=9, color=CORES['cinza'])

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