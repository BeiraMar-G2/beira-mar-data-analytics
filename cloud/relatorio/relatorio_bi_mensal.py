#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           RELATÓRIO DE BI MENSAL - CLÍNICA BEIRA-MAR                        ║
║                                                                              ║
║  Relatório automático de Business Intelligence para acompanhamento          ║
║  mensal da saúde financeira e operacional da clínica.                       ║
║                                                                              ║
║  Autor: Projeto Beira-Mar                                                   ║
║  Data: Dezembro 2025                                                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime, timedelta
from calendar import monthrange
import locale
import os
import warnings

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURAÇÕES DE CORES E ESTILO DA CLÍNICA BEIRA-MAR
# ============================================================================

# Paleta de cores oficial da clínica
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
    'preto': '#2C3E50'
}

# Gradiente de cores para gráficos
GRADIENTE_TURQUESA = ['#E0FFFF', '#AFEEEE', '#7FFFD4', '#40E0D0', '#20B2AA', '#008B8B']
GRADIENTE_ROSA = ['#FFF0F5', '#FFB6C1', '#FF69B4', '#FF1493', '#C71585']

# Configurações globais do matplotlib
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 11
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = '#FAFAFA'
plt.rcParams['axes.edgecolor'] = '#CCCCCC'
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.3

# Tentar configurar locale para português
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
    'password': 'Lqsym@2020',  # Ajuste conforme necessário
    'database': 'BeiraMar'
}

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def formatar_moeda(valor):
    """Formata valor para moeda brasileira"""
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def formatar_percentual(valor):
    """Formata valor para percentual"""
    return f"{valor:.1f}%"

def get_nome_mes(mes):
    """Retorna o nome do mês em português"""
    meses = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    return meses.get(mes, '')

def get_dia_semana(dia):
    """Retorna o nome do dia da semana em português"""
    dias = {
        0: 'Segunda', 1: 'Terça', 2: 'Quarta', 3: 'Quinta',
        4: 'Sexta', 5: 'Sábado', 6: 'Domingo'
    }
    return dias.get(dia, '')

def criar_kpi_card(ax, titulo, valor, subtitulo="", cor=CORES['turquesa'], icone=""):
    """Cria um card visual para KPI"""
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    # Fundo do card
    card = mpatches.FancyBboxPatch(
        (0.02, 0.02), 0.96, 0.96,
        boxstyle="round,pad=0.02,rounding_size=0.05",
        facecolor=CORES['branco'],
        edgecolor=cor,
        linewidth=3
    )
    ax.add_patch(card)
    
    # Barra superior colorida
    barra = mpatches.FancyBboxPatch(
        (0.02, 0.75), 0.96, 0.23,
        boxstyle="round,pad=0.02,rounding_size=0.05",
        facecolor=cor,
        edgecolor=cor
    )
    ax.add_patch(barra)
    
    # Textos
    ax.text(0.5, 0.87, titulo, ha='center', va='center', fontsize=11, 
            fontweight='bold', color=CORES['branco'])
    ax.text(0.5, 0.45, str(valor), ha='center', va='center', fontsize=24, 
            fontweight='bold', color=CORES['cinza_escuro'])
    if subtitulo:
        ax.text(0.5, 0.15, subtitulo, ha='center', va='center', fontsize=9, 
                color=CORES['cinza'])

# ============================================================================
# CLASSE PRINCIPAL DO RELATÓRIO
# ============================================================================

class RelatorioBIBeiraMar:
    """Classe para geração do relatório de BI mensal"""
    
    def __init__(self, ano=None, mes=None, usar_dados_mock=True):
        """
        Inicializa o relatório
        
        Args:
            ano: Ano do relatório (default: ano atual)
            mes: Mês do relatório (default: mês anterior)
            usar_dados_mock: Se True, usa dados mockados para demo
        """
        if ano is None or mes is None:
            # Por padrão, gera relatório do mês anterior
            hoje = datetime.now()
            if mes is None:
                mes = hoje.month - 1 if hoje.month > 1 else 12
            if ano is None:
                ano = hoje.year if hoje.month > 1 else hoje.year - 1
        
        self.ano = ano
        self.mes = mes
        self.usar_dados_mock = usar_dados_mock
        self.data_geracao = datetime.now()
        
        # DataFrames
        self.df_agendamentos = None
        self.df_servicos = None
        self.df_clientes = None
        
        # KPIs calculados
        self.kpis = {}
        
    def conectar_banco(self):
        """Estabelece conexão com o banco de dados"""
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            print(f"⚠️ Erro ao conectar ao banco: {e}")
            return None
    
    def carregar_dados(self):
        """Carrega os dados do banco de dados"""
        
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
                # Query para agendamentos do mês
                query_agendamentos = f"""
                SELECT 
                    a.id_agendamento,
                    a.dt_hora,
                    a.valor_pago,
                    a.status,
                    s.id_servico,
                    s.nome as servico_nome,
                    s.preco as servico_preco,
                    s.duracao as servico_duracao,
                    u.id_usuario as cliente_id,
                    u.nome as cliente_nome,
                    u.dt_nasc as cliente_nascimento
                FROM agendamento a
                JOIN servico s ON a.fk_servico = s.id_servico
                JOIN usuario u ON a.fk_cliente = u.id_usuario
                WHERE YEAR(a.dt_hora) = {self.ano} 
                    AND MONTH(a.dt_hora) = {self.mes}
                ORDER BY a.dt_hora
                """
                
                self.df_agendamentos = pd.read_sql(query_agendamentos, conn)
                
                # Query para serviços
                query_servicos = "SELECT * FROM servico"
                self.df_servicos = pd.read_sql(query_servicos, conn)
                
                conn.close()
                print(f"✅ Dados carregados: {len(self.df_agendamentos)} agendamentos")
                
            except Exception as e:
                print(f"⚠️ Erro ao carregar dados: {e}")
                print("⚠️ Usando dados mockados como fallback...")
                self._carregar_dados_mock()
    
    def _carregar_dados_mock(self):
        """Carrega dados mockados para demonstração"""
        
        # Dados de serviços (conforme documentação)
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
        
        # Simular agendamentos de novembro 2025 (conforme script SQL)
        # Esta é uma representação simplificada dos dados mockados
        agendamentos_data = []
        
        # Dados baseados no script SQL fornecido
        agendamentos_raw = [
            # Semana 1: 03-08 de Novembro
            # Segunda 03/11 - DIA FRACO
            (8, '2025-11-03 09:00:00', 30.00, 'Concluido'),
            (8, '2025-11-03 10:00:00', 30.00, 'Cancelado'),
            (4, '2025-11-03 14:00:00', 100.00, 'Concluido'),
            (2, '2025-11-03 16:00:00', 100.00, 'Concluido'),
            # Terça 04/11 - DIA FRACO
            (8, '2025-11-04 09:00:00', 30.00, 'Cancelado'),
            (9, '2025-11-04 10:00:00', 35.00, 'Concluido'),
            (1, '2025-11-04 11:00:00', 90.00, 'Concluido'),
            (6, '2025-11-04 14:00:00', 150.00, 'Concluido'),
            (8, '2025-11-04 17:00:00', 30.00, 'Cancelado'),
            # Quarta 05/11 - DIA BOM
            (3, '2025-11-05 08:00:00', 180.00, 'Concluido'),
            (8, '2025-11-05 09:30:00', 30.00, 'Concluido'),
            (7, '2025-11-05 10:30:00', 45.00, 'Concluido'),
            (1, '2025-11-05 12:00:00', 90.00, 'Concluido'),
            (2, '2025-11-05 14:00:00', 100.00, 'Concluido'),
            (5, '2025-11-05 15:30:00', 180.00, 'Concluido'),
            (8, '2025-11-05 17:00:00', 30.00, 'Cancelado'),
            (4, '2025-11-05 18:00:00', 100.00, 'Concluido'),
            # Quinta 06/11 - MELHOR DIA
            (3, '2025-11-06 08:00:00', 180.00, 'Concluido'),
            (1, '2025-11-06 09:00:00', 90.00, 'Concluido'),
            (8, '2025-11-06 10:00:00', 30.00, 'Concluido'),
            (7, '2025-11-06 11:00:00', 45.00, 'Concluido'),
            (6, '2025-11-06 12:00:00', 150.00, 'Concluido'),
            (2, '2025-11-06 14:00:00', 100.00, 'Concluido'),
            (10, '2025-11-06 15:30:00', 150.00, 'Concluido'),
            (11, '2025-11-06 17:00:00', 90.00, 'Concluido'),
            (8, '2025-11-06 18:00:00', 30.00, 'Cancelado'),
            (4, '2025-11-06 19:00:00', 100.00, 'Concluido'),
            # Sexta 07/11 - DIA BOM
            (8, '2025-11-07 09:00:00', 30.00, 'Concluido'),
            (9, '2025-11-07 10:00:00', 35.00, 'Concluido'),
            (1, '2025-11-07 11:00:00', 90.00, 'Concluido'),
            (3, '2025-11-07 12:00:00', 180.00, 'Concluido'),
            (2, '2025-11-07 14:30:00', 100.00, 'Concluido'),
            (8, '2025-11-07 16:00:00', 30.00, 'Cancelado'),
            (7, '2025-11-07 17:00:00', 45.00, 'Concluido'),
            (4, '2025-11-07 18:30:00', 100.00, 'Concluido'),
            # Sábado 08/11 - ALTA DEMANDA
            (8, '2025-11-08 08:00:00', 30.00, 'Cancelado'),
            (8, '2025-11-08 09:00:00', 30.00, 'Concluido'),
            (7, '2025-11-08 10:00:00', 45.00, 'Concluido'),
            (9, '2025-11-08 11:00:00', 35.00, 'Cancelado'),
            (1, '2025-11-08 12:00:00', 90.00, 'Concluido'),
            (6, '2025-11-08 13:00:00', 150.00, 'Concluido'),
            (2, '2025-11-08 14:30:00', 100.00, 'Concluido'),
            (3, '2025-11-08 15:30:00', 180.00, 'Concluido'),
            (8, '2025-11-08 17:00:00', 30.00, 'Cancelado'),
            (11, '2025-11-08 18:00:00', 90.00, 'Concluido'),
            (4, '2025-11-08 19:00:00', 100.00, 'Cancelado'),
            (5, '2025-11-08 20:00:00', 180.00, 'Concluido'),
            # Semana 2: 10-15 de Novembro
            # Segunda 10/11
            (8, '2025-11-10 09:00:00', 30.00, 'Concluido'),
            (4, '2025-11-10 14:00:00', 100.00, 'Concluido'),
            (8, '2025-11-10 16:00:00', 30.00, 'Cancelado'),
            # Terça 11/11
            (2, '2025-11-11 10:00:00', 100.00, 'Concluido'),
            (8, '2025-11-11 11:00:00', 30.00, 'Cancelado'),
            (9, '2025-11-11 14:00:00', 35.00, 'Concluido'),
            (1, '2025-11-11 16:00:00', 90.00, 'Concluido'),
            # Quarta 12/11
            (3, '2025-11-12 08:00:00', 180.00, 'Concluido'),
            (8, '2025-11-12 09:30:00', 30.00, 'Concluido'),
            (7, '2025-11-12 10:30:00', 45.00, 'Concluido'),
            (6, '2025-11-12 12:00:00', 150.00, 'Concluido'),
            (2, '2025-11-12 14:00:00', 100.00, 'Concluido'),
            (10, '2025-11-12 15:30:00', 150.00, 'Concluido'),
            (8, '2025-11-12 17:00:00', 30.00, 'Cancelado'),
            # Quinta 13/11
            (3, '2025-11-13 08:00:00', 180.00, 'Concluido'),
            (1, '2025-11-13 09:00:00', 90.00, 'Concluido'),
            (8, '2025-11-13 10:00:00', 30.00, 'Concluido'),
            (7, '2025-11-13 11:00:00', 45.00, 'Concluido'),
            (5, '2025-11-13 12:00:00', 180.00, 'Concluido'),
            (2, '2025-11-13 14:00:00', 100.00, 'Concluido'),
            (11, '2025-11-13 15:30:00', 90.00, 'Concluido'),
            (4, '2025-11-13 17:00:00', 100.00, 'Concluido'),
            (8, '2025-11-13 18:00:00', 30.00, 'Concluido'),
            # Sexta 14/11
            (8, '2025-11-14 09:00:00', 30.00, 'Concluido'),
            (9, '2025-11-14 10:00:00', 35.00, 'Concluido'),
            (1, '2025-11-14 11:00:00', 90.00, 'Concluido'),
            (3, '2025-11-14 12:00:00', 180.00, 'Concluido'),
            (6, '2025-11-14 14:00:00', 150.00, 'Concluido'),
            (8, '2025-11-14 16:00:00', 30.00, 'Cancelado'),
            (2, '2025-11-14 17:00:00', 100.00, 'Concluido'),
            # Sábado 15/11 - FERIADO (sem agendamentos)
            # Semana 3: 17-22 de Novembro
            # Segunda 17/11
            (8, '2025-11-17 10:00:00', 30.00, 'Cancelado'),
            (4, '2025-11-17 14:00:00', 100.00, 'Concluido'),
            (2, '2025-11-17 16:00:00', 100.00, 'Concluido'),
            # Terça 18/11
            (8, '2025-11-18 09:00:00', 30.00, 'Concluido'),
            (9, '2025-11-18 10:00:00', 35.00, 'Cancelado'),
            (1, '2025-11-18 14:00:00', 90.00, 'Concluido'),
            (7, '2025-11-18 16:00:00', 45.00, 'Concluido'),
            # Quarta 19/11
            (3, '2025-11-19 08:00:00', 180.00, 'Concluido'),
            (8, '2025-11-19 09:30:00', 30.00, 'Concluido'),
            (6, '2025-11-19 11:00:00', 150.00, 'Concluido'),
            (2, '2025-11-19 14:00:00', 100.00, 'Concluido'),
            (8, '2025-11-19 16:00:00', 30.00, 'Cancelado'),
            (4, '2025-11-19 18:00:00', 100.00, 'Concluido'),
            # Quinta 20/11 - FERIADO (funcionando)
            (3, '2025-11-20 08:00:00', 180.00, 'Concluido'),
            (8, '2025-11-20 09:00:00', 30.00, 'Concluido'),
            (7, '2025-11-20 10:00:00', 45.00, 'Concluido'),
            (1, '2025-11-20 11:00:00', 90.00, 'Concluido'),
            (5, '2025-11-20 12:00:00', 180.00, 'Concluido'),
            (2, '2025-11-20 14:00:00', 100.00, 'Concluido'),
            (10, '2025-11-20 15:30:00', 150.00, 'Concluido'),
            (11, '2025-11-20 17:00:00', 90.00, 'Concluido'),
            (8, '2025-11-20 18:00:00', 30.00, 'Cancelado'),
            (4, '2025-11-20 19:00:00', 100.00, 'Concluido'),
            # Sexta 21/11
            (8, '2025-11-21 09:00:00', 30.00, 'Concluido'),
            (9, '2025-11-21 10:00:00', 35.00, 'Concluido'),
            (3, '2025-11-21 11:00:00', 180.00, 'Concluido'),
            (6, '2025-11-21 13:00:00', 150.00, 'Concluido'),
            (2, '2025-11-21 15:00:00', 100.00, 'Concluido'),
            (8, '2025-11-21 16:30:00', 30.00, 'Cancelado'),
            (1, '2025-11-21 17:30:00', 90.00, 'Concluido'),
            # Sábado 22/11
            (8, '2025-11-22 08:00:00', 30.00, 'Concluido'),
            (7, '2025-11-22 09:00:00', 45.00, 'Concluido'),
            (9, '2025-11-22 10:00:00', 35.00, 'Cancelado'),
            (1, '2025-11-22 11:00:00', 90.00, 'Concluido'),
            (3, '2025-11-22 12:00:00', 180.00, 'Concluido'),
            (6, '2025-11-22 14:00:00', 150.00, 'Concluido'),
            (2, '2025-11-22 15:30:00', 100.00, 'Concluido'),
            (8, '2025-11-22 16:30:00', 30.00, 'Cancelado'),
            (11, '2025-11-22 17:30:00', 90.00, 'Concluido'),
            (5, '2025-11-22 19:00:00', 180.00, 'Concluido'),
            # Semana 4: 24-29 de Novembro (Black Friday Week)
            # Segunda 24/11
            (8, '2025-11-24 09:00:00', 30.00, 'Concluido'),
            (4, '2025-11-24 11:00:00', 100.00, 'Concluido'),
            (2, '2025-11-24 14:00:00', 100.00, 'Concluido'),
            (8, '2025-11-24 15:30:00', 30.00, 'Cancelado'),
            (9, '2025-11-24 17:00:00', 35.00, 'Concluido'),
            # Terça 25/11
            (1, '2025-11-25 09:00:00', 90.00, 'Concluido'),
            (8, '2025-11-25 10:00:00', 30.00, 'Concluido'),
            (7, '2025-11-25 11:00:00', 45.00, 'Concluido'),
            (3, '2025-11-25 12:00:00', 180.00, 'Concluido'),
            (2, '2025-11-25 14:30:00', 100.00, 'Concluido'),
            (8, '2025-11-25 16:00:00', 30.00, 'Cancelado'),
            # Quarta 26/11
            (3, '2025-11-26 08:00:00', 180.00, 'Concluido'),
            (8, '2025-11-26 09:30:00', 30.00, 'Concluido'),
            (6, '2025-11-26 10:30:00', 150.00, 'Concluido'),
            (1, '2025-11-26 12:00:00', 90.00, 'Concluido'),
            (5, '2025-11-26 14:00:00', 180.00, 'Concluido'),
            (2, '2025-11-26 15:30:00', 100.00, 'Concluido'),
            (10, '2025-11-26 17:00:00', 150.00, 'Concluido'),
            (8, '2025-11-26 18:30:00', 30.00, 'Concluido'),
            # Quinta 27/11 - Véspera Black Friday
            (3, '2025-11-27 08:00:00', 180.00, 'Concluido'),
            (8, '2025-11-27 09:00:00', 30.00, 'Concluido'),
            (7, '2025-11-27 10:00:00', 45.00, 'Concluido'),
            (1, '2025-11-27 11:00:00', 90.00, 'Concluido'),
            (6, '2025-11-27 12:00:00', 150.00, 'Concluido'),
            (5, '2025-11-27 14:00:00', 180.00, 'Concluido'),
            (2, '2025-11-27 15:30:00', 100.00, 'Concluido'),
            (11, '2025-11-27 17:00:00', 90.00, 'Concluido'),
            (4, '2025-11-27 18:00:00', 100.00, 'Concluido'),
            (8, '2025-11-27 19:00:00', 30.00, 'Cancelado'),
            # Sexta 28/11 - BLACK FRIDAY!
            (3, '2025-11-28 08:00:00', 162.00, 'Concluido'),
            (8, '2025-11-28 09:00:00', 27.00, 'Concluido'),
            (7, '2025-11-28 09:30:00', 40.50, 'Concluido'),
            (9, '2025-11-28 10:00:00', 31.50, 'Concluido'),
            (1, '2025-11-28 10:30:00', 81.00, 'Concluido'),
            (6, '2025-11-28 11:30:00', 135.00, 'Concluido'),
            (5, '2025-11-28 13:30:00', 162.00, 'Concluido'),
            (2, '2025-11-28 14:30:00', 90.00, 'Concluido'),
            (10, '2025-11-28 15:30:00', 135.00, 'Concluido'),
            (8, '2025-11-28 16:30:00', 27.00, 'Cancelado'),
            (11, '2025-11-28 17:00:00', 81.00, 'Concluido'),
            (4, '2025-11-28 18:00:00', 90.00, 'Concluido'),
            (8, '2025-11-28 19:00:00', 27.00, 'Concluido'),
            (3, '2025-11-28 19:30:00', 162.00, 'Concluido'),
            # Sábado 29/11 - Pós Black Friday
            (8, '2025-11-29 08:00:00', 30.00, 'Concluido'),
            (7, '2025-11-29 09:00:00', 45.00, 'Concluido'),
            (9, '2025-11-29 10:00:00', 35.00, 'Cancelado'),
            (1, '2025-11-29 10:30:00', 90.00, 'Concluido'),
            (6, '2025-11-29 12:00:00', 150.00, 'Concluido'),
            (3, '2025-11-29 14:00:00', 180.00, 'Concluido'),
            (2, '2025-11-29 15:30:00', 100.00, 'Concluido'),
            (8, '2025-11-29 16:30:00', 30.00, 'Cancelado'),
            (11, '2025-11-29 17:30:00', 90.00, 'Concluido'),
            (5, '2025-11-29 19:00:00', 180.00, 'Concluido'),
        ]
        
        for i, (servico_id, dt_hora, valor, status) in enumerate(agendamentos_raw, 1):
            servico_info = self.df_servicos[self.df_servicos['id_servico'] == servico_id].iloc[0]
            agendamentos_data.append({
                'id_agendamento': i,
                'dt_hora': pd.to_datetime(dt_hora),
                'valor_pago': valor,
                'status': status,
                'id_servico': servico_id,
                'servico_nome': servico_info['nome'],
                'servico_preco': servico_info['preco'],
                'servico_duracao': servico_info['duracao']
            })
        
        self.df_agendamentos = pd.DataFrame(agendamentos_data)
        self.ano = 2025
        self.mes = 11
        
        print(f"✅ Dados mockados carregados: {len(self.df_agendamentos)} agendamentos")
    
    def calcular_kpis(self):
        """Calcula todos os KPIs do relatório"""
        
        df = self.df_agendamentos.copy()
        
        # KPIs Principais
        self.kpis['total_agendamentos'] = len(df)
        self.kpis['concluidos'] = len(df[df['status'] == 'Concluido'])
        self.kpis['cancelados'] = len(df[df['status'] == 'Cancelado'])
        
        # Taxas
        self.kpis['taxa_comparecimento'] = (self.kpis['concluidos'] / self.kpis['total_agendamentos']) * 100
        self.kpis['taxa_cancelamento'] = (self.kpis['cancelados'] / self.kpis['total_agendamentos']) * 100
        
        # Financeiro
        self.kpis['faturamento'] = df[df['status'] == 'Concluido']['valor_pago'].sum()
        self.kpis['ticket_medio'] = self.kpis['faturamento'] / self.kpis['concluidos'] if self.kpis['concluidos'] > 0 else 0
        self.kpis['perda_cancelamentos'] = df[df['status'] == 'Cancelado']['valor_pago'].sum()
        
        # Dias trabalhados (excluindo domingos)
        df['dia_semana'] = df['dt_hora'].dt.dayofweek
        dias_unicos = df['dt_hora'].dt.date.nunique()
        self.kpis['dias_trabalhados'] = dias_unicos
        self.kpis['media_dia'] = self.kpis['faturamento'] / dias_unicos if dias_unicos > 0 else 0
        self.kpis['atendimentos_por_dia'] = self.kpis['concluidos'] / dias_unicos if dias_unicos > 0 else 0
        
        # Análise por dia da semana
        self.kpis['por_dia_semana'] = df.groupby('dia_semana').agg({
            'id_agendamento': 'count',
            'status': lambda x: (x == 'Cancelado').sum()
        }).rename(columns={'id_agendamento': 'total', 'status': 'cancelados'})
        self.kpis['por_dia_semana']['taxa_cancelamento'] = (
            self.kpis['por_dia_semana']['cancelados'] / self.kpis['por_dia_semana']['total'] * 100
        )
        
        # Análise por serviço
        self.kpis['por_servico'] = df.groupby('servico_nome').agg({
            'id_agendamento': 'count',
            'status': lambda x: (x == 'Cancelado').sum(),
            'valor_pago': lambda x: x[df.loc[x.index, 'status'] == 'Concluido'].sum()
        }).rename(columns={'id_agendamento': 'total', 'status': 'cancelados', 'valor_pago': 'faturamento'})
        self.kpis['por_servico']['taxa_cancelamento'] = (
            self.kpis['por_servico']['cancelados'] / self.kpis['por_servico']['total'] * 100
        )
        
        # Análise por semana do mês
        df['semana'] = df['dt_hora'].dt.isocalendar().week
        self.kpis['por_semana'] = df.groupby('semana').agg({
            'id_agendamento': 'count',
            'valor_pago': lambda x: x[df.loc[x.index, 'status'] == 'Concluido'].sum()
        }).rename(columns={'id_agendamento': 'total', 'valor_pago': 'faturamento'})
        
        # Horários mais movimentados
        df['hora'] = df['dt_hora'].dt.hour
        self.kpis['por_hora'] = df.groupby('hora')['id_agendamento'].count()
        
        print("✅ KPIs calculados com sucesso")
    
    def gerar_pagina_capa(self, pdf):
        """Gera a página de capa do relatório"""
        
        fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
        
        # Fundo
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        # Faixa superior turquesa
        faixa_sup = mpatches.Rectangle((0, 0.65), 1, 0.35, 
                                        facecolor=CORES['turquesa'], 
                                        edgecolor='none')
        ax.add_patch(faixa_sup)
        
        # Faixa inferior rosa
        faixa_inf = mpatches.Rectangle((0, 0), 1, 0.15, 
                                        facecolor=CORES['rosa'], 
                                        edgecolor='none')
        ax.add_patch(faixa_inf)
        
        # Título principal
        ax.text(0.5, 0.82, 'Relatório de Business Intelligence', 
                ha='center', va='center', fontsize=32, fontweight='bold', 
                color=CORES['branco'])
        
        # Subtítulo
        ax.text(0.5, 0.72, 'Clínica de Estética Beira-Mar', 
                ha='center', va='center', fontsize=24, 
                color=CORES['branco'])
        
        # Mês/Ano
        nome_mes = get_nome_mes(self.mes)
        ax.text(0.5, 0.45, f'{nome_mes} {self.ano}', 
                ha='center', va='center', fontsize=48, fontweight='bold', 
                color=CORES['turquesa'])
        
        # Resumo rápido
        resumo = f"📊 {self.kpis['total_agendamentos']} agendamentos  •  " \
                 f"💰 {formatar_moeda(self.kpis['faturamento'])}  •  " \
                 f"✅ {formatar_percentual(self.kpis['taxa_comparecimento'])} comparecimento"
        ax.text(0.5, 0.30, resumo, 
                ha='center', va='center', fontsize=14, 
                color=CORES['cinza_escuro'])
        
        # Data de geração
        ax.text(0.5, 0.08, 
                f"Relatório gerado em {self.data_geracao.strftime('%d/%m/%Y às %H:%M')}", 
                ha='center', va='center', fontsize=10, 
                color=CORES['branco'])
        
        # Localização
        ax.text(0.5, 0.04, "Mauá, São Paulo", 
                ha='center', va='center', fontsize=10, 
                color=CORES['branco'])
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def gerar_pagina_kpis(self, pdf):
        """Gera a página de KPIs principais"""
        
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.suptitle('Indicadores Principais do Mês', fontsize=20, fontweight='bold', 
                     color=CORES['cinza_escuro'], y=0.98)
        
        # Grid de KPIs (2 linhas x 4 colunas)
        gs = fig.add_gridspec(2, 4, hspace=0.3, wspace=0.2, 
                              left=0.05, right=0.95, top=0.88, bottom=0.1)
        
        # KPIs
        kpi_data = [
            ('Total de\nAgendamentos', str(self.kpis['total_agendamentos']), '', CORES['turquesa']),
            ('Atendimentos\nRealizados', str(self.kpis['concluidos']), 
             f"{formatar_percentual(self.kpis['taxa_comparecimento'])} comparecimento", CORES['verde']),
            ('Cancelamentos', str(self.kpis['cancelados']), 
             f"{formatar_percentual(self.kpis['taxa_cancelamento'])} do total", CORES['vermelho']),
            ('Dias\nTrabalhados', str(self.kpis['dias_trabalhados']), '', CORES['rosa']),
            ('Faturamento\nTotal', formatar_moeda(self.kpis['faturamento']), '', CORES['turquesa_escuro']),
            ('Ticket\nMédio', formatar_moeda(self.kpis['ticket_medio']), 'por atendimento', CORES['turquesa']),
            ('Média\nDiária', formatar_moeda(self.kpis['media_dia']), 'faturamento/dia', CORES['rosa']),
            ('Perda com\nCancelamentos', formatar_moeda(self.kpis['perda_cancelamentos']), 'potencial perdido', CORES['vermelho']),
        ]
        
        for idx, (titulo, valor, subtitulo, cor) in enumerate(kpi_data):
            row = idx // 4
            col = idx % 4
            ax = fig.add_subplot(gs[row, col])
            criar_kpi_card(ax, titulo, valor, subtitulo, cor)
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def gerar_pagina_servicos(self, pdf):
        """Gera a página de análise por serviços"""
        
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.suptitle('Análise por Serviços', fontsize=20, fontweight='bold', 
                     color=CORES['cinza_escuro'], y=0.98)
        
        # Grid 2x2
        gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.25, 
                              left=0.08, right=0.95, top=0.88, bottom=0.08)
        
        # Dados
        df_servicos = self.kpis['por_servico'].sort_values('total', ascending=True)
        
        # Gráfico 1: Serviços mais procurados (Top 10)
        ax1 = fig.add_subplot(gs[0, 0])
        top_servicos = df_servicos.tail(10)
        cores_barras = [CORES['turquesa'] if i < 7 else CORES['turquesa_escuro'] 
                        for i in range(len(top_servicos))]
        bars = ax1.barh(range(len(top_servicos)), top_servicos['total'], color=cores_barras)
        ax1.set_yticks(range(len(top_servicos)))
        ax1.set_yticklabels([nome[:25] + '...' if len(nome) > 25 else nome 
                            for nome in top_servicos.index], fontsize=8)
        ax1.set_xlabel('Quantidade de Agendamentos')
        ax1.set_title('🏆 Serviços Mais Procurados', fontweight='bold', pad=10)
        
        # Adicionar valores nas barras
        for bar, val in zip(bars, top_servicos['total']):
            ax1.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2, 
                    str(int(val)), va='center', fontsize=8)
        
        # Gráfico 2: Faturamento por serviço
        ax2 = fig.add_subplot(gs[0, 1])
        top_fat = df_servicos.sort_values('faturamento', ascending=True).tail(10)
        cores_fat = [CORES['verde'] for _ in range(len(top_fat))]
        bars2 = ax2.barh(range(len(top_fat)), top_fat['faturamento'], color=cores_fat)
        ax2.set_yticks(range(len(top_fat)))
        ax2.set_yticklabels([nome[:25] + '...' if len(nome) > 25 else nome 
                            for nome in top_fat.index], fontsize=8)
        ax2.set_xlabel('Faturamento (R$)')
        ax2.set_title('💰 Faturamento por Serviço', fontweight='bold', pad=10)
        
        for bar, val in zip(bars2, top_fat['faturamento']):
            ax2.text(bar.get_width() + 20, bar.get_y() + bar.get_height()/2, 
                    formatar_moeda(val), va='center', fontsize=7)
        
        # Gráfico 3: Taxa de cancelamento por serviço
        ax3 = fig.add_subplot(gs[1, 0])
        df_cancel = df_servicos[df_servicos['cancelados'] > 0].sort_values('taxa_cancelamento', ascending=True)
        cores_cancel = [CORES['amarelo'] if v < 20 else CORES['vermelho'] 
                       for v in df_cancel['taxa_cancelamento']]
        bars3 = ax3.barh(range(len(df_cancel)), df_cancel['taxa_cancelamento'], color=cores_cancel)
        ax3.set_yticks(range(len(df_cancel)))
        ax3.set_yticklabels([nome[:25] + '...' if len(nome) > 25 else nome 
                            for nome in df_cancel.index], fontsize=8)
        ax3.set_xlabel('Taxa de Cancelamento (%)')
        ax3.set_title('⚠️ Taxa de Cancelamento por Serviço', fontweight='bold', pad=10)
        ax3.axvline(x=20, color=CORES['vermelho'], linestyle='--', alpha=0.5, label='Meta: 20%')
        ax3.legend(loc='lower right', fontsize=8)
        
        for bar, val in zip(bars3, df_cancel['taxa_cancelamento']):
            ax3.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, 
                    f'{val:.1f}%', va='center', fontsize=8)
        
        # Gráfico 4: Distribuição do faturamento (pizza)
        ax4 = fig.add_subplot(gs[1, 1])
        top5_fat = df_servicos.nlargest(5, 'faturamento')
        outros = df_servicos['faturamento'].sum() - top5_fat['faturamento'].sum()
        
        labels = list(top5_fat.index) + ['Outros']
        sizes = list(top5_fat['faturamento']) + [outros]
        cores_pizza = [CORES['turquesa'], CORES['rosa'], CORES['verde'], 
                       CORES['amarelo'], CORES['turquesa_escuro'], CORES['cinza']]
        
        wedges, texts, autotexts = ax4.pie(sizes, labels=None, autopct='%1.1f%%', 
                                            colors=cores_pizza, startangle=90)
        ax4.set_title('📊 Distribuição do Faturamento', fontweight='bold', pad=10)
        ax4.legend(wedges, [f'{l[:15]}...' if len(l) > 15 else l for l in labels], 
                   loc='center left', bbox_to_anchor=(1, 0.5), fontsize=7)
        
        plt.setp(autotexts, size=8, weight='bold')
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def gerar_pagina_temporal(self, pdf):
        """Gera a página de análise temporal"""
        
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.suptitle('Análise Temporal', fontsize=20, fontweight='bold', 
                     color=CORES['cinza_escuro'], y=0.98)
        
        gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.25, 
                              left=0.08, right=0.95, top=0.88, bottom=0.08)
        
        # Gráfico 1: Agendamentos por dia da semana
        ax1 = fig.add_subplot(gs[0, 0])
        dias_nomes = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb']
        df_dias = self.kpis['por_dia_semana'].reindex(range(6), fill_value=0)
        
        x = range(len(dias_nomes))
        width = 0.35
        
        bars1 = ax1.bar([i - width/2 for i in x], df_dias['total'] - df_dias['cancelados'], 
                        width, label='Realizados', color=CORES['verde'])
        bars2 = ax1.bar([i + width/2 for i in x], df_dias['cancelados'], 
                        width, label='Cancelados', color=CORES['vermelho'])
        
        ax1.set_xticks(x)
        ax1.set_xticklabels(dias_nomes)
        ax1.set_ylabel('Quantidade')
        ax1.set_title('📅 Agendamentos por Dia da Semana', fontweight='bold', pad=10)
        ax1.legend(loc='upper right', fontsize=8)
        
        # Adicionar valores
        for bar in bars1:
            height = bar.get_height()
            if height > 0:
                ax1.text(bar.get_x() + bar.get_width()/2., height, f'{int(height)}',
                        ha='center', va='bottom', fontsize=8)
        
        # Gráfico 2: Taxa de cancelamento por dia
        ax2 = fig.add_subplot(gs[0, 1])
        taxas = df_dias['taxa_cancelamento'].values
        cores_taxa = [CORES['verde'] if t < 15 else (CORES['amarelo'] if t < 25 else CORES['vermelho']) 
                      for t in taxas]
        bars3 = ax2.bar(dias_nomes, taxas, color=cores_taxa)
        ax2.axhline(y=self.kpis['taxa_cancelamento'], color=CORES['turquesa'], 
                    linestyle='--', label=f"Média: {self.kpis['taxa_cancelamento']:.1f}%")
        ax2.set_ylabel('Taxa de Cancelamento (%)')
        ax2.set_title('📉 Taxa de Cancelamento por Dia', fontweight='bold', pad=10)
        ax2.legend(loc='upper right', fontsize=8)
        
        for bar, val in zip(bars3, taxas):
            ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.5, 
                    f'{val:.1f}%', ha='center', va='bottom', fontsize=8)
        
        # Gráfico 3: Faturamento por semana
        ax3 = fig.add_subplot(gs[1, 0])
        df_semana = self.kpis['por_semana']
        semanas = [f'Semana {i+1}' for i in range(len(df_semana))]
        
        ax3.fill_between(semanas, df_semana['faturamento'], alpha=0.3, color=CORES['turquesa'])
        ax3.plot(semanas, df_semana['faturamento'], 'o-', color=CORES['turquesa'], 
                linewidth=2, markersize=8)
        
        for i, (s, v) in enumerate(zip(semanas, df_semana['faturamento'])):
            ax3.annotate(formatar_moeda(v), (i, v), textcoords="offset points", 
                        xytext=(0, 10), ha='center', fontsize=8)
        
        ax3.set_ylabel('Faturamento (R$)')
        ax3.set_title('📈 Evolução Semanal do Faturamento', fontweight='bold', pad=10)
        
        # Gráfico 4: Horários mais movimentados
        ax4 = fig.add_subplot(gs[1, 1])
        horas = self.kpis['por_hora']
        horas_labels = [f'{h}h' for h in horas.index]
        
        cores_horas = [CORES['turquesa'] if v < horas.median() else CORES['rosa'] 
                       for v in horas.values]
        bars4 = ax4.bar(horas_labels, horas.values, color=cores_horas)
        ax4.set_xlabel('Horário')
        ax4.set_ylabel('Quantidade de Agendamentos')
        ax4.set_title('🕐 Horários Mais Movimentados', fontweight='bold', pad=10)
        ax4.tick_params(axis='x', rotation=45)
        
        # Destacar horário de pico
        pico_idx = horas.values.argmax()
        bars4[pico_idx].set_color(CORES['rosa_escuro'])
        ax4.annotate('PICO', (pico_idx, horas.values[pico_idx]), 
                    textcoords="offset points", xytext=(0, 5), 
                    ha='center', fontsize=8, fontweight='bold', color=CORES['rosa_escuro'])
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def gerar_pagina_insights(self, pdf):
        """Gera a página de insights e recomendações"""
        
        fig = plt.figure(figsize=(11.69, 8.27))
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        # Título
        ax.text(0.5, 0.95, '💡 Insights e Recomendações', 
                ha='center', va='center', fontsize=24, fontweight='bold', 
                color=CORES['cinza_escuro'])
        
        # Analisar dados para gerar insights
        df_dias = self.kpis['por_dia_semana']
        melhor_dia = df_dias['taxa_cancelamento'].idxmin()
        pior_dia = df_dias['taxa_cancelamento'].idxmax()
        
        df_servicos = self.kpis['por_servico']
        servico_mais_cancel = df_servicos['taxa_cancelamento'].idxmax()
        servico_menos_cancel = df_servicos['taxa_cancelamento'].idxmin()
        servico_mais_fat = df_servicos['faturamento'].idxmax()
        
        insights = [
            {
                'icone': '📅',
                'titulo': 'Melhor Dia para Agendamentos',
                'texto': f'{get_dia_semana(melhor_dia)} é o dia com menor taxa de cancelamento '
                        f'({df_dias.loc[melhor_dia, "taxa_cancelamento"]:.1f}%). '
                        f'Priorize agendamentos importantes neste dia.'
            },
            {
                'icone': '⚠️',
                'titulo': 'Atenção: Dias de Alta Evasão',
                'texto': f'{get_dia_semana(pior_dia)} apresenta a maior taxa de cancelamento '
                        f'({df_dias.loc[pior_dia, "taxa_cancelamento"]:.1f}%). '
                        f'Considere estratégias de confirmação reforçada.'
            },
            {
                'icone': '💎',
                'titulo': 'Serviço Mais Rentável',
                'texto': f'{servico_mais_fat} foi o serviço que mais faturou no mês '
                        f'({formatar_moeda(df_servicos.loc[servico_mais_fat, "faturamento"])}). '
                        f'Considere pacotes promocionais.'
            },
            {
                'icone': '🎯',
                'titulo': 'Oportunidade de Melhoria',
                'texto': f'{servico_mais_cancel} tem a maior taxa de cancelamento '
                        f'({df_servicos.loc[servico_mais_cancel, "taxa_cancelamento"]:.1f}%). '
                        f'Avalie política de confirmação específica.'
            },
            {
                'icone': '✅',
                'titulo': 'Destaque Positivo',
                'texto': f'{servico_menos_cancel} tem a menor taxa de cancelamento '
                        f'({df_servicos.loc[servico_menos_cancel, "taxa_cancelamento"]:.1f}%). '
                        f'Clientes deste serviço são mais comprometidos.'
            },
            {
                'icone': '💰',
                'titulo': 'Análise Financeira',
                'texto': f'O faturamento médio diário foi de {formatar_moeda(self.kpis["media_dia"])}. '
                        f'Potencial perdido com cancelamentos: {formatar_moeda(self.kpis["perda_cancelamentos"])}.'
            },
        ]
        
        y_pos = 0.82
        for insight in insights:
            # Card de insight
            card = mpatches.FancyBboxPatch(
                (0.05, y_pos - 0.10), 0.90, 0.12,
                boxstyle="round,pad=0.01,rounding_size=0.02",
                facecolor=CORES['branco'],
                edgecolor=CORES['turquesa'],
                linewidth=2
            )
            ax.add_patch(card)
            
            # Ícone
            ax.text(0.08, y_pos - 0.04, insight['icone'], fontsize=20, va='center')
            
            # Título
            ax.text(0.14, y_pos - 0.02, insight['titulo'], fontsize=11, 
                    fontweight='bold', color=CORES['cinza_escuro'], va='center')
            
            # Texto
            ax.text(0.14, y_pos - 0.065, insight['texto'], fontsize=9, 
                    color=CORES['cinza'], va='center', wrap=True)
            
            y_pos -= 0.14
        
        # Rodapé
        ax.text(0.5, 0.03, 
                f"Relatório de BI - {get_nome_mes(self.mes)} {self.ano} | Clínica Beira-Mar", 
                ha='center', va='center', fontsize=9, color=CORES['cinza'])
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def gerar_pagina_comparativo(self, pdf):
        """Gera página com análise comparativa e tendências"""
        
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.suptitle('Dashboard de Performance', fontsize=20, fontweight='bold', 
                     color=CORES['cinza_escuro'], y=0.98)
        
        gs = fig.add_gridspec(2, 3, hspace=0.35, wspace=0.3, 
                              left=0.08, right=0.95, top=0.88, bottom=0.08)
        
        # Gauge de Taxa de Comparecimento
        ax1 = fig.add_subplot(gs[0, 0], projection='polar')
        self._criar_gauge(ax1, self.kpis['taxa_comparecimento'], 
                         'Taxa de Comparecimento', meta=85)
        
        # Gauge de Ocupação (estimativa baseada em horário comercial)
        ax2 = fig.add_subplot(gs[0, 1], projection='polar')
        ocupacao = min((self.kpis['atendimentos_por_dia'] / 8) * 100, 100)  # Assumindo 8 slots por dia
        self._criar_gauge(ax2, ocupacao, 'Taxa de Ocupação', meta=75)
        
        # Gauge de Faturamento vs Meta
        ax3 = fig.add_subplot(gs[0, 2], projection='polar')
        meta_faturamento = 12000  # Meta mensal estimada
        perc_meta = (self.kpis['faturamento'] / meta_faturamento) * 100
        self._criar_gauge(ax3, min(perc_meta, 150), 'Meta de Faturamento', meta=100)
        
        # Gráfico de Evolução Diária
        ax4 = fig.add_subplot(gs[1, :2])
        df = self.df_agendamentos.copy()
        df['data'] = df['dt_hora'].dt.date
        
        evolucao = df[df['status'] == 'Concluido'].groupby('data')['valor_pago'].sum()
        evolucao_acum = evolucao.cumsum()
        
        ax4.fill_between(range(len(evolucao_acum)), evolucao_acum.values, 
                         alpha=0.3, color=CORES['turquesa'])
        ax4.plot(range(len(evolucao_acum)), evolucao_acum.values, 
                'o-', color=CORES['turquesa'], linewidth=2, markersize=4)
        
        ax4.set_xlabel('Dias do Mês')
        ax4.set_ylabel('Faturamento Acumulado (R$)')
        ax4.set_title('📈 Evolução do Faturamento Acumulado', fontweight='bold', pad=10)
        
        # Adicionar linha de meta
        meta_diaria = meta_faturamento / len(evolucao_acum)
        meta_acum = [meta_diaria * (i+1) for i in range(len(evolucao_acum))]
        ax4.plot(range(len(meta_acum)), meta_acum, '--', color=CORES['rosa'], 
                label='Meta Linear', linewidth=2)
        ax4.legend(loc='upper left')
        
        # Tabela resumo
        ax5 = fig.add_subplot(gs[1, 2])
        ax5.axis('off')
        
        # Criar tabela
        tabela_data = [
            ['Métrica', 'Valor'],
            ['Total Agendamentos', str(self.kpis['total_agendamentos'])],
            ['Realizados', str(self.kpis['concluidos'])],
            ['Cancelados', str(self.kpis['cancelados'])],
            ['Faturamento', formatar_moeda(self.kpis['faturamento'])],
            ['Ticket Médio', formatar_moeda(self.kpis['ticket_medio'])],
            ['Dias Trabalhados', str(self.kpis['dias_trabalhados'])],
        ]
        
        table = ax5.table(cellText=tabela_data[1:], 
                         colLabels=tabela_data[0],
                         loc='center',
                         cellLoc='center',
                         colColours=[CORES['turquesa'], CORES['turquesa']])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.2, 1.8)
        
        ax5.set_title('📋 Resumo do Mês', fontweight='bold', pad=20)
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def _criar_gauge(self, ax, valor, titulo, meta=80):
        """Cria um gráfico de gauge (velocímetro)"""
        
        # Limpar e configurar o eixo polar
        ax.set_theta_zero_location('N')
        ax.set_theta_direction(-1)
        ax.set_thetamin(0)
        ax.set_thetamax(180)
        
        # Fundo do gauge
        theta_bg = np.linspace(0, np.pi, 100)
        ax.fill_between(theta_bg, 0, 1, alpha=0.1, color=CORES['cinza'])
        
        # Zonas coloridas
        zona_verde = np.linspace(0, np.pi * (meta/100), 50)
        zona_amarela = np.linspace(np.pi * (meta/100), np.pi * 0.85, 30)
        zona_vermelha = np.linspace(np.pi * 0.85, np.pi, 20)
        
        ax.fill_between(zona_verde, 0.7, 1, alpha=0.3, color=CORES['verde'])
        ax.fill_between(zona_amarela, 0.7, 1, alpha=0.3, color=CORES['amarelo'])
        ax.fill_between(zona_vermelha, 0.7, 1, alpha=0.3, color=CORES['vermelho'])
        
        # Ponteiro
        angulo = np.pi * (1 - min(valor, 100) / 100)
        ax.annotate('', xy=(angulo, 0.9), xytext=(angulo, 0),
                   arrowprops=dict(arrowstyle='->', color=CORES['cinza_escuro'], lw=3))
        
        # Valor central
        ax.text(np.pi/2, 0.3, f'{valor:.1f}%', ha='center', va='center', 
               fontsize=14, fontweight='bold', color=CORES['cinza_escuro'])
        
        # Título
        ax.text(np.pi/2, -0.2, titulo, ha='center', va='center', 
               fontsize=10, color=CORES['cinza'], wrap=True)
        
        # Remover elementos do eixo
        ax.set_rticks([])
        ax.set_xticks([])
        ax.spines['polar'].set_visible(False)
    
    def gerar_relatorio(self, caminho_saida=None):
        """Gera o relatório completo em PDF"""
        
        print("\n" + "="*60)
        print("       GERAÇÃO DO RELATÓRIO DE BI - BEIRA-MAR")
        print("="*60)
        
        # Carregar dados
        self.carregar_dados()
        
        # Calcular KPIs
        self.calcular_kpis()
        
        # Definir caminho de saída
        if caminho_saida is None:
            nome_mes = get_nome_mes(self.mes).lower()
            caminho_saida = f"relatorio_bi_{nome_mes}_{self.ano}.pdf"
        
        print(f"\n📄 Gerando PDF: {caminho_saida}")
        
        # Gerar PDF
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


# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    # Criar e gerar relatório
    # Por padrão, usa dados mockados de novembro 2025
    relatorio = RelatorioBIBeiraMar(ano=2025, mes=11, usar_dados_mock=False)
    
    # Gerar o relatório
    caminho = relatorio.gerar_relatorio()
    
    # Opcional: abrir o PDF automaticamente (funciona no Windows)
    # import subprocess
    # subprocess.Popen([caminho], shell=True)
