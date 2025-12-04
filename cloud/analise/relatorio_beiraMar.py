"""
Relatório Beira-Mar - Análise de Faltas em Consultas
Gerador de PDF profissional para análise de no-shows
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle, Paragraph, 
                                Spacer, PageBreak, Image, KeepTogether)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.pdfgen import canvas
from datetime import datetime
import warnings
import io

warnings.filterwarnings('ignore')

# Cores da marca Beira-Mar (tons de azul/turquesa e rosa)
COR_PRINCIPAL = colors.HexColor('#00BCD4')  # Turquesa
COR_SECUNDARIA = colors.HexColor('#E91E63')  # Rosa/Magenta
COR_TEXTO = colors.HexColor('#2C3E50')  # Azul escuro
COR_FUNDO_CLARO = colors.HexColor('#F8F9FA')

class RelatorioBeiraMar:
    def __init__(self, arquivo_dados):
        """Inicializa o gerador de relatório"""
        self.df = pd.read_csv(arquivo_dados)
        self.preparar_dados()
        self.graficos = {}
        
    def preparar_dados(self):
        """Prepara e transforma os dados para análise"""
        # Converter datas
        self.df['SCHEDULEDDAY'] = pd.to_datetime(self.df['SCHEDULEDDAY'])
        # APPOINTMENTDAY pode vir com ou sem hora, então deixamos o pandas inferir
        self.df['APPOINTMENTDAY'] = pd.to_datetime(self.df['APPOINTMENTDAY'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        # Se falhar, tentar sem hora
        if self.df['APPOINTMENTDAY'].isna().any():
            self.df['APPOINTMENTDAY'] = pd.to_datetime(self.df['APPOINTMENTDAY'], format='mixed', dayfirst=True)
        
        # Criar variáveis derivadas
        self.df['DIAS_ANTECEDENCIA'] = (self.df['APPOINTMENTDAY'] - 
                                         self.df['SCHEDULEDDAY']).dt.days
        
        # Faixas etárias
        bins_idade = [0, 18, 30, 45, 60, 100]
        labels_idade = ['0-18', '19-30', '31-45', '46-60', '60+']
        self.df['FAIXA_ETARIA'] = pd.cut(self.df['AGE'], bins=bins_idade, 
                                          labels=labels_idade, right=False)
        
        # Faixas de preço
        bins_preco = [0, 50, 100, 150, 200, 1000]
        labels_preco = ['R$0-50', 'R$51-100', 'R$101-150', 'R$151-200', 'R$200+']
        self.df['FAIXA_PRECO'] = pd.cut(self.df['PRICE'], bins=bins_preco, 
                                         labels=labels_preco, right=False)
        
        # Dia da semana
        dias_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        self.df['DIA_SEMANA'] = self.df['APPOINTMENTDAY'].dt.dayofweek
        self.df['DIA_SEMANA_NOME'] = self.df['DIA_SEMANA'].map(
            {i: dia for i, dia in enumerate(dias_semana)}
        )
        
        # Hora do agendamento
        self.df['HORA_AGENDAMENTO'] = pd.to_datetime(self.df['SCHEDULEDDAY']).dt.hour
        
    def criar_grafico_taxa_noshow(self):
        """Gráfico: Taxa geral de no-show"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Calcular taxas
        taxa_show = (1 - self.df['NO-SHOW'].mean()) * 100
        taxa_noshow = self.df['NO-SHOW'].mean() * 100
        
        # Criar gráfico de pizza elegante
        colors_pie = ['#4CAF50', '#FF5252']
        explode = (0.05, 0.05)
        
        wedges, texts, autotexts = ax.pie(
            [taxa_show, taxa_noshow],
            labels=['Compareceram', 'Faltaram'],
            autopct='%1.1f%%',
            colors=colors_pie,
            explode=explode,
            shadow=True,
            startangle=90,
            textprops={'fontsize': 14, 'weight': 'bold'}
        )
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(16)
        
        ax.set_title('Como Está a Frequência nas Consultas?', 
                     fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def criar_grafico_impacto_sms(self):
        """Gráfico: Impacto do SMS nos no-shows"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Calcular taxas
        sms_data = self.df.groupby('SMS_RECEIVED')['NO-SHOW'].mean() * 100
        
        # Criar gráfico de barras
        bars = ax.bar(['Sem Lembrete SMS', 'Com Lembrete SMS'], 
                      sms_data.values,
                      color=['#FF7043', '#66BB6A'],
                      width=0.6,
                      edgecolor='white',
                      linewidth=2)
        
        # Adicionar valores nas barras
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}%',
                   ha='center', va='bottom', fontsize=14, fontweight='bold')
        
        # Adicionar linha de economia
        ax.axhline(y=sms_data.values[1], color='green', linestyle='--', 
                   alpha=0.3, linewidth=2)
        
        economia = sms_data.values[0] - sms_data.values[1]
        ax.text(0.5, sms_data.values[0] - economia/2, 
               f'Redução de {economia:.1f}%\ncom SMS!',
               ha='center', fontsize=12, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7))
        
        ax.set_ylabel('Taxa de Faltas (%)', fontsize=12, fontweight='bold')
        ax.set_title('O Lembrete por SMS Funciona?', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.set_ylim(0, max(sms_data.values) * 1.3)
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def criar_grafico_por_faixa_etaria(self):
        """Gráfico: No-show por faixa etária"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Calcular taxas por faixa etária
        idade_data = self.df.groupby('FAIXA_ETARIA')['NO-SHOW'].mean() * 100
        idade_data = idade_data.sort_values()
        
        # Cores gradientes
        colors_bars = plt.cm.RdYlGn_r(np.linspace(0.3, 0.9, len(idade_data)))
        
        bars = ax.barh(range(len(idade_data)), idade_data.values, 
                       color=colors_bars, height=0.6,
                       edgecolor='white', linewidth=2)
        
        # Adicionar valores
        for i, (bar, val) in enumerate(zip(bars, idade_data.values)):
            ax.text(val + 0.5, bar.get_y() + bar.get_height()/2,
                   f'{val:.1f}%',
                   va='center', fontsize=11, fontweight='bold')
        
        ax.set_yticks(range(len(idade_data)))
        ax.set_yticklabels([f'{idade} anos' for idade in idade_data.index], fontsize=11)
        ax.set_xlabel('Taxa de Faltas (%)', fontsize=12, fontweight='bold')
        ax.set_title('Quem Falta Mais? Por Idade', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def criar_grafico_por_preco(self):
        """Gráfico: No-show por faixa de preço"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Calcular taxas por preço
        preco_data = self.df.groupby('FAIXA_PRECO')['NO-SHOW'].mean() * 100
        
        # Criar gráfico de linhas com pontos
        ax.plot(range(len(preco_data)), preco_data.values, 
               marker='o', markersize=12, linewidth=3, 
               color='#FF6B6B', markerfacecolor='white',
               markeredgewidth=3, markeredgecolor='#FF6B6B')
        
        # Adicionar valores
        for i, val in enumerate(preco_data.values):
            ax.text(i, val + 1, f'{val:.1f}%',
                   ha='center', fontsize=11, fontweight='bold')
        
        # Destacar insight
        min_idx = np.argmin(preco_data.values)
        ax.annotate('Procedimentos mais\ncaros têm menos faltas!',
                   xy=(min_idx, preco_data.values[min_idx]),
                   xytext=(min_idx + 0.8, preco_data.values[min_idx] + 5),
                   arrowprops=dict(arrowstyle='->', color='green', lw=2),
                   fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))
        
        ax.set_xticks(range(len(preco_data)))
        ax.set_xticklabels(preco_data.index, fontsize=11)
        ax.set_ylabel('Taxa de Faltas (%)', fontsize=12, fontweight='bold')
        ax.set_title('Preço Influencia no Comparecimento?', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.grid(alpha=0.3)
        ax.set_ylim(0, max(preco_data.values) * 1.2)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def criar_grafico_por_servico(self):
        """Gráfico: No-show por tipo de serviço"""
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Calcular taxas por serviço
        servico_data = self.df.groupby('SERVICENAME').agg({
            'NO-SHOW': 'mean',
            'ID': 'count'
        })
        servico_data.columns = ['taxa_noshow', 'quantidade']
        servico_data = servico_data[servico_data['quantidade'] > 50]  # Apenas serviços significativos
        servico_data['taxa_noshow'] = servico_data['taxa_noshow'] * 100
        servico_data = servico_data.sort_values('taxa_noshow')
        
        # Criar nomes curtos para os serviços
        nomes_curtos = [nome[:35] + '...' if len(nome) > 35 else nome 
                       for nome in servico_data.index]
        
        # Cores: vermelho para alto, verde para baixo
        colors_bars = ['#FF5252' if x > 20 else '#FFA726' if x > 15 else '#66BB6A' 
                      for x in servico_data['taxa_noshow'].values]
        
        bars = ax.barh(range(len(servico_data)), servico_data['taxa_noshow'].values,
                      color=colors_bars, height=0.7,
                      edgecolor='white', linewidth=1.5)
        
        # Adicionar valores
        for i, (bar, val) in enumerate(zip(bars, servico_data['taxa_noshow'].values)):
            ax.text(val + 0.5, bar.get_y() + bar.get_height()/2,
                   f'{val:.1f}%',
                   va='center', fontsize=9, fontweight='bold')
        
        ax.set_yticks(range(len(servico_data)))
        ax.set_yticklabels(nomes_curtos, fontsize=10)
        ax.set_xlabel('Taxa de Faltas (%)', fontsize=12, fontweight='bold')
        ax.set_title('Quais Procedimentos Têm Mais Faltas?', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)
        
        # Legenda de cores
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor='#FF5252', label='Alto (>20%)'),
                          Patch(facecolor='#FFA726', label='Médio (15-20%)'),
                          Patch(facecolor='#66BB6A', label='Baixo (<15%)')]
        ax.legend(handles=legend_elements, loc='lower right', fontsize=10)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def criar_grafico_por_dia_semana(self):
        """Gráfico: No-show por dia da semana"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Calcular taxas por dia da semana
        dia_data = self.df.groupby('DIA_SEMANA_NOME')['NO-SHOW'].mean() * 100
        dias_ordenados = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        dia_data = dia_data.reindex([d for d in dias_ordenados if d in dia_data.index])
        
        # Cores diferentes por dia
        colors_days = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', 
                      '#98D8C8', '#FFE66D', '#A8E6CF'][:len(dia_data)]
        
        bars = ax.bar(range(len(dia_data)), dia_data.values,
                     color=colors_days, width=0.7,
                     edgecolor='white', linewidth=2)
        
        # Adicionar valores
        for bar, val in zip(bars, dia_data.values):
            ax.text(bar.get_x() + bar.get_width()/2, val + 0.5,
                   f'{val:.1f}%',
                   ha='center', fontsize=11, fontweight='bold')
        
        # Destacar melhor e pior dia
        melhor_dia_idx = np.argmin(dia_data.values)
        pior_dia_idx = np.argmax(dia_data.values)
        
        ax.annotate('Melhor dia!',
                   xy=(melhor_dia_idx, dia_data.values[melhor_dia_idx]),
                   xytext=(melhor_dia_idx, dia_data.values[melhor_dia_idx] + 5),
                   arrowprops=dict(arrowstyle='->', color='green', lw=2),
                   fontsize=10, fontweight='bold', ha='center',
                   bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))
        
        ax.set_xticks(range(len(dia_data)))
        ax.set_xticklabels(dia_data.index, fontsize=11, rotation=45, ha='right')
        ax.set_ylabel('Taxa de Faltas (%)', fontsize=12, fontweight='bold')
        ax.set_title('Qual Dia da Semana Tem Mais Faltas?', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3)
        ax.set_ylim(0, max(dia_data.values) * 1.3)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def criar_grafico_temperatura(self):
        """Gráfico: Influência da temperatura"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Calcular taxas por classificação de temperatura
        temp_data = self.df.groupby('CLASSIFICACAO_TEMP')['NO-SHOW'].mean() * 100
        ordem_temp = ['MUITO FRIO', 'FRIO', 'AGRADÁVEL', 'QUENTE', 'MUITO QUENTE']
        temp_data = temp_data.reindex([t for t in ordem_temp if t in temp_data.index])
        
        # Cores de temperatura
        temp_colors = ['#5DADE2', '#85C1E2', '#95D5B2', '#FFD93D', '#FF6B6B'][:len(temp_data)]
        
        bars = ax.bar(range(len(temp_data)), temp_data.values,
                     color=temp_colors, width=0.6,
                     edgecolor='white', linewidth=2)
        
        # Adicionar valores
        for bar, val in zip(bars, temp_data.values):
            ax.text(bar.get_x() + bar.get_width()/2, val + 0.5,
                   f'{val:.1f}%',
                   ha='center', fontsize=11, fontweight='bold')
        
        ax.set_xticks(range(len(temp_data)))
        ax.set_xticklabels(temp_data.index, fontsize=11, rotation=30, ha='right')
        ax.set_ylabel('Taxa de Faltas (%)', fontsize=12, fontweight='bold')
        ax.set_title('O Clima Influencia no Comparecimento?', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3)
        ax.set_ylim(0, max(temp_data.values) * 1.2)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def criar_grafico_antecedencia(self):
        """Gráfico: No-show por antecedência do agendamento"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Criar faixas de antecedência
        bins_antec = [0, 1, 3, 7, 14, 30, 365]
        labels_antec = ['Mesmo dia', '1-2 dias', '3-6 dias', 
                       '1-2 semanas', '2-4 semanas', '1+ mês']
        self.df['FAIXA_ANTECEDENCIA'] = pd.cut(self.df['DIAS_ANTECEDENCIA'], 
                                                bins=bins_antec, labels=labels_antec)
        
        antec_data = self.df.groupby('FAIXA_ANTECEDENCIA')['NO-SHOW'].mean() * 100
        
        # Gráfico de linha com área
        x = range(len(antec_data))
        ax.plot(x, antec_data.values, marker='o', markersize=10, 
               linewidth=3, color='#E74C3C', 
               markerfacecolor='white', markeredgewidth=3)
        ax.fill_between(x, antec_data.values, alpha=0.3, color='#E74C3C')
        
        # Adicionar valores
        for i, val in enumerate(antec_data.values):
            ax.text(i, val + 1, f'{val:.1f}%',
                   ha='center', fontsize=10, fontweight='bold')
        
        # Insight
        max_idx = np.argmax(antec_data.values)
        ax.annotate('Agendamentos com muita\nantecedência têm mais faltas!',
                   xy=(max_idx, antec_data.values[max_idx]),
                   xytext=(max_idx - 1, antec_data.values[max_idx] + 8),
                   arrowprops=dict(arrowstyle='->', color='red', lw=2),
                   fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='#FFE5E5', alpha=0.9))
        
        ax.set_xticks(x)
        ax.set_xticklabels(antec_data.index, fontsize=10, rotation=30, ha='right')
        ax.set_ylabel('Taxa de Faltas (%)', fontsize=12, fontweight='bold')
        ax.set_title('Agendar Com Antecedência É Bom ou Ruim?', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.grid(alpha=0.3)
        
        plt.tight_layout()
        return self._fig_to_image(fig)
    
    def _fig_to_image(self, fig):
        """Converte figura matplotlib para imagem ReportLab"""
        img_buffer = io.BytesIO()
        fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
        img_buffer.seek(0)
        plt.close(fig)
        return Image(img_buffer, width=16*cm, height=10*cm)
    
    def gerar_pdf(self, nome_arquivo='relatorio_beira_mar.pdf'):
        """Gera o relatório completo em PDF"""
        print("📄 Gerando relatório PDF...")
        
        # Configurar documento
        doc = SimpleDocTemplate(
            nome_arquivo,
            pagesize=A4,
            topMargin=2*cm,
            bottomMargin=2*cm,
            leftMargin=2*cm,
            rightMargin=2*cm
        )
        
        # Container para elementos
        elements = []
        
        # Estilos
        styles = getSampleStyleSheet()
        
        # Estilo customizado para título
        style_titulo = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=28,
            textColor=COR_PRINCIPAL,
            spaceAfter=30,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        style_subtitulo = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=COR_TEXTO,
            spaceAfter=12,
            spaceBefore=12,
            fontName='Helvetica-Bold'
        )
        
        style_texto = ParagraphStyle(
            'CustomBody',
            parent=styles['BodyText'],
            fontSize=11,
            textColor=COR_TEXTO,
            spaceAfter=12,
            alignment=TA_JUSTIFY,
            leading=16
        )
        
        style_destaque = ParagraphStyle(
            'Destaque',
            parent=styles['BodyText'],
            fontSize=12,
            textColor=colors.white,
            backColor=COR_PRINCIPAL,
            spaceAfter=12,
            spaceBefore=12,
            borderPadding=10,
            fontName='Helvetica-Bold',
            alignment=TA_CENTER
        )
        
        # ============= CAPA =============
        elements.append(Spacer(1, 3*cm))
        
        # Título principal
        elements.append(Paragraph(
            "Relatório de Análise",
            style_titulo
        ))
        elements.append(Paragraph(
            "Clínica de Estética Beira-Mar",
            style_titulo
        ))
        
        elements.append(Spacer(1, 1*cm))
        
        # Subtítulo
        elements.append(Paragraph(
            "<b>Análise Completa de Faltas em Consultas</b><br/>"
            "Insights e Recomendações para Melhorar o Comparecimento",
            ParagraphStyle('Subtitle', parent=style_texto, fontSize=14, 
                          alignment=TA_CENTER, textColor=COR_SECUNDARIA)
        ))
        
        elements.append(Spacer(1, 2*cm))
        
        # Data
        data_atual = datetime.now().strftime("%d de %B de %Y")
        meses = {
            'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Março',
            'April': 'Abril', 'May': 'Maio', 'June': 'Junho',
            'July': 'Julho', 'August': 'Agosto', 'September': 'Setembro',
            'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'
        }
        for eng, pt in meses.items():
            data_atual = data_atual.replace(eng, pt)
        
        elements.append(Paragraph(
            f"São Paulo - {data_atual}",
            ParagraphStyle('Date', parent=style_texto, alignment=TA_CENTER, 
                          fontSize=12, textColor=colors.grey)
        ))
        
        elements.append(PageBreak())
        
        # ============= SUMÁRIO EXECUTIVO =============
        elements.append(Paragraph("Sumário Executivo", style_subtitulo))
        elements.append(Spacer(1, 0.5*cm))
        
        # Calcular métricas principais
        total_agendamentos = len(self.df)
        taxa_noshow = self.df['NO-SHOW'].mean() * 100
        taxa_comparecimento = (1 - self.df['NO-SHOW'].mean()) * 100
        
        # SMS Impact
        sms_impact = self.df.groupby('SMS_RECEIVED')['NO-SHOW'].mean()
        reducao_sms = (sms_impact[0] - sms_impact[1]) * 100
        
        elements.append(Paragraph(
            f"Este relatório apresenta uma análise detalhada de <b>{total_agendamentos:,}</b> "
            f"consultas realizadas na Clínica Beira-Mar. O objetivo é identificar os principais "
            f"fatores que levam clientes a faltarem nas consultas agendadas e propor soluções "
            f"práticas para melhorar o comparecimento.",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        
        # Box com principais números
        kpi_data = [
            ['MÉTRICA', 'VALOR'],
            ['Taxa de Comparecimento', f'{taxa_comparecimento:.1f}%'],
            ['Taxa de Faltas', f'{taxa_noshow:.1f}%'],
            ['Redução de Faltas com SMS', f'{reducao_sms:.1f} pontos percentuais'],
            ['Total de Consultas Analisadas', f'{total_agendamentos:,}']
        ]
        
        kpi_table = Table(kpi_data, colWidths=[10*cm, 6*cm])
        kpi_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), COR_PRINCIPAL),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('TOPPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, COR_FUNDO_CLARO]),
        ]))
        
        elements.append(kpi_table)
        elements.append(PageBreak())
        
        # ============= ANÁLISE 1: VISÃO GERAL =============
        elements.append(Paragraph("1. Como Está o Comparecimento Geral?", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        elements.append(Paragraph(
            f"De todas as consultas agendadas, <b>{taxa_comparecimento:.1f}%</b> das clientes "
            f"comparecem normalmente, enquanto <b>{taxa_noshow:.1f}%</b> não aparecem. "
            f"Isso significa que aproximadamente <b>1 em cada 5 consultas</b> resulta em falta.",
            style_texto
        ))
        
        elements.append(self.criar_grafico_taxa_noshow())
        elements.append(PageBreak())
        
        # ============= ANÁLISE 2: IMPACTO DO SMS =============
        elements.append(Paragraph("2. O Lembrete por SMS Funciona?", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        elements.append(Paragraph(
            f"<b>Sim! E muito bem!</b> O envio de lembrete por SMS reduz as faltas em "
            f"<b>{reducao_sms:.1f} pontos percentuais</b>. Clientes que recebem o lembrete "
            f"faltam <b>{sms_impact[1]*100:.1f}%</b> das vezes, enquanto aquelas que não recebem "
            f"faltam <b>{sms_impact[0]*100:.1f}%</b>.",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        
        elements.append(Paragraph(
            "💡 RECOMENDAÇÃO: Enviar SMS automático para TODAS as clientes 24-48h antes da consulta",
            style_destaque
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        elements.append(self.criar_grafico_impacto_sms())
        elements.append(PageBreak())
        
        # ============= ANÁLISE 3: IDADE =============
        elements.append(Paragraph("3. Quem Falta Mais? Análise por Idade", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        idade_noshow = self.df.groupby('FAIXA_ETARIA')['NO-SHOW'].mean().sort_values(ascending=False)
        
        elements.append(Paragraph(
            f"Clientes mais jovens (19-30 anos) apresentam a <b>maior taxa de faltas "
            f"({idade_noshow.values[0]*100:.1f}%)</b>, enquanto clientes acima de 60 anos são as "
            f"mais pontuais (<b>{idade_noshow.values[-1]*100:.1f}%</b> de faltas).",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.3*cm))
        
        elements.append(Paragraph(
            "💡 RECOMENDAÇÃO: Criar estratégias específicas para clientes jovens, como "
            "lembretes mais frequentes ou incentivos para comparecimento",
            style_destaque
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        elements.append(self.criar_grafico_por_faixa_etaria())
        elements.append(PageBreak())
        
        # ============= ANÁLISE 4: PREÇO =============
        elements.append(Paragraph("4. O Preço Influencia no Comparecimento?", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        preco_noshow = self.df.groupby('FAIXA_PRECO')['NO-SHOW'].mean()
        
        elements.append(Paragraph(
            f"<b>Sim, e muito!</b> Procedimentos mais baratos (R$0-50) têm taxa de falta de "
            f"<b>{preco_noshow.values[0]*100:.1f}%</b>, enquanto procedimentos mais caros "
            f"(acima de R$150) têm apenas <b>{preco_noshow.values[-2]*100:.1f}%</b> de faltas. "
            f"Quando as clientes investem mais, elas valorizam mais a consulta.",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.3*cm))
        
        elements.append(Paragraph(
            "💡 RECOMENDAÇÃO: Considerar política de depósito ou sinal para procedimentos "
            "de menor valor, especialmente para clientes com histórico de faltas",
            style_destaque
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        elements.append(self.criar_grafico_por_preco())
        elements.append(PageBreak())
        
        # ============= ANÁLISE 5: SERVIÇOS =============
        elements.append(Paragraph("5. Quais Procedimentos Têm Mais Faltas?", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        servico_noshow = self.df.groupby('SERVICENAME').agg({
            'NO-SHOW': 'mean',
            'ID': 'count'
        })
        servico_noshow = servico_noshow[servico_noshow['ID'] > 50]
        servico_noshow = servico_noshow.sort_values('NO-SHOW', ascending=False)
        
        elements.append(Paragraph(
            f"O serviço com <b>maior taxa de faltas</b> é <i>{servico_noshow.index[0]}</i> "
            f"({servico_noshow['NO-SHOW'].values[0]*100:.1f}%), enquanto "
            f"<i>{servico_noshow.index[-1]}</i> tem a menor taxa "
            f"({servico_noshow['NO-SHOW'].values[-1]*100:.1f}%). Procedimentos mais longos "
            f"e caros tendem a ter melhor comparecimento.",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        elements.append(self.criar_grafico_por_servico())
        elements.append(PageBreak())
        
        # ============= ANÁLISE 6: DIA DA SEMANA =============
        elements.append(Paragraph("6. Qual o Melhor Dia para Agendar?", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        dia_noshow = self.df.groupby('DIA_SEMANA_NOME')['NO-SHOW'].mean()
        dias_ordenados = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        dia_noshow = dia_noshow.reindex([d for d in dias_ordenados if d in dia_noshow.index])
        
        melhor_dia = dia_noshow.idxmin()
        pior_dia = dia_noshow.idxmax()
        
        elements.append(Paragraph(
            f"O <b>{melhor_dia}</b> tem a menor taxa de faltas ({dia_noshow.min()*100:.1f}%), "
            f"sendo o melhor dia para agendamentos importantes. Já a <b>{pior_dia}</b> "
            f"apresenta mais faltas ({dia_noshow.max()*100:.1f}%).",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        elements.append(self.criar_grafico_por_dia_semana())
        elements.append(PageBreak())
        
        # ============= ANÁLISE 7: TEMPERATURA/CLIMA =============
        elements.append(Paragraph("7. O Clima Influencia?", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        temp_noshow = self.df.groupby('CLASSIFICACAO_TEMP')['NO-SHOW'].mean()
        
        elements.append(Paragraph(
            "Sim! Dias muito frios ou muito quentes apresentam ligeiramente mais faltas. "
            "O clima agradável favorece o comparecimento. Isso é especialmente importante "
            "para clientes que dependem de transporte público.",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        elements.append(self.criar_grafico_temperatura())
        elements.append(PageBreak())
        
        # ============= ANÁLISE 8: ANTECEDÊNCIA =============
        elements.append(Paragraph("8. Agendar Com Antecedência É Bom?", style_subtitulo))
        elements.append(Spacer(1, 0.3*cm))
        
        elements.append(Paragraph(
            "Com moderação! Agendamentos feitos com <b>muita antecedência</b> (mais de 1 mês) "
            "têm taxa de falta significativamente maior. O ideal é agendar com <b>1-2 semanas "
            "de antecedência</b>, combinado com lembretes por SMS próximo à data.",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.3*cm))
        
        elements.append(Paragraph(
            "💡 RECOMENDAÇÃO: Para agendamentos feitos com muita antecedência, enviar "
            "múltiplos lembretes: 1 semana antes, 3 dias antes e 1 dia antes",
            style_destaque
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        elements.append(self.criar_grafico_antecedencia())
        elements.append(PageBreak())
        
        # ============= RECOMENDAÇÕES FINAIS =============
        elements.append(Paragraph("Recomendações Práticas", style_subtitulo))
        elements.append(Spacer(1, 0.5*cm))
        
        recomendacoes = [
            {
                'titulo': '📱 1. Sistema de Lembretes Automáticos',
                'descricao': f'Implementar envio automático de SMS para todas as clientes. '
                            f'Isso pode reduzir as faltas em até {reducao_sms:.0f}%, '
                            f'representando economia significativa de tempo e dinheiro.'
            },
            {
                'titulo': '💰 2. Política de Sinal para Serviços Específicos',
                'descricao': f'Considerar solicitar sinal de 20-30% para procedimentos de menor '
                            f'valor ou para clientes com histórico de faltas, especialmente '
                            f'na faixa etária 19-30 anos.'
            },
            {
                'titulo': '🎯 3. Estratégia por Faixa Etária',
                'descricao': f'Criar abordagens diferenciadas: lembretes extras para clientes '
                            f'mais jovens, e programas de fidelidade para clientes acima de 45 anos '
                            f'(que já são pontuais).'
            },
            {
                'titulo': '📅 4. Otimização da Agenda',
                'descricao': f'Priorizar {melhor_dia} para procedimentos mais importantes ou caros. '
                            f'Usar {pior_dia} para atendimentos de menor valor ou clientes fiéis.'
            },
            {
                'titulo': '⏰ 5. Gestão de Antecedência',
                'descricao': f'Para agendamentos com mais de 1 mês de antecedência, criar sistema '
                            f'de confirmação 1 semana antes. Se não confirmar, abrir a vaga para '
                            f'lista de espera.'
            },
            {
                'titulo': '🌡️ 6. Considerar Fatores Climáticos',
                'descricao': f'Nos dias de previsão de clima muito ruim, enviar lembrete adicional '
                            f'e oferecer facilidade de reagendamento sem multa se necessário.'
            }
        ]
        
        for rec in recomendacoes:
            elements.append(Paragraph(f"<b>{rec['titulo']}</b>", 
                                    ParagraphStyle('RecTitle', parent=style_texto, 
                                                  fontSize=12, textColor=COR_SECUNDARIA)))
            elements.append(Paragraph(rec['descricao'], style_texto))
            elements.append(Spacer(1, 0.3*cm))
        
        elements.append(PageBreak())
        
        # ============= CONCLUSÃO =============
        elements.append(Paragraph("Conclusão", style_subtitulo))
        elements.append(Spacer(1, 0.5*cm))
        
        # Calcular economia potencial
        faltas_atuais = int(total_agendamentos * (taxa_noshow/100))
        economia_com_sms = int(faltas_atuais * (reducao_sms/taxa_noshow))
        valor_medio_consulta = self.df['PRICE'].mean()
        economia_financeira = economia_com_sms * valor_medio_consulta
        
        elements.append(Paragraph(
            f"A análise de <b>{total_agendamentos:,}</b> consultas revelou que as faltas "
            f"podem ser significativamente reduzidas com estratégias simples e de baixo custo. "
            f"A implementação do sistema de lembretes por SMS sozinho pode <b>evitar "
            f"aproximadamente {economia_com_sms} faltas</b>, representando uma economia potencial "
            f"de <b>R$ {economia_financeira:,.2f}</b> em receita recuperada.",
            style_texto
        ))
        
        elements.append(Spacer(1, 0.5*cm))
        
        elements.append(Paragraph(
            "Além do impacto financeiro direto, a redução de faltas permite:",
            style_texto
        ))
        
        beneficios = [
            "Melhor aproveitamento da agenda e redução de horários ociosos",
            "Atendimento a mais clientes na lista de espera",
            "Melhor planejamento de estoque e materiais",
            "Redução do estresse da equipe com reagendamentos",
            "Melhoria na experiência geral das clientes"
        ]
        
        for beneficio in beneficios:
            elements.append(Paragraph(f"• {beneficio}", style_texto))
        
        elements.append(Spacer(1, 1*cm))
        
        elements.append(Paragraph(
            "<b>As ações recomendadas neste relatório são práticas, de baixo custo e podem ser "
            "implementadas gradualmente, priorizando aquelas com maior impacto imediato.</b>",
            ParagraphStyle('FinalBox', parent=style_destaque, 
                          backColor=COR_SECUNDARIA, fontSize=11)
        ))
        
        elements.append(Spacer(1, 1*cm))
        
        elements.append(Paragraph(
            f"<i>Relatório gerado em {data_atual}</i><br/>"
            f"<i>Clínica de Estética Beira-Mar - São Paulo</i>",
            ParagraphStyle('Footer', parent=style_texto, 
                          fontSize=9, textColor=colors.grey, alignment=TA_CENTER)
        ))
        
        # Gerar PDF
        doc.build(elements)
        print(f"✅ Relatório gerado com sucesso: {nome_arquivo}")
        return nome_arquivo


def main():
    """Função principal para gerar o relatório"""
    print("🚀 Iniciando geração do Relatório Beira-Mar...")
    print()
    
    # Lista de caminhos possíveis para o arquivo de dados
    caminhos_possiveis = [
        '../../dados/refined/cancelamentos_com_clima.csv',
        '../dados/refined/cancelamentos_com_clima.csv',
        'dados/refined/cancelamentos_com_clima.csv',
        '../../cancelamentos_com_clima.csv',
        '../cancelamentos_com_clima.csv',
        'cancelamentos_com_clima.csv',
        'dados_beira_mar.csv'
    ]
    
    # Procurar arquivo
    arquivo_dados = None
    print("📂 Procurando arquivo de dados...")
    
    import os
    for caminho in caminhos_possiveis:
        if os.path.exists(caminho):
            arquivo_dados = caminho
            print(f"✅ Arquivo encontrado: {caminho}")
            break
        else:
            print(f"   Tentando: {caminho} ... não encontrado")
    
    if arquivo_dados is None:
        print("\n❌ ERRO: Arquivo de dados não encontrado!")
        print("\n💡 Soluções:")
        print("   1. Execute o script 'verificar_dados.py' primeiro")
        print("   2. Certifique-se de estar na pasta correta")
        print("   3. Ajuste o caminho do arquivo manualmente na linha 741")
        print(f"\n   Diretório atual: {os.getcwd()}")
        print("\n   Caminhos tentados:")
        for caminho in caminhos_possiveis:
            print(f"      • {caminho}")
        return
    
    print()
    
    try:
        # Criar instância do gerador
        relatorio = RelatorioBeiraMar(arquivo_dados)
        
        # Gerar PDF
        nome_arquivo = relatorio.gerar_pdf('Relatorio_Analise_Beira_Mar.pdf')
        
        print()
        print("=" * 60)
        print("🎉 RELATÓRIO GERADO COM SUCESSO!")
        print("=" * 60)
        print(f"📁 Arquivo: {nome_arquivo}")
        print("📊 O relatório está pronto para ser apresentado!")
        print()
        
    except FileNotFoundError:
        print("❌ ERRO: Arquivo de dados não encontrado!")
        print(f"   Certifique-se de que o arquivo '{arquivo_dados}' existe.")
        print("   Execute 'python verificar_dados.py' para diagnosticar o problema.")
    except KeyError as e:
        print(f"❌ ERRO: Coluna obrigatória não encontrada: {e}")
        print("   Execute 'python verificar_dados.py' para ver quais colunas estão faltando.")
    except Exception as e:
        print(f"❌ ERRO ao gerar relatório: {str(e)}")
        print("\n   Execute 'python verificar_dados.py' para diagnosticar o problema.")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()