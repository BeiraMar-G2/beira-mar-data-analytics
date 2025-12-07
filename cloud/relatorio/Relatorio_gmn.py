import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
from datetime import datetime
import os

# --- CONFIGURAÇÃO DO BANCO ---
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Lqsym@2020', # Alterar senha
    'database': 'BeiraMar'
}

# --- 1. EXTRAÇÃO DE DADOS ---
def get_data():
    conn = mysql.connector.connect(**db_config)
    
    query = """
    SELECT 
        a.id_agendamento,
        s.nome AS servico,
        s.preco,
        a.dt_hora,
        a.valor_pago,
        a.status_agendamento AS status,
        DAYNAME(a.dt_hora) as dia_semana,
        DAYOFWEEK(a.dt_hora) as dia_num
    FROM agendamento a
    JOIN servico s ON a.fk_servico = s.id_servico
    WHERE a.dt_hora BETWEEN DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND CURDATE()
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- 2. ANÁLISE E GERAÇÃO DE GRÁFICOS ---
def generate_charts(df):
    # Configuração de Estilo
    sns.set_theme(style="whitegrid")
    
    # KPI Calculations
    total_agendamentos = len(df)
    faturamento_total = df[df['status'] == 'Concluido']['valor_pago'].sum()
    total_cancelados = len(df[df['status'] == 'Cancelado'])
    taxa_cancelamento = (total_cancelados / total_agendamentos) * 100 if total_agendamentos > 0 else 0
    ticket_medio = df[df['status'] == 'Concluido']['valor_pago'].mean()

    # Dicionário para tradução dos dias
    dias_traducao = {1: 'Dom', 2: 'Seg', 3: 'Ter', 4: 'Qua', 5: 'Qui', 6: 'Sex', 7: 'Sab'}
    df['dia_pt'] = df['dia_num'].map(dias_traducao)

    # GRÁFICO 1: Faturamento vs Cancelamento por Serviço
    plt.figure(figsize=(10, 6))
    servico_stats = df.groupby('servico').agg(
        Qtd=('id_agendamento', 'count'),
        Cancelados=('status', lambda x: (x == 'Cancelado').sum())
    ).reset_index()
    
    # Ordenar por popularidade
    servico_stats = servico_stats.sort_values('Qtd', ascending=False)
    
    bar1 = sns.barplot(data=servico_stats, x='Qtd', y='servico', color='#4a90e2', label='Total')
    sns.barplot(data=servico_stats, x='Cancelados', y='servico', color='#e74c3c', label='Cancelados')
    
    plt.title('Demanda de Serviços x Cancelamentos')
    plt.xlabel('Quantidade')
    plt.ylabel('')
    plt.legend()
    plt.tight_layout()
    plt.savefig('grafico_servicos.png')
    plt.close()

    # GRÁFICO 2: Consultas por Dia da Semana (Heatmap temporal)
    plt.figure(figsize=(10, 5))
    # Ordenar dias corretamente (Seg a Dom ou Dom a Sab)
    dias_ordem = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sab', 'Dom']
    
    agendamentos_dia = df.groupby('dia_pt')['id_agendamento'].count().reindex(dias_ordem).fillna(0)
    
    colors = ['#ffcccc' if x < agendamentos_dia.mean() else '#ccffcc' for x in agendamentos_dia.values]
    
    bars = plt.bar(agendamentos_dia.index, agendamentos_dia.values, color=colors, edgecolor='grey')
    plt.title('Volume de Atendimentos por Dia da Semana')
    plt.ylabel('Qtd. Consultas')
    
    # Adicionar linha de média
    plt.axhline(y=agendamentos_dia.mean(), color='blue', linestyle='--', label=f'Média: {int(agendamentos_dia.mean())}')
    plt.legend()
    
    plt.tight_layout()
    plt.savefig('grafico_dias.png')
    plt.close()

    # GRÁFICO 3: Receita Share (Donut)
    plt.figure(figsize=(6, 6))
    receita_servico = df[df['status'] == 'Concluido'].groupby('servico')['valor_pago'].sum()
    plt.pie(receita_servico, labels=receita_servico.index, autopct='%1.1f%%', startangle=90, colors=sns.color_palette("pastel"))
    plt.title('Composição do Faturamento')
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    plt.tight_layout()
    plt.savefig('grafico_receita.png')
    plt.close()

    return {
        'faturamento': faturamento_total,
        'total': total_agendamentos,
        'cancelados': total_cancelados,
        'taxa_cancel': taxa_cancelamento,
        'ticket': ticket_medio,
        'dia_fraco': agendamentos_dia.idxmin(),
        'dia_forte': agendamentos_dia.idxmax(),
        'servico_top': servico_stats.iloc[0]['servico'],
        'servico_cancel': servico_stats.sort_values('Cancelados', ascending=False).iloc[0]['servico']
    }

# --- 3. GERAÇÃO DO PDF ---
class PDF(FPDF):
    def header(self):
        # Logo ou Nome da Clinica
        self.set_font('Arial', 'B', 20)
        self.set_text_color(23, 162, 184) # Cor Azul Beira-Mar
        self.cell(0, 10, 'Clínica Beira-Mar | Relatório Mensal de BI', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'C')

def create_pdf(kpis):
    pdf = PDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # Data de geração
    pdf.set_font('Arial', '', 10)
    pdf.set_text_color(100)
    pdf.cell(0, 10, f"Gerado em: {datetime.now().strftime('%d/%m/%Y')}", 0, 1, 'R')
    pdf.ln(5)

    # --- SEÇÃO 1: KPIs Principais ---
    pdf.set_font('Arial', 'B', 14)
    pdf.set_text_color(0)
    pdf.cell(0, 10, '1. Saúde Financeira e Operacional', 0, 1)
    
    # Desenhar caixas de KPI
    pdf.set_font('Arial', 'B', 12)
    
    # Faturamento
    pdf.set_fill_color(220, 255, 220) # Verde claro
    pdf.cell(60, 15, f"R$ {kpis['faturamento']:,.2f}", 1, 0, 'C', True)
    
    # Consultas Totais
    pdf.set_fill_color(220, 230, 255) # Azul claro
    pdf.cell(60, 15, f"{kpis['total']} Agendamentos", 1, 0, 'C', True)
    
    # Ticket Médio
    pdf.set_fill_color(255, 250, 205) # Amarelo
    pdf.cell(60, 15, f"Ticket Médio: R$ {kpis['ticket']:.0f}", 1, 1, 'C', True)
    
    pdf.ln(5)
    
    # Linha de Cancelamento
    pdf.set_font('Arial', '', 11)
    cor_texto = (255, 0, 0) if kpis['taxa_cancel'] > 15 else (0, 0, 0)
    pdf.set_text_color(*cor_texto)
    pdf.cell(0, 10, f"Taxa de Cancelamento: {kpis['taxa_cancel']:.1f}% ({kpis['cancelados']} consultas perdidas)", 0, 1)
    pdf.set_text_color(0) # Reset cor
    
    pdf.ln(5)

    # --- SEÇÃO 2: Performance de Serviços ---
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, '2. Análise de Procedimentos', 0, 1)
    
    pdf.image('grafico_servicos.png', x=10, w=180)
    pdf.ln(5)
    
    # Insights Automáticos (Storytelling)
    pdf.set_font('Arial', '', 11)
    pdf.multi_cell(0, 7, f"O carro-chefe da clínica este mês foi o serviço '{kpis['servico_top']}'. \n"
                         f"No entanto, atenção para '{kpis['servico_cancel']}', que apresentou o maior volume absoluto de cancelamentos. "
                         "Sugerimos revisar a política de confirmação para este procedimento específico.")
    pdf.ln(5)

    pdf.image('grafico_receita.png', x=50, w=110)
    pdf.ln(5)

    # --- SEÇÃO 3: Otimização de Agenda ---
    pdf.add_page()
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, '3. Otimização de Agenda', 0, 1)
    
    pdf.image('grafico_dias.png', x=10, w=180)
    
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Insights de Negócio:', 0, 1)
    
    pdf.set_font('Arial', '', 11)
    insight_text = (
        f"- Dia de Maior Movimento: {kpis['dia_forte']}. A equipe deve estar completa nestes dias.\n"
        f"- Dia de Menor Movimento: {kpis['dia_fraco']}. \n\n"
        f"AÇÃO SUGERIDA: Foi identificada uma baixa ocupação nas {kpis['dia_fraco']}s-feiras. "
        "Recomendamos criar uma promoção 'Happy Hour da Beleza' com 15% de desconto para agendamentos neste dia "
        "para equilibrar a agenda e aumentar o faturamento semanal."
    )
    pdf.multi_cell(0, 7, insight_text)

    # Salvar
    pdf.output("Relatorio_BI_BeiraMar.pdf")
    print("Relatório PDF gerado com sucesso: Relatorio_BI_BeiraMar.pdf")

    # Limpar imagens temporárias
    if os.path.exists("grafico_servicos.png"): os.remove("grafico_servicos.png")
    if os.path.exists("grafico_dias.png"): os.remove("grafico_dias.png")
    if os.path.exists("grafico_receita.png"): os.remove("grafico_receita.png")

# --- EXECUÇÃO ---
try:
    print("Conectando ao banco de dados...")
    df_dados = get_data()
    
    if not df_dados.empty:
        print("Dados extraídos. Gerando gráficos e KPIs...")
        kpis = generate_charts(df_dados)
        
        print("Criando PDF...")
        create_pdf(kpis)
    else:
        print("Nenhum dado encontrado no período.")
except Exception as e:
    print(f"Erro: {e}")