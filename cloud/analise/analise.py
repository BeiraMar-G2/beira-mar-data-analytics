# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go

# %%
df = pd.read_csv('../../dados/refined/cancelamentos_com_clima.csv')

# %%
df.head(9)

# %%
df.sample(5)

# %%
df.info()

# %%
df.describe()

# %%


# %%
df['SCHEDULEDDAY'] = pd.to_datetime(df['SCHEDULEDDAY'])
df['APPOINTMENTDAY'] = pd.to_datetime(df['APPOINTMENTDAY'])
# Calculate the average for no-shows only
tempo_medio_antecedencia_faltaram = df[df['NO-SHOW'] == 1]['Antecedencia_Dias'].mean()
# Calculate the difference in days
df['Antecedencia_Dias'] = (df['APPOINTMENTDAY'] - df['SCHEDULEDDAY']).dt.days

# Calculate the average
# Calculate the no-show rate
taxa_no_show = (df['NO-SHOW'].sum() / len(df)) * 100

total_consultas = len(df)
print(f"Total de Consultas Agendadas: {total_consultas}")
print(f"Taxa de No-Show: {taxa_no_show:.2f}%")
tempo_medio_antecedencia = df['Antecedencia_Dias'].mean()

print(f"Tempo Médio de Antecedência: {tempo_medio_antecedencia:.2f} dias")
print(f"Tempo Médio de Antecedência (Faltaram): {tempo_medio_antecedencia_faltaram:.2f} dias")


# %%

fig = go.Figure([
    go.Bar(x=['Sem SMS', 'Com SMS'], 
        y=[no_show_by_sms_dict[0], no_show_by_sms_dict[1]],
        marker=dict(color=['#FF6B6B', '#4ECDC4']),
        text=[f'{no_show_by_sms_dict[0]:.2f}%', f'{no_show_by_sms_dict[1]:.2f}%'],
        textposition='auto')
])

fig.update_layout(
    title='Taxa de No-Show por Recebimento de SMS',
    xaxis_title='SMS Recebido',
    yaxis_title='% de No-Show',
    plot_bgcolor='rgba(240, 240, 240, 0.5)',
    hovermode='x unified',
    template='plotly_white',
    showlegend=False
)

fig.show()

# %%
# Calculate no-show rate by temperature classification
no_show_by_temp = df.groupby('Classificacao_Temp')['NO-SHOW'].apply(lambda x: (x.sum() / len(x)) * 100)

# Define the correct order and labels for temperature classifications (4 categories)
temp_order = ['FRIO', 'AGRADAVEL', 'QUENTE', 'MUITO_QUENTE']
temp_labels = ['Frio\n(< 17°)', 'Agradável\n(17° - 24°)', 'Quente\n(24° - 30°)', 'Muito Quente\n(> 30°)']

# Reindex to ensure correct order and fill missing categories with 0
no_show_by_temp = no_show_by_temp.reindex(temp_order, fill_value=0)

# Ensure we have exactly 4 values
if len(no_show_by_temp) != 4:
    # Create a complete series with all 4 categories
    no_show_by_temp = pd.Series([0, 0, 0, 0], index=temp_order)
    # Recalculate with the available data
    temp_data = df.groupby('Classificacao_Temp')['NO-SHOW'].apply(lambda x: (x.sum() / len(x)) * 100)
    for category in temp_data.index:
        if category in temp_order:
            no_show_by_temp[category] = temp_data[category]

# Create the bar chart with exactly 4 bars
fig_temp = go.Figure([
    go.Bar(x=temp_labels,
           y=no_show_by_temp.values,
           marker=dict(color=['#3498db', '#2ecc71', '#f39c12', '#e74c3c']),
           text=[f'{val:.2f}%' for val in no_show_by_temp.values],
           textposition='auto')
])

fig_temp.update_layout(
    title='Impacto da Temperatura em No-Shows (%)',
    xaxis_title='Classificação de Temperatura',
    yaxis_title='% de No-Show',
    plot_bgcolor='rgba(240, 240, 240, 0.5)',
    hovermode='x unified',
    template='plotly_white',
    showlegend=False
)

fig_temp.show()

# %%
faixa_etaria = ['0-9 anos', '10-19 anos', '20-29 anos', '30-39 anos', '40-49 anos', '50-59 anos', '60-69 anos', '70-79 anos', '80+ anos']
taxa_noshow = [19.9, 25.2, 24.7, 21.8, 20.3, 17.5, 15.1, 15.2, 16.4]

fig_idade = go.Figure([
    go.Bar(
        x=faixa_etaria,
        y=taxa_noshow,
        marker=dict(color=['#e74c3c', '#e67e22', '#f39c12', '#f1c40f', '#2ecc71', '#3498db', '#9b59b6', '#8e44ad', '#34495e']),
        text=[f'{val:.1f}%' for val in taxa_noshow],
        textposition='auto'
    )
])

fig_idade.update_layout(
    title='Taxa de No-Show por Faixa Etária',
    xaxis_title='Faixa Etária',
    yaxis_title='% de No-Show',
    plot_bgcolor='rgba(240, 240, 240, 0.5)',
    hovermode='x unified',
    template='plotly_white',
    showlegend=False
)

fig_idade.show()

# %%
taxa_por_antecedencia = df.groupby('Antecedencia_Dias')['NO-SHOW'].apply(lambda x: (x.sum() / len(x)) * 100).sort_index()

fig_antecedencia = go.Figure([
    go.Scatter(
        x=taxa_por_antecedencia.index,
        y=taxa_por_antecedencia.values,
        mode='lines+markers',
        name='Taxa de No-Show',
        line=dict(color='#e74c3c', width=3),
        marker=dict(size=6)
    )
])

fig_antecedencia.update_layout(
    title='Taxa de No-Show por Dias de Antecedência de Agendamento',
    xaxis_title='Dias de Antecedência',
    yaxis_title='% de No-Show',
    plot_bgcolor='rgba(240, 240, 240, 0.5)',
    hovermode='x unified',
    template='plotly_white',
    showlegend=False
)

fig_antecedencia.show()

# %%
# Boxplot comparando quem compareceu vs quem faltou
fig_boxplot_comp = go.Figure()

# Separar por comparecimento
df_compareceu = df_boxplot[df_boxplot['compareceu'] == True]
df_faltou = df_boxplot[df_boxplot['compareceu'] == False]

fig_boxplot_comp.add_trace(go.Box(
    y=df_compareceu['dias_antecedencia'],
    name='Compareceu',
    marker_color='#27ae60',
    boxmean='sd'
))

fig_boxplot_comp.add_trace(go.Box(
    y=df_faltou['dias_antecedencia'],
    name='Faltou (No-Show)',
    marker_color='#e74c3c',
    boxmean='sd'
))

fig_boxplot_comp.update_layout(
    title={
        'text': 'Dias de Antecedência: Comparecimento vs No-Show',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 20, 'color': '#2c3e50'}
    },
    yaxis_title='Dias de Antecedência',
    showlegend=True,
    height=600,
    template='plotly_white',
    font=dict(size=12),
    hovermode='closest'
)

fig_boxplot_comp.show()


