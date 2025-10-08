# ...existing code...
df_med = pd.read_csv(r"C:\Users\demor\Documents\beira-mar-data-analytics\04. Fato\raw\medical_appointment_no_show.csv")
df_clima = pd.read_csv(r"C:\Users\demor\Documents\beira-mar-data-analytics\04. Fato\raw\meteorologia_tratada_2016.csv")

# Renomeando a primeira coluna do arquivo de meteorologia para 'DATA'
df_clima = df_clima.rename(columns={df_clima.columns[0]: 'DATA'})

# Convertendo a coluna SCHEDULEDDAY para datetime com formato específico
df_med['SCHEDULEDDAY'] = pd.to_datetime(df_med['SCHEDULEDDAY'], dayfirst=True)
df_med['DATA'] = df_med['SCHEDULEDDAY'].dt.date

# Convertendo a coluna DATA do arquivo de meteorologia para datetime
df_clima['DATA'] = pd.to_datetime(df_clima['DATA'], dayfirst=True).dt.date

# Fazendo o merge dos dataframes usando a data como chave
df_final = pd.merge(df_med, df_clima, on='DATA', how='left')

# Removendo a coluna DATA auxiliar que foi criada para o merge
df_final = df_final.drop(columns=['DATA'])

# Salvando o arquivo final
output_path = r"C:\Users\demor\Documents\beira-mar-data-analytics\04. Fato\trusted\appointments_with_weather_2016.csv"

# Criando a pasta trusted se ela não existir
import os
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df_final.to_csv(output_path, index=False)
print("Processamento concluído! Arquivo salvo em:", output_path)

# Convertendo a coluna SCHEDULEDDAY para datetime
df_med['SCHEDULEDDAY'] = pd.to_datetime(df_med['SCHEDULEDDAY'])
df_med['DATA'] = df_med['SCHEDULEDDAY'].dt.date

# Convertendo a coluna DATA do arquivo de meteorologia para datetime
df_clima['DATA'] = pd.to_datetime(df_clima['DATA']).dt.date

# Fazendo o merge dos dataframes usando a data como chave
df_final = pd.merge(df_med, df_clima, on='DATA', how='left')

# Removendo a coluna DATA auxiliar que foi criada para o merge
df_final = df_final.drop(columns=['DATA'])

# Salvando o arquivo final
output_path = r"C:\Users\demor\Documents\beira-mar-data-analytics\04. Fato\trusted\appointments_with_weather.csv"
df_final.to_csv(output_path, index=False)

print("Processamento concluído! Arquivo salvo em:", output_path)