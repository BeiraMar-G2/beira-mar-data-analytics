# %%
import pandas as pd
import unicodedata
import glob
import os


# %% [markdown]
# ### Sumário de padrões para tratamento de dados:
# 
# - Data: DD/MM/YYYY
# 
# - Data hora: DD/MM/YYYY HH:MM:SS
# 
# - Binário: 0 ou 1
# 
# - Strings e colunas: Maiusculas e sem acento: 
# 
# - Espaços: Manter apenas um espaço no intervalo entre palavras
# 
# - Sexo: M e F
# 
# - Delimitador: Vírgula
# 
# - Armazenamento: Todos os arquivos em .csv
# 

# %% [markdown]
# ### Funções para padronização:

# %%
# Formato em 2016-04-29T18:38:08Z
def padronizar_data_hora(df, coluna):

  df[coluna] = pd.to_datetime(df[coluna])
  
  df[coluna] = df[coluna].dt.strftime('%d/%m/%Y %H:%M:%S')
  
  return df


# %%
#Formato em MM/DD/AA
def padronizar_data(df, coluna):

  df[coluna] = pd.to_datetime(df[coluna], format='%m/%d/%Y')
  
  df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')
  
  return df

# %%
def padronizar_colunas(df):

    df.columns = df.columns.str.upper()
    
    return df

# %%
def converter_para_binario(df, coluna):
    mapeamento = {'Yes': 1, 'No': 0}
    df[coluna].replace(mapeamento, inplace=True)
    return df

# %%
def remover_acentos(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df


# %%
def padronizar_maiusculo(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].astype(str).str.upper()
    return df

# %%
df_med.head()

# %%
df_HairSalon.head()

# %% [markdown]
# ### Tratamento de dados

# %%
df_med_raw = pd.read_csv("01. Dataset - Medical Appointment No Shows/raw/medical_appointments.csv")


# %%
df_med = df_med_raw

# %%
df_med = padronizar_data_hora(df_med, 'ScheduledDay')
df_med = padronizar_data_hora(df_med, 'AppointmentDay')
df_med = padronizar_colunas(df_med)
df_med = converter_para_binario(df_med, 'NO-SHOW')
df_med = remover_acentos(df_med)
df_med = padronizar_maiusculo(df_med)

# %%
df_med_trusted = df_med
df_med_trusted.to_csv('01. Dataset - Medical Appointment No Shows/trusted/medical_appointment_no_show.csv')

# %%
df_HairSalon_row = pd.read_csv("02. Dataset - Hair Salon No-Show/raw/Client Cancellations0.csv")
df_HairSalon = df_HairSalon_row

# %%
df_HairSalon = padronizar_data(df_HairSalon,'Cancel Date' )
df_HairSalon = padronizar_data(df_HairSalon,'Booking Date' )
df_HairSalon = padronizar_colunas(df_HairSalon)
df_HairSalon = padronizar_maiusculo(df_HairSalon)
df_HairSalon = remover_acentos(df_HairSalon)

# %%
df_HairSalon_trusted = df_HairSalon
df_med_trusted.to_csv('02. Dataset - Hair Salon No-Show/trusted/hair_salon_client_cancel.csv')

# %%

df = pd.read_csv('03. Dataset - Meteorologia/raw/meteorologia2016.csv', ignore_index=True)

# %%


df.columns = [
    "DATA",
    "HORA_UTC",
    "PRECIPITACAO_MM",
    "PRESSAO_ESTACAO_MB",
    "PRESSAO_MAX_MB",
    "PRESSAO_MIN_MB",
    "RADIACAO_KJ_M2",
    "TEMP_AR_C",
    "TEMP_ORVALHO_C",
    "TEMP_MAX_C",
    "TEMP_MIN_C",
    "TEMP_ORVALHO_MAX_C",
    "TEMP_ORVALHO_MIN_C",
    "UMIDADE_MAX",
    "UMIDADE_MIN",
    "UMIDADE_RELATIVA",
    "VENTO_DIRECAO_GRAUS",
    "VENTO_RAJADA_MAX_MS",
    "VENTO_VELOCIDADE_MS",
    "DESCARTAR"
]

df = df.drop(columns=["DESCARTAR"])

df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")

df["HORA_UTC"] = pd.to_datetime(df["HORA_UTC"], format="%H:%M", errors="coerce").dt.time


num_cols = df.columns.drop(["DATA", "HORA_UTC"])
for col in num_cols:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .replace("-9999", "0")
        .astype(float)
    )


df_diario = df.groupby("DATA").agg({
    "PRECIPITACAO_MM": "sum",
    "TEMP_AR_C": ["mean", "max", "min"],
    "UMIDADE_RELATIVA": "mean",
    "VENTO_VELOCIDADE_MS": "mean"
}).reset_index()


df_diario.columns = [
    "DATA",
    "PRECIPITACAO_TOTAL_MM",
    "TEMP_MEDIA_C",
    "TEMP_MAXIMA_C",
    "TEMP_MINIMA_C",
    "UMIDADE_MEDIA",
    "VENTO_VELOCIDADE_MEDIA_MS"
]


num_cols_diario = df_diario.columns.drop("DATA")
df_diario[num_cols_diario] = df_diario[num_cols_diario].round(2)


df_diario["DATA_ANO_ANTERIOR"] = df_diario["DATA"] - pd.DateOffset(years=1)

df_aux = df_diario[["DATA", "TEMP_MEDIA_C", "TEMP_MAXIMA_C", "TEMP_MINIMA_C", "UMIDADE_MEDIA", "VENTO_VELOCIDADE_MEDIA_MS"]].copy()
df_aux = df_aux.rename(columns={
    "DATA": "DATA_ANO_ANTERIOR",
    "TEMP_MEDIA_C": "TEMP_MEDIA_C_ANO_ANTERIOR",
    "TEMP_MAXIMA_C": "TEMP_MAXIMA_C_ANO_ANTERIOR",
    "TEMP_MINIMA_C": "TEMP_MINIMA_C_ANO_ANTERIOR",
    "UMIDADE_MEDIA": "UMIDADE_MEDIA_ANO_ANTERIOR",
    "VENTO_VELOCIDADE_MEDIA_MS": "VENTO_VELOCIDADE_MEDIA_MS_ANO_ANTERIOR"
})

df_diario = df_diario.merge(df_aux, on="DATA_ANO_ANTERIOR", how="left")

df_diario = df_diario.drop(columns=[
    "DATA_ANO_ANTERIOR",
    "TEMP_MEDIA_C_ANO_ANTERIOR",
    "TEMP_MAXIMA_C_ANO_ANTERIOR",
    "TEMP_MINIMA_C_ANO_ANTERIOR",
    "UMIDADE_MEDIA_ANO_ANTERIOR",
    "VENTO_VELOCIDADE_MEDIA_MS_ANO_ANTERIOR"
])


def definir_estacao(data):
    mes = data.month
    dia = data.day
    if (mes == 12 and dia >= 21) or (mes in [1, 2]) or (mes == 3 and dia < 21):
        return "VERAO"
    elif (mes == 3 and dia >= 21) or (mes in [4, 5]) or (mes == 6 and dia < 21):
        return "OUTONO"
    elif (mes == 6 and dia >= 21) or (mes in [7, 8]) or (mes == 9 and dia < 22):
        return "INVERNO"
    elif (mes == 9 and dia >= 22) or (mes in [10, 11]) or (mes == 12 and dia < 21):
        return "PRIMAVERA"
    else:
        return pd.NA
df_diario["CLASSIFICACAO_ESTACAO"] = df_diario["DATA"].apply(definir_estacao)


def classificar_temp_media(temp):
    if temp < 10:
        return "MUITO_FRIO"
    elif 10 <= temp < 17:
        return "FRIO"
    elif 17 <= temp < 24:
        return "AGRADAVEL"
    elif 24 <= temp < 30:
        return "QUENTE"
    elif temp >= 30:
        return "MUITO_QUENTE"
    else:
        return pd.NA

df_diario["CLASSIFICACAO_TEMP_MEDIA"] = df_diario["TEMP_MEDIA_C"].apply(classificar_temp_media)


dias_semana = {
    "Monday": "SEGUNDA-FEIRA",
    "Tuesday": "TERÇA-FEIRA",
    "Wednesday": "QUARTA-FEIRA",
    "Thursday": "QUINTA-FEIRA",
    "Friday": "SEXTA-FEIRA",
    "Saturday": "SÁBADO",
    "Sunday": "DOMINGO"
}
df_diario["DIA_SEMANA"] = df_diario["DATA"].dt.day_name().map(dias_semana)

df_diario["DATA"] = df_diario["DATA"].dt.strftime("%d/%m/%Y")

df_diario.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"Base de meteorologia tratada!")



