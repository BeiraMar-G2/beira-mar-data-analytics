# %%
import pandas as pd
import unicodedata
import glob
import os

def padronizar_data_hora(df, coluna):
    df[coluna] = pd.to_datetime(df[coluna])
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y %H:%M:%S')
    return df

def padronizar_data(df, coluna):
    df[coluna] = pd.to_datetime(df[coluna], format='%m/%d/%Y')
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')
    return df

def padronizar_data2(df, coluna):
    df[coluna] = pd.to_datetime(df[coluna], format='%Y-%m-%d')
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')
    return df

def padronizar_colunas(df):
    df.columns = df.columns.str.upper()
    return df

def converter_para_binario(df, coluna):
    mapeamento = {'Yes': 1, 'No': 0}
    df[coluna].replace(mapeamento, inplace=True)
    return df

def remover_acentos(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df

def padronizar_maiusculo(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].astype(str).str.upper()
    return df

def padronizar_decimal_para_ponto(df):
    colunas_string = df.select_dtypes(include=['object']).columns
    for coluna in colunas_string:
        coluna_limpa = df[coluna].astype(str).str.replace(',', '.', regex=False)
        coluna_convertida = pd.to_numeric(coluna_limpa, errors='coerce')
        limiar_sucesso = 0.8
        if coluna_convertida.count() / len(coluna_convertida) > limiar_sucesso:
            df[coluna] = coluna_convertida
    return df

df_med = pd.read_csv("dados/raw/medical_appointments.csv")
df_clima = pd.read_csv('dados/raw/meteorologia2016.csv', sep=';')

df_med = padronizar_data_hora(df_med, 'ScheduledDay')
df_med = padronizar_data_hora(df_med, 'AppointmentDay')
df_med = padronizar_colunas(df_med)
df_med = converter_para_binario(df_med, 'NO-SHOW')
df_med = remover_acentos(df_med)
df_med = padronizar_maiusculo(df_med)
df_med = df_med[df_med['AGE'] >= 0]

df_clima.columns = [
    "DATA", "HORA_UTC", "PRECIPITACAO_MM", "PRESSAO_ESTACAO_MB", "PRESSAO_MAX_MB",
    "PRESSAO_MIN_MB", "RADIACAO_KJ_M2", "TEMP_AR_C", "TEMP_ORVALHO_C", "TEMP_MAX_C",
    "TEMP_MIN_C", "TEMP_ORVALHO_MAX_C", "TEMP_ORVALHO_MIN_C", "UMIDADE_MAX",
    "UMIDADE_MIN", "UMIDADE_RELATIVA", "VENTO_DIRECAO_GRAUS", "VENTO_RAJADA_MAX_MS",
    "VENTO_VELOCIDADE_MS", "DESCARTAR"
]
df_clima = df_clima.drop(columns=["DESCARTAR"])
df_clima = padronizar_data2(df_clima, 'DATA')
df_clima = padronizar_decimal_para_ponto(df_clima)

df_med.to_csv('dados/trusted/medical_appointment_no_show.csv', index=False)
df_clima.to_csv('dados/trusted/clima.csv', index=False)


