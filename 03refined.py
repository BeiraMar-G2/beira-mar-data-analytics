# %%
import pandas as pd

df_med = pd.read_csv("dados/trusted/medical_appointment_no_show.csv")
df_clima = pd.read_csv('dados/trusted/clima.csv')

def criar_coluna_estacao(df, coluna_data):
    def _definir_estacao_logica(data):
        if pd.isna(data):
            return pd.NA
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
    coluna_dt = pd.to_datetime(df[coluna_data], format='%d/%m/%Y', errors='coerce')
    df['Estacao_Ano'] = coluna_dt.apply(_definir_estacao_logica)
    return df

df_clima = criar_coluna_estacao(df_clima, 'DATA')

def criar_coluna_classificacao_temp(df, coluna_temp):
    def _classificar_temp_logica(temp):
        if pd.isna(temp) or not isinstance(temp, (int, float)):
            return pd.NA
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
    df['Classificacao_Temp'] = df[coluna_temp].apply(_classificar_temp_logica)
    return df

df_clima = criar_coluna_classificacao_temp(df_clima, 'TEMP_AR_C')

def preparar_df_clima(df_clima):
    coluna_limpa = df_clima['TEMP_AR_C'].astype(str).str.replace(',', '.', regex=False)
    df_clima['TEMP_AR_C'] = pd.to_numeric(coluna_limpa, errors='coerce')
    df_clima['DATA_HORA_CLIMA'] = df_clima['DATA'] + ' ' + df_clima['HORA_UTC']
    df_clima['DATA_HORA_CLIMA'] = pd.to_datetime(df_clima['DATA_HORA_CLIMA'], format='%d/%m/%Y %H:%M', errors='coerce')
    df_clima['CHAVE_HORA'] = df_clima['DATA_HORA_CLIMA']
    return df_clima.drop(columns=['DATA', 'HORA_UTC'])

def preparar_df_med(df_med, coluna_base):
    df_med[coluna_base] = pd.to_datetime(df_med[coluna_base], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    df_med['CHAVE_HORA'] = df_med[coluna_base].dt.floor('H')
    return df_med

df_clima_processado = preparar_df_clima(df_clima.copy())
df_med_processado = preparar_df_med(df_med.copy(), 'SCHEDULEDDAY')

df_final = pd.merge(
    df_med_processado, 
    df_clima_processado, 
    on='CHAVE_HORA', 
    how='left'
)

colunas_para_dropar = [
    'ALCOHOLISM',
    'PRESSAO_ESTACAO_MB',
    'PRESSAO_MAX_MB',
    'PRESSAO_MIN_MB',
    'RADIACAO_KJ_M2',
    'TEMP_ORVALHO_C',
    'TEMP_ORVALHO_MAX_C',
    'TEMP_ORVALHO_MIN_C',
    'UMIDADE_MAX',
    'UMIDADE_MIN',
    'VENTO_DIRECAO_GRAUS',
    'VENTO_RAJADA_MAX_MS',
    'VENTO_VELOCIDADE_MS'
]

df_final.drop(colunas_para_dropar, axis=1, inplace=True)

df_final.to_csv('dados/refined/cancelamentos_com_clima.csv', index=False)


