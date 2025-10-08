import pandas as pd
import unicodedata
import os
import io
import boto3

# Nota: s3fs não é necessário importar explicitamente
# O pandas usa automatamente quando encontra URLs s3://

# Variáveis de ambiente (configuradas no Terraform)
BUCKET_RAW = os.environ.get('BUCKET_RAW', 'raw-beira-mar')
BUCKET_TRUSTED = os.environ.get('BUCKET_TRUSTED', 'trusted-beira-mar')

CHAVE_MED = "medical_appointments.csv"
CHAVE_CLIMA = "meteorologia2016.csv"


def padronizar_data_hora(df, coluna):
    """Padroniza colunas de data e hora para formato brasileiro"""
    df[coluna] = pd.   (df[coluna])
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y %H:%M:%S')
    return df


def padronizar_data(df, coluna):
    """Padroniza datas no formato MM/DD/YYYY para DD/MM/YYYY"""
    df[coluna] = pd.to_datetime(df[coluna], format='%m/%d/%Y')
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')
    return df


def padronizar_data2(df, coluna):
    """Padroniza datas no formato YYYY-MM-DD para DD/MM/YYYY"""
    df[coluna] = pd.to_datetime(df[coluna], format='%Y-%m-%d')
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')
    return df


def padronizar_colunas(df):
    """Converte nomes das colunas para maiúsculas"""
    df.columns = df.columns.str.upper()
    return df


def converter_para_binario(df, coluna):
    """Converte valores Yes/No para 1/0"""
    mapeamento = {'Yes': 1, 'No': 0}
    df[coluna] = df[coluna].replace(mapeamento)
    return df


def remover_acentos(df):
    """Remove acentos de todas as colunas de texto"""
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = (
                df[coluna]
                .astype(str)
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
            )
    return df


def padronizar_maiusculo(df):
    """Converte todas as strings para maiúsculas"""
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].astype(str).str.upper()
    return df


def padronizar_decimal_para_ponto(df):
    """Converte vírgulas decimais para pontos e tenta converter para numérico"""
    colunas_string = df.select_dtypes(include=['object']).columns
    
    for coluna in colunas_string:
        # Substituir vírgula por ponto
        coluna_limpa = df[coluna].astype(str).str.replace(',', '.', regex=False)
        
        # Tentar converter para numérico
        coluna_convertida = pd.to_numeric(coluna_limpa, errors='coerce')
        
        # Se mais de 80% dos valores forem convertidos com sucesso, usar a conversão
        limiar_sucesso = 0.8
        if coluna_convertida.count() / len(coluna_convertida) > limiar_sucesso:
            df[coluna] = coluna_convertida
    
    return df


def lambda_handler(event, context):
    """
    Handler principal da Lambda Function
    Processa dados de consultas médicas e clima, salvando no bucket trusted
    """
    
    print("=" * 60)
    print("🚀 Iniciando processamento ETL")
    print("=" * 60)
    
    # 1. Definir caminhos S3
    path_med_raw = f"s3://{BUCKET_RAW}/{CHAVE_MED}"
    path_clima_raw = f"s3://{BUCKET_RAW}/{CHAVE_CLIMA}"
    
    # 2. Leitura dos dados do S3
    try:
        print(f"\n📖 Lendo dados de medical_appointments...")
        print(f"   Origem: {path_med_raw}")
        df_med = pd.read_csv(path_med_raw)
        print(f"   ✅ {len(df_med)} registros lidos")
        
        print(f"\n📖 Lendo dados de clima...")
        print(f"   Origem: {path_clima_raw}")
        df_clima = pd.read_csv(path_clima_raw, sep=';')
        print(f"   ✅ {len(df_clima)} registros lidos")
        
    except Exception as e:
        print(f"\n❌ ERRO ao ler dados do S3: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro na leitura do S3: {str(e)}'
        }
    
    # 3. Tratamento dos dados médicos
    print(f"\n🔧 Tratando dados médicos...")
    try:
        df_med = padronizar_data_hora(df_med, 'ScheduledDay')
        df_med = padronizar_data_hora(df_med, 'AppointmentDay')
        df_med = padronizar_colunas(df_med)
        df_med = converter_para_binario(df_med, 'NO-SHOW')
        df_med = remover_acentos(df_med)
        df_med = padronizar_maiusculo(df_med)
        
        # Filtrar idades inválidas
        registros_antes = len(df_med)
        df_med = df_med[df_med['AGE'] >= 0]
        registros_removidos = registros_antes - len(df_med)
        
        if registros_removidos > 0:
            print(f"   ⚠️  {registros_removidos} registros com idade negativa removidos")
        
        print(f"   ✅ Dados médicos tratados: {len(df_med)} registros")
        
    except Exception as e:
        print(f"\n❌ ERRO no tratamento de dados médicos: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro no tratamento de dados médicos: {str(e)}'
        }
    
    # 4. Tratamento dos dados climáticos
    print(f"\n🔧 Tratando dados climáticos...")
    try:
        # Renomear colunas
        df_clima.columns = [
            "DATA", "HORA_UTC", "PRECIPITACAO_MM", "PRESSAO_ESTACAO_MB", 
            "PRESSAO_MAX_MB", "PRESSAO_MIN_MB", "RADIACAO_KJ_M2", "TEMP_AR_C", 
            "TEMP_ORVALHO_C", "TEMP_MAX_C", "TEMP_MIN_C", "TEMP_ORVALHO_MAX_C", 
            "TEMP_ORVALHO_MIN_C", "UMIDADE_MAX", "UMIDADE_MIN", "UMIDADE_RELATIVA", 
            "VENTO_DIRECAO_GRAUS", "VENTO_RAJADA_MAX_MS", "VENTO_VELOCIDADE_MS", 
            "DESCARTAR"
        ]
        
        # Remover coluna desnecessária
        df_clima = df_clima.drop(columns=["DESCARTAR"])
        
        # Padronizar data e decimais
        df_clima = padronizar_data2(df_clima, 'DATA')
        df_clima = padronizar_decimal_para_ponto(df_clima)
        
        print(f"   ✅ Dados climáticos tratados: {len(df_clima)} registros")
        
    except Exception as e:
        print(f"\n❌ ERRO no tratamento de dados climáticos: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro no tratamento de dados climáticos: {str(e)}'
        }
    
    # 5. Salvar dados tratados no bucket trusted
    path_med_trusted = f"s3://{BUCKET_TRUSTED}/clinica/medical_appointment_no_show.csv"
    path_clima_trusted = f"s3://{BUCKET_TRUSTED}/clima/clima.csv"
    
    try:
        print(f"\n💾 Salvando dados médicos...")
        print(f"   Destino: {path_med_trusted}")
        df_med.to_csv(path_med_trusted, index=False)
        print(f"   ✅ Salvo com sucesso")
        
        print(f"\n💾 Salvando dados climáticos...")
        print(f"   Destino: {path_clima_trusted}")
        df_clima.to_csv(path_clima_trusted, index=False)
        print(f"   ✅ Salvo com sucesso")
        
    except Exception as e:
        print(f"\n❌ ERRO ao salvar dados no S3: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro ao salvar dados no S3: {str(e)}'
        }
    
    # 6. Retorno de sucesso
    print("\n" + "=" * 60)
    print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
    print("=" * 60)
    
    return {
        'statusCode': 200,
        'body': {
            'mensagem': 'Processamento de dados concluído com sucesso',
            'registros_medicos': len(df_med),
            'registros_clima': len(df_clima),
            'arquivos_gerados': [
                path_med_trusted,
                path_clima_trusted
            ]
        }
    }