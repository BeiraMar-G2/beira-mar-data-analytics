import pandas as pd
import boto3
import io
import os

# Configuração direta dos buckets
BUCKET_RAW = 'raw-beira-mar'
BUCKET_TRUSTED = 'trusted-beira-mar'

CHAVE_MED = "medical_appointments.csv"
CHAVE_CLIMA = "meteorologia2016.csv"

# Cliente S3
s3_client = boto3.client('s3')


def ler_csv_do_s3(bucket, key, **kwargs):
    """Lê arquivo CSV do S3 usando boto3"""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()), **kwargs)
    except Exception as e:
        raise Exception(f"Erro ao ler {key} do bucket {bucket}: {str(e)}")


def salvar_csv_no_s3(df, bucket, key):
    """Salva DataFrame como CSV no S3"""
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue()
        )
    except Exception as e:
        raise Exception(f"Erro ao salvar {key} no bucket {bucket}: {str(e)}")


def padronizar_data_hora(df, coluna):
    """Padroniza colunas de data e hora para formato brasileiro"""
    df[coluna] = pd.to_datetime(df[coluna])
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
    
    # Usar variáveis de ambiente do Terraform ou valores padrão
    bucket_raw = os.environ.get('BUCKET_RAW', BUCKET_RAW)
    bucket_trusted = os.environ.get('BUCKET_TRUSTED', BUCKET_TRUSTED)
    
    print(f"\n📦 Buckets configurados:")
    print(f"   RAW: {bucket_raw}")
    print(f"   TRUSTED: {bucket_trusted}")
    
    # 1. Leitura dos dados do S3
    try:
        print(f"\n📖 Lendo dados de medical_appointments...")
        print(f"   Origem: s3://{bucket_raw}/{CHAVE_MED}")
        df_med = ler_csv_do_s3(bucket_raw, CHAVE_MED)
        print(f"   ✅ {len(df_med)} registros lidos")
        
        print(f"\n📖 Lendo dados de clima...")
        print(f"   Origem: s3://{bucket_raw}/{CHAVE_CLIMA}")
        df_clima = ler_csv_do_s3(bucket_raw, CHAVE_CLIMA, sep=';')
        print(f"   ✅ {len(df_clima)} registros lidos")
        
    except Exception as e:
        print(f"\n❌ ERRO ao ler dados do S3: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro na leitura do S3: {str(e)}'
        }
    
    # 2. Tratamento dos dados médicos
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
    
    # 3. Tratamento dos dados climáticos
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
    
    # 4. Salvar dados tratados no bucket trusted
    try:
        print(f"\n💾 Salvando dados médicos...")
        key_med = "clinica/medical_appointment_no_show.csv"
        print(f"   Destino: s3://{bucket_trusted}/{key_med}")
        salvar_csv_no_s3(df_med, bucket_trusted, key_med)
        print(f"   ✅ Salvo com sucesso")
        
        print(f"\n💾 Salvando dados climáticos...")
        key_clima = "clima/clima.csv"
        print(f"   Destino: s3://{bucket_trusted}/{key_clima}")
        salvar_csv_no_s3(df_clima, bucket_trusted, key_clima)
        print(f"   ✅ Salvo com sucesso")
        
    except Exception as e:
        print(f"\n❌ ERRO ao salvar dados no S3: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro ao salvar dados no S3: {str(e)}'
        }
    
    # 5. Retorno de sucesso
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
                f"s3://{bucket_trusted}/clinica/medical_appointment_no_show.csv",
                f"s3://{bucket_trusted}/clima/clima.csv"
            ]
        }
    }