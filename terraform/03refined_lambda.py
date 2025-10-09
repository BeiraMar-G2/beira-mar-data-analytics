import pandas as pd
import boto3
import io
import os

# Configuração dos buckets
BUCKET_TRUSTED = 'trusted-beira-mar'
BUCKET_REFINED = 'refined-beira-mar'

# Caminhos dos arquivos
CHAVE_MED_TRUSTED = "clinica/medical_appointment_no_show.csv"
CHAVE_CLIMA_TRUSTED = "clima/clima.csv"
CHAVE_REFINED = "clinica_com_clima/cancelamentos_com_clima.csv"

# Cliente S3
s3_client = boto3.client('s3')


def ler_csv_do_s3(bucket, key, **kwargs):
    """Lê arquivo CSV do S3 usando boto3"""
    try:
        print(f"   📥 Lendo: s3://{bucket}/{key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), **kwargs)
        print(f"   ✅ {len(df)} registros lidos")
        return df
    except Exception as e:
        raise Exception(f"Erro ao ler {key} do bucket {bucket}: {str(e)}")


def salvar_csv_no_s3(df, bucket, key):
    """Salva DataFrame como CSV no S3"""
    try:
        print(f"   💾 Salvando: s3://{bucket}/{key}")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        print(f"   ✅ {len(df)} registros salvos")
    except Exception as e:
        raise Exception(f"Erro ao salvar {key} no bucket {bucket}: {str(e)}")


def criar_coluna_estacao(df, coluna_data):
    """Cria coluna com estação do ano baseada na data"""
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
    df['ESTACAO_ANO'] = coluna_dt.apply(_definir_estacao_logica)
    return df


def criar_coluna_classificacao_temp(df, coluna_temp):
    """Classifica temperatura em categorias"""
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
    
    df['CLASSIFICACAO_TEMP'] = df[coluna_temp].apply(_classificar_temp_logica)
    return df


def preparar_df_clima(df_clima):
    """Prepara DataFrame de clima para o merge"""
    # Garantir que temperatura é numérica
    coluna_limpa = df_clima['TEMP_AR_C'].astype(str).str.replace(',', '.', regex=False)
    df_clima['TEMP_AR_C'] = pd.to_numeric(coluna_limpa, errors='coerce')
    
    # Criar coluna de data/hora completa
    df_clima['DATA_HORA_CLIMA'] = df_clima['DATA'] + ' ' + df_clima['HORA_UTC']
    df_clima['DATA_HORA_CLIMA'] = pd.to_datetime(
        df_clima['DATA_HORA_CLIMA'], 
        format='%d/%m/%Y %H:%M', 
        errors='coerce'
    )
    
    # Criar chave de hora para o merge
    df_clima['CHAVE_HORA'] = df_clima['DATA_HORA_CLIMA']
    
    # Remover colunas originais de data/hora
    return df_clima.drop(columns=['DATA', 'HORA_UTC'])


def preparar_df_med(df_med, coluna_base):
    """Prepara DataFrame médico para o merge"""
    # Converter coluna base para datetime
    df_med[coluna_base] = pd.to_datetime(
        df_med[coluna_base], 
        format='%d/%m/%Y %H:%M:%S', 
        errors='coerce'
    )
    
    # Criar chave de hora (arredondada para a hora) para o merge
    df_med['CHAVE_HORA'] = df_med[coluna_base].dt.floor('H')
    
    return df_med


def lambda_handler(event, context):
    """
    Handler principal da Lambda Function
    Integra dados de clima e consultas médicas, salvando no bucket refined
    """
    
    print("=" * 60)
    print("🚀 Iniciando integração TRUSTED → REFINED")
    print("=" * 60)
    
    # Usar variáveis de ambiente ou valores padrão
    bucket_trusted = os.environ.get('BUCKET_TRUSTED', BUCKET_TRUSTED)
    bucket_refined = os.environ.get('BUCKET_REFINED', BUCKET_REFINED)
    
    print(f"\n📦 Buckets configurados:")
    print(f"   TRUSTED: {bucket_trusted}")
    print(f"   REFINED: {bucket_refined}")
    
    # 1. Leitura dos dados do bucket TRUSTED
    try:
        print(f"\n📖 Lendo dados do bucket TRUSTED...")
        df_med = ler_csv_do_s3(bucket_trusted, CHAVE_MED_TRUSTED)
        df_clima = ler_csv_do_s3(bucket_trusted, CHAVE_CLIMA_TRUSTED)
        
    except Exception as e:
        print(f"\n❌ ERRO ao ler dados: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro na leitura: {str(e)}'
        }
    
    # 2. Enriquecimento dos dados de clima
    print(f"\n🔧 Enriquecendo dados de clima...")
    try:
        df_clima = criar_coluna_estacao(df_clima, 'DATA')
        print(f"   ✅ Coluna ESTACAO_ANO criada")
        
        df_clima = criar_coluna_classificacao_temp(df_clima, 'TEMP_AR_C')
        print(f"   ✅ Coluna CLASSIFICACAO_TEMP criada")
        
    except Exception as e:
        print(f"\n❌ ERRO ao enriquecer dados de clima: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro no enriquecimento: {str(e)}'
        }
    
    # 3. Preparação dos DataFrames para integração
    print(f"\n🔧 Preparando dados para integração...")
    try:
        df_clima_processado = preparar_df_clima(df_clima.copy())
        print(f"   ✅ Dados de clima preparados")
        
        df_med_processado = preparar_df_med(df_med.copy(), 'SCHEDULEDDAY')
        print(f"   ✅ Dados médicos preparados")
        
    except Exception as e:
        print(f"\n❌ ERRO na preparação: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro na preparação: {str(e)}'
        }
    
    # 4. Integração (merge) dos dados
    print(f"\n🔗 Integrando dados médicos + clima...")
    try:
        df_final = pd.merge(
            df_med_processado, 
            df_clima_processado, 
            on='CHAVE_HORA', 
            how='left'
        )
        print(f"   ✅ {len(df_final)} registros integrados")
        
        # Verificar % de match
        registros_com_clima = df_final['TEMP_AR_C'].notna().sum()
        percentual_match = (registros_com_clima / len(df_final)) * 100
        print(f"   📊 {percentual_match:.2f}% dos registros têm dados de clima")
        
    except Exception as e:
        print(f"\n❌ ERRO na integração: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro na integração: {str(e)}'
        }
    
    # 5. Remoção de colunas desnecessárias
    print(f"\n🧹 Removendo colunas desnecessárias...")
    try:
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
        
        # Remover apenas colunas que existem
        colunas_existentes = [col for col in colunas_para_dropar if col in df_final.columns]
        df_final.drop(colunas_existentes, axis=1, inplace=True)
        print(f"   ✅ {len(colunas_existentes)} colunas removidas")
        
    except Exception as e:
        print(f"\n❌ ERRO ao remover colunas: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro ao remover colunas: {str(e)}'
        }
    
    # 6. Salvar no bucket REFINED
    try:
        print(f"\n💾 Salvando no bucket REFINED...")
        salvar_csv_no_s3(df_final, bucket_refined, CHAVE_REFINED)
        
    except Exception as e:
        print(f"\n❌ ERRO ao salvar: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro ao salvar: {str(e)}'
        }
    
    # 7. Retorno de sucesso
    print("\n" + "=" * 60)
    print("✅ INTEGRAÇÃO CONCLUÍDA COM SUCESSO!")
    print("=" * 60)
    
    return {
        'statusCode': 200,
        'body': {
            'mensagem': 'Integração concluída com sucesso',
            'registros_totais': len(df_final),
            'registros_com_clima': int(registros_com_clima),
            'percentual_match': f"{percentual_match:.2f}%",
            'colunas_finais': len(df_final.columns),
            'arquivo_gerado': f"s3://{bucket_refined}/{CHAVE_REFINED}"
        }
    }