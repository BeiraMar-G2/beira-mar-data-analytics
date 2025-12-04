# test_transformacao_local.py
import pandas as pd
import os
import sys

# ============================================================
# DETECTAR DIRETÓRIO E ARQUIVOS
# ============================================================

def encontrar_arquivo(nome_arquivo):
    """Procura o arquivo em vários locais possíveis"""
    locais_possiveis = [
        # Relativo ao script
        os.path.join('dados', 'raw', nome_arquivo),
        os.path.join('..', 'dados', 'raw', nome_arquivo),
        
        # Caminho completo da estrutura do projeto
        os.path.join('dados não estruturados', 'dados', 'raw', nome_arquivo),
        os.path.join('..', 'dados não estruturados', 'dados', 'raw', nome_arquivo),
        
        # Diretório atual
        nome_arquivo,
        os.path.join('raw', nome_arquivo),
        
        # Possível localização no OneDrive
        os.path.join(os.path.expanduser('~'), 'OneDrive - SPTech School', 'SPTECH', 
                     'PROJETOPI', '2 ANO', 'beira-mar-data-analytics', 
                     'dados não estruturados', 'dados', 'raw', nome_arquivo)
    ]
    
    for caminho in locais_possiveis:
        if os.path.exists(caminho):
            return os.path.abspath(caminho)
    
    return None


print("=" * 70)
print("🚀 TESTE LOCAL - TRANSFORMAÇÃO DE DADOS")
print("=" * 70)

# Mostrar diretório atual
print(f"\n📂 Diretório atual: {os.getcwd()}")
print(f"📂 Diretório do script: {os.path.dirname(os.path.abspath(__file__))}")

# Procurar arquivos
ARQUIVO_MED = 'medical_appointments.csv'
ARQUIVO_CLIMA = 'meteorologia2016.csv'

print(f"\n🔍 Procurando arquivos...")
caminho_med = encontrar_arquivo(ARQUIVO_MED)
caminho_clima = encontrar_arquivo(ARQUIVO_CLIMA)

if not caminho_med:
    print(f"\n❌ ERRO: Arquivo {ARQUIVO_MED} não encontrado!")
    print(f"\n💡 Por favor, execute o script a partir de um destes diretórios:")
    print(f"   • beira-mar-data-analytics/")
    print(f"   • dados não estruturados/")
    print(f"\nOu coloque os arquivos em uma pasta 'dados/raw/' relativa ao script.")
    sys.exit(1)

if not caminho_clima:
    print(f"\n❌ ERRO: Arquivo {ARQUIVO_CLIMA} não encontrado!")
    sys.exit(1)

print(f"✅ Encontrado: {caminho_med}")
print(f"✅ Encontrado: {caminho_clima}")

# Definir pasta de saída
pasta_saida = os.path.join(os.path.dirname(caminho_med), '..', 'trusted')
os.makedirs(os.path.join(pasta_saida, 'clinica'), exist_ok=True)
os.makedirs(os.path.join(pasta_saida, 'clima'), exist_ok=True)

print(f"📁 Pasta de saída: {os.path.abspath(pasta_saida)}")

# ============================================================
# MAPEAMENTOS DE SERVIÇOS MÉDICOS → ESTÉTICOS
# ============================================================

MAPEAMENTO_SERVICOS = {
    'Biópsias simples': 'Design Simples de Sobrancelhas',
    'Nutricionista': 'Depilação Facial',
    'Fisioterapia': 'Design de Sobrancelhas com Henna',
    'Pediatria': 'Massagem Modeladora',
    'Ginecologia': 'Pump Up (Glúteos) + Eletroestimulação',
    'Dermatologia': 'Massagem Relaxante',
    'Cardiologia': 'Drenagem Linfática',
    'Ecocardiograma': 'Limpeza de Pele',
    'Tomografia': 'Detox Corporal',
    'Ressonancia Magnetica': 'Aplicação de Enzimas',
    'Preenchimento Facial': 'Hidrolipo NA'
}

MAPEAMENTO_PRECOS = {
    'Biópsias simples': 30.00,
    'Nutricionista': 35.00,
    'Fisioterapia': 45.00,
    'Pediatria': 90.00,
    'Ginecologia': 90.00,
    'Dermatologia': 100.00,
    'Cardiologia': 100.00,
    'Ecocardiograma': 150.00,
    'Tomografia': 150.00,
    'Ressonancia Magnetica': 180.00,
    'Preenchimento Facial': 180.00
}

MAPEAMENTO_DURACAO = {
    'Biópsias simples': 30,
    'Nutricionista': 30,
    'Fisioterapia': 90,
    'Pediatria': 40,
    'Ginecologia': 60,
    'Dermatologia': 60,
    'Cardiologia': 60,
    'Ecocardiograma': 120,
    'Tomografia': 120,
    'Ressonancia Magnetica': 60,
    'Preenchimento Facial': 150
}

# ============================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# ============================================================

def mapear_servicos_esteticos(df):
    """Converte serviços médicos para serviços estéticos"""
    print(f"\n🔄 Mapeando serviços médicos → estéticos...")
    
    # DEBUG: Mostrar serviços originais únicos
    servicos_originais = df['ServiceName'].unique()
    print(f"   📋 Serviços originais ({len(servicos_originais)}):")
    for servico in servicos_originais:
        print(f"      - {servico}")
    
    # Aplicar mapeamento de nomes
    print(f"\n   🔀 Aplicando mapeamento de nomes...")
    df['ServiceName'] = df['ServiceName'].map(MAPEAMENTO_SERVICOS).fillna(df['ServiceName'])
    
    servicos_mapeados = df['ServiceName'].unique()
    print(f"   📋 Serviços após mapeamento ({len(servicos_mapeados)}):")
    for servico in servicos_mapeados:
        print(f"      - {servico}")
    
    # Aplicar mapeamento de preços
    print(f"\n   💰 Aplicando mapeamento de preços...")
    
    # Criar dicionário reverso (serviço estético -> serviço médico original)
    reverso = {v: k for k, v in MAPEAMENTO_SERVICOS.items()}
    
    def obter_preco(nome_servico_estetico):
        # Buscar o serviço médico original
        servico_medico = reverso.get(nome_servico_estetico, nome_servico_estetico)
        # Buscar o preço
        preco = MAPEAMENTO_PRECOS.get(servico_medico)
        
        if preco is None:
            print(f"      ⚠️  Preço não encontrado para: {nome_servico_estetico} (original: {servico_medico})")
        
        return preco
    
    df['Price'] = df['ServiceName'].apply(obter_preco)
    
    # Aplicar mapeamento de duração
    print(f"\n   ⏱️  Aplicando mapeamento de duração...")
    
    def obter_duracao(nome_servico_estetico):
        servico_medico = reverso.get(nome_servico_estetico, nome_servico_estetico)
        duracao = MAPEAMENTO_DURACAO.get(servico_medico)
        
        if duracao is None:
            print(f"      ⚠️  Duração não encontrada para: {nome_servico_estetico} (original: {servico_medico})")
        
        return duracao
    
    df['Duration'] = df['ServiceName'].apply(obter_duracao)
    
    # Verificar resultados
    print(f"\n   ✅ Mapeamento concluído!")
    print(f"   📊 Resumo:")
    print(f"      - Serviços únicos: {df['ServiceName'].nunique()}")
    print(f"      - Registros com preço: {df['Price'].notna().sum()}")
    print(f"      - Registros sem preço: {df['Price'].isna().sum()}")
    print(f"      - Registros com duração: {df['Duration'].notna().sum()}")
    print(f"      - Registros sem duração: {df['Duration'].isna().sum()}")
    
    # Mostrar primeiras linhas
    print(f"\n   👀 Primeiras 5 linhas após mapeamento:")
    print(df[['ServiceName', 'Price', 'Duration']].head())
    
    return df


def padronizar_data_hora(df, coluna):
    """Padroniza colunas de data e hora para formato brasileiro"""
    print(f"   🕐 Padronizando {coluna}...")
    df[coluna] = pd.to_datetime(df[coluna])
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y %H:%M:%S')
    return df


def padronizar_data2(df, coluna):
    """Padroniza datas no formato YYYY-MM-DD para DD/MM/YYYY"""
    print(f"   📅 Padronizando {coluna} (YYYY-MM-DD -> DD/MM/YYYY)...")
    df[coluna] = pd.to_datetime(df[coluna], format='%Y-%m-%d')
    df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')
    return df


def padronizar_colunas(df):
    """Converte nomes das colunas para maiúsculas"""
    print(f"   🔠 Convertendo colunas para maiúsculas...")
    print(f"      Antes: {list(df.columns)[:5]}...")
    df.columns = df.columns.str.upper()
    print(f"      Depois: {list(df.columns)[:5]}...")
    return df


def converter_para_binario(df, coluna):
    """Converte valores Yes/No para 1/0"""
    print(f"   🔢 Convertendo {coluna} para binário (Yes/No -> 1/0)...")
    
    # Verificar valores únicos antes
    valores_antes = df[coluna].unique()
    print(f"      Valores únicos antes: {valores_antes}")
    
    mapeamento = {'Yes': 1, 'No': 0}
    df[coluna] = df[coluna].replace(mapeamento)
    
    # Verificar valores únicos depois
    valores_depois = df[coluna].unique()
    print(f"      Valores únicos depois: {valores_depois}")
    
    return df


def remover_acentos(df):
    """Remove acentos de todas as colunas de texto"""
    print(f"   📝 Removendo acentos de colunas de texto...")
    
    colunas_texto = df.select_dtypes(include=['object']).columns
    print(f"      Processando {len(colunas_texto)} colunas de texto...")
    
    for coluna in colunas_texto:
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
    print(f"   🔠 Convertendo strings para MAIÚSCULAS...")
    
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].astype(str).str.upper()
    
    return df


def padronizar_decimal_para_ponto(df):
    """Converte vírgulas decimais para pontos e tenta converter para numérico"""
    print(f"   🔢 Padronizando decimais (vírgula -> ponto)...")
    
    colunas_string = df.select_dtypes(include=['object']).columns
    
    convertidas = 0
    for coluna in colunas_string:
        # Substituir vírgula por ponto
        coluna_limpa = df[coluna].astype(str).str.replace(',', '.', regex=False)
        
        # Tentar converter para numérico
        coluna_convertida = pd.to_numeric(coluna_limpa, errors='coerce')
        
        # Se mais de 80% dos valores forem convertidos com sucesso, usar a conversão
        limiar_sucesso = 0.8
        taxa_sucesso = coluna_convertida.count() / len(coluna_convertida)
        
        if taxa_sucesso > limiar_sucesso:
            df[coluna] = coluna_convertida
            convertidas += 1
    
    print(f"      ✅ {convertidas} colunas convertidas para numérico")
    
    return df


# ============================================================
# SCRIPT PRINCIPAL
# ============================================================

def main():
    # 1. LEITURA DOS DADOS
    print(f"\n📖 ETAPA 1: LEITURA DOS DADOS")
    print("-" * 70)
    
    try:
        print(f"📂 Lendo: {caminho_med}")
        df_med = pd.read_csv(caminho_med)
        print(f"✅ {len(df_med)} registros lidos")
        print(f"📋 Colunas: {list(df_med.columns)}")
        
    except Exception as e:
        print(f"❌ ERRO ao ler dados médicos: {e}")
        import traceback
        traceback.print_exc()
        return
    
    try:
        print(f"\n📂 Lendo: {caminho_clima}")
        df_clima = pd.read_csv(caminho_clima, sep=';')
        print(f"✅ {len(df_clima)} registros lidos")
        print(f"📋 Colunas: {list(df_clima.columns)}")
        
    except Exception as e:
        print(f"❌ ERRO ao ler dados climáticos: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 2. TRATAMENTO DOS DADOS MÉDICOS
    print(f"\n🔧 ETAPA 2: TRATAMENTO DOS DADOS MÉDICOS")
    print("-" * 70)
    
    try:
        # MAPEAR SERVIÇOS (antes de padronizar)
        df_med = mapear_servicos_esteticos(df_med)
        
        # Padronizar datas
        df_med = padronizar_data_hora(df_med, 'ScheduledDay')
        df_med = padronizar_data_hora(df_med, 'AppointmentDay')
        
        # Padronizar colunas
        df_med = padronizar_colunas(df_med)
        
        # Converter No-Show para binário
        df_med = converter_para_binario(df_med, 'NO-SHOW')
        
        # Remover acentos
        df_med = remover_acentos(df_med)
        
        # Padronizar para maiúsculas
        df_med = padronizar_maiusculo(df_med)
        
        # Filtrar idades inválidas
        print(f"\n   🔍 Filtrando idades inválidas...")
        registros_antes = len(df_med)
        df_med = df_med[df_med['AGE'] >= 0]
        registros_removidos = registros_antes - len(df_med)
        
        if registros_removidos > 0:
            print(f"   ⚠️  {registros_removidos} registros removidos")
        else:
            print(f"   ✅ Nenhum registro removido")
        
        print(f"\n✅ Dados médicos tratados: {len(df_med)} registros")
        
    except Exception as e:
        print(f"\n❌ ERRO no tratamento de dados médicos: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 3. TRATAMENTO DOS DADOS CLIMÁTICOS
    print(f"\n🔧 ETAPA 3: TRATAMENTO DOS DADOS CLIMÁTICOS")
    print("-" * 70)
    
    try:
        # Renomear colunas
        print(f"   🏷️  Renomeando colunas...")
        df_clima.columns = [
            "DATA", "HORA_UTC", "PRECIPITACAO_MM", "PRESSAO_ESTACAO_MB", 
            "PRESSAO_MAX_MB", "PRESSAO_MIN_MB", "RADIACAO_KJ_M2", "TEMP_AR_C", 
            "TEMP_ORVALHO_C", "TEMP_MAX_C", "TEMP_MIN_C", "TEMP_ORVALHO_MAX_C", 
            "TEMP_ORVALHO_MIN_C", "UMIDADE_MAX", "UMIDADE_MIN", "UMIDADE_RELATIVA", 
            "VENTO_DIRECAO_GRAUS", "VENTO_RAJADA_MAX_MS", "VENTO_VELOCIDADE_MS", 
            "DESCARTAR"
        ]
        
        # Remover coluna desnecessária
        print(f"   🗑️  Removendo coluna DESCARTAR...")
        df_clima = df_clima.drop(columns=["DESCARTAR"])
        
        # Padronizar data e decimais
        df_clima = padronizar_data2(df_clima, 'DATA')
        df_clima = padronizar_decimal_para_ponto(df_clima)
        
        print(f"\n✅ Dados climáticos tratados: {len(df_clima)} registros")
        
    except Exception as e:
        print(f"\n❌ ERRO no tratamento de dados climáticos: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 4. SALVAR DADOS TRATADOS
    print(f"\n💾 ETAPA 4: SALVANDO DADOS TRATADOS")
    print("-" * 70)
    
    try:
        caminho_med_trusted = os.path.join(pasta_saida, 'clinica', 'medical_appointment_no_show.csv')
        print(f"💾 Salvando: {caminho_med_trusted}")
        df_med.to_csv(caminho_med_trusted, index=False)
        print(f"✅ Salvo ({len(df_med)} registros)")
        
        caminho_clima_trusted = os.path.join(pasta_saida, 'clima', 'clima.csv')
        print(f"\n💾 Salvando: {caminho_clima_trusted}")
        df_clima.to_csv(caminho_clima_trusted, index=False)
        print(f"✅ Salvo ({len(df_clima)} registros)")
        
    except Exception as e:
        print(f"\n❌ ERRO ao salvar: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 5. RESUMO FINAL
    print("\n" + "=" * 70)
    print("✅ PROCESSAMENTO CONCLUÍDO!")
    print("=" * 70)
    print(f"📊 Registros médicos: {len(df_med)}")
    print(f"📊 Registros climáticos: {len(df_clima)}")
    print("=" * 70)


if __name__ == "__main__":
    main()