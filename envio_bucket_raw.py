import boto3
import os

# ====================== #
#        Config          #
# ====================== #

regiao = 'us-east-1'
nome_bucket_raw = 'raw-beira-mar'

client = boto3.client('s3', region_name=regiao)

pastas_para_enviar = {
    "/01. Dataset - Medical Appointment No Shows/raw/": "ClinicaMed/",
    "/02. Dataset - Hair Salon No-Show/raw/": "Salao/",
    "/03. Dataset - Meteorologia/raw/": "Clima/",
}

# ====================== #
#      Functions         #
# ====================== #

def subir_arquivo(bucket_name, file_path_local, file_path_s3):
    """
    Sobe um arquivo para um bucket S3.
    :param bucket_name: Nome do bucket de destino.
    :param file_path_local: Caminho completo do arquivo local.
    :param file_path_s3: Caminho e nome do arquivo no S3 (ex: 'pasta/nome_arquivo.csv').
    """
    try:
        print(f"Enviando '{file_path_local}' para S3 em '{file_path_s3}'...")
        client.upload_file(file_path_local, bucket_name, file_path_s3)
        print(f"Upload de '{file_path_local}' concluído!")
    except Exception as e:
        print(f"Erro ao enviar o arquivo '{file_path_local}': {e}")

# ====================== #
#        Main            #
# ====================== #


for pasta_local, pasta_s3 in pastas_para_enviar.items():
        if not os.path.isdir(pasta_local):
            print(f"Aviso: Diretório local '{pasta_local}' não encontrado. Pulando...")
            continue

        for nome_arquivo in os.listdir(pasta_local):
            if os.path.isfile(os.path.join(pasta_local, nome_arquivo)):
                caminho_local_completo = os.path.join(pasta_local, nome_arquivo)

                caminho_s3_completo = os.path.join(pasta_s3, nome_arquivo).replace("\\", "/")
                
                subir_arquivo(nome_bucket_raw, caminho_local_completo, caminho_s3_completo)

print("\nProcesso de upload concluído.")