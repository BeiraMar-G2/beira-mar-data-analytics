terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.92"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
  required_version = ">= 1.2"
}

provider "aws" {
  region = "us-east-1"
}

# ---------------------------------------------------------
# --- BUCKETS S3 ---
# ---------------------------------------------------------

resource "aws_s3_bucket" "raw" {
  bucket = "raw-beira-mar"
}

resource "aws_s3_bucket" "trusted" {
  bucket = "trusted-beira-mar"
}

resource "aws_s3_object" "trusted_pastas" {
  count   = length(var.trusted_folders)
  bucket  = aws_s3_bucket.trusted.id
  key     = "${var.trusted_folders[count.index]}/"
  content = ""
  etag    = md5("") 
}

resource "aws_s3_bucket" "refined" {
  bucket = "refined-beira-mar"
}

resource "aws_s3_object" "refined_pastas" {
  count   = length(var.refined_folders)
  bucket  = aws_s3_bucket.refined.id
  key     = "${var.refined_folders[count.index]}/"
  content = ""
  etag    = md5("")
}

# Bucket para resultados do Athena
resource "aws_s3_bucket" "athena_results" {
  bucket = "athena-results-beira-mar"
}

# ---------------------------------------------------------
# --- VARIÁVEIS ---
# ---------------------------------------------------------

variable "trusted_folders" {
  description = "Lista de pastas a serem criadas no bucket trusted"
  type        = list(string)
  default     = ["clima", "clinica"]
}

variable "refined_folders" {
  description = "Lista de pastas a serem criadas no bucket refined"
  type        = list(string)
  default     = ["clinica_com_clima"]
}

# ---------------------------------------------------------
# --- IAM ROLE ---
# ---------------------------------------------------------

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# ---------------------------------------------------------
# --- LAMBDA 1: RAW -> TRUSTED ---
# ---------------------------------------------------------

data "archive_file" "lambda_tratamento_zip" {
  type        = "zip"
  source_file = "02tratamento_lambda.py"
  output_path = "02tratamento_lambda.zip"
}

resource "aws_lambda_function" "tratamento_lambda" {
  depends_on = [
    data.aws_iam_role.lab_role,
    data.archive_file.lambda_tratamento_zip
  ]
  
  function_name    = "LambdaTratamentoBeiraMar"
  handler          = "02tratamento_lambda.lambda_handler"
  role             = data.aws_iam_role.lab_role.arn
  
  filename         = data.archive_file.lambda_tratamento_zip.output_path
  source_code_hash = data.archive_file.lambda_tratamento_zip.output_base64sha256
  
  runtime          = "python3.12"
  timeout          = 300
  memory_size      = 512
  
  layers = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:19"]
  
  environment {
    variables = {
      BUCKET_RAW     = aws_s3_bucket.raw.id
      BUCKET_TRUSTED = aws_s3_bucket.trusted.id
    }
  }
}

# ---------------------------------------------------------
# --- LAMBDA 2: TRUSTED -> REFINED ---
# ---------------------------------------------------------

data "archive_file" "lambda_refined_zip" {
  type        = "zip"
  source_file = "03refined_lambda.py"
  output_path = "03refined_lambda.zip"
}

resource "aws_lambda_function" "refined_lambda" {
  depends_on = [
    data.aws_iam_role.lab_role,
    data.archive_file.lambda_refined_zip
  ]
  
  function_name    = "LambdaRefinedBeiraMar"
  handler          = "03refined_lambda.lambda_handler"
  role             = data.aws_iam_role.lab_role.arn
  
  filename         = data.archive_file.lambda_refined_zip.output_path
  source_code_hash = data.archive_file.lambda_refined_zip.output_base64sha256
  
  runtime          = "python3.12"
  timeout          = 600
  memory_size      = 1024
  
  layers = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:19"]
  
  environment {
    variables = {
      BUCKET_TRUSTED = aws_s3_bucket.trusted.id
      BUCKET_REFINED = aws_s3_bucket.refined.id
    }
  }
}

# ---------------------------------------------------------
# --- GLUE DATABASE ---
# ---------------------------------------------------------

# Database para tabela base (REFINED)
resource "aws_glue_catalog_database" "refined_db" {
  name        = "refined_beira_mar"
  description = "Database com dados integrados (clima + consultas)"
}

# Database para modelagem estrela
resource "aws_glue_catalog_database" "star_schema_db" {
  name        = "star_schema_beira_mar"
  description = "Database com modelagem estrela para análises"
}

# ---------------------------------------------------------
# --- GLUE CRAWLER ---
# ---------------------------------------------------------

resource "aws_glue_crawler" "refined_crawler" {
  name          = "refined-clinica-clima-crawler"
  role          = data.aws_iam_role.lab_role.arn
  database_name = aws_glue_catalog_database.refined_db.name
  
  description = "Crawler para catalogar dados do bucket refined"
  
  s3_target {
    path = "s3://${aws_s3_bucket.refined.id}/clinica_com_clima/"
  }
  
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
  
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# ---------------------------------------------------------
# --- ATHENA WORKGROUP ---
# ---------------------------------------------------------

resource "aws_athena_workgroup" "beira_mar_workgroup" {
  name        = "beira-mar-analytics"
  description = "Workgroup para análises de dados Beira Mar"
  
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/output/"
    }
    
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
  }
}

# ---------------------------------------------------------
# --- ATHENA NAMED QUERIES (Modelagem Estrela) ---
# ---------------------------------------------------------

# Query para criar a tabela FATO_CONSULTAS
resource "aws_athena_named_query" "create_fato_consultas" {
  name        = "create_fato_consultas"
  description = "Cria view da tabela fato de consultas"
  database    = aws_glue_catalog_database.star_schema_db.name
  workgroup   = aws_athena_workgroup.beira_mar_workgroup.name
  
  query = <<-EOQ
    CREATE OR REPLACE VIEW ${aws_glue_catalog_database.star_schema_db.name}.fato_consultas AS
    SELECT 
      -- Chaves
      CONCAT(
        CAST(appointmentid AS VARCHAR), '_',
        CAST(scheduledday AS VARCHAR)
      ) AS appointment_id,
      patientid AS patient_id,
      DATE(scheduledday) AS data_agendamento_key,
      DATE(appointmentday) AS data_consulta_key,
      neighbourhood AS bairro_key,
      CONCAT(
        CAST(DATE(data_hora_clima) AS VARCHAR), '_',
        CAST(HOUR(data_hora_clima) AS VARCHAR)
      ) AS clima_key,
      
      -- Atributos
      age AS idade,
      CASE WHEN "no-show" = 0 THEN 1 ELSE 0 END AS compareceu,
      sms_received AS sms_recebido,
      
      -- Métricas agregáveis
      1 AS qtd_consultas,
      CASE WHEN "no-show" = 1 THEN 1 ELSE 0 END AS qtd_no_shows
      
    FROM ${aws_glue_catalog_database.refined_db.name}.clinica_com_clima
    WHERE appointmentid IS NOT NULL
      AND scheduledday IS NOT NULL;
  EOQ
}

# Query para criar DIM_PACIENTE
resource "aws_athena_named_query" "create_dim_paciente" {
  name        = "create_dim_paciente"
  description = "Cria view da dimensão paciente"
  database    = aws_glue_catalog_database.star_schema_db.name
  workgroup   = aws_athena_workgroup.beira_mar_workgroup.name
  
  query = <<-EOQ
    CREATE OR REPLACE VIEW ${aws_glue_catalog_database.star_schema_db.name}.dim_paciente AS
    SELECT DISTINCT
      patientid AS patient_key,
      patientid AS patient_id,
      gender AS genero,
      scholarship AS tem_bolsa,
      hipertension AS tem_hipertensao,
      diabetes AS tem_diabetes,
      handcap AS tem_deficiencia,
      
      -- Atributos derivados
      CASE 
        WHEN age < 18 THEN 'CRIANCA'
        WHEN age < 60 THEN 'ADULTO'
        ELSE 'IDOSO'
      END AS faixa_etaria
      
    FROM ${aws_glue_catalog_database.refined_db.name}.clinica_com_clima
    WHERE patientid IS NOT NULL;
  EOQ
}

# Query para criar DIM_DATA
resource "aws_athena_named_query" "create_dim_data" {
  name        = "create_dim_data"
  description = "Cria view da dimensão temporal"
  database    = aws_glue_catalog_database.star_schema_db.name
  workgroup   = aws_athena_workgroup.beira_mar_workgroup.name
  
  query = <<-EOQ
    CREATE OR REPLACE VIEW ${aws_glue_catalog_database.star_schema_db.name}.dim_data AS
    SELECT DISTINCT
      DATE(dt) AS data_key,
      dt AS data_completa,
      YEAR(dt) AS ano,
      MONTH(dt) AS mes,
      DAY(dt) AS dia,
      DAY_OF_WEEK(dt) AS dia_semana,
      QUARTER(dt) AS trimestre,
      estacao_ano,
      
      -- Atributos derivados
      CASE 
        WHEN DAY_OF_WEEK(dt) IN (6, 7) THEN 'FIM_DE_SEMANA'
        ELSE 'DIA_UTIL'
      END AS tipo_dia,
      
      CASE MONTH(dt)
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Marco'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
      END AS nome_mes
      
    FROM (
      SELECT DISTINCT scheduledday AS dt, estacao_ano
      FROM ${aws_glue_catalog_database.refined_db.name}.clinica_com_clima
      WHERE scheduledday IS NOT NULL
      
      UNION
      
      SELECT DISTINCT appointmentday AS dt, estacao_ano
      FROM ${aws_glue_catalog_database.refined_db.name}.clinica_com_clima
      WHERE appointmentday IS NOT NULL
    ) datas;
  EOQ
}

# Query para criar DIM_BAIRRO
resource "aws_athena_named_query" "create_dim_bairro" {
  name        = "create_dim_bairro"
  description = "Cria view da dimensão bairro"
  database    = aws_glue_catalog_database.star_schema_db.name
  workgroup   = aws_athena_workgroup.beira_mar_workgroup.name
  
  query = <<-EOQ
    CREATE OR REPLACE VIEW ${aws_glue_catalog_database.star_schema_db.name}.dim_bairro AS
    SELECT DISTINCT
      neighbourhood AS bairro_key,
      neighbourhood AS nome_bairro,
      
      -- Você pode adicionar mais atributos aqui no futuro
      -- como: zona (norte/sul/leste/oeste), 
      --       nivel_socioeconomico, etc.
      COUNT(*) OVER (PARTITION BY neighbourhood) AS total_consultas_bairro
      
    FROM ${aws_glue_catalog_database.refined_db.name}.clinica_com_clima
    WHERE neighbourhood IS NOT NULL;
  EOQ
}

# Query para criar DIM_CLIMA
resource "aws_athena_named_query" "create_dim_clima" {
  name        = "create_dim_clima"
  description = "Cria view da dimensão clima"
  database    = aws_glue_catalog_database.star_schema_db.name
  workgroup   = aws_athena_workgroup.beira_mar_workgroup.name
  
  query = <<-EOQ
    CREATE OR REPLACE VIEW ${aws_glue_catalog_database.star_schema_db.name}.dim_clima AS
    SELECT DISTINCT
      CONCAT(
        CAST(DATE(data_hora_clima) AS VARCHAR), '_',
        CAST(HOUR(data_hora_clima) AS VARCHAR)
      ) AS clima_key,
      data_hora_clima,
      temp_ar_c AS temperatura_media,
      temp_max_c AS temperatura_maxima,
      temp_min_c AS temperatura_minima,
      umidade_relativa,
      precipitacao_mm,
      classificacao_temp,
      estacao_ano
      
    FROM ${aws_glue_catalog_database.refined_db.name}.clinica_com_clima
    WHERE data_hora_clima IS NOT NULL;
  EOQ
}

# Query exemplo de análise usando a modelagem estrela
resource "aws_athena_named_query" "analise_no_show_por_clima" {
  name        = "analise_no_show_por_clima"
  description = "Análise de taxa de no-show por condições climáticas"
  database    = aws_glue_catalog_database.star_schema_db.name
  workgroup   = aws_athena_workgroup.beira_mar_workgroup.name
  
  query = <<-EOQ
    SELECT 
      dc.classificacao_temp,
      dc.estacao_ano,
      COUNT(fc.appointment_id) AS total_consultas,
      SUM(fc.qtd_no_shows) AS total_no_shows,
      ROUND(AVG(CAST(fc.qtd_no_shows AS DOUBLE)) * 100, 2) AS taxa_no_show_pct,
      ROUND(AVG(dc.temperatura_media), 1) AS temp_media,
      ROUND(AVG(dc.umidade_relativa), 1) AS umidade_media
      
    FROM ${aws_glue_catalog_database.star_schema_db.name}.fato_consultas fc
    LEFT JOIN ${aws_glue_catalog_database.star_schema_db.name}.dim_clima dc 
      ON fc.clima_key = dc.clima_key
    LEFT JOIN ${aws_glue_catalog_database.star_schema_db.name}.dim_data dd 
      ON fc.data_consulta_key = dd.data_key
      
    WHERE dd.ano = 2016
    GROUP BY dc.classificacao_temp, dc.estacao_ano
    ORDER BY taxa_no_show_pct DESC;
  EOQ
}

# ---------------------------------------------------------
# --- OUTPUTS ---
# ---------------------------------------------------------

output "lambda_tratamento_name" {
  value = aws_lambda_function.tratamento_lambda.function_name
}

output "lambda_refined_name" {
  value = aws_lambda_function.refined_lambda.function_name
}

output "glue_database_refined" {
  value = aws_glue_catalog_database.refined_db.name
}

output "glue_database_star_schema" {
  value = aws_glue_catalog_database.star_schema_db.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.refined_crawler.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.beira_mar_workgroup.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.id
}

output "instrucoes_completas" {
  description = "Instruções completas para usar o pipeline"
  value = <<-EOT
    
    ✅ PIPELINE COMPLETO CONFIGURADO!
    
    📋 PASSO A PASSO:
    
    1️⃣  Envie CSVs para RAW (seu script)
    
    2️⃣  Execute Lambda 1 (RAW → TRUSTED):
        aws lambda invoke --function-name ${aws_lambda_function.tratamento_lambda.function_name} --payload '{}' response1.json
    
    3️⃣  Execute Lambda 2 (TRUSTED → REFINED):
        aws lambda invoke --function-name ${aws_lambda_function.refined_lambda.function_name} --payload '{}' response2.json
    
    4️⃣  Execute Glue Crawler (catalogar REFINED):
        aws glue start-crawler --name ${aws_glue_crawler.refined_crawler.name}
        
        # Aguarde o crawler terminar (1-2 min):
        aws glue get-crawler --name ${aws_glue_crawler.refined_crawler.name} --query 'Crawler.State'
    
    5️⃣  Criar views da Modelagem Estrela no Athena:
        
        a) Acesse o Athena Console: https://console.aws.amazon.com/athena/
        b) Selecione workgroup: ${aws_athena_workgroup.beira_mar_workgroup.name}
        c) Execute as Saved Queries na ordem:
           - create_dim_paciente
           - create_dim_data
           - create_dim_bairro
           - create_dim_clima
           - create_fato_consultas
    
    6️⃣  Testar consulta de exemplo:
        Execute a query: analise_no_show_por_clima
    
    7️⃣  Conectar Grafana:
        Database: ${aws_glue_catalog_database.star_schema_db.name}
        Workgroup: ${aws_athena_workgroup.beira_mar_workgroup.name}
        Results: s3://${aws_s3_bucket.athena_results.id}/output/
    
    📊 Databases criados:
        - ${aws_glue_catalog_database.refined_db.name} (tabela base)
        - ${aws_glue_catalog_database.star_schema_db.name} (modelagem estrela)
    
    🔍 Ver logs:
        aws logs tail /aws/lambda/${aws_lambda_function.tratamento_lambda.function_name} --follow
        aws logs tail /aws/lambda/${aws_lambda_function.refined_lambda.function_name} --follow
    
  EOT
}