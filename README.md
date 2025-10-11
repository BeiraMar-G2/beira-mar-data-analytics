# 🚀 Pipeline Data Analytics - Beira Mar 

## 📁 Estrutura Final do Projeto

```
beira-mar-data-analytics/
├── main.tf                      # Infraestrutura Terraform
├── 02tratamento_lambda.py       # Lambda RAW → TRUSTED
├── 03refined_lambda.py          # Lambda TRUSTED → REFINED
├── run_pipeline.sh              # Script automático completo
└── README.md                    # Este guia
```

---

## 🧹 PASSO 0: Limpar Tudo (se já tentou antes)

```bash
# Dentro da pasta terraform/
terraform destroy
# Digite: yes

# Remover arquivos temporários
rm -f *.zip response*.json terraform.tfstate*
```

---

## 📝 PASSO 1: Criar os Arquivos

### 1.1 Criar `02tratamento_lambda.py`

Copie o código do artifact **"Lambda Trusted para Refined"** (o que usa boto3)

### 1.2 Criar `03refined_lambda.py`

Copie o código do artifact **"Lambda Trusted para Refined"**

### 1.3 Criar `main.tf`

Use o código abaixo (simplificado e sem Named Queries):

```hcl
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
  count   = 2
  bucket  = aws_s3_bucket.trusted.id
  key     = "${element(["clima", "clinica"], count.index)}/"
  content = ""
  etag    = md5("") 
}

resource "aws_s3_bucket" "refined" {
  bucket = "refined-beira-mar"
}

resource "aws_s3_object" "refined_pastas" {
  bucket  = aws_s3_bucket.refined.id
  key     = "clinica_com_clima/"
  content = ""
  etag    = md5("")
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "athena-results-beira-mar"
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
  function_name    = "LambdaTratamentoBeiraMar"
  handler          = "02tratamento_lambda.lambda_handler"
  role             = data.aws_iam_role.lab_role.arn
  filename         = data.archive_file.lambda_tratamento_zip.output_path
  source_code_hash = data.archive_file.lambda_tratamento_zip.output_base64sha256
  runtime          = "python3.12"
  timeout          = 300
  memory_size      = 512
  layers           = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:19"]
  
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
  function_name    = "LambdaRefinedBeiraMar"
  handler          = "03refined_lambda.lambda_handler"
  role             = data.aws_iam_role.lab_role.arn
  filename         = data.archive_file.lambda_refined_zip.output_path
  source_code_hash = data.archive_file.lambda_refined_zip.output_base64sha256
  runtime          = "python3.12"
  timeout          = 600
  memory_size      = 1024
  layers           = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:19"]
  
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

resource "aws_glue_catalog_database" "refined_db" {
  name        = "refined_beira_mar"
  description = "Database com dados integrados (clima + consultas)"
}

resource "aws_glue_catalog_database" "star_schema_db" {
  name        = "star_schema_beira_mar"
  description = "Database com modelagem estrela"
}

# ---------------------------------------------------------
# --- GLUE CRAWLER ---
# ---------------------------------------------------------

resource "aws_glue_crawler" "refined_crawler" {
  name          = "refined-clinica-clima-crawler"
  role          = data.aws_iam_role.lab_role.arn
  database_name = aws_glue_catalog_database.refined_db.name
  
  s3_target {
    path = "s3://${aws_s3_bucket.refined.id}/clinica_com_clima/"
  }
  
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

# ---------------------------------------------------------
# --- ATHENA WORKGROUP ---
# ---------------------------------------------------------

resource "aws_athena_workgroup" "beira_mar_workgroup" {
  name = "beira-mar-analytics"
  
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/output/"
    }
    enforce_workgroup_configuration = true
  }
}

# ---------------------------------------------------------
# --- OUTPUTS ---
# ---------------------------------------------------------

output "bucket_raw" {
  value = aws_s3_bucket.raw.id
}

output "bucket_trusted" {
  value = aws_s3_bucket.trusted.id
}

output "bucket_refined" {
  value = aws_s3_bucket.refined.id
}

output "instrucoes" {
  value = <<-EOT
    ✅ Infraestrutura criada!
    
    Execute: ./run_pipeline.sh
  EOT
}
```

### 1.4 Criar `run_pipeline.sh`

```bash
#!/bin/bash
set -e

echo "═══════════════════════════════════════════════════════"
echo "🚀 PIPELINE BEIRA MAR - ETL COMPLETO"
echo "═══════════════════════════════════════════════════════"

# 1. Lambda RAW → TRUSTED
echo ""
echo "1️⃣  Executando Lambda: RAW → TRUSTED..."
aws lambda invoke \
  --function-name LambdaTratamentoBeiraMar \
  --payload '{}' \
  response1.json > /dev/null

if grep -q '"statusCode": 200' response1.json; then
  echo "✅ Lambda 1 concluída!"
else
  echo "❌ Erro na Lambda 1"
  cat response1.json
  exit 1
fi

# 2. Lambda TRUSTED → REFINED
echo ""
echo "2️⃣  Executando Lambda: TRUSTED → REFINED..."
aws lambda invoke \
  --function-name LambdaRefinedBeiraMar \
  --payload '{}' \
  response2.json > /dev/null

if grep -q '"statusCode": 200' response2.json; then
  echo "✅ Lambda 2 concluída!"
else
  echo "❌ Erro na Lambda 2"
  cat response2.json
  exit 1
fi

# 3. Glue Crawler
echo ""
echo "3️⃣  Iniciando Glue Crawler..."
aws glue start-crawler --name refined-clinica-clima-crawler 2>/dev/null || true

echo "⏳ Aguardando crawler..."
sleep 10

CONTADOR=0
while true; do
    STATUS=$(aws glue get-crawler --name refined-clinica-clima-crawler --query 'Crawler.State' --output text)
    
    if [ "$STATUS" = "READY" ]; then
        echo "✅ Crawler concluído!"
        break
    fi
    
    CONTADOR=$((CONTADOR + 1))
    echo "   Status: $STATUS (${CONTADOR}0s)"
    
    if [ $CONTADOR -gt 30 ]; then
        echo "⚠️  Timeout do crawler"
        exit 1
    fi
    
    sleep 10
done

# 4. Criar Views da Modelagem Estrela
echo ""
echo "4️⃣  Criando Modelagem Estrela..."

DB="star_schema_beira_mar"
WG="beira-mar-analytics"
OUTPUT="s3://athena-results-beira-mar/output/"

function run_query() {
    local NAME=$1
    local SQL=$2
    
    echo "   Criando $NAME..."
    
    EXEC_ID=$(aws athena start-query-execution \
        --query-string "$SQL" \
        --query-execution-context "Database=$DB" \
        --result-configuration "OutputLocation=$OUTPUT" \
        --work-group "$WG" \
        --query 'QueryExecutionId' \
        --output text)
    
    # Aguardar conclusão
    for i in {1..30}; do
        STATUS=$(aws athena get-query-execution \
            --query-execution-id "$EXEC_ID" \
            --query 'QueryExecution.Status.State' \
            --output text)
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
            echo "   ✅ $NAME criada!"
            return 0
        elif [ "$STATUS" = "FAILED" ]; then
            echo "   ❌ Erro ao criar $NAME"
            return 1
        fi
        
        sleep 2
    done
}

# DIM_PACIENTE
run_query "DIM_PACIENTE" "
CREATE OR REPLACE VIEW star_schema_beira_mar.dim_paciente AS
SELECT DISTINCT
  patientid AS patient_key,
  patientid AS patient_id,
  gender AS genero,
  scholarship AS tem_bolsa,
  hipertension AS tem_hipertensao,
  diabetes AS tem_diabetes,
  handcap AS tem_deficiencia,
  CASE 
    WHEN age < 18 THEN 'CRIANCA'
    WHEN age < 60 THEN 'ADULTO'
    ELSE 'IDOSO'
  END AS faixa_etaria
FROM refined_beira_mar.clinica_com_clima
WHERE patientid IS NOT NULL
"

# DIM_DATA
run_query "DIM_DATA" "
CREATE OR REPLACE VIEW star_schema_beira_mar.dim_data AS
SELECT DISTINCT
  DATE(dt) AS data_key,
  dt AS data_completa,
  YEAR(dt) AS ano,
  MONTH(dt) AS mes,
  DAY(dt) AS dia,
  DAY_OF_WEEK(dt) AS dia_semana,
  QUARTER(dt) AS trimestre,
  estacao_ano,
  CASE 
    WHEN DAY_OF_WEEK(dt) IN (6, 7) THEN 'FIM_DE_SEMANA'
    ELSE 'DIA_UTIL'
  END AS tipo_dia
FROM (
  SELECT DISTINCT scheduledday AS dt, estacao_ano
  FROM refined_beira_mar.clinica_com_clima
  WHERE scheduledday IS NOT NULL
  UNION
  SELECT DISTINCT appointmentday AS dt, estacao_ano
  FROM refined_beira_mar.clinica_com_clima
  WHERE appointmentday IS NOT NULL
) datas
"

# DIM_BAIRRO
run_query "DIM_BAIRRO" "
CREATE OR REPLACE VIEW star_schema_beira_mar.dim_bairro AS
SELECT DISTINCT
  neighbourhood AS bairro_key,
  neighbourhood AS nome_bairro
FROM refined_beira_mar.clinica_com_clima
WHERE neighbourhood IS NOT NULL
"

# DIM_CLIMA
run_query "DIM_CLIMA" "
CREATE OR REPLACE VIEW star_schema_beira_mar.dim_clima AS
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
FROM refined_beira_mar.clinica_com_clima
WHERE data_hora_clima IS NOT NULL
"

# FATO_CONSULTAS
run_query "FATO_CONSULTAS" "
CREATE OR REPLACE VIEW star_schema_beira_mar.fato_consultas AS
SELECT 
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
  age AS idade,
  CASE WHEN \"no-show\" = 0 THEN 1 ELSE 0 END AS compareceu,
  sms_received AS sms_recebido,
  1 AS qtd_consultas,
  CASE WHEN \"no-show\" = 1 THEN 1 ELSE 0 END AS qtd_no_shows
FROM refined_beira_mar.clinica_com_clima
WHERE appointmentid IS NOT NULL
  AND scheduledday IS NOT NULL
"

# 5. Teste Final
echo ""
echo "5️⃣  Testando modelagem..."
TEST_QUERY="SELECT COUNT(*) as total FROM star_schema_beira_mar.fato_consultas"

EXEC_ID=$(aws athena start-query-execution \
    --query-string "$TEST_QUERY" \
    --query-execution-context "Database=$DB" \
    --result-configuration "OutputLocation=$OUTPUT" \
    --work-group "$WG" \
    --query 'QueryExecutionId' \
    --output text)

sleep 5

RESULT=$(aws athena get-query-results \
    --query-execution-id "$EXEC_ID" \
    --query 'ResultSet.Rows[1].Data[0].VarCharValue' \
    --output text)

echo "✅ Total de registros na tabela fato: $RESULT"

echo ""
echo "═══════════════════════════════════════════════════════"
echo "✅ PIPELINE COMPLETO EXECUTADO COM SUCESSO!"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "📊 Modelagem Estrela criada:"
echo "   - dim_paciente"
echo "   - dim_data"
echo "   - dim_bairro"
echo "   - dim_clima"
echo "   - fato_consultas"
echo ""
echo "🔗 Conecte o Grafana:"
echo "   Database: star_schema_beira_mar"
echo "   Workgroup: beira-mar-analytics"
echo ""
```

Torne executável:
```bash
chmod +x run_pipeline.sh
```

---

## 🚀 PASSO 2: Configurar AWS

```bash
# Abra AWS Academy → Learner Lab → Start Lab
# AWS Details → Show → Copie as credenciais

export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
```

---

## 🏗️ PASSO 3: Deploy da Infraestrutura

```bash
terraform init
terraform apply
# Digite: yes
```

Aguarde 2-3 minutos.

---

## 📤 PASSO 4: Enviar CSVs

Use seu script para enviar os CSVs para `s3://raw-beira-mar/`

---

## ⚡ PASSO 5: Executar Pipeline Completo

```bash
./run_pipeline.sh
```

**Isso vai:**
1. ✅ Executar Lambda 1 (RAW → TRUSTED)
2. ✅ Executar Lambda 2 (TRUSTED → REFINED)
3. ✅ Executar Glue Crawler (catalogar)
4. ✅ Criar todas as 5 views da modelagem estrela
5. ✅ Testar consulta na tabela fato

**Tempo total: ~3-5 minutos**

---

## 🎯 PASSO 6: Verificar no Athena

1. Console AWS → Athena
2. Selecione database: `star_schema_beira_mar`
3. Você verá 5 views:
   - dim_paciente
   - dim_data
   - dim_bairro
   - dim_clima
   - fato_consultas

Execute query de teste:
```sql
SELECT 
  dc.classificacao_temp,
  COUNT(*) as total,
  SUM(fc.qtd_no_shows) as no_shows,
  ROUND(AVG(CAST(fc.qtd_no_shows AS DOUBLE)) * 100, 2) as taxa_pct
FROM star_schema_beira_mar.fato_consultas fc
LEFT JOIN star_schema_beira_mar.dim_clima dc ON fc.clima_key = dc.clima_key
GROUP BY dc.classificacao_temp
ORDER BY taxa_pct DESC;
```

---

## 📈 PASSO 7: Conectar Grafana

```
Data Source: Amazon Athena
Database: star_schema_beira_mar
Workgroup: beira-mar-analytics
Output Location: s3://athena-results-beira-mar/output/
```

---

## 🧹 Limpar Tudo

```bash
# Esvaziar buckets
aws s3 rm s3://raw-beira-mar/ --recursive
aws s3 rm s3://trusted-beira-mar/ --recursive
aws s3 rm s3://refined-beira-mar/ --recursive
aws s3 rm s3://athena-results-beira-mar/ --recursive

# Destruir infraestrutura
terraform destroy
```

---

## ✅ Checklist Final

- [ ] AWS Academy iniciado
- [ ] Credenciais configuradas
- [ ] 3 arquivos Python criados
- [ ] main.tf criado
- [ ] run_pipeline.sh criado e executável
- [ ] `terraform apply` executado
- [ ] CSVs enviados para RAW
- [ ] `./run_pipeline.sh` executado com sucesso
- [ ] Views aparecendo no Athena
- [ ] Grafana conectado

**PRONTO! 🎉**