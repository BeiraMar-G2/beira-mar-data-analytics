# 🚀 Guia Completo - Pipeline ETL com Modelagem Estrela

## 📁 Estrutura de Arquivos Necessária

```
projeto-lambda/
├── main.tf                      # Terraform com Glue + Athena
├── 02tratamento_lambda.py       # Lambda RAW → TRUSTED
└── 03refined_lambda.py          # Lambda TRUSTED → REFINED
```

---

## 🔧 Passo 1: Deploy da Infraestrutura

### 1.1 Iniciar AWS Academy
```bash
# 1. Abra AWS Academy → Learner Lab
# 2. Start Lab (aguarde ficar verde)
# 3. AWS Details → Show → Copie as 3 linhas de credenciais
```

### 1.2 Configurar credenciais
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
```

### 1.3 Deploy com Terraform
```bash
cd projeto-lambda
terraform init
terraform apply
# Digite 'yes'
```

**Aguarde 2-3 minutos. Será criado:**
- ✅ 4 Buckets S3 (raw, trusted, refined, athena-results)
- ✅ 2 Lambdas
- ✅ 2 Databases Glue
- ✅ 1 Crawler Glue
- ✅ 1 Workgroup Athena
- ✅ 5 Queries Athena (saved queries)

---

## 📤 Passo 2: Executar Pipeline ETL

### 2.1 Enviar CSVs para RAW
```bash
# Use seu script existente para enviar:
# medical_appointments.csv
# meteorologia2016.csv
# Para: s3://raw-beira-mar/
```

### 2.2 Executar Lambda 1 (RAW → TRUSTED)
```bash
aws lambda invoke \
  --function-name LambdaTratamentoBeiraMar \
  --payload '{}' \
  response1.json

cat response1.json
```

**Esperado:**
```json
{
  "statusCode": 200,
  "body": {
    "mensagem": "Processamento de dados concluído com sucesso",
    "registros_medicos": 110527,
    "registros_clima": 8784
  }
}
```

### 2.3 Executar Lambda 2 (TRUSTED → REFINED)
```bash
aws lambda invoke \
  --function-name LambdaRefinedBeiraMar \
  --payload '{}' \
  response2.json

cat response2.json
```

**Esperado:**
```json
{
  "statusCode": 200,
  "body": {
    "mensagem": "Integração concluída com sucesso",
    "registros_totais": 110527,
    "percentual_match": "95.23%"
  }
}
```

---

## 📊 Passo 3: Catalogar com Glue Crawler

### 3.1 Executar Crawler
```bash
aws glue start-crawler --name refined-clinica-clima-crawler
```

### 3.2 Aguardar conclusão (1-2 minutos)
```bash
# Verificar status
aws glue get-crawler --name refined-clinica-clima-crawler \
  --query 'Crawler.State' --output text

# Quando mostrar "READY", prossiga
```

### 3.3 Verificar tabela criada
```bash
aws glue get-tables \
  --database-name refined_beira_mar \
  --query 'TableList[*].Name'
```

**Esperado:**
```json
[
    "clinica_com_clima"
]
```

---

## 🌟 Passo 4: Criar Modelagem Estrela no Athena

### Opção A: Via Console AWS (RECOMENDADO para primeira vez)

1. **Acesse o Athena:**
   - Console AWS → Busque "Athena"
   - Ou acesse: https://console.aws.amazon.com/athena/

2. **Configurar Workgroup:**
   - No menu lateral esquerdo: "Workgroup"
   - Selecione: `beira-mar-analytics`

3. **Executar Saved Queries:**
   
   Vá em "Saved queries" no menu lateral e execute **NA ORDEM**:
   
   ```
   1. create_dim_paciente     ✅
   2. create_dim_data         ✅
   3. create_dim_bairro       ✅
   4. create_dim_clima        ✅
   5. create_fato_consultas   ✅
   ```
   
   Para cada query:
   - Clique no nome
   - Clique em "Run"
   - Aguarde "Query successful"

4. **Testar a modelagem:**
   
   Execute a query de teste: `analise_no_show_por_clima`
   
   Você verá resultados como:
   ```
   classificacao_temp | estacao_ano | total_consultas | taxa_no_show_pct
   QUENTE            | VERAO       | 35420          | 24.5%
   AGRADAVEL         | PRIMAVERA   | 28910          | 19.8%
   ...
   ```

### Opção B: Via AWS CLI

```bash
# Database star schema
DB="star_schema_beira_mar"
WG="beira-mar-analytics"

# 1. DIM_PACIENTE
aws athena start-query-execution \
  --query-string "$(terraform output -raw create_dim_paciente_query)" \
  --query-execution-context Database=$DB \
  --work-group $WG

# 2. DIM_DATA
aws athena start-query-execution \
  --query-string "$(terraform output -raw create_dim_data_query)" \
  --query-execution-context Database=$DB \
  --work-group $WG

# 3. DIM_BAIRRO
aws athena start-query-execution \
  --query-string "$(terraform output -raw create_dim_bairro_query)" \
  --query-execution-context Database=$DB \
  --work-group $WG

# 4. DIM_CLIMA
aws athena start-query-execution \
  --query-string "$(terraform output -raw create_dim_clima_query)" \
  --query-execution-context Database=$DB \
  --work-group $WG

# 5. FATO_CONSULTAS
aws athena start-query-execution \
  --query-string "$(terraform output -raw create_fato_consultas_query)" \
  --query-execution-context Database=$DB \
  --work-group $WG
```

---

## 📈 Passo 5: Conectar Grafana

### 5.1 Configurar Data Source no Grafana

```
Type: Amazon Athena
Name: Beira Mar Analytics

Authentication:
├─ Access & Secret Key (use suas credenciais AWS)

Settings:
├─ Default Region: us-east-1
├─ Catalog: AwsDataCatalog
├─ Database: star_schema_beira_mar
├─ Workgroup: beira-mar-analytics
└─ Output Location: s3://athena-results-beira-mar/output/
```

### 5.2 Exemplo de Query para Dashboard

**Taxa de No-Show por Clima:**
```sql
SELECT 
  dc.classificacao_temp AS metric,
  COUNT(*) AS "Total Consultas",
  SUM(fc.qtd_no_shows) AS "No-Shows",
  ROUND(AVG(CAST(fc.qtd_no_shows AS DOUBLE)) * 100, 2) AS "Taxa %"
FROM star_schema_beira_mar.fato_consultas fc
LEFT JOIN star_schema_beira_mar.dim_clima dc 
  ON fc.clima_key = dc.clima_key
GROUP BY dc.classificacao_temp
ORDER BY "Taxa %" DESC;
```

**No-Show por Dia da Semana:**
```sql
SELECT 
  dd.dia_semana,
  COUNT(*) AS total,
  ROUND(AVG(CAST(fc.qtd_no_shows AS DOUBLE)) * 100, 2) AS taxa_pct
FROM star_schema_beira_mar.fato_consultas fc
JOIN star_schema_beira_mar.dim_data dd 
  ON fc.data_consulta_key = dd.data_key
GROUP BY dd.dia_semana
ORDER BY dd.dia_semana;
```

**No-Show por Faixa Etária:**
```sql
SELECT 
  dp.faixa_etaria,
  COUNT(*) AS total,
  ROUND(AVG(CAST(fc.qtd_no_shows AS DOUBLE)) * 100, 2) AS taxa_pct
FROM star_schema_beira_mar.fato_consultas fc
JOIN star_schema_beira_mar.dim_paciente dp 
  ON fc.patient_id = dp.patient_key
GROUP BY dp.faixa_etaria
ORDER BY taxa_pct DESC;
```

---

## 🔄 Workflow Completo Automatizado

### Script para executar tudo de uma vez:

Crie um arquivo `run_pipeline.sh`:

```bash
#!/bin/bash

echo "🚀 Iniciando Pipeline Beira Mar..."

# 1. Lambda RAW → TRUSTED
echo "1️⃣  Executando ETL: RAW → TRUSTED..."
aws lambda invoke \
  --function-name LambdaTratamentoBeiraMar \
  --payload '{}' \
  response1.json > /dev/null
  
if grep -q '"statusCode": 200' response1.json; then
  echo "✅ Lambda