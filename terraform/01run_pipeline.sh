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