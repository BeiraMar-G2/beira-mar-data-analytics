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
  echo "✅ Lambda 1 concluída com sucesso!"
  cat response1.json | jq '.body'
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
  echo "✅ Lambda 2 concluída com sucesso!"
  cat response2.json | jq '.body'
else
  echo "❌ Erro na Lambda 2"
  cat response2.json
  exit 1
fi

# 3. Glue Crawler
echo ""
echo "3️⃣  Iniciando Glue Crawler..."
aws glue start-crawler --name refined-clinica-clima-crawler 2>/dev/null || echo "Crawler já em execução"

echo "⏳ Aguardando crawler catalogar os dados..."
CONTADOR=0
while true; do
    STATUS=$(aws glue get-crawler --name refined-clinica-clima-crawler --query 'Crawler.State' --output text)
    
    if [ "$STATUS" = "READY" ]; then
        echo "✅ Crawler concluído!"
        
        # Mostrar estatísticas
        aws glue get-crawler --name refined-clinica-clima-crawler \
          --query 'Crawler.LastCrawl' \
          --output table
        break
    fi
    
    CONTADOR=$((CONTADOR + 1))
    echo "   Status: $STATUS (${CONTADOR}0s decorridos)"
    
    if [ $CONTADOR -gt 30 ]; then
        echo "⚠️  Crawler está demorando muito (>5min). Verifique manualmente."
        exit 1
    fi
    
    sleep 10
done

# 4. Verificar tabela criada
echo ""
echo "4️⃣  Verificando tabelas catalogadas..."
TABELAS=$(aws glue get-tables \
  --database-name refined_beira_mar \
  --query 'TableList[*].Name' \
  --output text)

if echo "$TABELAS" | grep -q "clinica_com_clima"; then
  echo "✅ Tabela 'clinica_com_clima' catalogada com sucesso!"
else
  echo "❌ Tabela não encontrada"
  exit 1
fi

# 5. Finalização
echo ""
echo "═══════════════════════════════════════════════════════"
echo "✅ PIPELINE CONCLUÍDO COM SUCESSO!"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "📋 Próximos passos:"
echo "   1. Acesse o Athena Console"
echo "   2. Execute as Saved Queries para criar as views"
echo "   3. Conecte o Grafana ao database: star_schema_beira_mar"
echo ""