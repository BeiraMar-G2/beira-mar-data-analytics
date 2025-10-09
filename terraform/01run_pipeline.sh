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