# 🚀 Pipeline Beira Mar - Guia de Deploy

> Pipeline ETL completo: RAW → TRUSTED → REFINED → Glue → Athena → Grafana  
> Com modelagem estrela automatizada para analytics

## 📁 Estrutura do Projeto

```
beira-mar-data-analytics/
├── main.tf                      # Infraestrutura AWS (Terraform)
├── 02tratamento_lambda.py       # Lambda: RAW → TRUSTED
├── 03refined_lambda.py          # Lambda: TRUSTED → REFINED
├── run_pipeline.sh              # Script de execução automática
└── README.md                    # Este guia
```

---

## 📋 Pré-requisitos

- ✅ Conta AWS Academy (Learner Lab)
- ✅ Terraform instalado ([Download](https://www.terraform.io/downloads))
- ✅ AWS CLI instalado ([Download](https://aws.amazon.com/cli/))
- ✅ Git instalado
- ✅ Script de upload dos CSVs configurado

---

## 🚀 Passo 1: Clonar o Repositório

```bash
git clone <url-do-repositorio>
cd beira-mar-data-analytics/terraform
```

---

## 🔑 Passo 2: Configurar Credenciais AWS

### 2.1 Iniciar AWS Academy
1. Acesse **AWS Academy → Learner Lab**
2. Clique em **"Start Lab"** 
3. Aguarde o ícone ficar verde ✅

### 2.2 Obter Credenciais
1. Clique em **"AWS Details"**
2. Clique em **"Show"** ao lado de "AWS CLI"
3. Copie as três linhas de credenciais

### 2.3 Configurar no Terminal

**Linux/Mac:**
```bash
export AWS_ACCESS_KEY_ID=ASIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
```

**Windows (PowerShell):**
```powershell
$env:AWS_ACCESS_KEY_ID="ASIA..."
$env:AWS_SECRET_ACCESS_KEY="..."
$env:AWS_SESSION_TOKEN="..."
```

### 2.4 Testar Conexão
```bash
aws sts get-caller-identity
```

Se retornar seus dados da AWS, está configurado! ✅

---

## 🏗️ Passo 3: Deploy da Infraestrutura

### 3.1 Inicializar Terraform
```bash
terraform init
```

**Saída esperada:**
```
Terraform has been successfully initialized!
```

### 3.2 Revisar o Plano
```bash
terraform plan
```

Será criado:
- 4 Buckets S3 (raw, trusted, refined, athena-results)
- 2 Lambda Functions
- 2 Glue Databases
- 1 Glue Crawler
- 1 Athena Workgroup

### 3.3 Aplicar Infraestrutura
```bash
terraform apply
```

Digite `yes` quando solicitado.

**Tempo estimado:** 2-3 minutos

**Saída esperada:**
```
Apply complete! Resources: 12 added, 0 changed, 0 destroyed.

Outputs:
bucket_raw = "raw-beira-mar"
bucket_trusted = "trusted-beira-mar"
bucket_refined = "refined-beira-mar"
instrucoes = "✅ Infraestrutura criada! Execute: ./run_pipeline.sh"
```

---

## 📤 Passo 4: Upload dos Dados

Use seu script existente para enviar os CSVs para o bucket RAW:

```bash
# Seu script deve enviar:
# - medical_appointments.csv
# - meteorologia2016.csv
# Para: s3://raw-beira-mar/
```

### Verificar Upload
```bash
aws s3 ls s3://raw-beira-mar/
```

**Deve listar:**
```
medical_appointments.csv
meteorologia2016.csv
```

---

## ⚡ Passo 5: Executar Pipeline Completo

### 5.1 Dar Permissão de Execução
```bash
chmod +x run_pipeline.sh
```

### 5.2 Executar
```bash
./run_pipeline.sh
```

### 5.3 O que o Script Faz

O script executa automaticamente:

1. **Lambda 1** - Processa dados brutos (RAW → TRUSTED)
2. **Lambda 2** - Integra clima + consultas (TRUSTED → REFINED)
3. **Glue Crawler** - Cataloga tabela base no Data Catalog
4. **Criação de Views** - Cria modelagem estrela no Athena:
   - `dim_paciente` - Dimensão de pacientes
   - `dim_data` - Dimensão temporal
   - `dim_bairro` - Dimensão geográfica
   - `dim_clima` - Dimensão climática
   - `fato_consultas` - Tabela fato principal
5. **Teste** - Valida a modelagem com query de contagem

**Tempo total:** ~3-5 minutos

### 5.4 Saída Esperada

```
═══════════════════════════════════════════════════════
🚀 PIPELINE BEIRA MAR - ETL COMPLETO
═══════════════════════════════════════════════════════

1️⃣  Executando Lambda: RAW → TRUSTED...
✅ Lambda 1 concluída!

2️⃣  Executando Lambda: TRUSTED → REFINED...
✅ Lambda 2 concluída!

3️⃣  Iniciando Glue Crawler...
⏳ Aguardando crawler...
✅ Crawler concluído!

4️⃣  Criando Modelagem Estrela...
   Criando DIM_PACIENTE...
   ✅ DIM_PACIENTE criada!
   Criando DIM_DATA...
   ✅ DIM_DATA criada!
   Criando DIM_BAIRRO...
   ✅ DIM_BAIRRO criada!
   Criando DIM_CLIMA...
   ✅ DIM_CLIMA criada!
   Criando FATO_CONSULTAS...
   ✅ FATO_CONSULTAS criada!

5️⃣  Testando modelagem...
✅ Total de registros na tabela fato: 110527

═══════════════════════════════════════════════════════
✅ PIPELINE COMPLETO EXECUTADO COM SUCESSO!
═══════════════════════════════════════════════════════

📊 Modelagem Estrela criada:
   - dim_paciente
   - dim_data
   - dim_bairro
   - dim_clima
   - fato_consultas

🔗 Conecte o Grafana:
   Database: star_schema_beira_mar
   Workgroup: beira-mar-analytics
```

---

## 🔍 Passo 6: Verificar no Athena

### 6.1 Acessar Console Athena
1. Console AWS → Busque "Athena"
2. Ou acesse: https://console.aws.amazon.com/athena/

### 6.2 Selecionar Database
No menu lateral esquerdo:
- Database: `star_schema_beira_mar`

Você verá 5 views:
- ✅ `dim_paciente`
- ✅ `dim_data`
- ✅ `dim_bairro`
- ✅ `dim_clima`
- ✅ `fato_consultas`

### 6.3 Testar Query

Execute no Query Editor:

```sql
-- Taxa de no-show por classificação de temperatura
SELECT 
  dc.classificacao_temp,
  COUNT(*) as total_consultas,
  SUM(fc.qtd_no_shows) as total_no_shows,
  ROUND(AVG(CAST(fc.qtd_no_shows AS DOUBLE)) * 100, 2) as taxa_no_show_pct
FROM star_schema_beira_mar.fato_consultas fc
LEFT JOIN star_schema_beira_mar.dim_clima dc 
  ON fc.clima_key = dc.clima_key
GROUP BY dc.classificacao_temp
ORDER BY taxa_no_show_pct DESC;
```

---

## 📈 Passo 7: Conectar Grafana

### 7.1 Configurar Data Source

```
Type: Amazon Athena
Name: Beira Mar Analytics

Authentication:
  Access Key ID: <suas-credenciais-aws>
  Secret Access Key: <suas-credenciais-aws>

Settings:
  Default Region: us-east-1
  Catalog: AwsDataCatalog
  Database: star_schema_beira_mar
  Workgroup: beira-mar-analytics
  Output Location: s3://athena-results-beira-mar/output/
```

---

## 🔄 Executar Novamente (Dados Atualizados)

Quando houver novos dados:

```bash
# 1. Upload de novos CSVs (seu script)
# 2. Executar pipeline novamente
./run_pipeline.sh
```

---

## 🐛 Troubleshooting

### ❌ Erro: "LabRole not found"
**Causa:** Lab não foi iniciado ou credenciais expiraram

**Solução:**
```bash
# 1. Volte ao AWS Academy → Start Lab
# 2. AWS Details → Show → Copie credenciais novamente
# 3. Configure no terminal (Passo 2.3)
```

---

### ❌ Erro: Lambda timeout
**Causa:** Processamento demorou mais que o limite

**Solução:**
```bash
# Edite main.tf, aumente o timeout:
# timeout = 600  # para Lambda RAW → TRUSTED
# timeout = 900  # para Lambda TRUSTED → REFINED

terraform apply
./run_pipeline.sh
```

---

### ❌ Erro: "Bucket already exists"
**Causa:** Buckets já existem na AWS

**Solução:**
```bash
# Opção 1: Importar buckets existentes
terraform import aws_s3_bucket.raw raw-beira-mar
terraform import aws_s3_bucket.trusted trusted-beira-mar
terraform import aws_s3_bucket.refined refined-beira-mar
terraform import aws_s3_bucket.athena_results athena-results-beira-mar
terraform apply

# Opção 2: Destruir e recriar (CUIDADO: apaga dados!)
terraform destroy
terraform apply
```

---

### ❌ Erro: Crawler não encontra dados
**Causa:** CSVs não foram enviados para o bucket RAW

**Solução:**
```bash
# Verificar se arquivos estão lá
aws s3 ls s3://raw-beira-mar/

# Se não estiverem, execute seu script de upload novamente
```

---

### 📊 Ver Logs das Lambdas

```bash
# Lambda 1 (RAW → TRUSTED)
aws logs tail /aws/lambda/LambdaTratamentoBeiraMar --follow

# Lambda 2 (TRUSTED → REFINED)
aws logs tail /aws/lambda/LambdaRefinedBeiraMar --follow
```

---

## 📊 Arquitetura do Pipeline

```
┌─────────────────┐
│  Seu Script     │ → Upload de CSVs
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  raw-beira-mar  │ (dados brutos)
└────────┬────────┘
         │ Lambda 1: 02tratamento_lambda.py
         ▼
┌─────────────────┐
│trusted-beira-mar│ (dados limpos)
│  ├─ clinica/    │
│  └─ clima/      │
└────────┬────────┘
         │ Lambda 2: 03refined_lambda.py
         ▼
┌─────────────────┐
│refined-beira-mar│ (dados integrados)
│  └─ clinica_com_│
│      clima/     │
└────────┬────────┘
         │ Glue Crawler
         ▼
┌─────────────────┐
│  Glue Catalog   │
│  Database:      │
│  refined_       │
│  beira_mar      │
└────────┬────────┘
         │ run_pipeline.sh (cria views)
         ▼
┌─────────────────────────────┐
│  Athena - Modelagem Estrela │
│  Database: star_schema_     │
│            beira_mar        │
│                             │
│  Views:                     │
│  ├─ dim_paciente            │
│  ├─ dim_data                │
│  ├─ dim_bairro              │
│  ├─ dim_clima               │
│  └─ fato_consultas          │
└──────────┬──────────────────┘
           │
           ▼
     ┌──────────┐
     │ Grafana  │ → Dashboards
     └──────────┘
```

---

## 💡 Observações Importantes

### ⏰ Credenciais AWS Academy
- ⚠️ Credenciais expiram a cada **4 horas**
- 💡 Reconfigure seguindo o **Passo 2** quando necessário

### 💰 Custos
- Lambda: ~$0.01 por execução
- S3: ~$0.023 por GB/mês
- Athena: ~$5 por TB escaneado
- Glue Crawler: ~$0.44 por hora (executa em segundos)

### 🔒 Segurança
- Nunca commite credenciais AWS no Git
- Use `.gitignore` para arquivos sensíveis:
  ```
  .terraform/
  *.tfstate*
  response*.json
  *.zip
  ```

---

## 📚 Recursos Adicionais

- [Documentação AWS Lambda](https://docs.aws.amazon.com/lambda/)
- [Documentação AWS Glue](https://docs.aws.amazon.com/glue/)
- [Documentação Amazon Athena](https://docs.aws.amazon.com/athena/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)

---

## 🆘 Suporte

Problemas? Abra uma issue no repositório ou contate o time de desenvolvimento.

---

**✨ Pipeline configurado com sucesso! Bom trabalho! 🎉**