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
# --- RECURSOS S3 (Buckets e Pastas) ---
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
  timeout          = 600  # 10 minutos (merge pode demorar)
  memory_size      = 1024 # Mais memória para o merge
  
  layers = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:19"]
  
  environment {
    variables = {
      BUCKET_TRUSTED = aws_s3_bucket.trusted.id
      BUCKET_REFINED = aws_s3_bucket.refined.id
    }
  }
}

# ---------------------------------------------------------
# --- OUTPUTS ---
# ---------------------------------------------------------

output "lambda_tratamento_arn" {
  description = "ARN da Lambda de Tratamento (RAW -> TRUSTED)"
  value       = aws_lambda_function.tratamento_lambda.arn
}

output "lambda_tratamento_name" {
  description = "Nome da Lambda de Tratamento"
  value       = aws_lambda_function.tratamento_lambda.function_name
}

output "lambda_refined_arn" {
  description = "ARN da Lambda Refined (TRUSTED -> REFINED)"
  value       = aws_lambda_function.refined_lambda.arn
}

output "lambda_refined_name" {
  description = "Nome da Lambda Refined"
  value       = aws_lambda_function.refined_lambda.function_name
}

output "bucket_raw_name" {
  description = "Nome do bucket RAW"
  value       = aws_s3_bucket.raw.id
}

output "bucket_trusted_name" {
  description = "Nome do bucket TRUSTED"
  value       = aws_s3_bucket.trusted.id
}

output "bucket_refined_name" {
  description = "Nome do bucket REFINED"
  value       = aws_s3_bucket.refined.id
}

output "workflow_completo" {
  description = "Como executar o workflow completo"
  value = <<-EOT
    
    ✅ Infraestrutura criada com sucesso!
    
    📋 WORKFLOW COMPLETO - Pipeline ETL:
    
    1️⃣  Use seu script para enviar CSVs ao bucket RAW
    
    2️⃣  Execute a Lambda de Tratamento (RAW -> TRUSTED):
        aws lambda invoke --function-name LambdaTratamentoBeiraMar --payload '{}' response1.json
        cat response1.json
    
    3️⃣  Execute a Lambda Refined (TRUSTED -> REFINED):
        aws lambda invoke --function-name LambdaRefinedBeiraMar --payload '{}' response2.json
        cat response2.json
    
    4️⃣  Verifique o resultado final:
        aws s3 ls s3://refined-beira-mar/clinica_com_clima/
    
    📊 Ver logs:
        aws logs tail /aws/lambda/LambdaTratamentoBeiraMar --follow
        aws logs tail /aws/lambda/LambdaRefinedBeiraMar --follow
    
    🔄 Pipeline completo em um comando:
        aws lambda invoke --function-name LambdaTratamentoBeiraMar --payload '{}' response1.json && \
        aws lambda invoke --function-name LambdaRefinedBeiraMar --payload '{}' response2.json && \
        echo "✅ Pipeline concluído!"
    
  EOT
}