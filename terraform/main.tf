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

# Bucket para armazenar a Lambda Layer
resource "aws_s3_bucket" "lambda_artifacts" {
  bucket = "lambda-artifacts-beira-mar-${random_string.suffix.result}"
}

resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
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
# --- LAMBDA LAYER CUSTOMIZADA (via S3) ---
# ---------------------------------------------------------

# Upload da Layer para S3
resource "aws_s3_object" "pandas_layer_zip" {
  bucket = aws_s3_bucket.lambda_artifacts.id
  key    = "layers/pandas-s3fs-layer.zip"
  source = "lambda_layer.zip"
  etag   = filemd5("lambda_layer.zip")
}

# Criar Layer apontando para S3
resource "aws_lambda_layer_version" "pandas_layer" {
  layer_name          = "pandas-s3fs-custom-layer"
  s3_bucket           = aws_s3_bucket.lambda_artifacts.id
  s3_key              = aws_s3_object.pandas_layer_zip.key
  compatible_runtimes = ["python3.9"]
  description         = "Layer customizada com pandas, s3fs e boto3"
  
  depends_on = [aws_s3_object.pandas_layer_zip]
}

# ---------------------------------------------------------
# --- LAMBDA FUNCTION ---
# ---------------------------------------------------------

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "02tratamento_lambda.py"
  output_path = "02tratamento_lambda.zip"
}

resource "aws_lambda_function" "tratamento_lambda" {
  depends_on = [
    data.aws_iam_role.lab_role,
    data.archive_file.lambda_zip,
    aws_lambda_layer_version.pandas_layer
  ]
  
  function_name    = "LambdaTratamentoBeiraMar"
  handler          = "02tratamento_lambda.lambda_handler"
  role             = data.aws_iam_role.lab_role.arn
  
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  
  runtime          = "python3.9"
  timeout          = 300
  memory_size      = 512
  
  # Anexar a Layer customizada
  layers = [aws_lambda_layer_version.pandas_layer.arn]
  
  environment {
    variables = {
      BUCKET_RAW     = aws_s3_bucket.raw.id
      BUCKET_TRUSTED = aws_s3_bucket.trusted.id
    }
  }
}

# ---------------------------------------------------------
# --- OUTPUTS ---
# ---------------------------------------------------------

output "lambda_function_arn" {
  description = "ARN da função Lambda"
  value       = aws_lambda_function.tratamento_lambda.arn
}

output "lambda_function_name" {
  description = "Nome da função Lambda"
  value       = aws_lambda_function.tratamento_lambda.function_name
}

output "layer_arn" {
  description = "ARN da Lambda Layer customizada"
  value       = aws_lambda_layer_version.pandas_layer.arn
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