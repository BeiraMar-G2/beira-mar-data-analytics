terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.92"
    }
  }
  required_version = ">= 1.2"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "raw" {
  bucket = "raw-beira-mar"
}

resource "aws_s3_object" "raw_pastas" {
  count  = length(var.raw_folders)
  bucket = aws_s3_bucket.raw.id
  key    = "${var.raw_folders[count.index]}/"
  source = "empty_file" 
  etag   = filemd5("empty_file")
}

resource "aws_s3_bucket" "trusted" {
  bucket = "trusted-beira-mar"
}

resource "aws_s3_object" "trusted_pastas" {
  count  = length(var.trusted_folders)
  bucket = aws_s3_bucket.trusted.id
  key    = "${var.trusted_folders[count.index]}/"
  source = "empty_file"
  etag   = filemd5("empty_file")
}

resource "aws_s3_bucket" "refined" {
  bucket = "refined-beira-mar"
}

resource "aws_s3_object" "refined_pastas" {
  count  = length(var.refined_folders)
  bucket = aws_s3_bucket.refined.id
  key    = "${var.refined_folders[count.index]}/"
  source = "empty_file"
  etag   = filemd5("empty_file")
}

variable "raw_folders" {
  description = "Lista de pastas a serem criadas no bucket raw."
  type        = list(string)
  default     = ["ClinicaMed", "Salao", "Clima"]
}

variable "trusted_folders" {
  description = "Lista de pastas a serem criadas no bucket trusted."
  type        = list(string)
  default     = ["ClinicaMed", "Salao", "Clima"]
}

variable "refined_folders" {
  description = "Lista de pastas a serem criadas no bucket refined."
  type        = list(string)
  default     = ["ClinicaMed", "Salao", "Clima", "imagens"]
}

variable "lambda_function_trusted" {
  description = "lambda_function_trusted"
  type        = string
  default     = "s3-data-processor-beira-mar"
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.lambda_function_trusted}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name    = "Lambda Execution Role"
    Project = "Beira Mar"
  }
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3_policy" {
  name = "${var.lambda_function_trusted}-s3-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = [
          "${aws_s3_bucket.trusted.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_lambda_layer_version" "pandas_layer" {
  filename            = "pandas_layer.zip"
  layer_name          = "pandas-numpy-layer-beira-mar"
  compatible_runtimes = ["python3.11", "python3.10", "python3.9"]
  description = "Layer com Pandas, NumPy e dependências"
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda_function_trusted.py"
  output_path = "${path.module}/lambda_function_trusted.zip"
}

resource "aws_lambda_function" "data_processor" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = var.lambda_function_trusted
  role            = aws_iam_role.lambda_role.arn
  handler         = "lambda_function.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 512

  layers = [aws_lambda_layer_version.pandas_layer.arn]

  environment {
    variables = {
      BUCKET_RAW     = aws_s3_bucket.raw.id
      BUCKET_TRUSTED = aws_s3_bucket.trusted.id
    }
  }

  tags = {
    Name    = "S3 Data Processor"
    Project = "Beira Mar"
  }
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.lambda_function_trusted}"
  retention_in_days = 7

  tags = {
    Name    = "Lambda Logs"
    Project = "Beira Mar"
  }
}

output "lambda_function_arn" {
  description = "ARN da função Lambda"
  value       = aws_lambda_function.data_processor.arn
}

output "lambda_function_name" {
  description = "Nome da função Lambda"
  value       = aws_lambda_function.data_processor.function_name
}

output "bucket_raw_name" {
  description = "Nome do bucket raw"
  value       = aws_s3_bucket.raw.id
}

output "bucket_trusted_name" {
  description = "Nome do bucket trusted"
  value       = aws_s3_bucket.trusted.id
}

output "bucket_refined_name" {
  description = "Nome do bucket refined"
  value       = aws_s3_bucket.refined.id
}