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
    Próximos passos:
    1. Execute o arquivo .py 01_envio_bucket_raw.py
    2. Execute no bash: ./01run_pipeline.sh
  EOT
}