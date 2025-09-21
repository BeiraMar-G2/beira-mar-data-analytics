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