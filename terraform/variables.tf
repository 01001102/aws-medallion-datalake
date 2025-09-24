variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "medallion-pipeline"
}

variable "environment" {
  description = "Ambiente (dev, hom, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "hom", "prod"], var.environment)
    error_message = "Environment deve ser dev, hom ou prod."
  }
}

variable "dev_account_id" {
  description = "ID da conta AWS de desenvolvimento"
  type        = string
}

variable "prod_account_id" {
  description = "ID da conta AWS de produção (hom + prod)"
  type        = string
}

variable "api_urls" {
  description = "URLs das 4 APIs para consumo"
  type        = list(string)
  default = [
    "https://api1.example.com/data",
    "https://api2.example.com/data",
    "https://api3.example.com/data",
    "https://api4.example.com/data"
  ]
}