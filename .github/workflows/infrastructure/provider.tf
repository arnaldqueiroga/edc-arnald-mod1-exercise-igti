provider "aws" {
  region = var.aws_region
}

# Centralizar o arquivo de controle de estado do terraform
terraform {
  backend "s3" {
    bucket = "terraform-state-igit-arnald"
    key = "state/igti/edc/mod1/terraform.tfstate"
    region = "eu-east-2"
  }
}