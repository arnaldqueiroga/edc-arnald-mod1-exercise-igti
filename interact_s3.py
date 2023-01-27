import boto3
import pandas as pd

# Criar um cliente para interagir com a AWS S3
s3_client = boto3.client('s3', aws_region_name='us-east-2')

# Download do arquivo CSV
s3_client.download_file("datalake-arnald-900605953217","raw-data/MICRODADOS_ENEM_2020.csv", "EDC/MICRODADOS_ENEM_2020.csv")

df = pd.read_csv("EDC/enem2020.csv")
print(df)