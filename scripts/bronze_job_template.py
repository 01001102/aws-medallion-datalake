import sys
import requests
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'API_URL', 'OUTPUT_BUCKET', 'DATABASE_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Consumir API
response = requests.get(args['API_URL'])
data = response.json()

# Converter para DataFrame
df = spark.createDataFrame([data] if isinstance(data, dict) else data)

# Adicionar metadados
df = df.withColumn("ingestion_timestamp", lit(datetime.now().isoformat()))
df = df.withColumn("source_api", lit(args['API_URL']))

# Salvar na camada Bronze
output_path = f"s3://{args['OUTPUT_BUCKET']}/bronze/api_data/"
df.write.mode("append").parquet(output_path)

job.commit()