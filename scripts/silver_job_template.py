import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET', 'DATABASE_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler dados da camada Bronze
input_path = f"s3://{args['INPUT_BUCKET']}/bronze/api_data/"
df = spark.read.parquet(input_path)

# Limpeza e transformações
df_clean = df.dropDuplicates() \
    .filter(col("ingestion_timestamp").isNotNull()) \
    .withColumn("processed_timestamp", lit(current_timestamp()))

# Salvar na camada Silver
output_path = f"s3://{args['OUTPUT_BUCKET']}/silver/api_data/"
df_clean.write.mode("overwrite").parquet(output_path)

job.commit()