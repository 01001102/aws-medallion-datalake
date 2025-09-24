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

# Configurar Iceberg
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['OUTPUT_BUCKET']}/gold/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")

# Ler dados da camada Silver
input_path = f"s3://{args['INPUT_BUCKET']}/silver/api_data/"
df = spark.read.parquet(input_path)

# Agregações e métricas de negócio
df_gold = df.groupBy("source_api", date_format("processed_timestamp", "yyyy-MM-dd").alias("date")) \
    .agg(
        count("*").alias("record_count"),
        max("processed_timestamp").alias("last_processed")
    )

# Salvar como tabela Iceberg
table_name = f"glue_catalog.{args['DATABASE_NAME']}.gold_api_iceberg"
df_gold.writeTo(table_name).using("iceberg").createOrReplace()

job.commit()