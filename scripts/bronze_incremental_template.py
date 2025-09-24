import sys
import boto3
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'API_URL', 'OUTPUT_BUCKET', 'WATERMARK_TABLE', 'API_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(args['WATERMARK_TABLE'])

# Buscar último watermark
try:
    response = table.get_item(Key={'api_name': args['API_NAME']})
    last_run = response.get('Item', {}).get('last_run_date', '1900-01-01')
except:
    last_run = '1900-01-01'

# Data D-1
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Consumir API com filtro incremental
import requests
api_url = f"{args['API_URL']}?since={last_run}&until={yesterday}"
response = requests.get(api_url)
data = response.json()

if data:
    df = spark.createDataFrame(data if isinstance(data, list) else [data])
    df = df.withColumn("partition_date", lit(yesterday))
    
    # Salvar particionado por data
    output_path = f"s3://{args['OUTPUT_BUCKET']}/bronze/api_{args['API_NAME']}/date={yesterday}/"
    df.write.mode("overwrite").parquet(output_path)
    
    # Atualizar watermark
    table.put_item(Item={
        'api_name': args['API_NAME'],
        'last_run_date': yesterday,
        'updated_at': datetime.now().isoformat()
    })

job.commit()