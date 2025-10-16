import sys
import json
import requests
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'ENDPOINT_TYPE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze_bucket = args['BRONZE_BUCKET']
endpoint_type = args['ENDPOINT_TYPE']

def get_auth_token():
    secrets_client = boto3.client('secretsmanager')
    secret_response = secrets_client.get_secret_value(SecretId='medallion-pipeline/digit-api')
    credentials = json.loads(secret_response['SecretString'])
    
    base_url = "https://www.digitca.com.br/api/ServiceLayerPlano"
    form_data = {
        'cliente_nome': credentials['cliente_nome'],
        'chave_secreta': credentials['chave_secreta']
    }
    response = requests.post(f"{base_url}/gerarToken", data=form_data, timeout=30)
    
    if response.status_code == 200:
        result = response.json()
        if result.get('success'):
            return result.get('token'), base_url
    raise Exception("Erro ao gerar token")

def download_master_data(session, base_url, endpoint_type):
    """Baixa dados mestres atualizados"""
    
    endpoint_map = {
        "obras": "/getObras",
        "funcionarios": "/getFuncionarios", 
        "cargos": "/getCargos"
    }
    
    endpoint_path = endpoint_map.get(endpoint_type)
    if not endpoint_path:
        raise Exception(f"Endpoint master {endpoint_type} não encontrado")
    
    print(f"Baixando dados mestres: {endpoint_type}")
    
    url = f"{base_url}{endpoint_path}"
    response = session.get(url, timeout=60)
    
    if response.status_code == 200:
        data = response.json()
        
        # Adicionar metadados
        for item in data:
            if isinstance(item, dict):
                item.update({
                    'data_type': endpoint_type,
                    'api_source': 'digit',
                    'extraction_date': datetime.now().date().isoformat(),
                    'extraction_timestamp': datetime.now().isoformat()
                })
        
        return data
    else:
        print(f"Erro {response.status_code}: {response.text}")
        return []

def main():
    try:
        token, base_url = get_auth_token()
        session = requests.Session()
        session.headers.update({"Authorization": token})
        
        print(f"Iniciando carga semanal dados mestres: {endpoint_type}")
        
        # Apenas dados mestres
        master_endpoints = ["obras", "funcionarios", "cargos"]
        
        if endpoint_type not in master_endpoints:
            print(f"AVISO: {endpoint_type} não é dado mestre")
            return
        
        all_data = download_master_data(session, base_url, endpoint_type)
        
        if all_data:
            # Converter para strings
            clean_data = []
            for item in all_data:
                clean_item = {}
                for k, v in item.items():
                    if isinstance(v, (list, dict)):
                        clean_item[k] = json.dumps(v, ensure_ascii=False)
                    else:
                        clean_item[k] = str(v) if v is not None else ""
                clean_data.append(clean_item)
            
            df = spark.createDataFrame(clean_data)
            
            # Sobrescrever dados mestres
            output_path = f"s3://{bronze_bucket}/digit/{endpoint_type}/"
            df.coalesce(1).write.mode("overwrite").parquet(output_path)
            
            print(f"OK {endpoint_type}: {len(clean_data)} registros mestres atualizados")
            print(f"Caminho: {output_path}")
        else:
            print(f"AVISO: Nenhum dado mestre encontrado para {endpoint_type}")
        
    except Exception as e:
        print(f"ERRO: {str(e)}")
        raise

if __name__ == "__main__":
    main()

job.commit()