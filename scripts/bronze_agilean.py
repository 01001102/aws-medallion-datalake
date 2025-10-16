"""
Bronze Agilean - Extração de dados da API Agilean
"""

import sys
import json
import requests
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'AGILEAN_SECRET_NAME', 'WATERMARK_TABLE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET = args['BRONZE_BUCKET']
WATERMARK_TABLE = args['WATERMARK_TABLE']
SECRET_NAME = args['AGILEAN_SECRET_NAME']

dynamodb = boto3.resource('dynamodb')
secretsmanager = boto3.client('secretsmanager')
watermark_table = dynamodb.Table(WATERMARK_TABLE)

def get_credentials():
    try:
        response = secretsmanager.get_secret_value(SecretId=SECRET_NAME)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Erro ao obter credenciais: {e}")
        raise

def authenticate(credentials):
    base_url = credentials.get('base_url', 'https://api.agilean.com.br')
    login_url = f"{base_url}/api/v1/users/login"
    
    username = credentials.get('username') or credentials.get('userName') or credentials.get('user')
    if not username:
        raise Exception("Username não encontrado nas credenciais")
    
    response = requests.post(login_url, json={
        'username': username,
        'password': credentials['password']
    }, timeout=30)
    
    if response.status_code == 200:
        result = response.json()
        token = result.get('accessToken')
        
        if not token:
            raise Exception(f"Token não encontrado na resposta: {result}")
        
        return token, base_url
    else:
        raise Exception(f"Falha na autenticação: {response.status_code} - {response.text}")

def create_table_from_data(data, table_name, data_type, project_id=None):
    today = datetime.now().date().isoformat()
    
    if not data or (isinstance(data, list) and len(data) == 0):
        schema = StructType([
            StructField("extraction_date", StringType(), True),
            StructField("api_source", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("raw_json", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)
        record_count = 0
    else:
        enriched_data = []
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    enriched_item = {
                        "extraction_date": today,
                        "api_source": "agilean",
                        "data_type": data_type,
                        "project_id": str(project_id) if project_id else "",
                        "raw_json": json.dumps(item, ensure_ascii=False, default=str)
                    }
                    enriched_data.append(enriched_item)
        elif isinstance(data, dict):
            enriched_item = {
                "extraction_date": today,
                "api_source": "agilean",
                "data_type": data_type,
                "project_id": str(project_id) if project_id else "",
                "raw_json": json.dumps(data, ensure_ascii=False, default=str)
            }
            enriched_data.append(enriched_item)
        
        if enriched_data:
            df = spark.createDataFrame(enriched_data)
            record_count = len(enriched_data)
        else:
            return None, 0
    
    table_path = f"s3://{BRONZE_BUCKET}/tables/{table_name}/"
    df.coalesce(1).write.mode("overwrite").parquet(table_path)
    
    print(f"{data_type}: Tabela {table_name} criada com {record_count} registros")
    return table_path, record_count

def fetch_and_create_table(base_url, token, endpoint, table_name, data_type, project_id=None):
    try:
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{base_url}{endpoint}"
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            response_data = response.json()
            
            if data_type == "projects":
                data = response_data.get('result', {}).get('data', [])
            elif data_type == "baseline_info":
                result = response_data.get('result', {})
                baseline_data = result.get('longBaseLineInformation', [])
                budget_data = result.get('longBudgetInformation', [])
                data = baseline_data + budget_data
            elif data_type == "scenario_values":
                result = response_data.get('result', {})
                if result.get('ready', False) and result.get('viewResult'):
                    data = [result]
                else:
                    data = [{
                        'ready': result.get('ready', False),
                        'viewResult': str(result.get('viewResult', '')),
                        'viewProcessingIds': str(result.get('viewProcessingIds', [])),
                        'errorMessage': response_data.get('errorMessage', ''),
                        'timeGenerated': response_data.get('timeGenerated', '')
                    }]
            else:
                data = response_data.get('result', {}).get('data', [])
            
            path, count = create_table_from_data(data, table_name, data_type, project_id)
            
            if isinstance(data, list):
                print(f"{data_type}: {len(data)} registros API")
                return len(data) if data else 0
            else:
                return 1 if data else 0
        else:
            print(f"{data_type}: HTTP {response.status_code}")
            return 0
            
    except Exception as e:
        print(f"{data_type}: Erro {str(e)}")
        return 0

def update_watermark(api_name, execution_date, status='completed', error_message=None, record_count=0):
    try:
        item = {
            'api_name': api_name,
            'last_execution': execution_date.isoformat(),
            'status': status,
            'record_count': record_count
        }
        if error_message:
            item['error_message'] = str(error_message)[:1000]
        
        watermark_table.put_item(Item=item)
    except Exception as e:
        print(f"Erro ao atualizar watermark: {str(e)}")

def main():
    execution_date = datetime.now()
    total_records = 0
    
    try:
        print(f"Iniciando Bronze Agilean: {execution_date}")
        
        credentials = get_credentials()
        token, base_url = authenticate(credentials)
        
        # 1. Buscar projetos
        print("=== PROJETOS ===")
        projects_count = fetch_and_create_table(
            base_url, token, "/api/v1/projects", "agilean_bronze_projects", "projects"
        )
        total_records += projects_count
        
        # 2. Buscar dados de cada projeto
        print("=== DADOS POR PROJETO ===")
        headers = {"Authorization": f"Bearer {token}"}
        projects_response = requests.get(f"{base_url}/api/v1/projects", headers=headers, timeout=30)
        
        if projects_response.status_code == 200:
            projects_data = projects_response.json()
            projects = projects_data.get('result', {}).get('data', [])
            
            print(f"Processando {len(projects)} projetos")
            
            for i, project in enumerate(projects):
                project_id = project.get('id')
                project_name = project.get('name', 'N/A')
                
                if not project_id:
                    continue
                
                print(f"Projeto {i+1}/{len(projects)}: {project_name}")
                
                # Baseline info
                baseline_count = fetch_and_create_table(
                    base_url, token, f"/api/v1/portal/long/budget-baseline-link-info/{project_id}",
                    "agilean_bronze_baseline_info", "baseline_info", project_id
                )
                total_records += baseline_count
                
                # Scenario data
                scenario_count = fetch_and_create_table(
                    base_url, token, f"/api/v1/portal/long/scenario-value-table/{project_id}",
                    "agilean_bronze_scenario_values", "scenario_values", project_id
                )
                total_records += scenario_count
        
        print(f"Bronze Agilean concluído: {total_records} registros")
        update_watermark('agilean', execution_date, record_count=total_records)
        
    except Exception as e:
        print(f"Erro durante Bronze Agilean: {str(e)}")
        update_watermark('agilean', execution_date, 'failed', str(e))
        raise

if __name__ == "__main__":
    main()

job.commit()