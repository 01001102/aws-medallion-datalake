import sys
import json
import requests
from datetime import datetime, timedelta
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

# Configurações Spark para Glue 4.0
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

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

def download_daily_data(session, base_url, endpoint_type):
    """Baixa dados D-1 (ontem)"""
    # Buscar obras primeiro
    obras_response = session.get(f"{base_url}/getObras", timeout=30)
    obras_ids = [obra.get('CodigoObra') for obra in obras_response.json() if obra.get('CodigoObra')]
    
    # Data D-1 (ontem)
    yesterday = datetime.now() - timedelta(days=1)
    data_d1 = yesterday.strftime('%d-%m-%Y')
    
    all_data = []
    
    endpoint_map = {
        "horas_funcao": "/getHorasTrabalhadasPorFuncao",
        "resumo_periodo": "/getResumoHorasPeriodo", 
        "relatorio_expandido": "/getRelatorioCargosExpandido"
    }
    
    endpoint_path = endpoint_map.get(endpoint_type)
    if not endpoint_path:
        raise Exception(f"Endpoint type {endpoint_type} não encontrado")
    
    print(f"Baixando {endpoint_type} - D-1: {data_d1}")
    print(f"Total de obras: {len(obras_ids)}")
    
    for i, obra_id in enumerate(obras_ids, 1):
        print(f"Processando obra {i}/{len(obras_ids)}: {obra_id}")
        
        url = f"{base_url}{endpoint_path}?CodigoObra={obra_id}&CodigoFilial&dataInicio={data_d1}&dataFim={data_d1}"
        
        try:
            response = session.get(url, timeout=60)
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            item.update({
                                'data_type': endpoint_type,
                                'codigo_obra': str(obra_id),
                                'api_source': 'digit',
                                'data_referencia': yesterday.date().isoformat(),
                                'extraction_date': datetime.now().date().isoformat()
                            })
                            all_data.append(item)
                elif isinstance(data, dict):
                    # Para endpoints que retornam objeto único
                    data.update({
                        'data_type': endpoint_type,
                        'codigo_obra': str(obra_id),
                        'api_source': 'digit',
                        'data_referencia': yesterday.date().isoformat(),
                        'extraction_date': datetime.now().date().isoformat()
                    })
                    all_data.append(data)
                
                print(f"  Obra {obra_id}: {len(data) if isinstance(data, list) else 1} registros")
        
        except Exception as e:
            print(f"  Erro obra {obra_id}: {str(e)}")
            continue
    
    return all_data, yesterday

def download_all_endpoints():
    """Baixa todos os endpoints como no histórico"""
    token, base_url = get_auth_token()
    session = requests.Session()
    session.headers.update({"Authorization": token})
    
    # Todos os endpoints como no histórico
    all_endpoints = {
        "obras": "/getObras",
        "funcionarios": "/getFuncionarios", 
        "cargos": "/getCargos",
        "horas_funcao": "/getHorasTrabalhadasPorFuncao",
        "resumo_periodo": "/getResumoHorasPeriodo",
        "relatorio_expandido": "/getRelatorioCargosExpandido"
    }
    
    yesterday = datetime.now() - timedelta(days=1)
    data_d1 = yesterday.strftime('%d-%m-%Y')
    
    for endpoint_name, endpoint_path in all_endpoints.items():
        print(f"\n=== Processando {endpoint_name} ===")
        
        try:
            if endpoint_name in ["obras", "funcionarios", "cargos"]:
                # Endpoints sem período
                response = session.get(f"{base_url}{endpoint_path}", timeout=60)
                if response.status_code == 200:
                    data = response.json()
                    save_endpoint_data(data, endpoint_name, yesterday, is_period=False)
            else:
                # Endpoints com período (D-1)
                all_data, _ = download_daily_data(session, base_url, endpoint_name)
                if all_data:
                    save_endpoint_data(all_data, endpoint_name, yesterday, is_period=True)
                    
        except Exception as e:
            print(f"ERRO {endpoint_name}: {str(e)}")
            continue

def save_endpoint_data(data, endpoint_name, yesterday, is_period=True):
    """Salva dados de um endpoint"""
    if not data:
        print(f"SKIP {endpoint_name}: Sem dados")
        return
        
    # Converter para formato padrão
    clean_data = []
    data_list = data if isinstance(data, list) else [data]
    
    for item in data_list:
        if isinstance(item, dict):
            clean_item = {}
            for k, v in item.items():
                if isinstance(v, (list, dict)):
                    clean_item[k] = json.dumps(v, ensure_ascii=False)
                else:
                    clean_item[k] = str(v) if v is not None else ""
            
            # Adicionar metadados
            clean_item.update({
                'data_type': endpoint_name,
                'api_source': 'digit',
                'extraction_date': datetime.now().date().isoformat()
            })
            
            if is_period:
                clean_item['data_referencia'] = yesterday.date().isoformat()
                
            clean_data.append(clean_item)
    
    if clean_data:
        df = spark.createDataFrame(clean_data)
        
        # Particionamento
        year = yesterday.year
        month = yesterday.month
        day = yesterday.day
        
        output_path = f"s3://{bronze_bucket}/digit_daily/{endpoint_name}/year={year}/month={month:02d}/day={day:02d}/"
        df.coalesce(2).write.mode("overwrite").parquet(output_path)
        
        print(f"OK {endpoint_name}: {len(clean_data)} registros salvos em {output_path}")

def main():
    try:
        print("Iniciando carga D-1 - TODOS OS ENDPOINTS")
        download_all_endpoints()
        print("\nCarga D-1 concluída com sucesso!")
        
    except Exception as e:
        print(f"ERRO CRÍTICO: {str(e)}")
        raise

if __name__ == "__main__":
    main()

job.commit()