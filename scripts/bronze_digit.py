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

# Configurações Spark
spark.conf.set("spark.rpc.message.maxSize", "512m")
spark.conf.set("spark.sql.adaptive.enabled", "true")

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

def main():
    try:
        token, base_url = get_auth_token()
        session = requests.Session()
        session.headers.update({"Authorization": token})
        
        print(f"Iniciando carga D-1: {endpoint_type}")
        
        # Apenas endpoints com período fazem sentido para D-1
        period_endpoints = ["horas_funcao", "resumo_periodo", "relatorio_expandido"]
        
        if endpoint_type not in period_endpoints:
            print(f"AVISO: Endpoint {endpoint_type} não suporta carga D-1")
            return
        
        all_data, yesterday = download_daily_data(session, base_url, endpoint_type)
        
        # Salvar no S3 particionado por data
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
            
            # Particionamento por ano/mês/dia
            year = yesterday.year
            month = yesterday.month
            day = yesterday.day
            
            output_path = f"s3://{bronze_bucket}/digit_daily/{endpoint_type}/year={year}/month={month:02d}/day={day:02d}/"
            df.coalesce(5).write.mode("overwrite").parquet(output_path)
            
            print(f"OK {endpoint_type}: {len(clean_data)} registros D-1 salvos")
            print(f"Caminho: {output_path}")
        else:
            print(f"AVISO Nenhum dado D-1 encontrado para {endpoint_type}")
        
    except Exception as e:
        print(f"ERRO: {str(e)}")
        raise

if __name__ == "__main__":
    main()

job.commit()