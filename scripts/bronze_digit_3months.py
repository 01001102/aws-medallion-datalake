import sys
import json
import requests
import time
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
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze_bucket = args['BRONZE_BUCKET']
endpoint_type = args['ENDPOINT_TYPE']

def get_auth_token():
    secrets_client = boto3.client('secretsmanager')
    secret_response = secrets_client.get_secret_value(SecretId='medallion-pipeline/digit-api')
    credentials = json.loads(secret_response['SecretString'])
    
    base_url = "https://www.digitca.com.br/api/ServiceLayerPlano"
    form_data = {'cliente_nome': credentials['cliente_nome'], 'chave_secreta': credentials['chave_secreta']}
    response = requests.post(f"{base_url}/gerarToken", data=form_data, timeout=30)
    
    if response.status_code == 200:
        result = response.json()
        if result.get('success'):
            return result.get('token'), base_url
    raise Exception("Erro ao gerar token")

def get_obras_list(session, base_url):
    """Busca lista de obras"""
    response = session.get(f"{base_url}/getObras", timeout=60)
    if response.status_code == 200:
        obras = response.json()
        return [obra['CodigoObra'] for obra in obras]
    return []

def fetch_master_data(session, base_url, endpoint_type):
    """Busca dados mestres (obras, funcionarios, cargos)"""
    endpoint_map = {
        "obras": "/getObras",
        "funcionarios": "/getFuncionarios", 
        "cargos": "/getCargos"
    }
    
    url = f"{base_url}{endpoint_map[endpoint_type]}"
    response = session.get(url, timeout=60)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"ERRO {response.status_code}: {response.text}")
        return []

def fetch_period_data(session, base_url, endpoint_type, obra_id):
    """Busca dados com período (3 meses)"""
    endpoint_map = {
        "horas_funcao": "/getHorasTrabalhadasPorFuncao",
        "resumo_periodo": "/getResumoHorasPeriodo",
        "relatorio_expandido": "/getRelatorioCargosExpandido"
    }
    
    # Período de 3 meses (maio-julho 2025)
    data_inicio = "01-05-2025"
    data_fim = "31-07-2025"
    
    url = f"{base_url}{endpoint_map[endpoint_type]}?CodigoObra={obra_id}&CodigoFilial=&dataInicio={data_inicio}&dataFim={data_fim}"
    response = session.get(url, timeout=60)
    
    if response.status_code == 200:
        data = response.json()
        
        # Tratar resumo_periodo que retorna objeto
        if endpoint_type == "resumo_periodo" and isinstance(data, dict):
            return [data]  # Converter objeto em array
        
        return data if isinstance(data, list) else []
    else:
        print(f"ERRO obra {obra_id}: {response.status_code} - {response.text}")
        return []

def process_data(data, endpoint_type, obra_id=None):
    """Processa e limpa dados para Spark"""
    if not data:
        return []
    
    clean_data = []
    for item in data:
        clean_item = {}
        
        # Processar campos do item
        for k, v in item.items():
            if isinstance(v, (list, dict)):
                clean_item[k] = json.dumps(v, ensure_ascii=False)
            else:
                clean_item[k] = str(v) if v is not None else ""
        
        # Adicionar metadados
        clean_item.update({
            'data_type': endpoint_type,
            'api_source': 'digit',
            'extraction_timestamp': str(int(time.time())),
            'extraction_date': datetime.now().strftime('%Y-%m-%d')
        })
        
        # Adicionar obra_id para dados de período
        if obra_id:
            clean_item['obra_id'] = str(obra_id)
        
        clean_data.append(clean_item)
    
    return clean_data

def main():
    try:
        print(f"INICIANDO PROCESSAMENTO: {endpoint_type}")
        
        # Gerar token
        token, base_url = get_auth_token()
        session = requests.Session()
        session.headers.update({"Authorization": token})
        
        all_data = []
        
        if endpoint_type in ["obras", "funcionarios", "cargos"]:
            # Dados mestres
            print(f"Buscando dados mestres: {endpoint_type}")
            data = fetch_master_data(session, base_url, endpoint_type)
            processed_data = process_data(data, endpoint_type)
            all_data.extend(processed_data)
            print(f"Coletados {len(processed_data)} registros")
            
        else:
            # Dados com período - processar todas as obras
            print(f"Buscando lista de obras...")
            obras_list = get_obras_list(session, base_url)
            print(f"Encontradas {len(obras_list)} obras")
            
            for obra_id in obras_list:
                print(f"Processando obra {obra_id}...")
                data = fetch_period_data(session, base_url, endpoint_type, obra_id)
                processed_data = process_data(data, endpoint_type, obra_id)
                all_data.extend(processed_data)
                print(f"Obra {obra_id}: {len(processed_data)} registros")
                time.sleep(1)  # Evitar sobrecarga da API
        
        # Salvar no S3
        if all_data:
            print(f"Salvando {len(all_data)} registros no S3...")
            
            df = spark.createDataFrame(all_data)
            output_path = f"s3://{bronze_bucket}/digit/{endpoint_type}/"
            
            df.coalesce(1).write.mode("overwrite").parquet(output_path)
            print(f"SUCESSO: Dados salvos em {output_path}")
            
        else:
            print(f"NENHUM DADO ENCONTRADO PARA {endpoint_type}")
        
    except Exception as e:
        print(f"ERRO CRÍTICO: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()

job.commit()