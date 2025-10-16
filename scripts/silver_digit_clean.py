import sys
import json
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze_bucket = args['BRONZE_BUCKET']
silver_bucket = args['SILVER_BUCKET']

def load_bronze_data():
    """Carrega dados do Bronze"""
    path = f"s3://{bronze_bucket}/digit_daily/"
    try:
        df = spark.read.parquet(path)
        print(f"Bronze carregado: {df.count()} registros")
        return df
    except Exception as e:
        print(f"Erro ao carregar Bronze: {str(e)}")
        return None

def load_bronze_table(table_name):
    """Carrega tabela do Bronze - formato particionado D-1"""
    # Tentar novo formato particionado primeiro
    path_daily = f"s3://{bronze_bucket}/digit_daily/{table_name}/"
    try:
        df = spark.read.parquet(path_daily)
        print(f"SUCESSO {table_name} (D-1): {df.count()} registros carregados")
        return df
    except Exception as e1:
        # Fallback para formato antigo
        path_old = f"s3://{bronze_bucket}/digit/{table_name}/"
        try:
            df = spark.read.parquet(path_old)
            print(f"SUCESSO {table_name} (antigo): {df.count()} registros carregados")
            return df
        except Exception as e2:
            print(f"ERRO ao carregar {table_name}: D-1={str(e1)}, Antigo={str(e2)}")
            return None

def clean_and_validate_bronze_data():
    """Limpa e valida dados Bronze - todos os endpoints disponíveis"""
    
    tables = {}
    
    # Carregar todos os endpoints do Bronze
    endpoints = ["obras", "funcionarios", "cargos", "horas_funcao", "resumo_periodo", "relatorio_expandido"]
    
    for endpoint in endpoints:
        bronze_df = load_bronze_table(endpoint)
        
        if bronze_df is None:
            print(f"SKIP {endpoint}: Não encontrado no Bronze")
            continue
            
        # Processar baseado no endpoint
        if endpoint == "horas_funcao":
            # Colunas: CodigoCargo, DescricaoCargo, TotalHoras, api_source, codigo_obra, data_referencia, data_type, day, extraction_date, month, year
            clean_df = bronze_df.select(
                F.col("codigo_obra").cast("string").alias("codigo_obra"),
                F.col("CodigoCargo").cast("string").alias("codigo_cargo"),
                F.trim(F.col("DescricaoCargo")).alias("nome_cargo"),
                F.col("TotalHoras").cast("double").alias("total_horas"),
                F.col("data_type").alias("tipo_endpoint"),
                F.col("api_source").alias("fonte_api"),
                F.col("data_referencia").alias("data_referencia"),
                F.col("extraction_date").alias("data_extracao"),
                F.current_timestamp().alias("data_processamento")
            ).filter(F.col("codigo_obra").isNotNull())
            
        elif endpoint == "obras":
            # Assumir colunas padrão de obras
            clean_df = bronze_df.select(
                F.col("CodigoObra").cast("string").alias("codigo_obra"),
                F.coalesce(F.col("DescricaoObra"), F.lit("")).alias("nome_obra"),
                F.coalesce(F.col("endereco"), F.lit("")).alias("endereco"),
                F.current_timestamp().alias("data_processamento")
            ).filter(F.col("codigo_obra").isNotNull()).distinct()
            
        elif endpoint == "funcionarios":
            # Assumir colunas padrão de funcionários
            clean_df = bronze_df.select(
                F.col("CodigoFuncionario").cast("string").alias("codigo_funcionario"),
                F.coalesce(F.col("NomeCompleto"), F.lit("")).alias("nome_completo"),
                F.current_timestamp().alias("data_processamento")
            ).filter(F.col("codigo_funcionario").isNotNull()).distinct()
            
        elif endpoint == "cargos":
            # Assumir colunas padrão de cargos
            clean_df = bronze_df.select(
                F.col("CodigoCargo").cast("string").alias("codigo_cargo"),
                F.coalesce(F.col("DescricaoCargo"), F.lit("")).alias("nome_cargo"),
                F.current_timestamp().alias("data_processamento")
            ).filter(F.col("codigo_cargo").isNotNull()).distinct()
            
        else:
            # Para outros endpoints, fazer limpeza básica
            clean_df = bronze_df.select(
                F.col("*"),
                F.current_timestamp().alias("data_processamento")
            )
        
        tables[endpoint] = clean_df
        print(f"PROCESSADO {endpoint}: {clean_df.count()} registros")
    
    return tables

def save_silver_table(df, table_name):
    """Salva tabela no Silver"""
    if df is None or df.count() == 0:
        print(f"AVISO {table_name}: Sem dados para salvar")
        return False
    
    path = f"s3://{silver_bucket}/digit/{table_name}/"
    df.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"SUCESSO {table_name}: {df.count()} registros salvos")
    return True

def create_silver_glue_tables():
    """Cria tabelas Silver no Glue Data Catalog"""
    import boto3
    
    glue = boto3.client('glue')
    database_name = 'digit_silver_db'
    
    # Criar database
    try:
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Digit Silver Layer Database'
            }
        )
        print(f"Database {database_name} criado")
    except:
        print(f"Database {database_name} já existe")
    
    # Definir tabelas Silver
    tables = {
        'obras': [
            {'Name': 'codigo_obra', 'Type': 'string'},
            {'Name': 'nome_obra', 'Type': 'string'},
            {'Name': 'endereco', 'Type': 'string'},
            {'Name': 'data_inicio', 'Type': 'string'},
            {'Name': 'data_fim', 'Type': 'string'},
            {'Name': 'margem_superior', 'Type': 'double'},
            {'Name': 'margem_inferior', 'Type': 'double'},
            {'Name': 'area_prefeitura', 'Type': 'string'},
            {'Name': 'tipologia', 'Type': 'string'},
            {'Name': 'uso', 'Type': 'string'},
            {'Name': 'data_processamento', 'Type': 'timestamp'}
        ],
        'funcionarios': [
            {'Name': 'codigo_funcionario', 'Type': 'string'},
            {'Name': 'nome_completo', 'Type': 'string'},
            {'Name': 'cpf', 'Type': 'string'},
            {'Name': 'genero', 'Type': 'string'},
            {'Name': 'data_nascimento', 'Type': 'string'},
            {'Name': 'data_admissao', 'Type': 'string'},
            {'Name': 'status_ativo', 'Type': 'boolean'},
            {'Name': 'primeiro_emprego', 'Type': 'boolean'},
            {'Name': 'codigo_filial', 'Type': 'string'},
            {'Name': 'descricao_filial', 'Type': 'string'},
            {'Name': 'data_processamento', 'Type': 'timestamp'}
        ],
        'cargos': [
            {'Name': 'codigo_cargo', 'Type': 'string'},
            {'Name': 'nome_cargo', 'Type': 'string'},
            {'Name': 'data_processamento', 'Type': 'timestamp'}
        ],
        'funcionarios_horas': [
            {'Name': 'codigo_obra', 'Type': 'string'},
            {'Name': 'codigo_funcionario', 'Type': 'string'},
            {'Name': 'codigo_cargo', 'Type': 'string'},
            {'Name': 'horas_trabalhadas', 'Type': 'double'},
            {'Name': 'total_horas_cargo', 'Type': 'double'},
            {'Name': 'nome_funcionario', 'Type': 'string'},
            {'Name': 'data_extracao', 'Type': 'string'},
            {'Name': 'timestamp_extracao', 'Type': 'timestamp'},
            {'Name': 'data_processamento', 'Type': 'timestamp'}
        ],
        'horas_funcao': [
            {'Name': 'codigo_obra', 'Type': 'string'},
            {'Name': 'codigo_cargo', 'Type': 'string'},
            {'Name': 'nome_cargo', 'Type': 'string'},
            {'Name': 'total_horas', 'Type': 'double'},
            {'Name': 'data_extracao', 'Type': 'string'},
            {'Name': 'timestamp_extracao', 'Type': 'timestamp'},
            {'Name': 'data_processamento', 'Type': 'timestamp'}
        ],
        'resumo_periodo': [
            {'Name': 'codigo_obra', 'Type': 'string'},
            {'Name': 'total_horas_periodo', 'Type': 'double'},
            {'Name': 'total_funcionarios', 'Type': 'int'},
            {'Name': 'dias_periodo', 'Type': 'int'},
            {'Name': 'data_extracao', 'Type': 'string'},
            {'Name': 'timestamp_extracao', 'Type': 'timestamp'},
            {'Name': 'data_processamento', 'Type': 'timestamp'}
        ]
    }
    
    # Criar cada tabela
    for table_name, columns in tables.items():
        try:
            glue.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': columns,
                        'Location': f's3://{silver_bucket}/digit/{table_name}/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE'
                }
            )
            print(f"Tabela Silver {table_name} criada no Athena")
        except Exception as e:
            print(f"Erro ao criar tabela {table_name}: {str(e)}")

def main():
    try:
        print("INICIANDO PROCESSAMENTO SILVER - LIMPEZA E VALIDAÇÃO")
        
        # Limpar e validar dados Bronze (conceito Silver correto)
        silver_tables = clean_and_validate_bronze_data()
        
        # Salvar tabelas Silver limpas
        for table_name, df in silver_tables.items():
            save_silver_table(df, table_name)
        
        # Criar tabelas no Athena
        create_silver_glue_tables()
        
        print("SILVER DIGIT CONCLUÍDO - DADOS LIMPOS E VALIDADOS")
        
    except Exception as e:
        print(f"ERRO CRÍTICO: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()

job.commit()