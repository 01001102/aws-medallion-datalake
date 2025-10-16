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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_BUCKET', 'GOLD_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

silver_bucket = args['SILVER_BUCKET']
gold_bucket = args['GOLD_BUCKET']

def load_silver_table(table_name):
    """Carrega tabela do Silver"""
    path = f"s3://{silver_bucket}/digit/{table_name}/"
    try:
        df = spark.read.parquet(path)
        print(f"SUCESSO {table_name}: {df.count()} registros carregados")
        return df
    except Exception as e:
        print(f"ERRO ao carregar {table_name}: {str(e)}")
        return None

def create_dim_obras():
    """Dimensão Obras"""
    print("Criando DIM_OBRAS...")
    
    obras_df = load_silver_table("obras")
    if obras_df is None:
        return None
    
    # Colunas disponíveis: endereco, nome_obra, codigo_obra, data_processamento
    dim_obras = obras_df.select(
        F.col("codigo_obra").alias("obra_key"),
        F.col("codigo_obra"),
        F.col("nome_obra"),
        F.col("endereco"),
        F.current_timestamp().alias("data_atualizacao")
    ).distinct()
    
    return dim_obras

def create_dim_funcionarios():
    """Dimensão Funcionários"""
    print("Criando DIM_FUNCIONARIOS...")
    
    funcionarios_df = load_silver_table("funcionarios")
    if funcionarios_df is None:
        return None
    
    # Usar apenas colunas disponíveis
    dim_funcionarios = funcionarios_df.select(
        F.col("codigo_funcionario").alias("funcionario_key"),
        F.col("codigo_funcionario"),
        F.col("nome_completo"),
        F.current_timestamp().alias("data_atualizacao")
    ).distinct()
    
    return dim_funcionarios

def create_dim_cargos():
    """Dimensão Cargos"""
    print("Criando DIM_CARGOS...")
    
    cargos_df = load_silver_table("cargos")
    if cargos_df is None:
        return None
    
    # Usar apenas colunas disponíveis
    dim_cargos = cargos_df.select(
        F.col("codigo_cargo").alias("cargo_key"),
        F.col("codigo_cargo"),
        F.col("nome_cargo"),
        F.current_timestamp().alias("data_atualizacao")
    ).distinct()
    
    return dim_cargos

def create_dim_tempo():
    """Dimensão Tempo"""
    print("Criando DIM_TEMPO...")
    
    # Usar horas_funcao que tem data_extracao
    horas_df = load_silver_table("horas_funcao")
    if horas_df is None:
        return None
    
    # Criar dimensão tempo baseada nas datas de extração
    dim_tempo = horas_df.select(
        F.col("data_extracao")
    ).distinct().select(
        F.col("data_extracao").alias("data_key"),
        F.col("data_extracao"),
        F.current_timestamp().alias("data_atualizacao")
    )
    
    return dim_tempo

def create_fato_horas_trabalhadas():
    """Fato principal - Horas Trabalhadas"""
    print("Criando FATO_HORAS_TRABALHADAS...")
    
    horas_df = load_silver_table("horas_funcao")
    if horas_df is None:
        return None
    
    # Usar colunas disponíveis em horas_funcao
    fato_horas = horas_df.select(
        F.col("codigo_obra").alias("obra_key"),
        F.col("codigo_cargo").alias("cargo_key"),
        F.col("nome_cargo"),
        F.col("total_horas"),
        F.col("tipo_endpoint"),
        F.col("fonte_api"),
        F.col("data_referencia"),
        F.col("data_extracao"),
        F.current_timestamp().alias("data_processamento")
    )
    
    return fato_horas

def create_fato_resumo_obras():
    """Fato agregado - Resumo por Obra"""
    print("Criando FATO_RESUMO_OBRAS...")
    
    resumo_df = load_silver_table("resumo_periodo")
    if resumo_df is None:
        return None
    
    # Usar apenas colunas disponíveis
    fato_resumo = resumo_df.select(
        F.col("*"),
        F.current_timestamp().alias("data_processamento_gold")
    )
    
    return fato_resumo

def save_gold_table(df, table_name):
    """Salva tabela no Gold"""
    if df is None or df.count() == 0:
        print(f"AVISO {table_name}: Sem dados para salvar")
        return False
    
    path = f"s3://{gold_bucket}/digit/{table_name}/"
    df.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"SUCESSO {table_name}: {df.count()} registros salvos")
    return True

def create_gold_glue_tables():
    """Cria tabelas Gold no Glue Data Catalog"""
    import boto3
    
    glue = boto3.client('glue')
    database_name = 'digit_gold_db'
    
    tables = {
        'dim_obras': [
            {'Name': 'obra_key', 'Type': 'string'},
            {'Name': 'codigo_obra', 'Type': 'string'},
            {'Name': 'nome_obra', 'Type': 'string'},
            {'Name': 'endereco', 'Type': 'string'},
            {'Name': 'data_atualizacao', 'Type': 'timestamp'}
        ],
        'dim_funcionarios': [
            {'Name': 'funcionario_key', 'Type': 'string'},
            {'Name': 'codigo_funcionario', 'Type': 'string'},
            {'Name': 'nome_completo', 'Type': 'string'},
            {'Name': 'data_atualizacao', 'Type': 'timestamp'}
        ],
        'dim_cargos': [
            {'Name': 'cargo_key', 'Type': 'string'},
            {'Name': 'codigo_cargo', 'Type': 'string'},
            {'Name': 'nome_cargo', 'Type': 'string'},
            {'Name': 'data_atualizacao', 'Type': 'timestamp'}
        ],
        'dim_tempo': [
            {'Name': 'data_key', 'Type': 'string'},
            {'Name': 'data_extracao', 'Type': 'string'},
            {'Name': 'data_atualizacao', 'Type': 'timestamp'}
        ],
        'fato_horas_trabalhadas': [
            {'Name': 'obra_key', 'Type': 'string'},
            {'Name': 'cargo_key', 'Type': 'string'},
            {'Name': 'nome_cargo', 'Type': 'string'},
            {'Name': 'total_horas', 'Type': 'double'},
            {'Name': 'tipo_endpoint', 'Type': 'string'},
            {'Name': 'fonte_api', 'Type': 'string'},
            {'Name': 'data_referencia', 'Type': 'string'},
            {'Name': 'data_extracao', 'Type': 'string'},
            {'Name': 'data_processamento', 'Type': 'timestamp'}
        ],
        'fato_resumo_obras': [
            {'Name': 'data_processamento_gold', 'Type': 'timestamp'}
        ]
    }
    
    for table_name, columns in tables.items():
        try:
            glue.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': columns,
                        'Location': f's3://{gold_bucket}/digit/{table_name}/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE'
                }
            )
            print(f"Tabela {table_name} criada no Glue Catalog")
        except Exception as e:
            if 'AlreadyExistsException' in str(e):
                print(f"Tabela {table_name} já existe")
            else:
                print(f"Erro ao criar tabela {table_name}: {str(e)}")

def main():
    try:
        print("INICIANDO PROCESSAMENTO GOLD")
        
        # Criar dimensões
        dim_obras = create_dim_obras()
        dim_funcionarios = create_dim_funcionarios()
        dim_cargos = create_dim_cargos()
        dim_tempo = create_dim_tempo()
        
        # Criar fatos
        fato_horas = create_fato_horas_trabalhadas()
        fato_resumo = create_fato_resumo_obras()
        
        # Salvar tabelas
        save_gold_table(dim_obras, "dim_obras")
        save_gold_table(dim_funcionarios, "dim_funcionarios")
        save_gold_table(dim_cargos, "dim_cargos")
        save_gold_table(dim_tempo, "dim_tempo")
        save_gold_table(fato_horas, "fato_horas_trabalhadas")
        save_gold_table(fato_resumo, "fato_resumo_obras")
        
        # Criar tabelas no Glue Catalog
        create_gold_glue_tables()
        
        print("PROCESSAMENTO GOLD CONCLUÍDO COM SUCESSO")
        
    except Exception as e:
        print(f"ERRO NO PROCESSAMENTO GOLD: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()