import boto3

def create_silver_tables():
    """Cria tabelas Silver no Athena agora"""
    
    glue = boto3.client('glue')
    database_name = 'digit_silver_db'
    silver_bucket = 'medallion-pipeline-dev-silver-65f61163'
    
    # Tabelas Silver
    tables = {
        'digit_obras': {
            'location': f's3://{silver_bucket}/digit/obras/',
            'columns': [
                {'Name': 'codigo_obra', 'Type': 'string'},
                {'Name': 'nome_obra', 'Type': 'string'},
                {'Name': 'endereco', 'Type': 'string'},
                {'Name': 'data_inicio', 'Type': 'string'},
                {'Name': 'data_fim', 'Type': 'string'},
                {'Name': 'margem_superior', 'Type': 'int'},
                {'Name': 'margem_inferior', 'Type': 'int'},
                {'Name': 'area_prefeitura', 'Type': 'string'},
                {'Name': 'tipologia', 'Type': 'string'},
                {'Name': 'uso', 'Type': 'string'},
                {'Name': 'data_processamento', 'Type': 'string'},
                {'Name': 'fonte', 'Type': 'string'}
            ]
        },
        'digit_funcionarios': {
            'location': f's3://{silver_bucket}/digit/funcionarios/',
            'columns': [
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
                {'Name': 'data_processamento', 'Type': 'string'},
                {'Name': 'fonte', 'Type': 'string'}
            ]
        },
        'digit_cargos': {
            'location': f's3://{silver_bucket}/digit/cargos/',
            'columns': [
                {'Name': 'codigo_cargo', 'Type': 'string'},
                {'Name': 'nome_cargo', 'Type': 'string'},
                {'Name': 'data_processamento', 'Type': 'string'},
                {'Name': 'fonte', 'Type': 'string'}
            ]
        },
        'digit_horas_trabalhadas': {
            'location': f's3://{silver_bucket}/digit/horas_trabalhadas/',
            'columns': [
                {'Name': 'codigo_obra', 'Type': 'string'},
                {'Name': 'codigo_cargo', 'Type': 'string'},
                {'Name': 'nome_cargo', 'Type': 'string'},
                {'Name': 'total_horas', 'Type': 'double'},
                {'Name': 'periodo_inicio', 'Type': 'string'},
                {'Name': 'periodo_fim', 'Type': 'string'},
                {'Name': 'data_extracao', 'Type': 'string'},
                {'Name': 'data_processamento', 'Type': 'string'},
                {'Name': 'fonte', 'Type': 'string'}
            ]
        },
        'digit_resumo_periodo': {
            'location': f's3://{silver_bucket}/digit/resumo_periodo/',
            'columns': [
                {'Name': 'codigo_obra', 'Type': 'string'},
                {'Name': 'total_horas_periodo', 'Type': 'double'},
                {'Name': 'total_funcionarios', 'Type': 'int'},
                {'Name': 'dias_periodo', 'Type': 'int'},
                {'Name': 'periodo_inicio', 'Type': 'string'},
                {'Name': 'periodo_fim', 'Type': 'string'},
                {'Name': 'data_extracao', 'Type': 'string'},
                {'Name': 'data_processamento', 'Type': 'string'},
                {'Name': 'fonte', 'Type': 'string'}
            ]
        },
        'digit_funcionarios_horas': {
            'location': f's3://{silver_bucket}/digit/funcionarios_horas/',
            'columns': [
                {'Name': 'codigo_obra', 'Type': 'string'},
                {'Name': 'codigo_funcionario', 'Type': 'string'},
                {'Name': 'codigo_cargo', 'Type': 'string'},
                {'Name': 'horas_trabalhadas', 'Type': 'double'},
                {'Name': 'total_horas_cargo', 'Type': 'double'},
                {'Name': 'periodo_inicio', 'Type': 'string'},
                {'Name': 'periodo_fim', 'Type': 'string'},
                {'Name': 'data_extracao', 'Type': 'string'},
                {'Name': 'data_processamento', 'Type': 'string'},
                {'Name': 'fonte', 'Type': 'string'}
            ]
        }
    }
    
    for table_name, config in tables.items():
        try:
            glue.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': config['columns'],
                        'Location': config['location'],
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE'
                }
            )
            print(f"✓ {table_name}")
        except:
            print(f"⚠ {table_name} já existe")

if __name__ == "__main__":
    create_silver_tables()
    print("\n✅ Tabelas Silver criadas no Athena!")