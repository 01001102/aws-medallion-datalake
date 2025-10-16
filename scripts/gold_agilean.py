import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SILVER_DATABASE',
    'GOLD_BUCKET'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def create_project_metrics():
    """Cria métricas de projetos Agilean"""
    try:
        projects_df = spark.sql(f"SELECT * FROM {args['SILVER_DATABASE']}.agilean_projects")
        
        if projects_df.count() == 0:
            return 0
        
        # Métricas de projetos
        project_metrics = projects_df.agg(
            F.count("*").alias("total_projects"),
            F.countDistinct("id").alias("unique_projects"),
            F.count(F.when(F.col("status") == "active", 1)).alias("active_projects"),
            F.count(F.when(F.col("status") == "inactive", 1)).alias("inactive_projects"),
            F.current_timestamp().alias("processing_date")
        ).withColumn("source", F.lit("agilean_projects"))
        
        # Salvar métricas
        output_path = f"s3://{args['GOLD_BUCKET']}/api=agilean/project_metrics/"
        project_metrics.coalesce(1).write.mode("overwrite").parquet(output_path)
        
        return project_metrics.count()
        
    except Exception as e:
        print(f"Erro ao criar métricas de projetos: {str(e)}")
        return 0

def create_baseline_analysis():
    """Cria análise de baseline/orçamento"""
    try:
        baseline_df = spark.sql(f"SELECT * FROM {args['SILVER_DATABASE']}.agilean_baseline_info")
        
        if baseline_df.count() == 0:
            return 0
        
        # Análise de baseline
        baseline_analysis = baseline_df.agg(
            F.count("*").alias("total_baselines"),
            F.sum("budget_amount").alias("total_budget"),
            F.avg("budget_amount").alias("avg_budget"),
            F.max("budget_amount").alias("max_budget"),
            F.min("budget_amount").alias("min_budget"),
            F.current_timestamp().alias("processing_date")
        ).withColumn("source", F.lit("agilean_baseline"))
        
        # Salvar análise
        output_path = f"s3://{args['GOLD_BUCKET']}/api=agilean/baseline_analysis/"
        baseline_analysis.coalesce(1).write.mode("overwrite").parquet(output_path)
        
        return baseline_analysis.count()
        
    except Exception as e:
        print(f"Erro ao criar análise de baseline: {str(e)}")
        return 0

def create_scenario_summary():
    """Cria resumo de cenários"""
    try:
        scenarios_df = spark.sql(f"SELECT * FROM {args['SILVER_DATABASE']}.agilean_scenario_data")
        
        if scenarios_df.count() == 0:
            return 0
        
        # Resumo de cenários
        scenario_summary = scenarios_df.groupBy("scenario_type").agg(
            F.count("*").alias("scenario_count"),
            F.sum("value").alias("total_value"),
            F.avg("value").alias("avg_value"),
            F.current_timestamp().alias("processing_date")
        ).withColumn("source", F.lit("agilean_scenarios"))
        
        # Salvar resumo
        output_path = f"s3://{args['GOLD_BUCKET']}/api=agilean/scenario_summary/"
        scenario_summary.coalesce(1).write.mode("overwrite").parquet(output_path)
        
        return scenario_summary.count()
        
    except Exception as e:
        print(f"Erro ao criar resumo de cenários: {str(e)}")
        return 0

def create_quality_metrics():
    """Cria métricas de qualidade dos dados Agilean"""
    try:
        tables = ["agilean_projects", "agilean_baseline_info", "agilean_scenario_data"]
        quality_metrics = []
        
        for table in tables:
            try:
                df = spark.sql(f"SELECT * FROM {args['SILVER_DATABASE']}.{table}")
                count = df.count()
                
                # Calcular completude
                total_fields = len(df.columns)
                non_null_counts = []
                
                for col in df.columns:
                    non_null_count = df.filter(F.col(col).isNotNull()).count()
                    non_null_counts.append(non_null_count)
                
                avg_completeness = sum(non_null_counts) / (len(non_null_counts) * count) if count > 0 else 0
                
                quality_metrics.append({
                    "table_name": table,
                    "total_records": count,
                    "total_fields": total_fields,
                    "completeness_pct": round(avg_completeness * 100, 2),
                    "quality_status": "GOOD" if avg_completeness > 0.8 else "FAIR" if avg_completeness > 0.5 else "POOR"
                })
                
            except Exception as e:
                print(f"Erro ao processar qualidade da tabela {table}: {str(e)}")
                quality_metrics.append({
                    "table_name": table,
                    "total_records": 0,
                    "total_fields": 0,
                    "completeness_pct": 0.0,
                    "quality_status": "ERROR"
                })
        
        # Criar DataFrame de qualidade
        schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("total_records", IntegerType(), True),
            StructField("total_fields", IntegerType(), True),
            StructField("completeness_pct", DoubleType(), True),
            StructField("quality_status", StringType(), True)
        ])
        
        quality_df = spark.createDataFrame(quality_metrics, schema)
        quality_df = quality_df.withColumn("processing_date", F.current_timestamp())
        
        # Salvar métricas de qualidade
        output_path = f"s3://{args['GOLD_BUCKET']}/api=agilean/quality_metrics/"
        quality_df.coalesce(1).write.mode("overwrite").parquet(output_path)
        
        return quality_df.count()
        
    except Exception as e:
        print(f"Erro ao criar métricas de qualidade: {str(e)}")
        return 0

# Executar todas as análises
try:
    print("Iniciando processamento Gold Agilean...")
    
    project_records = create_project_metrics()
    baseline_records = create_baseline_analysis()
    scenario_records = create_scenario_summary()
    quality_records = create_quality_metrics()
    
    total_records = project_records + baseline_records + scenario_records + quality_records
    
    print(f"Processamento Gold Agilean concluído:")
    print(f"  - Métricas de projetos: {project_records}")
    print(f"  - Análise de baseline: {baseline_records}")
    print(f"  - Resumo de cenários: {scenario_records}")
    print(f"  - Métricas de qualidade: {quality_records}")
    print(f"  - Total de registros: {total_records}")
    
except Exception as e:
    print(f"Erro no processamento Gold Agilean: {str(e)}")
    raise

job.commit()