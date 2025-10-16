import boto3
import time
from datetime import datetime

def run_digit_pipeline():
    """Executa pipeline Digit: Bronze -> Silver -> Gold"""
    
    glue = boto3.client('glue')
    
    # Nomes dos jobs
    jobs = [
        'bronze-digit-daily',
        'silver-digit-clean', 
        'gold-digit-star-schema'
    ]
    
    print("=== EXECUTANDO PIPELINE DIGIT ===")
    
    for i, job_name in enumerate(jobs, 1):
        print(f"\n{i}/3 - Executando {job_name}...")
        
        try:
            # Iniciar job
            response = glue.start_job_run(JobName=job_name)
            job_run_id = response['JobRunId']
            print(f"Job iniciado: {job_run_id}")
            
            # Aguardar conclusão
            while True:
                status = glue.get_job_run(JobName=job_name, RunId=job_run_id)
                state = status['JobRun']['JobRunState']
                
                if state == 'SUCCEEDED':
                    print(f"✓ {job_name} concluído com sucesso")
                    break
                elif state in ['FAILED', 'ERROR', 'TIMEOUT']:
                    print(f"✗ {job_name} falhou: {state}")
                    return False
                else:
                    print(f"  Status: {state}")
                    time.sleep(30)
                    
        except Exception as e:
            print(f"✗ Erro ao executar {job_name}: {str(e)}")
            return False
    
    print("\n🎉 PIPELINE CONCLUÍDO COM SUCESSO!")
    print("\nVerifique no Athena:")
    print("- Database: digit_silver_db")
    print("- Database: digit_gold_db")
    
    return True

if __name__ == "__main__":
    success = run_digit_pipeline()
    exit(0 if success else 1)