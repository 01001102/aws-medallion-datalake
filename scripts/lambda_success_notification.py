import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """Lambda para notificação de sucesso do pipeline"""
    
    print(f"Pipeline executado com sucesso: {json.dumps(event)}")
    
    pipeline = event.get('pipeline', 'unknown')
    status = event.get('status', 'SUCCESS')
    execution_date = event.get('execution_date', datetime.now().isoformat())
    
    # Log de sucesso
    print(f"✓ Pipeline {pipeline} concluído com sucesso")
    print(f"  Data de execução: {execution_date}")
    print(f"  Status: {status}")
    
    # Aqui você pode adicionar:
    # - Envio de email
    # - Notificação Slack
    # - Atualização de dashboard
    # - Métricas CloudWatch
    
    # Exemplo de métrica CloudWatch
    try:
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='MedallionPipeline',
            MetricData=[
                {
                    'MetricName': 'PipelineSuccess',
                    'Dimensions': [
                        {
                            'Name': 'Pipeline',
                            'Value': pipeline
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
        )
        print("✓ Métrica de sucesso enviada para CloudWatch")
    except Exception as e:
        print(f"⚠ Erro ao enviar métrica: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Pipeline {pipeline} executado com sucesso',
            'execution_date': execution_date,
            'status': status
        })
    }