import json
import boto3
import os
from datetime import datetime
from email_templates import generate_html_email_template

def lambda_handler(event, context):
    """
    Lambda para enviar notificação de erro do pipeline Digit
    """
    
    # Cliente SES para envio de email
    ses = boto3.client('ses', region_name='us-east-1')
    
    # Email de destino da variável de ambiente
    recipient_email = os.environ.get('RECIPIENT_EMAIL', 'ivan.franca@stitcloud.com')
    
    # Extrair informações do erro
    error_info = event.get('Error', 'Unknown Error')
    cause = event.get('Cause', 'Erro desconhecido')
    state_name = event.get('StateName', 'Unknown')
    execution_name = event.get('ExecutionName', 'Unknown')
    
    # Determinar camada baseado no job name do erro
    job_name = ''
    if isinstance(cause, str) and 'JobName' in cause:
        try:
            import re
            job_match = re.search(r'"JobName":"([^"]+)"', cause)
            if job_match:
                job_name = job_match.group(1)
        except:
            pass
    
    # Identificar camada pelo job name
    if 'bronze' in job_name.lower():
        camada = 'BRONZE'
        api_name = 'digit-bronze'
    elif 'silver' in job_name.lower():
        camada = 'SILVER'
        api_name = 'digit-silver'
    elif 'gold' in job_name.lower():
        camada = 'GOLD'
        api_name = 'digit-gold'
    else:
        camada = 'UNKNOWN'
        api_name = 'digit-pipeline'
    
    # Timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Preparar detalhes do erro para o template
    error_details = {
        'environment': 'dev',
        'api_name': api_name,
        'timestamp': timestamp,
        'error': error_info,
        'cause': cause,
        'execution_arn': execution_name,
        'layer': camada
    }
    
    # Gerar email usando template existente
    subject, body_html = generate_html_email_template(error_details, 'critical')
    
    # Corpo do email
    subject = f"🚨 ERRO Pipeline Digit - {camada} Layer"
    
    body_html = f"""
    <html>
    <body>
        <h2 style="color: #d32f2f;">❌ Pipeline Digit - Falha na Execução</h2>
        
        <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Timestamp</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{timestamp}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">API</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{api_name}</td>
            </tr>
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Camada</td>
                <td style="padding: 10px; border: 1px solid #ddd; color: #d32f2f; font-weight: bold;">{camada}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Tipo de Erro</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{error_info}</td>
            </tr>
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Detalhes</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{cause}</td>
            </tr>
        </table>
        
        <p><strong>Ação Necessária:</strong> Verificar logs do AWS Glue e corrigir o problema antes de reexecutar o pipeline.</p>
        
        <hr>
        <p style="font-size: 12px; color: #666;">
            Pipeline Medalhão Digit - Arquitetura AWS<br>
            Ambiente: DEV | Região: us-east-1
        </p>
    </body>
    </html>
    """
    
    try:
        # Enviar email
        response = ses.send_email(
            Source='portaldocliente@planoeplano.com.br',
            Destination={
                'ToAddresses': [recipient_email]
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Html': {
                        'Data': body_html,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        
        print(f"Email de erro enviado com sucesso: {response['MessageId']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Notificação de erro enviada',
                'camada': camada,
                'api': api_name,
                'error_type': error_info
            })
        }
        
    except Exception as e:
        print(f"Erro ao enviar email: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }