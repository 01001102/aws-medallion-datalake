import boto3
from datetime import datetime

class WatermarkHelper:
    def __init__(self, table_name="medallion-pipeline-dev-watermark"):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
    
    def get_last_processed_date(self, api_name, endpoint_type):
        """Busca última data processada"""
        key = f"{api_name}_{endpoint_type}"
        
        try:
            response = self.table.get_item(Key={'api_name': key})
            if 'Item' in response:
                return response['Item']['last_processed_date']
            return None
        except:
            return None
    
    def update_watermark(self, api_name, endpoint_type, processed_date):
        """Atualiza watermark com última data processada"""
        key = f"{api_name}_{endpoint_type}"
        
        self.table.put_item(
            Item={
                'api_name': key,
                'last_processed_date': processed_date,
                'updated_at': datetime.now().isoformat()
            }
        )
    
    def get_new_partitions(self, api_name, endpoint_type, all_partitions):
        """Retorna apenas partições não processadas"""
        last_date = self.get_last_processed_date(api_name, endpoint_type)
        
        if not last_date:
            return all_partitions  # Primeira execução
        
        # Filtrar apenas partições após última data
        new_partitions = [p for p in all_partitions if p > last_date]
        return new_partitions