"""
Email Template Generator - Templates HTML Visuais para Alertas

Este módulo gera templates HTML responsivos e visualmente atraentes para alertas do pipeline.

Autor: Ivan de França
Projeto: Pipeline Medalhão - Sistema de Alertas Visuais
"""

def generate_html_email_template(error_details, severity):
    """
    Gera template HTML responsivo e visualmente atrativo para alertas.
    
    Argumentos:
        error_details (dict): Detalhes do erro
        severity (str): Nível de severidade
        
    Retorna:
        tuple: (subject, html_body) do email
    """
    environment = error_details.get('environment', 'unknown')
    api_name = error_details.get('api_name', 'unknown')
    
    # Configurações visuais por severidade
    severity_config = {
        'critical': {
            'color': '#D32F2F',
            'bg_color': '#FFEBEE',
            'icon': '🚨',
            'label': 'CRÍTICO',
            'border': '#F44336'
        },
        'high': {
            'color': '#F57C00',
            'bg_color': '#FFF3E0',
            'icon': '️',
            'label': 'ALTO',
            'border': '#FF9800'
        },
        'medium': {
            'color': '#1976D2',
            'bg_color': '#E3F2FD',
            'icon': '⚡',
            'label': 'MÉDIO',
            'border': '#2196F3'
        },
        'low': {
            'color': '#388E3C',
            'bg_color': '#E8F5E8',
            'icon': 'ℹ',
            'label': 'BAIXO',
            'border': '#4CAF50'
        }
    }
    
    config = severity_config.get(severity, severity_config['high'])
    
    subject = f"{config['icon']} [{config['label']}] Pipeline Medalhão - {environment.upper()} - {api_name}"
    
    html_body = f"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Alerta Pipeline Medalhão</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
        }}
        
        .container {{
            max-width: 600px;
            margin: 20px auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, {config['color']}, {config['border']});
            color: white;
            padding: 30px 20px;
            text-align: center;
            position: relative;
        }}
        
        .header::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grain" width="100" height="100" patternUnits="userSpaceOnUse"><circle cx="25" cy="25" r="1" fill="white" opacity="0.1"/><circle cx="75" cy="75" r="1" fill="white" opacity="0.1"/></pattern></defs><rect width="100" height="100" fill="url(%23grain)"/></svg>');
            opacity: 0.3;
        }}
        
        .header-content {{
            position: relative;
            z-index: 1;
        }}
        
        .severity-badge {{
            display: inline-block;
            background: rgba(255,255,255,0.2);
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: bold;
            margin-bottom: 10px;
            backdrop-filter: blur(10px);
        }}
        
        .title {{
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 5px;
        }}
        
        .subtitle {{
            font-size: 16px;
            opacity: 0.9;
        }}
        
        .content {{
            padding: 30px 20px;
        }}
        
        .alert-info {{
            background: {config['bg_color']};
            border-left: 4px solid {config['color']};
            padding: 20px;
            margin-bottom: 25px;
            border-radius: 0 8px 8px 0;
        }}
        
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 25px;
        }}
        
        .info-card {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }}
        
        .info-label {{
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
            font-weight: 600;
            margin-bottom: 5px;
        }}
        
        .info-value {{
            font-size: 14px;
            font-weight: 500;
            color: #333;
            word-break: break-all;
        }}
        
        .error-details {{
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 25px;
        }}
        
        .error-title {{
            font-size: 16px;
            font-weight: 600;
            color: #856404;
            margin-bottom: 10px;
            display: flex;
            align-items: center;
        }}
        
        .error-content {{
            background: white;
            padding: 15px;
            border-radius: 6px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 13px;
            line-height: 1.4;
            color: #d63384;
            border-left: 3px solid #dc3545;
            overflow-x: auto;
        }}
        
        .actions {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 25px;
        }}
        
        .action-btn {{
            display: inline-block;
            padding: 12px 20px;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            text-align: center;
            transition: all 0.3s ease;
            font-size: 14px;
        }}
        
        .btn-primary {{
            background: linear-gradient(135deg, #007bff, #0056b3);
            color: white;
        }}
        
        .btn-secondary {{
            background: linear-gradient(135deg, #6c757d, #495057);
            color: white;
        }}
        
        .btn-success {{
            background: linear-gradient(135deg, #28a745, #1e7e34);
            color: white;
        }}
        
        .steps {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 25px;
        }}
        
        .steps-title {{
            font-size: 16px;
            font-weight: 600;
            color: #333;
            margin-bottom: 15px;
            display: flex;
            align-items: center;
        }}
        
        .step {{
            display: flex;
            align-items: flex-start;
            margin-bottom: 12px;
            padding: 10px;
            background: white;
            border-radius: 6px;
            border-left: 3px solid {config['color']};
        }}
        
        .step-number {{
            background: {config['color']};
            color: white;
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 12px;
            font-weight: bold;
            margin-right: 12px;
            flex-shrink: 0;
        }}
        
        .step-text {{
            font-size: 14px;
            color: #333;
        }}
        
        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            border-top: 1px solid #e9ecef;
        }}
        
        .footer-text {{
            font-size: 12px;
            color: #666;
            margin-bottom: 10px;
        }}
        
        .logo {{
            font-size: 18px;
            font-weight: bold;
            color: {config['color']};
        }}
        
        @media (max-width: 600px) {{
            .container {{
                margin: 10px;
                border-radius: 8px;
            }}
            
            .header {{
                padding: 20px 15px;
            }}
            
            .title {{
                font-size: 20px;
            }}
            
            .content {{
                padding: 20px 15px;
            }}
            
            .info-grid {{
                grid-template-columns: 1fr;
            }}
            
            .actions {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <div class="header-content">
                <div class="severity-badge">{config['icon']} {config['label']}</div>
                <div class="title">Pipeline Medalhão</div>
                <div class="subtitle">Sistema de Alertas Inteligente</div>
            </div>
        </div>
        
        <!-- Content -->
        <div class="content">
            <!-- Alert Info -->
            <div class="alert-info">
                <strong>{config['icon']} Alerta de Severidade {config['label']}</strong><br>
                Um erro foi detectado no pipeline de dados e requer atenção imediata.
            </div>
            
            <!-- Info Grid -->
            <div class="info-grid">
                <div class="info-card">
                    <div class="info-label">🌍 Ambiente</div>
                    <div class="info-value">{environment.upper()}</div>
                </div>
                <div class="info-card">
                    <div class="info-label">🔗 API</div>
                    <div class="info-value">{api_name}</div>
                </div>
                <div class="info-card">
                    <div class="info-label">⏰ Timestamp</div>
                    <div class="info-value">{error_details['timestamp']}</div>
                </div>
                <div class="info-card">
                    <div class="info-label">🆔 Execução</div>
                    <div class="info-value">{error_details.get('execution_arn', 'N/A')[-20:]}</div>
                </div>
            </div>
            
            <!-- Error Details -->
            <div class="error-details">
                <div class="error-title">
                    🐛 Detalhes do Erro
                </div>
                <div class="error-content">
                    <strong>Erro:</strong> {error_details.get('error', 'N/A')}<br><br>
                    <strong>Causa:</strong> {error_details.get('cause', 'N/A')}
                </div>
            </div>
            
            <!-- Action Buttons -->
            <div class="actions">
                <a href="https://console.aws.amazon.com/cloudwatch/home#logsV2:log-groups/log-group/$252Faws-glue$252Fjobs$252F{api_name}" class="action-btn btn-primary">
                    📊 Ver Logs CloudWatch
                </a>
                <a href="https://console.aws.amazon.com/states/home" class="action-btn btn-secondary">
                    🔄 Ver Step Functions
                </a>
                <a href="https://console.aws.amazon.com/dynamodbv2/home" class="action-btn btn-success">
                     Ver Watermarks
                </a>
            </div>
            
            <!-- Next Steps -->
            <div class="steps">
                <div class="steps-title">
                    🔍 Próximos Passos Recomendados
                </div>
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Verificar logs detalhados no CloudWatch para identificar a causa raiz</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Consultar métricas de qualidade na camada Gold para impacto</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Validar watermarks no DynamoDB para status de processamento</div>
                </div>
                <div class="step">
                    <div class="step-number">4</div>
                    <div class="step-text">Verificar conectividade e status da API externa</div>
                </div>
                <div class="step">
                    <div class="step-number">5</div>
                    <div class="step-text">Executar reprocessamento manual se necessário</div>
                </div>
            </div>
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <div class="footer-text">
                Sistema de Alertas Pipeline Medalhão<br>
                Gerado automaticamente em {error_details['timestamp']}
            </div>
            <div class="logo">STIT Cloud</div>
        </div>
    </div>
</body>
</html>
    """
    
    return subject, html_body