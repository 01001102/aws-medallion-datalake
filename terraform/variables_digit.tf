# Variáveis específicas para API Digit
variable "digit_cliente_nome" {
  description = "Nome do cliente para autenticação na API Digit"
  type        = string
  default     = "planoeplano"
  sensitive   = true
}

variable "digit_chave_secreta" {
  description = "Chave secreta para autenticação na API Digit"
  type        = string
  default     = "KeyPlano&Plano"
  sensitive   = true
}

variable "digit_schedule_enabled" {
  description = "Habilitar agendamento automático do Bronze Digit"
  type        = bool
  default     = true
}

variable "digit_execution_timeout" {
  description = "Timeout em minutos para execução do job Bronze Digit"
  type        = number
  default     = 60
}

variable "digit_worker_type" {
  description = "Tipo de worker para o Glue Job Bronze Digit"
  type        = string
  default     = "G.1X"
}

variable "digit_number_of_workers" {
  description = "Número de workers para o Glue Job Bronze Digit"
  type        = number
  default     = 2
}