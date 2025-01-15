import os
import time
import json
import boto3
import logging
from datetime import datetime
from ddtrace import tracer, config

# -----------------
# Configuração do Datadog e Logging
# -----------------
config.env = os.getenv("DD_ENV", "production")
config.service = os.getenv("DD_SERVICE", "ecs-task-gui")
config.version = os.getenv("DD_VERSION", "1.0.0")
config.logs_injection = True

# Configuração do logger com saída JSON
class JSONFormatter(logging.Formatter):
    def format(self, record):
        current_span = tracer.current_span()
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "filename": record.filename,
            "lineno": record.lineno,
            "dd.service": config.service,
            "dd.env": config.env,
            "dd.version": config.version,
            "dd.trace_id": current_span.trace_id if current_span else None,
            "dd.span_id": current_span.span_id if current_span else None,
        }
        return json.dumps(log_record)

json_formatter = JSONFormatter()
handler = logging.StreamHandler()
handler.setFormatter(json_formatter)
log = logging.getLogger(__name__)
log.addHandler(handler)
log.setLevel(logging.INFO)

# -----------------
# Configuração do SQS
# -----------------
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
DLQ_URL = os.getenv("DLQ_URL")  # Fila de Dead Letter
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
MAX_NUMBER_OF_MESSAGES = int(os.getenv("MAX_NUMBER_OF_MESSAGES", 5))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", 30))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 5))

sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# -----------------
# Funções Auxiliares
# -----------------

@tracer.wrap("process_message")
def process_message(message):
    """Processa uma mensagem SQS."""
    receipt_handle = message["ReceiptHandle"]
    body = message.get("Body", "{}")

    try:
        payload = json.loads(body)
        log.info({"action": "process_message", "status": "success", "payload": payload})
        # Simular processamento de 2 segundos
        time.sleep(2)
        
        # Marcar mensagem como processada (excluí-la da fila SQS)
        sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        log.info({"action": "delete_message", "status": "success"})
    except json.JSONDecodeError as e:
        log.error({"action": "parse_payload", "status": "error", "error": str(e), "message_body": body})
        send_to_dlq(body)  # Enviar para a Dead Letter Queue
    except Exception as e:
        log.error({"action": "process_message", "status": "error", "error": str(e)})

@tracer.wrap("send_to_dlq")
def send_to_dlq(message_body):
    """Envia uma mensagem para a Dead Letter Queue (DLQ)."""
    if not DLQ_URL:
        log.error({"action": "send_to_dlq", "status": "error", "message": "DLQ_URL não configurada."})
        return

    try:
        sqs_client.send_message(QueueUrl=DLQ_URL, MessageBody=message_body)
        log.info({"action": "send_to_dlq", "status": "success", "message_body": message_body})
    except Exception as e:
        log.error({"action": "send_to_dlq", "status": "error", "error": str(e)})

@tracer.wrap("poll_sqs")
def poll_sqs():
    """Consulta mensagens na fila SQS."""
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
                WaitTimeSeconds=10,  # Long Polling
                VisibilityTimeout=VISIBILITY_TIMEOUT,
            )
            messages = response.get("Messages", [])
            log.info({"action": "poll_sqs", "status": "fetching_messages", "message_count": len(messages)})

            for message in messages:
                with tracer.trace("process_message", resource="handle_message") as span:
                    span.set_tag("sqs.message_id", message.get("MessageId"))
                    span.set_tag("sqs.queue_url", SQS_QUEUE_URL)
                    process_message(message)

        except Exception as e:
            log.error({"action": "poll_sqs", "status": "error", "error": str(e)})

        # Aguarde antes de tentar novamente
        time.sleep(POLL_INTERVAL_SECONDS)


# -----------------
# Função Principal
# -----------------
def main():
    log.info({"action": "start_script", "status": "running"})
    if not SQS_QUEUE_URL:
        log.error({"action": "start_script", "status": "error", "message": "SQS_QUEUE_URL não configurada."})
        return
    poll_sqs()

if __name__ == "__main__":
    main()
