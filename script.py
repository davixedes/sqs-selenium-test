import os
import time
import uuid
import logging
import json
from datetime import datetime

import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from ddtrace import tracer, patch, config

# -----------------
# Configuração do Datadog e Logging
# -----------------
# Habilitar integrações automáticas do Datadog, como `boto3`
patch(logging=True)

# Configurar o tracer para habilitar o modo de debug e rastreamento detalhado
tracer.configure(
    settings={"logs": True}  # Habilitar envio de logs do tracer
)

# Configuração do logger com saída JSON para facilitar a integração com o Datadog
class JSONFormatter(logging.Formatter):
    def format(self, record):
        current_span = tracer.current_span()  # Obter o span atual
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

# Configuração do tracer com variáveis de ambiente
config.env = os.getenv("DD_ENV", "production")
config.service = os.getenv("DD_SERVICE", "ecs-task-gui")
config.version = os.getenv("DD_VERSION", "1.0.0")

# -----------------
# Variáveis de Ambiente
# -----------------
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver')
CHROME_BINARY_PATH = os.getenv('CHROME_BINARY_PATH', '/usr/bin/google-chrome')
WEBSITE_URL = os.getenv(
    'WEBSITE_URL',
    'https://satsp.fazenda.sp.gov.br/COMSAT/Public/ConsultaPublica/ConsultaPublicaCfe.aspx'
)
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # URL da fila SQS
DLQ_URL = os.getenv('DLQ_URL')  # URL da Dead Letter Queue (DLQ)
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')
MAX_NUMBER_OF_MESSAGES = int(os.getenv('MAX_NUMBER_OF_MESSAGES', 5))
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 5))

# Cliente SQS
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

# -----------------
# Funções
# -----------------

def setup_driver():
    """Configura o ChromeDriver com as opções adequadas."""
    chrome_options = Options()
    chrome_options.binary_location = CHROME_BINARY_PATH
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1200,800")
    chrome_options.add_argument("--disable-extensions")
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def send_to_dlq(message_body):
    """Envia uma mensagem para a Dead Letter Queue (DLQ)."""
    if not DLQ_URL:
        log.error("DLQ_URL não está definida. Mensagem não pode ser enviada para a DLQ.")
        return

    try:
        sqs_client.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=message_body
        )
        log.info({"action": "send_to_dlq", "status": "success", "message_body": message_body})
    except Exception as e:
        log.error({"action": "send_to_dlq", "status": "error", "error": str(e)})

def process_message(message):
    """Processa uma mensagem SQS e navega no site usando Selenium."""
    body = message.get('Body', '{}')
    receipt_handle = message['ReceiptHandle']
    log.info({"action": "receive_message", "message_body": body})

    # Parâmetros padrão e extraídos do payload
    request_id = str(uuid.uuid4())
    payload = {}
    site_url = WEBSITE_URL  # URL fixa como fallback

    # Parse do payload
    try:
        payload = json.loads(body)  # Converte o Body para dicionário
        site_url = payload.get('WEBSITE_URL', WEBSITE_URL)
    except Exception as e:
        log.error({"action": "parse_payload", "status": "error", "error": str(e)})
        send_to_dlq(body)  # Envia a mensagem para a DLQ em caso de erro no parse
        return

    log.info({"action": "parse_payload", "status": "success", "payload": payload})

    # Criar o trace para monitorar a navegação
    with tracer.trace("process_message", service="selenium-task", resource="process_message") as span:
        span.set_tag("request_id", request_id)
        span.set_tag("payload", payload)

        # Configura o driver e navega para o site
        driver = setup_driver()
        try:
            log.info({"action": "navigate_to_site", "url": site_url})
            driver.get(site_url)  # Navegar para o site
            time.sleep(10)  # Simula navegação no site
            log.info({"action": "navigate_to_site", "status": "success", "url": site_url})
        except Exception as e:
            log.error({"action": "navigate_to_site", "status": "error", "error": str(e)})
            span.set_tag("error", str(e))
            send_to_dlq(body)  # Envia a mensagem para a DLQ em caso de falha de navegação
        finally:
            driver.quit()

        # Marcar mensagem como processada (delete da fila)
        try:
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            log.info({"action": "delete_message", "status": "success"})
        except Exception as e:
            log.error({"action": "delete_message", "status": "error", "error": str(e)})
            send_to_dlq(body)  # Envia a mensagem para a DLQ se não puder ser removida da fila

def poll_sqs():
    """Loop para consultar mensagens na fila SQS."""
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,  # Processa apenas uma mensagem por vez
                WaitTimeSeconds=2,
                VisibilityTimeout=30
            )
            messages = response.get('Messages', [])
            if not messages:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            for message in messages:
                process_message(message)

        except Exception as e:
            log.error({"action": "poll_sqs", "status": "error", "error": str(e)})
            time.sleep(POLL_INTERVAL_SECONDS)

def main():
    log.info({"action": "start_script", "status": "running"})
    poll_sqs()

if __name__ == '__main__':
    main()
