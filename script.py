import os
import time
import uuid
import boto3
import logging
import json
from datetime import datetime
from ddtrace import tracer, patch
from ddtrace.profiling import Profiler

# Aplicar patch apenas para logs
patch(logging=True)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Configurar tracer
tracer.configure(
    hostname=os.getenv('DD_AGENT_HOST', 'localhost'),
    port=int(os.getenv('DD_TRACE_AGENT_PORT', 8126))
)

# Iniciar profiler
profiler = Profiler()
profiler.start()

# Configurar logger estruturado em JSON para correlação com Datadog APM
class JSONFormatter(logging.Formatter):
    """Formato de log estruturado em JSON para correlação com Datadog APM."""
    def format(self, record):
        span = tracer.current_span()  # Obter o span atual, se existir
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "dd.trace_id": span.trace_id if span else None,  # Evitar erro caso não haja span
            "dd.span_id": span.span_id if span else None,  # Evitar erro caso não haja span
        }
        return json.dumps(log_record)

log = logging.getLogger("ecs-task-gui")
log.setLevel(logging.INFO)

# Configurar handler para stdout
stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(JSONFormatter())  # Aplicar formatação JSON
log.addHandler(stdout_handler)

# Variáveis de Ambiente
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver')
CHROME_BINARY_PATH = os.getenv('CHROME_BINARY_PATH', '/usr/bin/google-chrome')
WEBSITE_URL = os.getenv(
    'WEBSITE_URL',
    'https://satsp.fazenda.sp.gov.br/COMSAT/Public/ConsultaPublica/ConsultaPublicaCfe.aspx'
)
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
DLQ_URL = os.getenv('DLQ_URL')  
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')

# Configurações do SQS
MAX_NUMBER_OF_MESSAGES = 1  
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 5))
VISIBILITY_TIMEOUT = int(os.getenv('VISIBILITY_TIMEOUT', 30))

sqs_client = boto3.client('sqs', region_name=AWS_REGION)

@tracer.wrap("setup_driver")
def setup_driver():
    chrome_options = Options()
    chrome_options.binary_location = CHROME_BINARY_PATH
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=500,500")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--user-data-dir=/tmp")
    chrome_options.add_argument("--disk-cache-dir=/tmp")
    chrome_options.add_argument("--enable-logging")
    chrome_options.add_argument("--log-level=0")
    chrome_options.add_argument("--disable-background-networking")
    chrome_options.add_argument("--disable-sync")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--remote-debugging-port=9222")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    log.info("Driver configurado com sucesso.")
    return driver

@tracer.wrap("process_message")
def process_message(message):
    body = message.get('Body', '')
    receipt_handle = message['ReceiptHandle']

    if not body or len(body.strip()) == 0:
        log.warning("Mensagem vazia ou inválida. Ignorando.")
        return

    driver = setup_driver()
    request_id = str(uuid.uuid4())

    try:
        with tracer.trace("selenium.load_page", resource=WEBSITE_URL) as span:
            start_time = time.time()
            driver.get(WEBSITE_URL)
            load_time = time.time() - start_time
            log.info(f"Navegando no site {WEBSITE_URL}", extra={"load_time_seconds": load_time})

        with tracer.trace("selenium.simulate_navigation") as span:
            start_time = time.time()
            time.sleep(20)  
            nav_time = time.time() - start_time
            log.info("Simulação de navegação concluída.", extra={"navigation_time_seconds": nav_time})

        log.info("Mensagem processada com sucesso.", extra={"request_id": request_id})

        # Remover mensagem da fila após processamento bem-sucedido
        sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)

    except Exception as e:
        log.error(f"Falha ao processar mensagem: {e}")

        if DLQ_URL:
            sqs_client.send_message(QueueUrl=DLQ_URL, MessageBody=body)

    finally:
        driver.quit()

def poll_sqs():
    """
    Loop que consulta a fila SQS e processa mensagens uma por vez.
    Agora não há mais um trace para polling, apenas quando há mensagens.
    """
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
                WaitTimeSeconds=10,
                VisibilityTimeout=VISIBILITY_TIMEOUT
            )
            messages = response.get("Messages", [])

            # Se não houver mensagens, evitar spans desnecessários
            if not messages:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Criar um novo trace apenas para mensagens recebidas
            for message in messages:
                with tracer.trace("sqs.receive_message", service="ecs-task-gui", span_type="queue"):
                    process_message(message)

        except Exception as e:
            log.error("Erro no loop principal de polling SQS.", extra={"exception": str(e)})

def main():
    log.info("Iniciando script de polling do SQS com Datadog APM...")
    poll_sqs()

if __name__ == '__main__':
    main()
