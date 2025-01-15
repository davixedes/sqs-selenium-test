import os
import time
import uuid
import psutil
import boto3
import logging
from datetime import datetime
from threading import Thread

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from ddtrace import tracer, patch
from ddtrace.profiling import Profiler

# ---------------
# Configuração do Datadog (Logs e APM)
# ---------------
patch(logging=True, boto3=True, threading=True)

# Configurar tracer
tracer.configure(
    hostname=os.getenv('DD_AGENT_HOST', 'localhost'),
    port=int(os.getenv('DD_TRACE_AGENT_PORT', 8126))
)

# Iniciar profiler
profiler = Profiler()
profiler.start()

# Configurar logger com formato personalizado
FORMAT = ('%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] '
          '[dd.service=%(dd.service)s dd.env=%(dd.env)s dd.version=%(dd.version)s dd.trace_id=%(dd.trace_id)s dd.span_id=%(dd.span_id)s] '
          '- %(message)s')
logging.basicConfig(format=FORMAT)
log = logging.getLogger("ecs-task-gui")
log.setLevel(logging.INFO)

# Adicionar variáveis de ambiente globais ao contexto do logger
logging.LoggerAdapter(log, extra={
    'dd.service': os.getenv('DD_SERVICE', 'ecs-task-gui'),
    'dd.env': os.getenv('DD_ENV', 'production'),
    'dd.version': os.getenv('DD_VERSION', '1.0.0'),
})


# ---------------
# Variáveis de Ambiente
# ---------------
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver')
CHROME_BINARY_PATH = os.getenv('CHROME_BINARY_PATH', '/usr/bin/google-chrome')
WEBSITE_URL = os.getenv(
    'WEBSITE_URL',
    'https://satsp.fazenda.sp.gov.br/COMSAT/Public/ConsultaPublica/ConsultaPublicaCfe.aspx'
)
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
DLQ_URL = os.getenv('DLQ_URL')  # Dead Letter Queue para mensagens com falha
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')

MAX_NUMBER_OF_MESSAGES = int(os.getenv('MAX_NUMBER_OF_MESSAGES', 5))
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 5))

sqs_client = boto3.client('sqs', region_name=AWS_REGION)


# ---------------
# Funções Auxiliares
# ---------------
@tracer.wrap("setup_driver")
def setup_driver():
    """
    Configura o ChromeDriver com as opções adequadas.
    """
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


@tracer.wrap("get_resource_usage")
def get_resource_usage():
    """
    Retorna o uso de memória (MB) e CPU (%) do processo atual.
    """
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / 1024 / 1024  # em MB
    cpu_usage = process.cpu_percent(interval=0.1)           # em %
    log.info(f"Uso de recursos coletado: Mem={memory_usage}MB, CPU={cpu_usage}%")
    return memory_usage, cpu_usage


@tracer.wrap("process_message")
def process_message(message, thread_id):
    """
    Processa uma mensagem específica da fila SQS:
    """
    body = message.get('Body', '')
    receipt_handle = message['ReceiptHandle']

    if not body or len(body.strip()) == 0:
        log.warning(f"[Thread {thread_id}] Mensagem vazia ou inválida. Ignorando.")
        return

    driver = setup_driver()
    request_id = str(uuid.uuid4())

    try:
        with tracer.trace("selenium.load_page", resource=WEBSITE_URL):
            driver.get(WEBSITE_URL)
            log.info(f"[Thread {thread_id}] Navegando no site {WEBSITE_URL}")

        with tracer.trace("selenium.simulate_navigation"):
            time.sleep(20)

        memory_usage, cpu_usage = get_resource_usage()
        log.info(
            f"[Thread {thread_id}] Mensagem processada com sucesso.",
            extra={"cpu_usage": cpu_usage, "memory_usage": memory_usage, "request_id": request_id}
        )

        sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)

    except Exception as e:
        log.error(f"[Thread {thread_id}] Falha ao processar mensagem: {e}", exc_info=True)

        if DLQ_URL:
            sqs_client.send_message(QueueUrl=DLQ_URL, MessageBody=body)

    finally:
        driver.quit()


@tracer.wrap("poll_sqs")
def poll_sqs():
    """
    Loop infinito que consulta a fila SQS e dispara threads para cada mensagem recebida.
    """
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
                WaitTimeSeconds=2,
                VisibilityTimeout=30
            )
            messages = response.get("Messages", [])

            if not messages:
                log.info("Nenhuma mensagem encontrada. Aguardando...")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            for i, msg in enumerate(messages):
                process_message(msg,i)

        except Exception as e:
            log.error("Erro no loop principal de polling SQS.", exc_info=True)


def main():
    log.info("Iniciando script de polling do SQS com Datadog APM...")
    poll_sqs()


if __name__ == '__main__':
    main()

