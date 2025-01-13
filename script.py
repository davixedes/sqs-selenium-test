import os
import time
import uuid
import psutil
import boto3
from datetime import datetime
from threading import Thread

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Datadog
from ddtrace import tracer, patch
from ddtrace.profiling import Profiler

# ---------------
# Inicialização do Datadog
# ---------------
# Ativar auto-patching de bibliotecas compatíveis
patch(boto3=True, threading=True)

# Configurar o tracer
tracer.configure(
    hostname=os.getenv('DD_AGENT_HOST', 'localhost'),  # Agente no localhost
    port=int(os.getenv('DD_TRACE_AGENT_PORT', 8126)),  # Porta padrão do APM
    debug=True  # Habilitar modo debug para logs detalhados
)

# Iniciar o Continuous Profiler
profiler = Profiler()
profiler.start()

# ---------------
# Variáveis de ambiente
# ---------------
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver')
CHROME_BINARY_PATH = os.getenv('CHROME_BINARY_PATH', '/usr/bin/google-chrome')
WEBSITE_URL = os.getenv(
    'WEBSITE_URL',
    'https://satsp.fazenda.sp.gov.br/COMSAT/Public/ConsultaPublica/ConsultaPublicaCfe.aspx'
)
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')

MAX_NUMBER_OF_MESSAGES = int(os.getenv('MAX_NUMBER_OF_MESSAGES', 5))
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 5))

# Cliente SQS
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

# ---------------
# Funções auxiliares
# ---------------
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

    # Se o DISPLAY estiver setado, adiciona o display
    if os.getenv('DISPLAY'):
        chrome_options.add_argument(f"--display={os.getenv('DISPLAY')}")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def get_resource_usage():
    """
    Retorna o uso de memória (MB) e CPU (%) do processo atual.
    """
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / 1024 / 1024  # em MB
    cpu_usage = process.cpu_percent(interval=0.1)           # em %
    return memory_usage, cpu_usage


@tracer.wrap(service="ecs-task-gui", resource="process_message")
def process_message(message, thread_id):
    """
    Processa uma mensagem específica da fila SQS:
    - Abre o site no Chrome
    - Coleta métricas (CPU, Memória)
    - Loga o resultado
    - Remove a mensagem da fila em caso de sucesso
    """
    body = message.get('Body', '')
    receipt_handle = message['ReceiptHandle']  # p/ deletar a mensagem após sucesso

    # Gera um span para a operação de processamento da mensagem
    with tracer.trace("sqs.message_processing", resource="selenium_action") as span:
        span.set_tag("message.body", body)
        span.set_tag("thread.id", thread_id)

        driver = setup_driver()
        request_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            driver.get(WEBSITE_URL)
            time.sleep(20)  # Simulando tempo de navegação

            memory_usage, cpu_usage = get_resource_usage()
            end_time = time.time()
            duration = round(end_time - start_time, 2)

            # Registrar métricas e informações no span
            span.set_metric("cpu.usage", cpu_usage)
            span.set_metric("memory.usage", memory_usage)
            span.set_tag("request.id", request_id)
            span.set_metric("processing.duration", duration)

            log_msg = (
                f"[Thread {thread_id}] "
                f"RequestID={request_id} "
                f"Body={body} "
                f"Status=Sucesso "
                f"CPU={round(cpu_usage, 2)}% "
                f"Mem={round(memory_usage, 2)}MB "
                f"Inicio={datetime.fromtimestamp(start_time).isoformat()} "
                f"Fim={datetime.fromtimestamp(end_time).isoformat()} "
                f"Duracao={duration}s"
            )
            print(log_msg)

            # Remove a mensagem da fila
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )

        except Exception as e:
            span.set_tag("error", True)
            span.set_tag("error.message", str(e))
            print(f"[Thread {thread_id}] Erro ao processar a mensagem ({body}): {e}")

        finally:
            driver.quit()


@tracer.wrap(service="ecs-task-gui", resource="poll_sqs")
def poll_sqs():
    """
    Loop infinito que consulta a fila SQS e dispara threads para cada mensagem recebida.
    """
    if not SQS_QUEUE_URL:
        print("ERRO: SQS_QUEUE_URL não foi definida. Encerrando.")
        return

    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
                WaitTimeSeconds=2,
                VisibilityTimeout=30
            )
            messages = response.get('Messages', [])

            if not messages:
                # Se não há mensagens, aguarda alguns segundos
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            threads = []
            for i, msg in enumerate(messages):
                t = Thread(target=process_message, args=(msg, i))
                threads.append(t)
                t.start()

            # Aguarda todas as threads finalizarem
            for t in threads:
                t.join()

        except Exception as e:
            print(f"Erro no loop principal de polling SQS: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)


def main():
    print("Iniciando script de polling do SQS (Datadog APM ativado)...")
    poll_sqs()


if __name__ == '__main__':
    main()
