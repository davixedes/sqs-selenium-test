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

# ---------------
# Variáveis de ambiente
# ---------------
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver')
CHROME_BINARY_PATH = os.getenv('CHROME_BINARY_PATH', '/usr/bin/google-chrome')
WEBSITE_URL = os.getenv(
    'WEBSITE_URL',
    'https://satsp.fazenda.sp.gov.br/COMSAT/Public/ConsultaPublica/ConsultaPublicaCfe.aspx'
)
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # Ex: https://sqs.sa-east-1.amazonaws.com/123456789012/minha-fila
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')

# Quantidade máxima de mensagens para buscar por vez no SQS.
MAX_NUMBER_OF_MESSAGES = int(os.getenv('MAX_NUMBER_OF_MESSAGES', 5))

# Tempo para aguardar entre uma requisição de mensagens e outra (se a fila estiver vazia).
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 5))

# ---------------
# Cliente SQS
# ---------------
# Se as credenciais (incluindo session token) estiverem em variáveis de ambiente,
# o boto3 as pegará automaticamente (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

def setup_driver():
    """
    Configura o ChromeDriver com as opções adequadas.
    Não inclui --headless, pois queremos ver a interface.
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

    driver = setup_driver()
    request_id = str(uuid.uuid4())
    start_time = time.time()

    try:
        driver.get(WEBSITE_URL)
        time.sleep(20)  # Simulando tempo de navegação

        memory_usage, cpu_usage = get_resource_usage()
        end_time = time.time()
        duration = round(end_time - start_time, 2)

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
        print(f"[Thread {thread_id}] Erro ao processar a mensagem ({body}): {e}")

    finally:
        driver.quit()

def poll_sqs():
    """
    Loop infinito que consulta a fila SQS e dispara threads para cada mensagem recebida.
    Não filtra por mensagem específica, processa qualquer uma que chegar.
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
    print("Iniciando script de polling do SQS (interface gráfica do Chrome, credenciais temporárias se definidas)...")
    poll_sqs()

if __name__ == '__main__':
    main()
