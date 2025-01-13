import os
import time
import uuid
import boto3
import logging
import json
from datetime import datetime
from threading import Thread
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from ddtrace import tracer, patch
from ddtrace.profiling import Profiler

# Ativar o Continuous Profiler
profiler = Profiler()
profiler.start()

# Ativar auto-patch para boto3 e threading
patch(boto3=True, threading=True)

# Configuração do tracer do Datadog
tracer.configure(
    hostname=os.getenv('DD_AGENT_HOST', 'localhost'),
    port=int(os.getenv('DD_TRACE_AGENT_PORT', 8126))
)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Variáveis de ambiente
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

    if os.getenv('DISPLAY'):
        chrome_options.add_argument(f"--display={os.getenv('DISPLAY')}")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def log_request(status, body, request_id, duration, error=None):
    log_data = {
        "status": status,
        "body": body,
        "request_id": request_id,
        "duration": round(duration, 2),
        "error": error,
        "timestamp": datetime.utcnow().isoformat()
    }
    logger.info(json.dumps(log_data))

def process_message(message, thread_id):
    body = message.get('Body', '')
    receipt_handle = message['ReceiptHandle']
    request_id = str(uuid.uuid4())
    start_time = time.time()

    with tracer.trace("process_message", service="selenium-script", resource="process_message") as span:
        span.set_tag("thread_id", thread_id)
        span.set_tag("message_body", body)

        driver = setup_driver()

        try:
            driver.get(WEBSITE_URL)
            span.set_tag("url", WEBSITE_URL)
            time.sleep(20)

            end_time = time.time()
            duration = round(end_time - start_time, 2)
            log_request("success", body, request_id, duration)

            span.set_tag("status", "success")

            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            error_message = str(e)
            log_request("error", body, request_id, 0, error=error_message)

            span.set_tag("status", "error")
            span.set_tag("error.msg", error_message)
            span.set_tag("error.type", type(e).__name__)

            raise e
        finally:
            driver.quit()

def poll_sqs():
    if not SQS_QUEUE_URL:
        logger.error("SQS_QUEUE_URL não foi definida. Encerrando.")
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
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            threads = []
            for i, msg in enumerate(messages):
                t = Thread(target=process_message, args=(msg, i))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()
        except Exception as e:
            logger.error(f"Erro no loop principal de polling SQS: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)

def main():
    logger.info("Iniciando script de polling do SQS...")
    poll_sqs()

if __name__ == '__main__':
    main()
