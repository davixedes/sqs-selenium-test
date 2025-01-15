import os
import time
import uuid
import boto3
import logging
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from ddtrace import tracer, patch, config

# -----------------
# Configuração do Datadog e Logging
# -----------------
patch(logging=True)

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

config.env = os.getenv("DD_ENV", "production")
config.service = os.getenv("DD_SERVICE", "ecs-task-gui")
config.version = os.getenv("DD_VERSION", "1.0.0")

# -----------------
# Variáveis de Ambiente
# -----------------
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver')
CHROME_BINARY_PATH = os.getenv('CHROME_BINARY_PATH', '/usr/bin/google-chrome')
WEBSITE_URL = os.getenv('WEBSITE_URL', 'https://example.com')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
DLQ_URL = os.getenv('DLQ_URL')
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 5))

sqs_client = boto3.client('sqs', region_name=AWS_REGION)

def setup_driver():
    chrome_options = Options()
    chrome_options.binary_location = CHROME_BINARY_PATH
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    service = Service(CHROMEDRIVER_PATH)
    return webdriver.Chrome(service=service, options=chrome_options)

def send_to_dlq(message_body):
    if not DLQ_URL:
        log.error("DLQ_URL não está definida.")
        return
    try:
        sqs_client.send_message(QueueUrl=DLQ_URL, MessageBody=message_body)
        log.info({"action": "send_to_dlq", "status": "success", "message_body": message_body})
    except Exception as e:
        log.error({"action": "send_to_dlq", "status": "error", "error": str(e)})

def process_message(message):
    body = message.get('Body', '{}')
    receipt_handle = message['ReceiptHandle']

    try:
        payload = json.loads(body)
        site_url = payload.get('WEBSITE_URL', WEBSITE_URL)
    except json.JSONDecodeError as e:
        log.error({"action": "parse_payload", "status": "error", "error": str(e)})
        send_to_dlq(body)
        return

    with tracer.trace("process_message", service="selenium-task", resource="navigate_to_site") as span:
        span.set_tag("message_body", body)
        span.set_tag("site_url", site_url)

        driver = setup_driver()
        try:
            driver.get(site_url)
            time.sleep(5)
            log.info({"action": "navigate_to_site", "status": "success", "url": site_url})
        except Exception as e:
            span.set_tag("error", str(e))
            send_to_dlq(body)
        finally:
            driver.quit()

    try:
        sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        log.info({"action": "delete_message", "status": "success"})
    except Exception as e:
        send_to_dlq(body)

def poll_sqs():
    while True:
        try:
            response = sqs_client.receive_message(QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=10)
            messages = response.get('Messages', [])
            if messages:
                for message in messages:
                    process_message(message)
            else:
                time.sleep(POLL_INTERVAL_SECONDS)
        except Exception as e:
            log.error({"action": "poll_sqs", "status": "error", "error": str(e)})

if __name__ == '__main__':
    poll_sqs()
