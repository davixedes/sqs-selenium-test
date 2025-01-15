import os
import time
import uuid
import json
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from ddtrace import tracer, patch, config

# -----------------
# Configuração do Datadog e Logging
# -----------------
# Configuração do tracer
config.env = os.getenv("DD_ENV", "production")
config.service = os.getenv("DD_SERVICE", "ecs-task-gui")
config.version = os.getenv("DD_VERSION", "1.0.0")

# Configuração do logger com saída JSON
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

# -----------------
# Variáveis de Ambiente
# -----------------
CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver')
CHROME_BINARY_PATH = os.getenv('CHROME_BINARY_PATH', '/usr/bin/google-chrome')
WEBSITE_URL = os.getenv('WEBSITE_URL', 'https://example.com')  # URL padrão
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # URL da fila SQS
DLQ_URL = os.getenv('DLQ_URL')  # URL da Dead Letter Queue (DLQ)
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')
MAX_NUMBER_OF_MESSAGES = int(os.getenv('MAX_NUMBER_OF_MESSAGES', 1))
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 5))
VISIBILITY_TIMEOUT = int(os.getenv('VISIBILITY_TIMEOUT', 30))

# -----------------
# Funções Auxiliares
# -----------------

@tracer.wrap("setup_driver")
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

    with tracer.trace("selenium.driver_setup", resource="setup_driver") as span:
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
            span.set_tag("status", "success")
        except Exception as e:
            span.set_tag("error", str(e))
            raise
    return driver

@tracer.wrap("process_message")
def process_message(message):
    """Processa uma mensagem SQS e navega no site usando Selenium."""
    body = message.get('Body', '{}')
    receipt_handle = message['ReceiptHandle']
    log.info(json.dumps({"action": "receive_message", "message_body": body}))

    # Parse do payload
    try:
        payload = json.loads(body)  # Converte o Body para dicionário
        site_url = payload.get('WEBSITE_URL', WEBSITE_URL)
    except Exception as e:
        log.error(json.dumps({"action": "parse_payload", "status": "error", "error": str(e)}))
        return

    with tracer.trace("selenium.process_message", resource="process_message") as span:
        span.set_tag("payload", payload)

        # Configura o driver e navega para o site
        driver = setup_driver()
        try:
            with tracer.trace("selenium.navigate", resource="navigate_to_site"):
                log.info(json.dumps({"action": "navigate_to_site", "url": site_url}))
                driver.get(site_url)  # Navegar para o site
                time.sleep(10)  # Simula navegação no site
                log.info(json.dumps({"action": "navigate_to_site", "status": "success", "url": site_url}))
        except Exception as e:
            log.error(json.dumps({"action": "navigate_to_site", "status": "error", "error": str(e)}))
            span.set_tag("error", str(e))
        finally:
            driver.quit()

        # Marcar mensagem como processada (delete da fila)
        log.info(json.dumps({"action": "delete_message", "status": "success"}))


@tracer.wrap("poll_sqs")
def poll_sqs():
    """Loop para consultar mensagens na fila SQS."""
    while True:
        try:
            response = {"Messages": [{"Body": "mock", "ReceiptHandle": 123}]}  # Mock de mensagens

            messages = response.get('Messages', [])
            if not messages:
                log.info(json.dumps({"action": "poll_sqs", "status": "no_messages"}))
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            for message in messages:
                process_message(message)

        except Exception as e:
            log.error(json.dumps({"action": "poll_sqs", "status": "error", "error": str(e)}))
            time.sleep(POLL_INTERVAL_SECONDS)

# -----------------
# Função Principal
# -----------------
def main():
    log.info(json.dumps({"action": "start_script", "status": "running"}))
    poll_sqs()

if __name__ == '__main__':
    main()
