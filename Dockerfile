# Usar imagem base oficial do Python
FROM python:3.9

# Atualizar pacotes e instalar dependências necessárias
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    libnss3 \
    libatk1.0-0 \
    libcups2 \
    libxss1 \
    libgtk-3-0 \
    libgbm1 \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    --no-install-recommends

# Adicionar repositório oficial do Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub \
    | gpg --dearmor > /usr/share/keyrings/google-chrome-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] \
    http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable

# Instalar ChromeDriver compatível com a versão do Chrome
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}') && \
    DRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_$CHROME_VERSION") && \
    wget -O /tmp/chromedriver.zip \
      "https://storage.googleapis.com/chrome-for-testing-public/$CHROME_VERSION/linux64/chromedriver-linux64.zip" && \
    unzip /tmp/chromedriver.zip -d /tmp/ && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver

# Limpeza para reduzir o tamanho da imagem
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Definir variáveis de ambiente (padrões, mas podem ser sobrescritos via -e)
ENV CHROMEDRIVER_PATH="/usr/local/bin/chromedriver"
ENV CHROME_BINARY_PATH="/usr/bin/google-chrome"

# Copiar dependências Python
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o script Python
COPY script.py /app/script.py

# Comando padrão
CMD ["python", "script.py"]
