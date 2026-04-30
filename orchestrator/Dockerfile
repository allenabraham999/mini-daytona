FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl gnupg && \
    curl -fsSL https://pkgs.zabbly.com/key.asc | gpg --dearmor -o /etc/apt/keyrings/zabbly.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/zabbly.gpg] https://pkgs.zabbly.com/incus/stable jammy main" \
    > /etc/apt/sources.list.d/zabbly-incus-stable.list && \
    apt-get update && apt-get install -y incus-client

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 9000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
