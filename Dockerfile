FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/data && touch /app/data/state.json /app/data/binlog_checkpoint.json

COPY . .

ENTRYPOINT ["python", "migres.py", "--config", "/app/config.yml"]
