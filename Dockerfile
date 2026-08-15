FROM python:3.11-slim

RUN apt-get update && apt-get install -y tini

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/data && touch /app/data/state.json

COPY . .

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "migres", "--config", "/app/config.yml"]