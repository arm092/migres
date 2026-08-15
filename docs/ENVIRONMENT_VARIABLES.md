# Environment Variables Support

The migres tool supports environment variable overrides for all configuration options. This allows you to configure the tool without modifying the `config.yml` file, which is useful for containerized deployments.

## 🔧 Supported Environment Variables

### MySQL Configuration
```bash
MYSQL_HOST=mysql-server.example.com
MYSQL_PORT=3306
MYSQL_USER=repluser
MYSQL_PASSWORD=your-password
MYSQL_DATABASE=source_db
# Optional TLS
MYSQL_SSL_CA=/path/to/ca.pem
MYSQL_SSL_DISABLED=false
```

### ClickHouse Configuration
```bash
CLICKHOUSE_HOST=clickhouse-server.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=target_db
# Optional TLS
CLICKHOUSE_SECURE=true
CLICKHOUSE_VERIFY=true
CLICKHOUSE_CA_CERTS=/path/to/ca.pem
```

### Migration Configuration
```bash
MIGRATION_MODE=cdc
MIGRATION_BATCH_ROWS=5000
MIGRATION_WORKERS=4
MIGRATION_CLICKHOUSE_TIMEZONE=UTC
MIGRATION_DEBUG=true
```
**Note:** `MIGRATION_DEBUG` enables verbose logging for CDC events.

### CDC Configuration
```bash
CDC_HEARTBEAT_SECONDS=5
CDC_CHECKPOINT_INTERVAL_ROWS=1000
CDC_BATCH_MAX_WAIT_SECONDS=60
CDC_PRODUCER_BATCH_SIZE=100
CDC_PRODUCER_FLUSH_INTERVAL=5
CDC_TRANSFORMER_POLL_INTERVAL=0.5
CDC_CONSUMER_POLL_INTERVAL=0.5
CDC_PREPARED_QUERIES_BATCH_LIMIT=100
CDC_FORCE_BINLOG_POSITION="mysql-bin.000123:6855245"
CDC_DB_DEBUG=false
CDC_USE_GTID=false
CDC_RAW_EVENTS_MAX=50000
```
**Notes:**
- `CDC_BATCH_DELAY_SECONDS` was **removed in 3.0.0**. Use `CDC_PRODUCER_FLUSH_INTERVAL`, `CDC_TRANSFORMER_POLL_INTERVAL`, and `CDC_CONSUMER_POLL_INTERVAL`. The old variable is ignored with a warning.
- `CDC_FORCE_BINLOG_POSITION` - Optional binlog position in "file:position" format (e.g., "mysql-bin.000123:6855245"). Loaded at process start; used only when SIGUSR2 is received (deletes `buffer.db`, writes the position into `state.json`, restarts pipeline threads). Changing the env while the process is running has no effect until you restart the process, then send SIGUSR2. If not set, SIGUSR2 is ignored. Default is not set (null).
- `CDC_DB_DEBUG` - If set to `true`, processed events and queries are archived to ClickHouse debug tables instead of being deleted. Useful for debugging and auditing. Default is `false`.

### Notifications Configuration
```bash
NOTIFICATIONS_ENABLED=true
NOTIFICATIONS_RATE_LIMIT_SECONDS=60
# Teams (NOTIFICATIONS_WEBHOOK_URL is a legacy alias for the Teams webhook)
NOTIFICATIONS_TEAMS_WEBHOOK_URL=https://your-org.webhook.office.com/webhookb2/your-webhook-url
NOTIFICATIONS_WEBHOOK_URL=https://your-org.webhook.office.com/webhookb2/your-webhook-url
# Slack incoming webhook
NOTIFICATIONS_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00/B00/XXX
# Telegram Bot API
NOTIFICATIONS_TELEGRAM_BOT_TOKEN=123456:ABC
NOTIFICATIONS_TELEGRAM_CHAT_ID=-1001234567890
```
Any subset of providers can be enabled at once; one failed channel does not block the others.

### File Paths
```bash
STATE_FILE=/app/data/state.json
BUFFER_FILE=/app/data/buffer.db
```

### Environment Name
```bash
ENVIRONMENT=prod
# or
ENVIRONMENT=dev
# or
ENVIRONMENT=stage
```
**Note:** The environment name is used in notification titles (e.g., "🚀 CDC Process Started [PROD]"). Defaults to `prod` if not set.

## 📋 Usage Examples

### Docker Run
```bash
docker run -e MYSQL_HOST=mysql-server \
           -e MYSQL_PASSWORD=secret \
           -e CLICKHOUSE_HOST=ch-server \
           -e CLICKHOUSE_PASSWORD=secret \
           -e BUFFER_FILE=/app/data/buffer.db \
           -e MIGRATION_DEBUG=true \
           migres:latest
```

### Docker Compose
```yaml
version: '3.8'
services:
  migres:
    image: migres:latest
    environment:
      - MYSQL_HOST=mysql-server
      - MYSQL_PASSWORD=secret
      - CLICKHOUSE_HOST=clickhouse-server
      - CLICKHOUSE_PASSWORD=secret
      - STATE_FILE=/app/data/state.json
      - BUFFER_FILE=/app/data/buffer.db
      - NOTIFICATIONS_ENABLED=true
      - NOTIFICATIONS_WEBHOOK_URL=https://your-webhook-url
      - ENVIRONMENT=prod
      - MIGRATION_DEBUG=true
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: migres
spec:
  template:
    spec:
      containers:
      - name: migres
        image: migres:latest
        env:
        - name: MYSQL_HOST
          value: "mysql-service"
        - name: MYSQL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: mysql-secret
              key: password
        - name: CLICKHOUSE_HOST
          value: "clickhouse-service"
        - name: CLICKHOUSE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: clickhouse-secret
              key: password
        - name: BUFFER_FILE
          value: "/app/data/buffer.db"
        - name: MIGRATION_DEBUG
          value: "true"
```

## 🔍 How It Works

1. **Load config.yml**: The tool first loads the base configuration from `config.yml`
2. **Apply overrides**: Environment variables override the corresponding config values
3. **Type conversion**: Automatic conversion for ports (int), booleans (true/false), and numbers
4. **Error handling**: Invalid values are logged as warnings and ignored
5. **Logging**: All overrides are logged for debugging

## ⚠️ Important Notes

- **Priority**: Environment variables always override config.yml values
- **Type Safety**: Invalid values (e.g., non-numeric ports) are ignored with warnings
- **Boolean Values**: For `NOTIFICATIONS_ENABLED`, `MIGRATION_DEBUG`, `CLICKHOUSE_SECURE`, `CLICKHOUSE_VERIFY`, and `MYSQL_SSL_DISABLED`, use `true`, `1`, `yes`, or `on` for true
- **Secrets**: Use environment variables for sensitive data like passwords
- **Buffer path**: `BUFFER_FILE` must be writable; there is no fallback to `/tmp`
- **Logging**: Check logs to see which values were overridden

## 🧪 Testing

You can test environment variable overrides by setting them before running the tool:

```bash
# Test MySQL override
export MYSQL_HOST=test-mysql.example.com
export MYSQL_PORT=3307
python migres.py

# Test ClickHouse override  
export CLICKHOUSE_HOST=test-ch.example.com
export CLICKHOUSE_PORT=9001
python migres.py

# Test buffer path
export BUFFER_FILE=/tmp/migres-test-buffer.db
python migres.py

# Test notifications
export NOTIFICATIONS_ENABLED=true
export NOTIFICATIONS_WEBHOOK_URL=https://test-webhook.com
python migres.py

# Test environment variable
export ENVIRONMENT=prod
python migres.py

# Test debug mode
export MIGRATION_DEBUG=true
python migres.py
```

For automated tests:
```bash
pytest test/unit
docker compose -f docker-compose.test.yml up -d && pytest -m e2e
```

The tool will log all overrides, making it easy to verify that environment variables are being applied correctly.
