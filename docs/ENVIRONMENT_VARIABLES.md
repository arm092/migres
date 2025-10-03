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
```

### ClickHouse Configuration
```bash
CLICKHOUSE_HOST=clickhouse-server.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=target_db
```

### Migration Configuration
```bash
MIGRATION_MODE=cdc
MIGRATION_BATCH_ROWS=5000
MIGRATION_WORKERS=4
MIGRATION_MYSQL_TIMEZONE=UTC
MIGRATION_CLICKHOUSE_TIMEZONE=UTC
```

### CDC Configuration
```bash
CDC_BATCH_DELAY_SECONDS=5
CDC_HEARTBEAT_SECONDS=5
CDC_CHECKPOINT_INTERVAL_ROWS=1000
CDC_CHECKPOINT_INTERVAL_SECONDS=5
```

### Notifications Configuration
```bash
NOTIFICATIONS_ENABLED=true
NOTIFICATIONS_WEBHOOK_URL=https://your-org.webhook.office.com/webhookb2/your-webhook-url
NOTIFICATIONS_RATE_LIMIT_SECONDS=60
```

### File Paths
```bash
CHECKPOINT_FILE=/app/data/binlog_checkpoint.json
STATE_FILE=/app/data/state.json
```

## 📋 Usage Examples

### Docker Run
```bash
docker run -e MYSQL_HOST=mysql-server \
           -e MYSQL_PASSWORD=secret \
           -e CLICKHOUSE_HOST=ch-server \
           -e CLICKHOUSE_PASSWORD=secret \
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
      - NOTIFICATIONS_ENABLED=true
      - NOTIFICATIONS_WEBHOOK_URL=https://your-webhook-url
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
- **Boolean Values**: For `NOTIFICATIONS_ENABLED`, use `true`, `1`, `yes`, or `on` for true
- **Secrets**: Use environment variables for sensitive data like passwords
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

# Test notifications
export NOTIFICATIONS_ENABLED=true
export NOTIFICATIONS_WEBHOOK_URL=https://test-webhook.com
python migres.py
```

The tool will log all overrides, making it easy to verify that environment variables are being applied correctly.

