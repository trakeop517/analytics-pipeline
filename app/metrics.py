from prometheus_client import Counter, Gauge, Histogram

EVENTS_PROCESSED_TOTAL = Counter("events_processed_total", "Количество успешно обработанных событий", ["source", "status"])
REDIS_QUEUE_SIZE = Gauge("redis_queue_size", "Размер основной очереди событий в Redis")
REDIS_DLQ_SIZE = Gauge("redis_dlq_size", "Размер очереди мертвых писем (Dead Letter Queue)")
WORKER_RETRIES_TOTAL = Counter("worker_retries_total", "Количество повторных попыток обработки событий", ["source"])
ACTIVE_WORKERS_COUNT = Gauge("active_workers_count", "Количество задействованных воркеров в данный момент")
WORKER_PROCESSING_LATENCY = Histogram("worker_processing_latency_seconds", "Задержка обработки одного события воркером (в секундах)", ["source"], buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0))