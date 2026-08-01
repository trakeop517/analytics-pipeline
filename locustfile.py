from locust import HttpUser, task, between, tag

class AnalyticsPipelineUser(HttpUser):
    wait_time = between(0.1, 1.0)
    @tag("health")
    @task(5)
    def check_health(self):
        self.client.get("/monitoring/health", name="GET /monitoring/health")
    @tag("monitoring")
    @task(3)
    def check_stats(self):
        self.client.get("/monitoring/stats", name="GET /monitoring/stats")
    @tag("monitoring")
    @task(3)
    def check_queue(self):
        self.client.get("/monitoring/queue", name="GET /monitoring/queue")
    @tag("analytics")
    @task(2)
    def get_analytics_summary(self):
        with self.client.get("/analytics/summary", name="GET /analytics/summary", catch_response=True) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if "total_events_in_db" not in data or "by_source" not in data:
                        response.failure("Ответ не содержит ключевых полей аналитики")
                except Exception as e:
                    response.failure(f"Ошибка парсинга JSON: {e}")
            else:
                response.failure(f"HTTP ошибка: {response.status_code}")
    @tag("metrics")
    @task(1)
    def get_prometheus_metrics(self):
        self.client.get("/monitoring/metrics", name="GET /monitoring/metrics")