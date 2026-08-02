# Analytics Async Pipeline

Асинхронный сервис для сбора и обработки событий из GitHub и Hacker News.

События загружаются фоновыми задачами, помещаются в Redis, обрабатываются несколькими воркерами и сохраняются в PostgreSQL. Для мониторинга используются Prometheus и Grafana.

## Возможности

- сбор событий из GitHub API и Hacker News API;
- асинхронная очередь Redis;
- несколько параллельных воркеров;
- обработка событий батчами;
- повторные попытки при ошибках PostgreSQL;
- Dead Letter Queue для проблемных событий;
- корректное завершение воркеров без потери буфера;
- API для просмотра состояния системы;
- метрики Prometheus;
- запуск через Docker Compose.

## Архитектура

Основные части проекта:

- **FastAPI** — API и запуск фоновых сервисов;
- **Fetchers** — получение событий из внешних источников;
- **Redis** — основная очередь и DLQ;
- **Workers** — обработка событий и запись батчей;
- **PostgreSQL** — хранение событий;
- **Prometheus** — сбор метрик;
- **Grafana** — отображение метрик.

![Architecture Diagram](docs/images/architecture-diagram.png)

## Поток обработки данных

1. GitHub и Hacker News возвращают новые события.
2. Fetchers приводят данные к нужному формату.
3. События добавляются в очередь `events_queue`.
4. Воркеры получают события через `BLPOP`.
5. События проверяются и собираются в батчи.
6. Батч записывается в PostgreSQL.
7. При временной ошибке выполняются повторные попытки.
8. После исчерпания попыток события отправляются в `events:dlq`.

Для очереди используется схема `RPUSH + BLPOP`, поэтому события обрабатываются в порядке FIFO.

![Data Flow Diagram](docs/images/data-flow-diagram.png)

## Развёртывание

Проект запускается через Docker Compose и состоит из пяти сервисов:

| Сервис | Назначение | Порт |
|---|---|---:|
| `app` | FastAPI, fetchers и workers | `8001` |
| `postgres_db` | PostgreSQL | `5433` |
| `redis` | Redis | `6380` |
| `prometheus` | Метрики | `9090` |
| `grafana` | Дашборды | `3000` |

Для приложения, PostgreSQL и Redis настроены healthcheck.

![Deployment Diagram](docs/images/deployment-diagram.png)

## Жизненный цикл события

Обычный сценарий:

1. Fetcher получает событие.
2. Событие добавляется в Redis.
3. Один из воркеров забирает его из очереди.
4. Воркер проверяет данные и добавляет событие в буфер.
5. После заполнения батча данные записываются в PostgreSQL.
6. Обновляются статистика и метрики.

Если данные некорректны, событие отправляется в DLQ. Если PostgreSQL недоступен, воркер выполняет несколько попыток с увеличивающейся задержкой.

![Sequence Diagram](docs/images/sequence-diagram.png)

## Используемые технологии

- Python
- FastAPI
- asyncio
- Redis
- PostgreSQL
- SQLAlchemy
- Prometheus
- Grafana
- Docker Compose
- pytest

## Запуск через Docker

Создай локальный файл `.env` на основе примера:

```powershell
Copy-Item .env.example .env
```

Укажи в `.env` пароль PostgreSQL и остальные необходимые значения.

Запусти проект:

```powershell
docker compose up -d --build
```

Проверь состояние контейнеров:

```powershell
docker compose ps
```

Остановка:

```powershell
docker compose down
```

Данные PostgreSQL, Redis, Prometheus и Grafana сохраняются в Docker volumes.

## Полезные адреса

После запуска доступны:

- API: `http://localhost:8001`
- Swagger: `http://localhost:8001/docs`
- Healthcheck: `http://localhost:8001/health`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Основные API-маршруты:

| Маршрут | Описание |
|---|---|
| `/health` | состояние приложения |
| `/analytics/summary` | краткая статистика по событиям |
| `/monitoring/stats` | статистика обработки |
| `/monitoring/workers` | статистика воркеров |
| `/monitoring/queue` | состояние Redis-очередей |
| `/monitoring/health` | проверка подключения к Redis |
| `/fetchers/github/status` | состояние GitHub fetcher |
| `/fetchers/github/hn/status` | состояние Hacker News fetcher |

## Проверка очередей

Размер основной очереди:

```powershell
docker compose exec redis redis-cli LLEN events_queue
```

Размер DLQ:

```powershell
docker compose exec redis redis-cli LLEN events:dlq
```

## Тесты

Запуск всех тестов:

```powershell
pytest -v
```

Текущий набор проверяет API, мониторинг, обработку батчей, повторные попытки, DLQ и корректную остановку воркера.

## Структура проекта

```text
app/
├── api/              # API-маршруты
├── consumers/        # воркеры
├── core/             # настройки приложения
├── db/               # подключения к Redis и PostgreSQL
├── models/           # модели базы данных
├── repositories/     # работа с событиями в PostgreSQL
├── services/         # GitHub и Hacker News fetchers
├── main.py           # запуск FastAPI и фоновых задач
└── metrics.py        # метрики Prometheus

tests/                # автоматические тесты
prometheus/           # конфигурация Prometheus
docs/images/          # схемы проекта
docker-compose.yml    # сервисы Docker Compose
Dockerfile            # образ приложения
```