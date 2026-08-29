# Flink для AISafetyOps: локальный MVP runbook

Дата актуальности: 2026-08-29

## Глоссарий

- **Apache Flink** — distributed stream processing engine, на котором исполняется локальный AISafetyOps MVP.
- **Job** — логический Flink-пайплайн, который вы собираете и отправляете в кластер.
- **JobManager** — координатор Flink job: scheduling, recovery, checkpoints.
- **TaskManager** — worker-процесс Flink, который исполняет subtasks.
- **Checkpoint** — согласованный snapshot состояния job для восстановления после сбоя.
- **Savepoint** — управляемый snapshot для ручного stop/resume, upgrade и миграции.
- **State** — данные, сохраняемые между событиями внутри оператора.
- **Watermark** — индикатор прогресса event time во Flink.
- **Event Time** — бизнес-время события из payload.
- **Window** — временная или счётная группа событий для агрегирования.
- **Allowed Lateness** — период, в который позднее событие ещё может обновить окно.
- **KeyBy** — разбиение потока по ключу, после которого события одного ключа идут в один logical shard state.
- **Parallelism** — число параллельных экземпляров оператора или job.
- **Subtask** — один параллельный экземпляр оператора.
- **Slot** — единица вычислительных ресурсов внутри TaskManager.
- **Local stand** — локальный Docker-стенд для MVP.
- **Replay dataset** — воспроизводимый набор событий для demo, regression и ручной проверки.
- **Regression suite** — набор локальных проверок, который прогоняется после каждого инкремента.
- **Policy file** — YAML-файл с thresholds и operational rules.
- **Topic bootstrap** — создание начальных Kafka topics для локального стенда.
- **Finding** — одно сырое событие срабатывания гардрейла по конкретному `requestId` или ответу агента.
- **Emission** — один опубликованный оконный агрегат в `guardrail-aggregates`.

## 1. Назначение

Этот runbook описывает, как:

- поднять локальный MVP-стенд;
- инициализировать Kafka topics;
- сгенерировать и загрузить replay dataset;
- обновить policy YAML;
- выполнять локальные проверки и regression;
- использовать систему после старта.

Документ рассчитан на локальную разработку на ноутбуке до переноса в банковскую инфраструктуру.

## 2. Состав локального стенда

Локальный MVP использует:

- `single-node Apache Kafka` в Docker;
- `Flink JobManager` в Docker;
- `Flink TaskManager` в Docker;
- `Prometheus` в Docker;
- `Grafana` в Docker;
- `bash`-скрипты для operational действий;
- `Python`-генератор для replay dataset;
- `JSON` как стартовый транспортный формат событий;
- `YAML` для policy/config defaults.

## 3. Структура файлов

- [docker-compose.yml](/home/bob/old_bob/IdeaProjects/flink/deployment/local/docker-compose.yml)
- [default-policy.yaml](/home/bob/old_bob/IdeaProjects/flink/config/policies/default-policy.yaml)
- [start-local.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/start-local.sh)
- [stop-local.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/stop-local.sh)
- [cleanup-local.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/cleanup-local.sh)
- [init-topics.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/init-topics.sh)
- [reset-topics.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/reset-topics.sh)
- [build-job.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/build-job.sh)
- [submit-job.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/submit-job.sh)
- [check-output-topics.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/check-output-topics.sh)
- [load-policies.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/load-policies.sh)
- [run-replay.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/run-replay.sh)
- [run-live-generator.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/run-live-generator.sh)
- [stream_live_events.py](/home/bob/old_bob/IdeaProjects/flink/tools/generators/stream_live_events.py)
- [run-regression.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/run-regression.sh)
- [prometheus.yml](/home/bob/old_bob/IdeaProjects/flink/observability/prometheus/prometheus.yml)
- [generate_events.py](/home/bob/old_bob/IdeaProjects/flink/tools/generators/generate_events.py)
- [test_generate_events.py](/home/bob/old_bob/IdeaProjects/flink/tools/tests/test_generate_events.py)

## 4. Topics локального MVP

На старте используются topics:

- `agent-requests`
- `agent-responses`
- `guardrail-findings`
- `normalized-events`
- `invalid-events`
- `late-events`
- `guardrail-aggregates`
- `guardrail-quality-metrics`
- `policy-updates`
- `debug-incidents`

Этого достаточно для:

- подачи входных событий;
- проверки нормализации и маршрутизации invalid/late событий;
- публикации оконных агрегатов в выходные topics;
- проверки quality/incident flows;
- будущего расширения policy stream;
- локального debug вывода.

Подробный контракт по topics и JSON-сообщениям вынесен в отдельный документ:

- [event-contracts.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/event-contracts.md)

Коротко для runbook:

- одно Kafka-сообщение = одно доменное событие;
- один пользовательский запрос = набор связанных событий;
- связь строится по `agentId`, `sessionId`, `requestId`;
- для одного `requestId` обычно есть `AGENT_REQUEST`, `AGENT_RESPONSE` и до 4 `GUARDRAIL_FINDING`;
- основные выходные topics для текущего MVP: `normalized-events`, `invalid-events`, `late-events`, `guardrail-aggregates`.

## 5. Старт локального стенда

### Полный reset локального стенда

Если нужно вернуть систему в полностью чистое начальное состояние, использовать:

```bash
bash tools/scripts/cleanup-local.sh
```

Что делает:

- останавливает и удаляет локальные контейнеры;
- удаляет Docker volumes и сеть локального стенда;
- очищает `runtime/replay/latest`;
- очищает `runtime/policies`;
- удаляет и заново создает `flink-job/target`.

Когда использовать:

- перед полным повторным прогоном с нуля;
- после конфликтов контейнеров или старого compose state;
- если нужно гарантированно пересобрать JAR и поднять новый стек;
- после изменений в `deployment/local/docker-compose.yml`.

После `cleanup-local.sh` обычный путь такой:

1. `bash tools/scripts/build-job.sh`
2. `bash tools/scripts/start-local.sh`
3. `bash tools/scripts/init-topics.sh`
4. `bash tools/scripts/load-policies.sh`
5. `bash tools/scripts/submit-job.sh`

### Шаг 1. Поднять Docker services

```bash
./tools/scripts/start-local.sh
```

Что делает:

- поднимает Kafka;
- поднимает JobManager;
- поднимает TaskManager;
- поднимает Prometheus.
- поднимает Grafana.

### Шаг 2. Инициализировать topics

```bash
./tools/scripts/init-topics.sh
```

Что делает:

- создает все нужные Kafka topics;
- безопасно пропускает уже существующие.

Если нужен чистый повторный прогон без исторических сообщений:

```bash
bash tools/scripts/reset-topics.sh
```

Что делает:

- удаляет входные и выходные MVP topics;
- создает их заново;
- позволяет валидировать текущий инкремент без смешения со старыми локальными прогонами.

Что не делает:

- не останавливает Docker-контур;
- не очищает `runtime/replay/latest`;
- не очищает `runtime/policies`;
- не удаляет `flink-job/target`;
- не пересоздает контейнеры.

Правило выбора:

- `reset-topics.sh` использовать, когда нужно только очистить Kafka-сообщения и прогнать pipeline повторно на уже поднятом стенде.
- `cleanup-local.sh` использовать, когда нужен полный reset всего локального окружения.

### Шаг 3. Загрузить policy YAML

```bash
./tools/scripts/load-policies.sh
```

Что делает:

- копирует policy YAML в локальную runtime-директорию;
- подготавливает активную policy для MVP.

Важно:

- на текущем этапе MVP этот шаг готовит локальный policy snapshot;
- текущая job пока не применяет `runtime/policies/active-policy.yaml` как live runtime source;
- это подготовка к следующему инкременту с внешним policy-driven поведением.

### Шаг 4. Собрать и отправить Flink job

```bash
./tools/scripts/build-job.sh
./tools/scripts/submit-job.sh
```

Что делает:

- собирает fat jar c `Increment 1`;
- отправляет job в локальный Flink cluster;
- job начинает читать `agent-requests`, `agent-responses`, `guardrail-findings`.

## 6. Запуск replay dataset

### Сценарии

Поддерживаются три профиля:

- `normal`
- `attack`
- `mixed`

### Пример запуска

```bash
./tools/scripts/run-replay.sh --scenario mixed --requests 200 --sessions 20 --agent-id agent-risk-01
```

Что делает:

- генерирует deterministic JSON Lines dataset;
- раскладывает события по topic-specific файлам;
- публикует события в локальные Kafka topics.

### Живой поток для Grafana и demo

Если нужен не мгновенный replay, а поток на несколько минут, используйте live-генератор:

```bash
bash tools/scripts/run-live-generator.sh
```

По умолчанию:

- длительность `300` секунд;
- скорость `1 request/sec`;
- scenario `mixed`.

Для переменной нагрузки:

```bash
bash tools/scripts/run-live-generator.sh \
  --duration-seconds 300 \
  --min-requests-per-second 1 \
  --max-requests-per-second 5 \
  --scenario mixed
```

Можно усилить динамику:

```bash
bash tools/scripts/run-live-generator.sh --duration-seconds 300 --requests-per-second 3 --scenario attack
```

Назначение:

- показать постепенное изменение дашбордов;
- проверить NRTP-обработку на живом потоке;
- убедиться, что окна `1m` и `5m` дают emissions не только на replay batch.

## 7. Как пользоваться системой после запуска

### Проверить, что данные реально идут

Для локального single-node Kafka надёжнее сначала смотреть offsets, а не `console-consumer` с обычным consumer group.

Рекомендуемая команда:

```bash
bash tools/scripts/check-output-topics.sh
```

Что она делает:

- показывает offsets по `normalized-events`, `invalid-events`, `late-events`;
- читает один образец из `normalized-events` через фиксированный `partition` и `offset`;
- убирает ложные `TimeoutException`, которые иногда встречаются при обычном `console-consumer` на локальном стенде.

Как читать бизнес-метрики:

- `Findings`
  - число сырых `GUARDRAIL_FINDING`, пришедших в pipeline;
- `Emissions`
  - число агрегатов, которые Flink уже выпустил downstream;
- `Triggered Findings`
  - подмножество findings, где detector сработал по порогу или boolean-условию.

Нормальная картина для MVP:

- findings растут быстрее emissions;
- emissions начинают заметно расти после прогресса watermark и закрытия окна;
- при `5m` окне первые emissions появляются позже, чем при `1m`.

Если нужно проверить входной topic вручную:

```bash
docker compose -f deployment/local/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic guardrail-findings \
  --partition 0 \
  --offset 0 \
  --max-messages 5
```

### Проверить policy

```bash
cat runtime/policies/active-policy.yaml
```

### Проверить generated replay files

```bash
ls -la runtime/replay/latest
```

### Проверить Prometheus и activity metrics

Открыть локально:

- `http://localhost:9090`
- `http://localhost:8081`

Полезные первые запросы в Prometheus:

- `flink_taskmanager_job_task_operator_valid_events_total`
- `flink_taskmanager_job_task_operator_invalid_events_total`
- `flink_taskmanager_job_task_operator_late_events_total`
- `flink_taskmanager_job_task_operator_on_time_events_total`

Если нужны сырые метрики без UI:

```bash
curl -s http://localhost:9249/metrics | head -40
curl -s http://localhost:9250/metrics | head -40
```

### Проверить incidents и debug output

На раннем MVP incidents пока должны быть доступны через:

- structured logs Flink job;
- локальные метрики;
- при необходимости `debug-incidents` topic.

## 8. Инкрементная разработка и regression

Это обязательное правило локального MVP:

- после каждого инкремента добавляется тест;
- после каждого инкремента прогоняется полный regression;
- новый replay scenario не должен ломать старые.

Команда:

```bash
./tools/scripts/run-regression.sh
```

Что проверяется:

- unit/smoke tests для Python-генератора;
- синтаксическая проверка shell-скриптов;
- базовая целостность generated datasets.

## 9. Типовой цикл работы

1. Поднять стенд.
2. Инициализировать topics.
3. Загрузить policy YAML.
4. Собрать и отправить Flink job.
5. Сгенерировать и прогнать `normal` replay.
6. Сгенерировать и прогнать `attack` replay.
7. Сгенерировать и прогнать `mixed` replay.
8. Проверить метрики, logs и output.
9. После изменения логики снова прогнать полный regression.

## 10. Когда нужен replay dataset

Replay dataset нужен для трех задач:

1. Demo
   - показать предсказуемые сценарии без ожидания живого трафика.
2. Regression
   - убедиться, что после изменения logic/policy старые сценарии не сломались.
3. Investigative debugging
   - воспроизвести проблемную сессию или паттерн атаки повторно.

Если упрощать:

- без replay dataset вы зависите от случайного живого трафика;
- с replay dataset вы получаете управляемую инженерную среду.

## 11. Типовые проблемы

### Kafka не поднялась

Проверить:

- `docker compose -f deployment/local/docker-compose.yml ps`
- `docker compose -f deployment/local/docker-compose.yml logs kafka`

### Output topic пустой, но Flink metrics растут

Проверить:

- `bash tools/scripts/check-output-topics.sh`
- offsets по `normalized-events`, `invalid-events`, `late-events`
- counters `flink_taskmanager_job_task_operator_valid_events_total`
- counters `flink_taskmanager_job_task_operator_on_time_events_total`

Если offsets растут, а `console-consumer` иногда не читает:

- это локальная особенность single-node Kafka consumer tooling;
- для runbook считать источником истины offsets и Prometheus metrics;
- для выборки сообщения использовать чтение по конкретному `partition` и `offset`.

### В output topic смешаны старые и новые локальные данные

Проверить:

- не использовался ли один и тот же локальный Kafka topic в нескольких прогонах;
- не читается ли `offset 0` после нескольких инкрементов подряд.

Решение:

- выполнить `bash tools/scripts/reset-topics.sh`;
- заново сделать `submit-job`;
- заново прогнать `run-replay`;
- после этого читать output topics уже на чистом наборе сообщений.

### Topics не создались

Проверить:

- что Kafka service уже healthy;
- что скрипт `init-topics.sh` видит контейнер `kafka`.

### Replay не публикуется

Проверить:

- что generator отработал и создал JSON Lines;
- что `kafka-console-producer.sh` доступен внутри контейнера;
- что topic существует.

### Policy не обновилась

Проверить:

- что `runtime/policies/active-policy.yaml` обновлен;
- что Flink job действительно читает этот файл в вашей реализации.

## 12. Что делать дальше

Следующая практическая итерация после этого runbook:

1. Добавить live-применение policy-конфига из внешнего источника.
2. Расширить output для quality/incidents потоков.
3. После каждого инкремента добавлять тест и прогонять `run-regression.sh`.
