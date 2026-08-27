# Flink для AISafetyOps: локальный MVP runbook

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
- `bash`-скрипты для operational действий;
- `Python`-генератор для replay dataset;
- `JSON` как стартовый транспортный формат событий;
- `YAML` для policy/config defaults.

## 3. Структура файлов

- [docker-compose.yml](/home/bob/old_bob/IdeaProjects/flink/docker-compose.yml)
- [policies/default-policy.yaml](/home/bob/old_bob/IdeaProjects/flink/policies/default-policy.yaml)
- [scripts/start-local.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/start-local.sh)
- [scripts/stop-local.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/stop-local.sh)
- [scripts/init-topics.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/init-topics.sh)
- [scripts/reset-topics.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/reset-topics.sh)
- [scripts/build-job.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/build-job.sh)
- [scripts/submit-job.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/submit-job.sh)
- [scripts/check-output-topics.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/check-output-topics.sh)
- [scripts/load-policies.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/load-policies.sh)
- [scripts/run-replay.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/run-replay.sh)
- [scripts/run-regression.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/run-regression.sh)
- [monitoring/prometheus.yml](/home/bob/old_bob/IdeaProjects/flink/monitoring/prometheus.yml)
- [scripts/generate_events.py](/home/bob/old_bob/IdeaProjects/flink/scripts/generate_events.py)
- [tests/test_generate_events.py](/home/bob/old_bob/IdeaProjects/flink/tests/test_generate_events.py)

## 4. Topics локального MVP

На старте используются topics:

- `agent-requests`
- `agent-responses`
- `guardrail-findings`
- `guardrail-quality-metrics`
- `policy-updates`
- `debug-incidents`

Этого достаточно для:

- подачи входных событий;
- проверки quality/incident flows;
- будущего расширения policy stream;
- локального debug вывода.

## 5. Старт локального стенда

### Шаг 1. Поднять Docker services

```bash
./scripts/start-local.sh
```

Что делает:

- поднимает Kafka;
- поднимает JobManager;
- поднимает TaskManager.
- поднимает Prometheus.

### Шаг 2. Инициализировать topics

```bash
./scripts/init-topics.sh
```

Что делает:

- создает все нужные Kafka topics;
- безопасно пропускает уже существующие.

Если нужен чистый повторный прогон без исторических сообщений:

```bash
bash scripts/reset-topics.sh
```

Что делает:

- удаляет входные и выходные MVP topics;
- создает их заново;
- позволяет валидировать текущий инкремент без смешения со старыми локальными прогонами.

### Шаг 3. Загрузить policy YAML

```bash
./scripts/load-policies.sh
```

Что делает:

- копирует policy YAML в локальную runtime-директорию;
- подготавливает активную policy для MVP.

### Шаг 4. Собрать и отправить Flink job

```bash
./scripts/build-job.sh
./scripts/submit-job.sh
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
./scripts/run-replay.sh --scenario mixed --requests 200 --sessions 20 --agent-id agent-risk-01
```

Что делает:

- генерирует deterministic JSON Lines dataset;
- раскладывает события по topic-specific файлам;
- публикует события в локальные Kafka topics.

## 7. Как пользоваться системой после запуска

### Проверить, что данные реально идут

Для локального single-node Kafka надёжнее сначала смотреть offsets, а не `console-consumer` с обычным consumer group.

Рекомендуемая команда:

```bash
bash scripts/check-output-topics.sh
```

Что она делает:

- показывает offsets по `normalized-events`, `invalid-events`, `late-events`;
- читает один образец из `normalized-events` через фиксированный `partition` и `offset`;
- убирает ложные `TimeoutException`, которые иногда встречаются при обычном `console-consumer` на локальном стенде.

Если нужно проверить входной topic вручную:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
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
./scripts/run-regression.sh
```

Что проверяется:

- unit/smoke tests для Python-генератора;
- синтаксическая проверка shell-скриптов;
- базовая целостность generated datasets.

## 9. Типовой цикл работы

1. Поднять стенд.
2. Инициализировать topics.
3. Загрузить policy YAML.
4. Сгенерировать и прогнать `normal` replay.
5. Сгенерировать и прогнать `attack` replay.
6. Сгенерировать и прогнать `mixed` replay.
7. Проверить метрики, logs и output.
8. После изменения логики снова прогнать полный regression.

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

- `docker compose ps`
- `docker compose logs kafka`

### Output topic пустой, но Flink metrics растут

Проверить:

- `bash scripts/check-output-topics.sh`
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

- выполнить `bash scripts/reset-topics.sh`;
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

1. Подключить реальный Java Flink job к этим topics.
2. Начать с этапа `Trusted Event Foundation`.
3. После каждого инкремента добавлять тест и прогонять `run-regression.sh`.
