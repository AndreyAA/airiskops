# Flink для AISafetyOps: локальный MVP runbook

## Глоссарий

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
- [scripts/load-policies.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/load-policies.sh)
- [scripts/run-replay.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/run-replay.sh)
- [scripts/run-regression.sh](/home/bob/old_bob/IdeaProjects/flink/scripts/run-regression.sh)
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

### Шаг 2. Инициализировать topics

```bash
./scripts/init-topics.sh
```

Что делает:

- создает все нужные Kafka topics;
- безопасно пропускает уже существующие.

### Шаг 3. Загрузить policy YAML

```bash
./scripts/load-policies.sh
```

Что делает:

- копирует policy YAML в локальную runtime-директорию;
- подготавливает активную policy для MVP.

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

Пример чтения Kafka topic:

```bash
docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic guardrail-findings \
  --from-beginning \
  --max-messages 10
```

### Проверить policy

```bash
cat runtime/policies/active-policy.yaml
```

### Проверить generated replay files

```bash
ls -la runtime/replay/latest
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
