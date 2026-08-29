# AGENTS.md

Дата актуальности: 2026-08-29

## Назначение

Этот файл задаёт рабочие правила для агентов и разработчиков, которые вносят изменения в репозиторий `AISafetyOps Flink MVP`.

Цель:

- не ломать локальный NRTP-стенд;
- сохранять чистую слоистую архитектуру `model / app / infra`;
- вносить изменения инкрементально;
- сопровождать изменения проверками, документацией и наблюдаемостью.

## Контекст проекта

Проект реализует локальный MVP для near-real-time обработки событий AISafetyOps на Apache Flink.

Основной сценарий:

- ingestion событий LLM-агентов и guardrail findings из Kafka;
- валидация и нормализация событий;
- выделение `invalid` и `late` потоков;
- оконная агрегация findings по `1m` и `5m`;
- экспорт результатов в Kafka;
- экспорт operational и business metrics в Prometheus/Grafana.

Текущие guardrail-ы:

- `PROMPT_INJECTION`
- `TOXICITY`
- `LOOPING`
- `SYSTEM_PROMPT_LEAKAGE`

## Структура репозитория

- `flink-job/`
  - production-код Flink job и Java/Flink tests.
- `deployment/`
  - локальный Docker deployment.
- `observability/`
  - Prometheus, Grafana, dashboards, provisioning.
- `tools/`
  - shell-скрипты, генераторы, Python tests.
- `docs/`
  - архитектура, runbooks, monitoring guides, MVP docs.
- `config/`
  - YAML-конфиги job и policy defaults.
- `runtime/`
  - локальные generated/runtime данные.

## Каноническая Java-архитектура

Использовать только следующие верхнеуровневые пакеты:

- `com.bank.aisafetyops.model`
- `com.bank.aisafetyops.app`
- `com.bank.aisafetyops.infra`

### `model`

Сюда помещаются:

- доменные records и value objects;
- enum-ы и константы предметной области;
- модели событий и агрегатов.

Запрещено:

- тянуть сюда Flink connectors;
- класть сюда parsing, I/O или deployment logic.

### `app`

Сюда помещаются:

- topology builders;
- Flink functions;
- orchestration pipeline;
- job config models;
- topology constants и runtime defaults.

Запрещено:

- прямое чтение файлов и YAML, если это infrastructure concern;
- размазывание Kafka-specific деталей по бизнес-логике.

### `infra`

Сюда помещаются:

- source/sink factories;
- YAML loader;
- parser;
- serde;
- адаптеры внешних форматов и транспортов.

Запрещено:

- доменная логика агрегирования;
- business decision rules, если они не относятся к преобразованию внешнего формата.

## Правила изменений

### 1. Сначала понять, где должна жить логика

Перед изменением определить:

- это доменная модель;
- это application/pipeline logic;
- это infrastructure adapter;
- это observability;
- это tooling;
- это documentation.

Не смешивать эти зоны в одном файле без явной причины.

### 2. Сохранять инкрементальный подход

Любой новый функционал добавлять по шагам:

1. минимальный рабочий каркас;
2. тесты;
3. наблюдаемость;
4. документация;
5. только потом расширение логики.

Не делать большой "переписывающий" рефакторинг вместе с новой бизнес-фичей, если это не согласовано отдельно.

### 3. После каждой доработки должен быть понятный business value

При добавлении этапа или фичи зафиксировать:

- что именно появилось;
- что можно увидеть руками;
- где это проверяется;
- какую пользу это даёт AISafetyOps / Operational Risk.

### 4. Все строковые и числовые константы выносить

Не копипастить:

- имена topics;
- names/uid операторов;
- metric names;
- window names;
- guardrail names;
- config keys;
- default thresholds или интервалы.

Использовать общие классы констант или config models.

### 5. Комментарии писать детально, но по делу

Комментарии нужны там, где без них непонятны:

- watermark / allowed lateness decisions;
- state semantics;
- window semantics;
- связь оператора с business intent;
- ограничения local MVP.

Не писать шумовые комментарии уровня "присваиваем значение переменной".

## Правила для Flink-изменений

### Когда меняется topology

При изменении topology проверить:

- не потерялись ли `uid()` у стабильных операторов;
- не сломались ли output topics;
- не изменились ли window semantics случайно;
- не выросла ли high-cardinality нагрузка на метрики.

### Когда добавляется новая метрика

Сначала определить:

- это raw metric или aggregate metric;
- на каком operator stage она истинна;
- что происходит при late events;
- нужен `Counter` или `Gauge`;
- не приведут ли labels к высокой cardinality.

Если метрика относится к оконному бизнес-результату, по умолчанию добавлять её на этапе `ProcessWindowFunction`.

### Когда добавляется новый guardrail

Нужно синхронно проверить:

- доменные константы;
- parser;
- генераторы test data;
- aggregation logic;
- metrics;
- Grafana dashboards;
- документацию.

## Правила для observability

Изменения в observability должны быть консистентны между:

- Flink metric name;
- Prometheus scrape/config;
- Grafana dashboard;
- runbook/documentation.

Если добавлена новая пользовательская метрика, нужно:

1. убедиться, что она реально экспортируется;
2. задокументировать её смысл;
3. при необходимости добавить panel в Grafana.

## Правила для tooling

### Shell scripts

Все operational scripts размещать в:

- `tools/scripts/`

Требования:

- `set -euo pipefail`;
- работа из корня репозитория;
- использование `deployment/local/docker-compose.yml`;
- понятные сообщения об ошибках.

### Python generators

Все генераторы держать в:

- `tools/generators/`

Требования:

- deterministic режим через `seed`, если это replay;
- возможность локально воспроизвести поток;
- явные параметры CLI;
- обновление тестов при изменении формата событий.

Python tests держать в:

- `tools/tests/`

## Правила для документации

Документация обязательна, если меняется одно из:

- структура репозитория;
- команды запуска;
- pipeline semantics;
- метрики;
- dashboards;
- генераторы;
- runbook;
- MVP scope.

Куда писать:

- `docs/architecture/` — архитектура и инженерные принципы;
- `docs/monitoring/` — monitoring/debugging;
- `docs/runbooks/` — пошаговые инструкции;
- `docs/mvp/` — спецификации и результаты этапов.

После переименования или переноса документа обновлять ссылки в:

- `README.md`
- `docs/README.md`

## Обязательные проверки

### Минимум после любого изменения

```bash
bash tools/scripts/run-regression.sh
```

Это включает:

- shell syntax checks;
- Python unit tests;
- Java/Flink tests.

### Если менялся Java-код job

Дополнительно проверить сборку:

```bash
bash tools/scripts/build-job.sh
```

### Если менялись Docker, Kafka, Flink submission, observability или runbook

Желательно проверить локальный путь вручную:

1. `bash tools/scripts/cleanup-local.sh`
2. `bash tools/scripts/start-local.sh`
3. `bash tools/scripts/init-topics.sh`
4. `bash tools/scripts/load-policies.sh`
5. `bash tools/scripts/submit-job.sh`
6. `bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01`
7. `bash tools/scripts/check-output-topics.sh`

### Если менялись live/replay generators

Дополнительно проверить:

```bash
python3 -m py_compile tools/generators/generate_events.py tools/generators/stream_live_events.py
```

## Что считать ошибкой

При обнаружении ошибки фиксировать три вещи:

1. ошибка;
2. причина;
3. как исправлена.

Особенно это важно для:

- path mismatches после рефакторинга;
- Docker compose path issues;
- Kafka advertised listeners / bootstrap mismatches;
- Flink watermark и late-event surprises;
- пустых Grafana dashboards;
- падений job submission;
- broken tests после изменения event schema.

## Ограничения и практические договорённости проекта

- Локальная разработка идёт через Docker.
- Основной транспорт MVP: Kafka.
- Формат событий на старте: JSON с возможностью дальнейшей замены.
- Конфигурация хранится в YAML.
- Окна анализа для кейса NRTP: `1-5 минут`.
- Реакция на триггеры ожидается в пределах `2-3 минут`.
- Главный аналитический ключ сейчас смещён в сторону `agentId`, а не только `requestId`.
- `sessionId` важен как группировка нескольких запросов одного пользователя.
- В MVP часть реакций пока может ограничиваться логированием и метриками без сложного automated response.

## Нежелательные практики

Не делать без отдельного обоснования:

- добавление high-cardinality labels в Prometheus;
- смешивание production-кода с demo/tooling logic;
- дублирование одинаковых констант по нескольким классам;
- изменение event semantics без обновления генераторов и tests;
- массовое переименование Java packages вместе с бизнес-изменением;
- silent changes в runbook без обновления документации.

## Ключевые файлы

- [README.md](/home/bob/old_bob/IdeaProjects/flink/README.md)
- [docs/README.md](/home/bob/old_bob/IdeaProjects/flink/docs/README.md)
- [flink-job/pom.xml](/home/bob/old_bob/IdeaProjects/flink/flink-job/pom.xml)
- [deployment/local/docker-compose.yml](/home/bob/old_bob/IdeaProjects/flink/deployment/local/docker-compose.yml)
- [config/job/local-job.yaml](/home/bob/old_bob/IdeaProjects/flink/config/job/local-job.yaml)
- [config/policies/default-policy.yaml](/home/bob/old_bob/IdeaProjects/flink/config/policies/default-policy.yaml)
- [tools/scripts/run-regression.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/run-regression.sh)
