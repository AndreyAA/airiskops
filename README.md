# AISafetyOps Flink MVP

Дата актуальности: 2026-08-29

## Назначение

Этот репозиторий содержит локальный MVP для NRTP-обработки событий AISafetyOps на Apache Flink.

Система предназначена для:

- приёма событий LLM-агентов и guardrail detectors из Kafka;
- нормализации и валидации входного потока;
- отделения `invalid` и `late` событий;
- оконной агрегации findings по `1m` и `5m`;
- публикации агрегатов и operational metrics в Kafka, Prometheus и Grafana;
- локальной разработки и demo на ноутбуке через Docker перед переносом в банковскую инфраструктуру.

Текущий доменный фокус:

- `PROMPT_INJECTION`
- `TOXICITY`
- `LOOPING`
- `SYSTEM_PROMPT_LEAKAGE`

## Архитектура репозитория

Репозиторий разделён по зонам ответственности, чтобы runtime-код Flink не смешивался с observability, tooling и документацией.

```text
.
├── flink-job/
├── deployment/
├── observability/
├── tools/
├── docs/
├── config/
├── runtime/
├── pom.xml
└── README.md
```

### `flink-job/`

Java-модуль с production-кодом Flink job.

Что лежит внутри:

- `src/main/java` — доменная модель, application logic и infrastructure adapters;
- `src/test/java` — unit и integration tests для Flink pipeline;
- `pom.xml` — Maven build для shaded job jar.

Собирать и тестировать нужно именно этот модуль.

### `deployment/`

Файлы локального окружения.

Сейчас:

- `deployment/local/docker-compose.yml`

Назначение:

- поднять локальный Kafka;
- поднять Flink JobManager и TaskManager;
- поднять Prometheus и Grafana;
- смонтировать собранный JAR и job config в контейнеры.

### `observability/`

Всё, что относится к наблюдаемости.

Подкаталоги:

- `observability/prometheus/`
- `observability/grafana/`

Назначение:

- хранить `prometheus.yml`;
- хранить provisioning Grafana;
- хранить dashboards AISafetyOps;
- отделять observability-артефакты от runtime-кода job.

### `tools/`

Вспомогательные скрипты и генераторы.

Подкаталоги:

- `tools/scripts/` — operational shell scripts;
- `tools/generators/` — Python-генераторы replay и live stream;
- `tools/tests/` — Python tests для генераторов.
- `tools/testdata/` — только маленькие фиксированные test fixtures, если они реально нужны.

Назначение:

- запускать локальный стенд;
- инициализировать topics;
- публиковать replay dataset;
- генерировать живой поток для Grafana;
- прогонять локальный regression.

Правило хранения данных:

- runtime replay datasets не хранятся в git;
- всё, что можно воспроизвести генератором, должно генерироваться;
- в репозитории допустимы только маленькие fixture-наборы для тестов.

### `docs/`

Вся проектная документация.

Подкаталоги:

- `docs/architecture/`
- `docs/monitoring/`
- `docs/runbooks/`
- `docs/mvp/`

Назначение:

- manual по Flink и AISafetyOps;
- monitoring/debugging guides;
- local walkthrough и runbooks;
- MVP spec и результаты инкрементов.

### `config/`

Конфигурация, которая используется job и локальным стендом.

Подкаталоги:

- `config/job/`
- `config/policies/`

Назначение:

- хранить YAML job config;
- хранить policy defaults;
- не смешивать конфиги с кодом и скриптами.

### `runtime/`

Локальные runtime-данные, которые создаются во время работы.

Примеры:

- `runtime/replay/latest/`
- `runtime/policies/`

Содержимое runtime не считается исходным кодом и может очищаться cleanup-скриптами.

## Архитектура Java-модуля `flink-job`

Внутри `flink-job` используется целевая слоистая схема:

- `model`
- `app`
- `infra`

Это основной корпоративный layout для развития проекта.

### `com.bank.aisafetyops.model`

Доменная модель и общие константы.

Примеры:

- `SafetyEvent`
- `GuardrailWindowAggregate`
- `InvalidEvent`
- `LateEvent`
- `EventType`
- `GuardrailNames`
- `WindowNames`

Назначение пакета:

- задавать контракты данных между этапами pipeline;
- хранить модели, не завязанные на конкретный source/sink;
- держать domain vocabulary в одном месте.

### `com.bank.aisafetyops.app`

Application layer: orchestration и бизнес-логика Flink job.

Подпакеты:

- `app/job`
- `app/usecase`
- `app/functions`
- `app/config`
- `app/support`

#### `app/job`

Точка входа в приложение.

Сейчас:

- `AiSafetyOpsMvpJob`

Назначение:

- прочитать конфигурацию;
- создать `StreamExecutionEnvironment`;
- передать управление topology builder;
- запустить job.

#### `app/usecase`

Сборка topology под конкретный инкремент или use case.

Сейчас:

- `IncrementOneTopologyBuilder`

Назначение:

- описывать полный dataflow;
- задавать порядок операторов;
- выделять бизнес-этапы пайплайна;
- удерживать main topology readable и testable.

#### `app/functions`

Пользовательские Flink operators и window logic.

Примеры:

- `ParseAndValidateFunction`
- `SplitParseResultsFunction`
- `RouteLateEventsFunction`
- `GuardrailAggregateKeySelector`
- `GuardrailWindowAggregateFunction`
- `GuardrailWindowProcessFunction`
- `SerializeGuardrailAggregateFunction`

Назначение:

- инкапсулировать отдельные шаги обработки;
- отделять parsing, routing, window aggregation и serialization;
- делать операторную логику изолированной для unit/integration tests.

#### `app/config`

Конфигурационные модели и чтение аргументов job.

Примеры:

- `JobConfig`
- `JobConfigOptions`
- `OutputTopics`

Назначение:

- централизовать имена параметров;
- собирать config из CLI и YAML;
- не размазывать строковые ключи и defaults по коду.

#### `app/support`

Технические константы topology и runtime defaults.

Примеры:

- `JobTopology`
- `FlinkEnvironmentDefaults`

Назначение:

- хранить `uid`, `name`, output tags и runtime defaults;
- обеспечивать стабильность topology для UI, metrics и future savepoint compatibility.

### `com.bank.aisafetyops.infra`

Infrastructure adapters для внешнего мира.

Подпакеты:

- `infra/source`
- `infra/sink`
- `infra/parser`
- `infra/serde`
- `infra/config`

#### `infra/source`

Factory и адаптеры чтения данных.

Сейчас:

- `KafkaSourceFactory`

Назначение:

- строить Flink source из внешнего транспорта;
- изолировать connector-specific код от business logic.

#### `infra/sink`

Factory и адаптеры записи данных.

Сейчас:

- `KafkaSinkFactory`

Назначение:

- создавать Kafka sinks;
- инкапсулировать delivery guarantees и serialization binding.

#### `infra/parser`

Parsing входного JSON и промежуточные parse-result объекты.

Сейчас:

- `SafetyEventParser`
- `ParseResult`

Назначение:

- отделять raw ingestion от нормализованной доменной модели;
- локализовать schema parsing и validation concerns.

#### `infra/serde`

Сериализация доменных объектов.

Сейчас:

- `JsonSerde`

Назначение:

- централизованно преобразовывать модели в JSON;
- упростить замену transport format позже.

#### `infra/config`

Чтение конфигурации из внешних файлов.

Сейчас:

- `YamlJobConfigLoader`

Назначение:

- изолировать YAML loading от application logic;
- дать единый вход для конфигурации job.

## Текущий dataflow

Текущий MVP pipeline выглядит так:

1. Kafka source читает raw JSON события из входных topics.
2. `ParseAndValidateFunction` парсит и валидирует payload.
3. `SplitParseResultsFunction` разделяет valid и invalid поток.
4. `RouteLateEventsFunction` после watermark assignment отделяет late события.
5. Valid on-time события публикуются в `normalized-events`.
6. Поток `GUARDRAIL_FINDING` фильтруется отдельно.
7. Для findings считаются tumbling event-time окна `1m` и `5m`.
8. `GuardrailWindowProcessFunction`:
   - формирует `GuardrailWindowAggregate`;
   - обновляет AISafetyOps metrics;
   - эмитит агрегаты downstream.
9. Агрегаты сериализуются и пишутся в `guardrail-aggregates`.

## Основные команды

Сборка:

```bash
bash tools/scripts/build-job.sh
```

Регресс:

```bash
bash tools/scripts/run-regression.sh
```

Поднять локальный стенд:

```bash
bash tools/scripts/start-local.sh
```

Инициализировать topics:

```bash
bash tools/scripts/init-topics.sh
```

Загрузить policy:

```bash
bash tools/scripts/load-policies.sh
```

Отправить job:

```bash
bash tools/scripts/submit-job.sh
```

Запустить replay:

```bash
bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
```

Запустить live generator:

```bash
bash tools/scripts/run-live-generator.sh --duration-seconds 300 --min-requests-per-second 1 --max-requests-per-second 5
```

Остановить и очистить локальный контур:

```bash
bash tools/scripts/cleanup-local.sh
```

## Где смотреть дальше

- Архитектурный manual:
  - [docs/architecture/aisafetyops-manual.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/aisafetyops-manual.md)
- Manual по добавлению новых агрегированных метрик:
  - [docs/architecture/adding-n-minute-metrics.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/adding-n-minute-metrics.md)
- Monitoring и debugging:
  - [docs/monitoring/monitoring-debugging-guide.md](/home/bob/old_bob/IdeaProjects/flink/docs/monitoring/monitoring-debugging-guide.md)
- Local walkthrough:
  - [docs/runbooks/local-walkthrough.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/local-walkthrough.md)
- MVP runbook:
  - [docs/runbooks/mvp-runbook.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/mvp-runbook.md)
