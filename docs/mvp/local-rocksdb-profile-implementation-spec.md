# Спецификация реализации local RocksDB profile для AIRiskOps MVP

Дата актуальности: 2026-09-04

## Назначение

Этот документ фиксирует согласованную спецификацию для добавления в репозиторий отдельного локального runtime-профиля с включённым `RocksDB state backend`.

Цель документа:

- зафиксировать границы изменений;
- описать минимальный и инкапсулированный способ переключения между режимами;
- исключить случайное смешивание business logic и runtime backend concerns;
- задать критерии готовности, проверки и риски до начала реализации.

## Контекст

На текущем этапе локальный AIRiskOps MVP:

- запускается через `docker compose`;
- использует единый `submit-job.sh`;
- читает runtime/business config из `config/job/local-job.yaml`;
- включает checkpointing, но не задаёт state backend явно в Java-коде;
- ориентирован на локальный demo/replay workload, а не на production deployment.

В проектной документации уже зафиксировано:

- `RocksDB` не обязателен по умолчанию для текущего локального MVP;
- при росте stateful workload он становится ожидаемым кандидатом;
- выбор backend должен быть осознанным и не менять event/window semantics молча.

## Проблема

Сейчас в репозитории нет удобного и инкапсулированного способа:

- поднять локальный профиль с `RocksDB`;
- переключаться между `default` и `rocksdb` режимами без ручных правок скриптов или compose;
- настраивать пути для local state, checkpoints и savepoints через project YAML;
- проверить, что backend реально включён, а не только задекларирован.

При этом нежелательно:

- ломать текущий default local path;
- размазывать `if rocksdb` по topology-коду;
- дублировать submit-логику в нескольких shell-скриптах без необходимости.

## Цели

Нужно реализовать:

- отдельный `optional` локальный профиль с `RocksDB`;
- переключение между профилями через параметр конфигурации, а не через ручное редактирование файлов;
- настройку путей `rocksdb`, `checkpoints`, `savepoints` через YAML;
- включение `incremental checkpoints` в RocksDB-профиле;
- явную документацию по запуску и валидации режима.

## Не-цели

В рамках этой задачи не планируется:

- менять default backend для текущего MVP;
- менять event semantics;
- менять window semantics, `allowed lateness` или watermark strategy;
- менять Kafka topics, contracts или naming;
- делать production sizing RocksDB memory/disk;
- добавлять глубокий RocksDB low-level tuning;
- менять deployment под Kubernetes или external object storage.

## Архитектурные требования

### 1. Инкапсуляция режимов

Переключение между режимами должно быть устроено так, чтобы:

- текущий default path остался минимально затронутым;
- различия между профилями были сосредоточены в одном конфигурационном слое;
- topology builder не знал о конкретном backend;
- добавление следующего режима не требовало переписывать существующие скрипты и topology.

Практически это означает:

- один общий `submit-job.sh`;
- разные YAML-профили;
- отдельный runtime-config applicator в Java;
- profile-specific startup logic только при реальной необходимости.

### 2. Разделение ответственности

- `submit-job.sh` отвечает только за submit jar и выбор `configFile`.
- YAML profile отвечает за выбор backend и runtime state paths.
- Java runtime-config layer отвечает за применение backend-настроек к `StreamExecutionEnvironment`.
- topology code отвечает только за pipeline semantics.

### 3. Безопасность изменений

Изменения не должны:

- менять существующий default local run без явного выбора RocksDB profile;
- ломать существующие команды для локального MVP;
- менять поведение бизнес-пайплайна между профилями, кроме state backend/runtime storage discipline.

## Текущее состояние

На момент подготовки спецификации:

- базовый локальный конфиг: `config/job/local-job.yaml`;
- общий submit: `tools/scripts/submit-job.sh`;
- локальный запуск контейнеров: `tools/scripts/start-local.sh`;
- основной compose: `deployment/local/docker-compose.yml`;
- job config model: `flink-job/src/main/java/com/bank/airiskops/app/config/JobConfig.java`;
- topology setup: `flink-job/src/main/java/com/bank/airiskops/app/usecase/IncrementOneTopologyBuilder.java`;
- entrypoint job: `flink-job/src/main/java/com/bank/airiskops/app/job/AiRiskOpsMvpJob.java`.

Техническое ограничение:

- базовый Docker image `flink:1.20.2-scala_2.12-java17` не гарантирует наличие RocksDB backend jar в runtime image;
- поэтому нельзя опираться только на декларативный `state.backend.type: rocksdb` без проверки classpath/runtime support.

## Целевое поведение

После реализации должно быть возможно:

1. Поднять локальный контур обычным способом, не включая RocksDB.
2. Поднять тот же локальный контур и отправить job с RocksDB profile.
3. Переключить профиль одной командой или одним параметром `--config`.
4. Переопределить через YAML:
   - `state backend type`;
   - `incremental checkpoints`;
   - `checkpoints dir`;
   - `savepoints dir`;
   - `rocksdb local dir`.
5. Убедиться через Flink UI/REST/exporter, что:
   - backend реально `rocksdb`;
   - incremental checkpoints включены;
   - checkpoint storage paths заданы ожидаемо.

## Предлагаемая структура решения

### 1. Конфигурационные профили

Должно быть два локальных job profile:

- `config/job/local-job.yaml`
  - текущий baseline профиль;
  - сохраняется как default.
- `config/job/local-rocksdb.yaml`
  - новый optional профиль;
  - повторяет базовые бизнес-параметры локального запуска;
  - добавляет runtime state backend settings.

### 2. Новый runtime state config block

В `JobConfig` должен появиться отдельный вложенный конфигурационный блок, например:

- `RuntimeStateConfig`
  - `backendType`
  - `incrementalCheckpointsEnabled`
  - `checkpointsDir`
  - `savepointsDir`
  - `rocksdbLocalDir`

Требования к блоку:

- он должен быть независим от business config;
- он должен иметь безопасные default values для профиля без RocksDB;
- он должен читаться из YAML тем же механизмом, что и существующие параметры job.

### 3. Applicator для runtime backend

Нужен отдельный Java-компонент, например:

- `RuntimeStateProfileApplier`

Его ответственность:

- получить `StreamExecutionEnvironment` и `RuntimeStateConfig`;
- применить backend/runtime settings в одном месте;
- скрыть детали того, как именно backend прокидывается в Flink config.

Требования:

- `AiRiskOpsMvpJob` вызывает applicator до построения topology;
- `IncrementOneTopologyBuilder` не получает knowledge о `rocksdb`;
- applicator должен быть маленьким, явно тестируемым и не смешиваться с parsing business config.

### 4. Общий submit path

`tools/scripts/submit-job.sh` должен остаться общим, но получить параметризацию:

- принимать путь к config file, например `--config config/job/local-job.yaml`;
- по умолчанию использовать текущий `config/job/local-job.yaml`;
- пробрасывать выбранный путь как `--configFile` в Flink job.

Требования:

- submit script не должен содержать `if rocksdb`;
- submit script не должен знать про конкретные storage paths;
- submit script не должен дублироваться для отдельных backend modes.

### 5. Startup profile только при необходимости

Предпочтительный порядок решений:

1. Сначала реализовать всё через YAML + Java runtime applicator.
2. Отдельный startup profile или compose override добавлять только если выяснится, что без него нельзя надёжно смонтировать локальные state directories.

Допустимые варианты:

- общий нейтральный mount в основном compose;
- отдельный `deployment/local/docker-compose.rocksdb.yml`, если без override инкапсуляция хуже.

Нежелательный вариант:

- жёстко зашить RocksDB-only storage path в default compose так, что он станет обязательным для всех запусков без причины.

## Детализация по изменениям

### Изменения в `flink-job`

Нужно:

- добавить dependency для RocksDB backend в shaded artifact;
- добавить config model для runtime state profile;
- расширить `JobConfig` чтением новых YAML-полей;
- добавить runtime applicator;
- подключить applicator в job entrypoint.

Не нужно:

- менять существующую topology semantics;
- переносить backend-логику в `IncrementOneTopologyBuilder`;
- трогать `uid`, operator names, keyBy/window configuration без необходимости.

### Изменения в `config/job`

Нужно:

- сохранить `local-job.yaml` как baseline;
- добавить `local-rocksdb.yaml`;
- сделать структуру нового YAML достаточно явной, чтобы later можно было добавлять и другие профили.

Требования к содержимому `local-rocksdb.yaml`:

- повторяет все параметры, необходимые текущему local run;
- задаёт backend как `rocksdb`;
- включает incremental checkpoints;
- задаёт filesystem paths для:
  - checkpoints;
  - savepoints;
  - local RocksDB state.

### Изменения в `tools/scripts`

Нужно:

- сделать `submit-job.sh` параметризуемым по `configFile`;
- при желании аналогично параметризовать `init.sh`, если это улучшает usability RocksDB profile.

Не нужно:

- плодить отдельные submit scripts для каждого backend;
- ломать текущий default сценарий.

### Изменения в `deployment/local`

Нужно только если потребуется runtime filesystem support:

- добавить mount для host path, который будет содержать:
  - checkpoints;
  - savepoints;
  - rocksdb local state.

Требования к mount path:

- путь должен быть стабильным и предсказуемым;
- путь должен жить внутри project-controlled runtime area;
- default режим не должен требовать ручного обслуживания RocksDB state.

Рекомендуемая host-структура:

- `runtime/flink-state/checkpoints`
- `runtime/flink-state/savepoints`
- `runtime/flink-state/rocksdb`

### Изменения в документации

Нужно обновить:

- `README.md`
- `docs/README.md`
- `docs/runbooks/local-walkthrough.md`

Нужно описать:

- зачем существует отдельный RocksDB profile;
- как его запускать;
- где менять storage paths;
- как переключаться обратно на default profile;
- как проверить backend через UI/REST/exporter;
- какие ограничения у локального RocksDB режима остаются.

## Предлагаемая YAML-модель

Точное имя полей может быть скорректировано на этапе реализации, но целевая модель должна быть близка к такой:

```yaml
runtimeState:
  backendType: rocksdb
  incrementalCheckpointsEnabled: true
  checkpointsDir: file:///opt/flink/state/checkpoints
  savepointsDir: file:///opt/flink/state/savepoints
  rocksdbLocalDir: /opt/flink/state/rocksdb
```

Для default local profile возможен один из двух допустимых подходов:

1. Явно хранить `runtimeState.backendType: hashmap`.
2. Не задавать блок вовсе и использовать Java defaults.

Предпочтение:

- оставить default profile максимально близким к текущему состоянию;
- при этом сделать структуру достаточно явной, чтобы конфиг читался одинаково в обоих профилях.

## Требования к валидации конфигурации

Конфигурация должна валидироваться так, чтобы ошибки были ранними и понятными.

Минимальные проверки:

- неизвестный backend type даёт понятную ошибку;
- `rocksdb` профиль без `checkpointsDir` считается некорректным;
- `rocksdb` профиль без `rocksdbLocalDir` считается некорректным;
- включённый `incrementalCheckpoints` без RocksDB либо явно поддерживается, либо явно запрещается в одном месте;
- пути не должны silently игнорироваться.

## Требования к тестам

Нужно добавить или обновить тесты на уровне `flink-job`.

Минимальный набор:

- parsing test для нового YAML profile;
- test на default values для режима без RocksDB;
- test на применение runtime state config к `StreamExecutionEnvironment` или к Flink `Configuration`;
- test на ошибку при некорректном RocksDB config.

Если runtime applicator сложно проверить напрямую через полноценный env:

- допустим unit-level тест компонента, который строит или модифицирует `Configuration`.

Не требуется:

- строить сложный e2e performance benchmark;
- доказывать production-grade tuning в тестах.

## План реализации

### Этап 1. Конфигурационная модель

- Добавить новый runtime state config class.
- Расширить `JobConfigOptions`, если это нужно для unified config reading.
- Научить `JobConfig` читать новый блок из YAML и подставлять default values.
- Добавить unit tests на parsing и defaults.

Результат этапа:

- job умеет загружать profile-aware runtime state config, но behaviour ещё может не измениться.

### Этап 2. Runtime applicator

- Добавить отдельный applicator backend config.
- Подключить его в job entrypoint до `IncrementOneTopologyBuilder.configure(...)`.
- Убедиться, что default local profile не меняет поведение относительно текущего состояния.

Результат этапа:

- backend/runtime settings применяются централизованно и изолированно.

### Этап 3. Submit/config profile switching

- Обновить `submit-job.sh`, чтобы он принимал путь к YAML config.
- Добавить `config/job/local-rocksdb.yaml`.
- Проверить, что переключение между профилями делается без отдельного submit script.

Результат этапа:

- один submit path, несколько конфигурационных режимов.

### Этап 4. Runtime filesystem support

- Проверить, нужен ли mount для state directories.
- Если нужен, выбрать минимальный способ:
  - общий нейтральный mount;
  - либо isolated compose override.
- Создать ожидаемые директории в `runtime/`, если это требуется startup path.

Результат этапа:

- RocksDB profile получает валидные filesystem paths без ручных ad-hoc действий.

### Этап 5. Документация и валидация

- Обновить `README.md`.
- Обновить `docs/README.md`.
- Обновить `docs/runbooks/local-walkthrough.md`.
- Описать команды запуска и шаги проверки.

Результат этапа:

- у команды есть воспроизводимый runbook для обеих локальных конфигураций.

## Acceptance criteria

Изменение считается завершённым, если выполняются все условия:

- default local run работает как раньше;
- RocksDB profile активируется без ручной правки кода или compose;
- переключение делается через выбор YAML config и, только при необходимости, через отдельный startup profile;
- `submit-job.sh` остаётся единым;
- topology semantics не меняются;
- checkpoints проходят;
- Flink REST/UI/exporter показывает backend `rocksdb` для RocksDB profile;
- Flink REST/UI/exporter показывает включённый incremental mode;
- документация описывает запуск, переключение и проверку режима.

## Проверки после реализации

Минимальные проверки:

```bash
bash tools/scripts/run-regression.sh
```

```bash
bash tools/scripts/build-job.sh
```

Локальная runtime-проверка:

```bash
bash tools/scripts/start-local.sh
```

```bash
bash tools/scripts/submit-job.sh --config config/job/local-rocksdb.yaml
```

Проверка job overview:

```bash
curl -s http://localhost:8081/jobs/overview
```

Проверка checkpoint/runtime config:

- через Flink UI;
- через Flink REST checkpoint config endpoint;
- через `checkpoint-exporter` и Grafana/Prometheus signals.

## Риски и ограничения

### 1. Runtime dependency risk

Если RocksDB backend artifact не окажется доступен в runtime classpath, профиль не стартует.

Следствие:

- dependency должна быть включена осознанно и проверена локальной сборкой и submit path.

### 2. Filesystem path risk

Если контейнер не видит директории для local state/checkpoints/savepoints, RocksDB профиль будет падать на старте или во время checkpointing.

Следствие:

- путь должен быть либо смонтирован, либо гарантированно существовать внутри container filesystem.

### 3. False sense of production readiness

Локальный RocksDB profile не делает MVP production-ready автоматически.

Он не решает:

- durable external checkpoint storage;
- production disk sizing;
- native memory sizing;
- restore SLA;
- rescale discipline.

### 4. Config drift risk

Если `local-job.yaml` и `local-rocksdb.yaml` начнут расходиться по business-параметрам, сравнение профилей станет нечистым.

Следствие:

- различия между профилями должны быть минимальными и intentional;
- документация должна явно фиксировать, что меняется только runtime profile.

## Business value

После реализации команда получает:

- безопасный способ локально проверить AIRiskOps с `RocksDB` без ломки текущего MVP path;
- воспроизводимый переключаемый runtime profile для сравнения backend behaviour;
- основу для следующего этапа stateful evolution без хаотичных ad-hoc правок;
- более честную инженерную подготовку к future production-like state workload.

## Решение, зафиксированное этой спецификацией

Для реализации принимается следующее решение:

- использовать один общий `submit-job.sh` с параметром выбора config file;
- добавить отдельный YAML `local-rocksdb.yaml`;
- инкапсулировать backend runtime settings в отдельном Java config/applicator слое;
- добавлять отдельный startup profile только если без него нельзя корректно поддержать filesystem paths;
- не менять default business/runtime path молча.
