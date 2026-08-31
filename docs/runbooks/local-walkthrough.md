# Локальный Walkthrough: как руками проверить AISafetyOps Flink MVP

Дата актуальности: 2026-08-31

Этот документ нужен для ручной проверки локального MVP на ноутбуке через Docker. Он описывает:

- что запускать;
- в каком порядке запускать;
- куда смотреть;
- как интерпретировать результат;
- как вернуть систему в чистое начальное состояние.

## Что поднимается локально

Локальный контур состоит из:

- `Kafka` как вход и выход для событий;
- `Flink JobManager` и `TaskManager`;
- `Prometheus` для метрик;
- `Grafana` с готовым dashboard для AISafetyOps;
- Java/Flink job, которая:
  - читает сырые события,
  - валидирует их,
  - маршрутизирует invalid и late события,
  - строит оконные агрегаты по `GUARDRAIL_FINDING`.

## Главная идея walkthrough

После прохождения сценария вы должны увидеть, что система уже умеет:

- принимать поток событий от LLM-агентов;
- обрабатывать сработки гардрейлов;
- публиковать нормализованные события в Kafka;
- публиковать оконные агрегаты в Kafka;
- отображать выполнение job в Flink UI;
- отдавать метрики в Prometheus.
- показывать operational dashboard в Grafana.

## Подготовка

Рабочая директория:

```bash
cd <repo-root>
```

Если хотите начать с полностью чистого состояния:

```bash
bash tools/scripts/cleanup-local.sh
```

Что делает cleanup:

- останавливает и удаляет локальные контейнеры;
- удаляет Docker volumes и сеть проекта;
- очищает локальные replay-файлы;
- очищает runtime policy snapshots;
- удаляет собранный `flink-job/target/`.

Это и есть основной полный reset script локального стенда.

Когда использовать именно его:

- нужно полностью пересоздать локальный Docker-контур;
- нужно убрать старые volumes/network state;
- изменился `deployment/local/docker-compose.yml`;
- нужно гарантированно пересобрать JAR и стартовать с нуля.

Если нужно очистить только Kafka topics, а контейнеры и runtime оставить, использовать не `cleanup-local.sh`, а:

```bash
bash tools/scripts/reset-topics.sh
```

Что делает `reset-topics.sh`:

- удаляет и пересоздает входные и выходные Kafka topics;
- не останавливает контейнеры;
- не чистит `runtime/replay/latest`;
- не удаляет `flink-job/target`.

## Шаг 1. Поднять локальный контур

```bash
bash tools/scripts/start-local.sh
```

Что делает:

- поднимает `kafka`;
- поднимает `jobmanager`;
- поднимает `taskmanager`;
- поднимает `prometheus`.
- поднимает `grafana`.

Что проверить:

```bash
docker compose -f deployment/local/docker-compose.yml ps
```

Что должно быть:

- все контейнеры в статусе `Up`.

Куда смотреть:

- Flink UI: `http://localhost:8081`
- Prometheus UI: `http://localhost:9090`
- Grafana UI: `http://localhost:3000`

Данные для входа в Grafana:

- login: `admin`
- password: `admin`

На этом этапе job может ещё отсутствовать, это нормально.

## Шаг 2. Создать Kafka topics

```bash
./tools/scripts/init-topics.sh
```

Что создаётся:

- `agent-requests`
- `agent-responses`
- `guardrail-findings`
- `normalized-events`
- `invalid-events`
- `late-events`
- `guardrail-aggregates`
- `basic-incidents`
- `guardrail-quality-metrics`
- `policy-updates`
- `debug-incidents`

Проверка:

```bash
docker compose -f deployment/local/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

## Шаг 3. Собрать job

```bash
bash tools/scripts/build-job.sh
```

Что делает:

- запускает Maven;
- прогоняет тесты;
- собирает shaded jar.

Что должно появиться:

- `flink-job/target/flink-aisafetyops-1.0.0-SNAPSHOT-all.jar`

Проверка:

```bash
ls -l flink-job/target
```

Важно:

- `flink-job/target/` не хранится в git;
- после cleanup его нужно собрать заново.

## Шаг 4. Отправить job в Flink

```bash
bash tools/scripts/submit-job.sh
```

Что делает:

- отправляет собранный jar в локальный Flink cluster.

Что увидеть:

- в консоли строку вида `Job has been submitted with JobID ...`

Где проверять:

- `http://localhost:8081`

Что должно появиться:

- job `AISafetyOps MVP Increment 1` в статусе `RUNNING`

## Шаг 5. Посмотреть topology в Flink UI

Откройте job в UI и посмотрите граф операторов.

Что вы увидите логически:

- source из Kafka;
- parse/validate;
- split valid/invalid;
- timestamps/watermarks;
- route late events;
- sink в `normalized-events`;
- sink в `late-events`;
- filter `GUARDRAIL_FINDING`;
- `Guardrail Aggregates 1m`;
- `Guardrail Aggregates 5m`;
- `Derive Guardrail Quality`;
- sink в `guardrail-quality-metrics`;
- `Kafka Policy Updates`;
- `Parse Policy Updates`;
- `Session Incident Evaluator`;
- sink в `guardrail-aggregates`;
- sink в `basic-incidents`.

Что это означает:

- основная ветка отвечает за intake и нормализацию;
- отдельная ветка отвечает за risk visibility по сработкам гардрейлов.

## Шаг 6. Загрузить replay dataset

```bash
bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
```

Что делает:

- генерирует тестовый набор событий;
- публикует его в Kafka;
- имитирует реальную нагрузку на локальном стенде.

Какие event types публикуются:

- `AGENT_REQUEST`
- `AGENT_RESPONSE`
- `GUARDRAIL_FINDING`

Какие гардрейлы покрываются:

- `PROMPT_INJECTION`
- `TOXICITY`
- `LOOPING`
- `SYSTEM_PROMPT_LEAKAGE`

## Шаг 7. Посмотреть результат быстрым smoke-скриптом

```bash
bash tools/scripts/check-output-topics.sh
```

Что показывает скрипт:

- offsets по output topics;
- sample из `normalized-events`;
- sample из `guardrail-aggregates`;
- sample из `basic-incidents`;
- sample из `guardrail-quality-metrics`.

Как интерпретировать:

- если `normalized-events` не пустой, intake/validation/main flow работает;
- если `guardrail-aggregates` не пустой, Stage 2 оконной агрегации работает;
- если `basic-incidents` не пустой, минимальный session correlation layer работает.
- если `guardrail-quality-metrics` не пустой, quality-ветка агрегатов работает.

## Шаг 8. Посмотреть Kafka output вручную

### Нормализованные события

```bash
docker compose -f deployment/local/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic normalized-events \
  --partition 0 \
  --offset 0 \
  --max-messages 5
```

### Агрегаты гардрейлов

```bash
docker compose -f deployment/local/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic guardrail-aggregates \
  --partition 0 \
  --offset 0 \
  --max-messages 10
```

На что смотреть в `guardrail-aggregates`:

- `agentId`
- `guardrailName`
- `windowName`
- `windowStartMillis`
- `windowEndMillis`
- `triggeredCount`
- `minConfidence`
- `avgConfidence`
- `maxConfidence`
- `p50Confidence`
- `p95Confidence`
- `triggeredP50Confidence`
- `triggeredP95Confidence`
- `minDetectorLatencyMs`
- `avgDetectorLatencyMs`
- `maxDetectorLatencyMs`
- `detectorErrorCount`

### Базовые incidents

```bash
docker compose -f deployment/local/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic basic-incidents \
  --partition 0 \
  --offset 0 \
  --max-messages 10
```

На что смотреть в `basic-incidents`:

- `incidentId`
- `agentId`
- `sessionId`
- `ruleName`
- `severity`
- `requestIds`
- `guardrailNames`
- `triggeredFindingsCount`
- `emissionRevision`
- `summary`

## Шаг 9. Как читать бизнес-смысл агрегатов

Примеры интерпретации:

- `PROMPT_INJECTION`, `windowName=1m`, `triggeredCount=7`
  - за минуту агент дал 7 сработок по инъекциям;
- `TOXICITY`, `avgConfidence=0.81`
  - средняя уверенность детектора токсичности высокая;
- `PROMPT_INJECTION`, `p95Confidence=0.97`
  - верхний хвост окна показывает очень сильные сигналы возможной инъекции;
- `TOXICITY`, `triggeredP50Confidence=0.88`
  - типичное уже сработавшее токсичное событие имеет высокий confidence;
- `LOOPING`, `triggeredCount=3`
  - зафиксированы 3 случая зацикливания;
- `SYSTEM_PROMPT_LEAKAGE`, `triggeredCount=1`
  - есть подтверждённый риск утечки системного промпта.

Для Operational Risk это уже даёт:

- основу для алертов;
- основу для дашбордов;
- основу для расследований по `agentId`.

## Шаг 10. Что смотреть в Flink UI после replay

Откройте job и проверьте:

- `Overview`
- `Vertices`
- `Exceptions`
- `Checkpoints`

На что смотреть:

- у source должны расти `read-records`;
- у `Guardrail Aggregates 1m` должны расти `read-records` и `write-records`;
- у `Serialize Guardrail Aggregates` должны расти входные записи;
- checkpoints должны быть `Completed`;
- в `Exceptions` не должно быть runtime failures.

## Шаг 11. Открыть Grafana и посмотреть готовый dashboard

Откройте:

- `http://localhost:3000`

Логин:

- `admin / admin`

Что должно быть:

- datasource `Prometheus` уже подключён автоматически;
- dashboard `AISafetyOps Flink Overview` уже загружен автоматически;
- dashboard `AISafetyOps Business Metrics` уже загружен автоматически;
- dashboard `AISafetyOps Capacity And Performance` уже загружен автоматически;
- dashboard `AISafetyOps Detector Quality` уже загружен автоматически;
- папка dashboard: `AISafetyOps`.

Что смотреть в первую очередь:

- `Running Jobs`
  - должен быть `1`, если job жива;
- `Completed Checkpoints`
  - должно расти со временем;
- `Last Checkpoint Duration`
  - показывает, не деградирует ли snapshot path;
- `Failed Checkpoints`
  - в норме `0`;
- `Records In Per Task`
  - показывает, какие task реально получают поток;
- `Records Out Per Task`
  - показывает, какие task реально отдают результат дальше;
- `Current Input Watermark By Task`
  - помогает понять, движется ли event time;
- `Guardrail Aggregate Emissions`
  - показывает, публикуются ли `1m` и `5m` aggregates;
- `AISafetyOps Domain Counters`
  - показывает `valid`, `invalid`, `late`, `on_time`.

Что означают `Findings` и `Emissions` на business dashboard:

- `Findings`
  - это число сырых событий `GUARDRAIL_FINDING`, учтённых business-метриками за последнюю минуту;
  - один `requestId` обычно порождает до 4 findings: prompt injection, toxicity, looping, system prompt leakage.
- `Emissions`
  - это число агрегатных сообщений, выпущенных оконным оператором в `guardrail-aggregates` за последнюю минуту наблюдения;
  - emission считается по факту публикации агрегата, а не по числу уникальных окон;
  - при late events одно и то же окно может эмититься повторно.

Как это интерпретировать вместе:

- `Findings` растут
  - значит детекторы и ingest работают;
- `Emissions` растут
  - значит окна закрываются и агрегаты реально публикуются;
- `Findings` есть, а `Emissions` нет
  - обычно надо смотреть watermarks, размер окна и allowed lateness;
- `Emissions` заметно меньше `Findings`
  - это нормально, потому что много raw findings схлопываются в один оконный агрегат.

Как интерпретировать:

- `Running Jobs = 0`
  - job не стартовала или упала;
- `Completed Checkpoints` не растёт
  - checkpointing завис или job не живёт достаточно долго;
- `Records In` есть, а `Records Out` нет
  - проблема локализуется на конкретном task/operator;
- watermark застыл
  - event-time окна могут не закрываться;
- `late` резко растёт
  - данные приходят слишком поздно или неверно публикуются;
- `invalid` растёт
  - upstream schema drift или ошибка нормализации;
- `Guardrail Aggregate Emissions` для `1m` растёт
  - Stage 2 реально формирует агрегаты.

## Шаг 11.0. Что показывает каждый dashboard Grafana

### `AISafetyOps Flink Overview`

Это runtime-dashboard. Он отвечает на вопрос, здорова ли Flink job как система обработки.

Панели:

- `Running Jobs`
  - число живых Flink jobs;
- `Completed Checkpoints`
  - сколько checkpoints завершилось успешно;
- `Last Checkpoint Duration`
  - сколько длился последний checkpoint;
- `Failed Checkpoints`
  - сколько checkpoints завершилось с ошибкой;
- `Records In Per Task`
  - входной throughput по task;
- `Records Out Per Task`
  - выходной throughput по task;
- `Mailbox Latency Samples By Task`
  - косвенный индикатор внутренней перегрузки task;
- `Current Input Watermark By Task`
  - прогресс event time;
- `Guardrail Aggregate Emissions`
  - сколько агрегатов уже опубликовано;
- `AISafetyOps Domain Counters`
  - сколько событий прошло как `valid`, `invalid`, `late`, `on_time`.

Когда смотреть:

- сразу после submit job;
- во время replay;
- во время live-генератора;
- при подозрении на остановку окон или деградацию processing.

### `AISafetyOps Business Metrics`

Это business-dashboard. Он отвечает на вопрос, какой risk-signal реально наблюдает AISafetyOps pipeline.

Панели:

- `1m Aggregate Emissions`
  - сколько агрегатных сообщений было выпущено downstream за последнюю минуту;
- `1m Triggered Findings`
  - сколько findings с `triggered=true` было учтено за последнюю минуту;
- `1m Detector Errors`
  - сколько detector errors было учтено за последнюю минуту;
- `1m Findings In Aggregates`
  - сколько сырых findings было учтено минутной business-метрикой за последнюю минуту;
- `Triggered Findings By Guardrail 1m`
  - triggered findings по каждому guardrail за последнюю минуту;
- `All Findings By Guardrail 1m`
  - все findings по каждому guardrail за последнюю минуту;
- `Triggered Share By Guardrail 1m`
  - доля triggered относительно всех findings за последнюю минуту;
- `Detector Errors By Guardrail 1m`
  - ошибки в разрезе guardrail-а за последнюю минуту;
- `Input Tokens By Guardrail 1m`
  - объём входных токенов по findings за последнюю минуту;
- `Output Tokens By Guardrail 1m`
  - объём выходных токенов по findings за последнюю минуту.
- `Last Emitted Confidence P50 By Guardrail Window`
  - показывает последнее эмитированное `p50Confidence` для `PROMPT_INJECTION` и `TOXICITY`;
  - даёт NRT-представление о типичном confidence по окнам `1m` и `5m`.
- `Last Emitted Confidence P95 By Guardrail Window`
  - показывает последнее эмитированное `p95Confidence`;
  - полезна для отслеживания сильного хвоста по confidence-based гардрейлам.
- `Last Emitted Triggered Confidence P50 By Guardrail Window`
  - показывает типичный confidence только среди уже triggered findings;
  - помогает оценивать качество текущих порогов.
- `Last Emitted Triggered Confidence P95 By Guardrail Window`
  - показывает верхний хвост уже сработавших findings;
  - особенно полезна на `attack` и `mixed` сценариях.

### `AISafetyOps Capacity And Performance`

Это dashboard для operational диагностики runtime contract и saturation.

Панели:

- `Runtime Contract Info`
  - показывает фактический window type, delivery guarantee и aggregate windows;
- `Out Of Orderness`
  - сколько reorder по event time job готова терпеть до продвижения watermark;
- `Late Tolerance`
  - сколько поздние события ещё могут обновлять уже закрытое окно;
- `Checkpoint Interval`
  - как часто выполняется checkpointing;
- `Auto Watermark Interval`
  - как часто runtime эмитит watermark ticks;
- `Configured Aggregate Windows`
  - какие окна реально активны, например `1m` и `5m`;
- `Open Incident Sessions`
  - сколько keyed session states сейчас живёт;
- `Records In/Out Per Second By Task`
  - где pipeline реально получает и выпускает поток;
- `Busy, Backpressured, Idle Time By Task`
  - где computation, downstream или source становятся ограничением;
- `Current Input Watermark By Task`
  - движется ли event time по каждой ветке job.

Когда смотреть:

- когда надо проверить, с каким именно контрактом запущена job;
- при replay `late-events` и `combined-chaos`;
- при локальной нагрузке, когда надо увидеть приближение к saturation;
- если `Business Metrics` выглядят странно и нужно отделить business effect от runtime issues.

### `AISafetyOps Detector Quality`

Это dashboard для качества самих guardrail detectors.

Что показывает:

- `detectorErrorRate`;
- `triggerRate`;
- `missingConfidenceRate`;
- `confidenceCoverageRate`;
- `avg/max detector latency`;
- разрез по `guardrail` и по окнам `1m` и `5m`.

Когда смотреть:

- при сценарии `detector-errors`;
- после обновления detector rules или parser logic;
- когда нужно отличить реальный всплеск атак от деградации телеметрии/детектора;
- при rollout новых версий guardrail-ов.

Когда смотреть:

- после появления первых aggregate emissions;
- при сравнении сценариев `normal`, `mixed`, `attack`;
- когда надо понять, какие именно guardrail-ы дают основной объём сработок;
- когда надо показать бизнесу, что поток не просто обрабатывается, а даёт осмысленные risk-метрики.

## Шаг 11.1. Запустить живой генератор для Grafana

Если хотите видеть, как панели меняются в реальном времени несколько минут подряд, используйте live-генератор:

```bash
bash tools/scripts/run-live-generator.sh
```

Поведение по умолчанию:

- публикует данные `300` секунд;
- шлёт `1` запрос в секунду;
- использует сценарий `mixed`;
- пишет в topics `agent-requests`, `agent-responses`, `guardrail-findings`.

Если нужна вариативная нагрузка, задайте диапазон:

```bash
bash tools/scripts/run-live-generator.sh \
  --duration-seconds 300 \
  --min-requests-per-second 1 \
  --max-requests-per-second 5 \
  --business-scenario mixed
```

Что это делает:

- каждую секунду выбирает новый RPS в диапазоне `1..5`;
- даёт более живую картину в Grafana;
- помогает проверить, как pipeline реагирует на плавающий входной поток.

Полезный вариант с чуть более заметной динамикой в Grafana:

```bash
bash tools/scripts/run-live-generator.sh \
  --duration-seconds 300 \
  --requests-per-second 3 \
  --business-scenario mixed \
  --delivery-mode baseline
```

Что вы увидите:

- рост `Records In` в Flink Overview;
- рост `Findings` на business dashboard;
- появление `Emissions` после накопления окна;
- обновление `Triggered Findings` по конкретным guardrail-ам.

Когда использовать live-генератор:

- для demo на живой системе;
- для ручной проверки panel refresh в Grafana;
- для smoke-проверки, что Kafka, Flink и Prometheus связаны корректно.

Полезные сценарные варианты:

```bash
bash tools/scripts/run-live-generator.sh \
  --duration-seconds 300 \
  --min-requests-per-second 2 \
  --max-requests-per-second 6 \
  --business-scenario prompt_injection_burst \
  --delivery-mode baseline \
  --burst-start-second 90 \
  --burst-duration-seconds 90 \
  --burst-multiplier 2.2
```

Ожидаемый эффект:

- на business dashboard растут p50/p95 confidence для `PROMPT_INJECTION`;
- увеличивается число triggered findings;
- при включённой incident policy появляются новые `basic-incidents`.

```bash
bash tools/scripts/run-live-generator.sh \
  --duration-seconds 300 \
  --requests-per-second 3 \
  --business-scenario mixed \
  --delivery-mode combined-chaos \
  --late-share 0.10 \
  --too-late-share 0.03 \
  --invalid-share 0.03 \
  --error-share 0.10
```

Ожидаемый эффект:

- часть payload попадёт в `invalid-events`;
- часть данных станет late/too-late;
- часть findings будет помечена как `detectorStatus=ERROR`;
- удобно для проверки, что мониторинг видит не только бизнес-аномалии, но и деградацию качества pipeline.

## Шаг 12. Запросы в Prometheus и что они означают

Откройте:

- `http://localhost:9090`

Ниже минимальный набор полезных запросов для ручной диагностики.

### 1. Есть ли живая job

```promql
flink_jobmanager_numRunningJobs
```

Что делает:

- показывает количество running jobs на локальном Flink.

Как читать:

- `1` — job запущена;
- `0` — job не запущена или упала.

### 2. Сколько checkpoint уже завершилось

```promql
flink_jobmanager_job_numberOfCompletedCheckpoints{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает число успешно завершённых checkpoint для нашей job.

Как читать:

- значение должно расти;
- если не растёт, checkpointing не работает как ожидается.

### 3. Есть ли failed checkpoints

```promql
flink_jobmanager_job_numberOfFailedCheckpoints{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает число неуспешных checkpoint.

Как читать:

- в норме `0`;
- рост означает проблемы со state backend, I/O, timer pressure или runtime.

### 4. Сколько длится последний checkpoint

```promql
flink_jobmanager_job_lastCheckpointDuration{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает длительность последнего completed checkpoint в миллисекундах.

Как читать:

- небольшие стабильные значения — норма;
- резкий рост — признак деградации storage или state.

### 5. Какие task получают данные

```promql
sum by (task_name) (
  rate(flink_taskmanager_job_task_numRecordsInPerSecond{job_name="AISafetyOps_MVP_Increment_1"}[1m])
)
```

Что делает:

- показывает входной throughput по task.

Как читать:

- видно, какие участки job реально получают поток;
- если source читает, а downstream пустой, проблема между ними.

### 6. Какие task отдают результат дальше

```promql
sum by (task_name) (
  rate(flink_taskmanager_job_task_numRecordsOutPerSecond{job_name="AISafetyOps_MVP_Increment_1"}[1m])
)
```

Что делает:

- показывает выходной throughput по task.

Как читать:

- помогает локализовать место, где поток перестал двигаться.

### 7. Движется ли watermark

```promql
flink_taskmanager_job_task_currentInputWatermark{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает текущий input watermark по task.

Как читать:

- watermark должен расти;
- если watermark стоит, event-time окна могут не закрываться.

### 8. Выпускаются ли оконные агрегаты

```promql
flink_taskmanager_job_task_operator_guardrail_aggregate_records_total_1m{job_name="AISafetyOps_MVP_Increment_1"}
```

```promql
flink_taskmanager_job_task_operator_guardrail_aggregate_records_total_5m{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает число эмитированных агрегатов по окнам `1m` и `5m`.

Как читать:

- `1m` должен расти уже на коротком replay;
- `5m` вырастет только если event-time диапазон действительно перекроет 5 минут.

### 9. Сколько событий прошло по бизнес-счётчикам

```promql
flink_taskmanager_job_task_operator_valid_events_total{job_name="AISafetyOps_MVP_Increment_1"}
```

```promql
flink_taskmanager_job_task_operator_invalid_events_total{job_name="AISafetyOps_MVP_Increment_1"}
```

```promql
flink_taskmanager_job_task_operator_late_events_total{job_name="AISafetyOps_MVP_Increment_1"}
```

```promql
flink_taskmanager_job_task_operator_on_time_events_total{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает доменные счётчики нашего MVP pipeline.

Как читать:

- `valid` растёт — поток успешно нормализуется;
- `invalid` растёт — проблемы со schema/валидацией;
- `late` растёт — события опаздывают относительно watermark;
- `on_time` растёт — основной NRTP path работает.

### 10. Есть ли перегрузка mailbox/task

```promql
flink_taskmanager_job_task_mailboxLatencyMs_count{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает накопление latency samples по mailbox на task.

Как читать:

- полезно как дополнительный индикатор активности task;
- в сочетании с throughput помогает понять, жив ли оператор и обрабатывает ли он события.

### 11. Живы ли Kafka producer paths

```promql
flink_taskmanager_job_task_operator_KafkaProducer_select_rate{job_name="AISafetyOps_MVP_Increment_1"}
```

Что делает:

- показывает активность Kafka producer внутри sink operator.

Как читать:

- ненулевое значение подтверждает, что sink path реально общается с Kafka.

### 12. Какой последний `p50Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_p50_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Что делает:

- показывает последнее эмитированное значение `p50Confidence` по окнам `1m` и `5m`.

Как читать:

- это типичный confidence на последнем агрегате, а не percentile по всей истории;
- `1m` полезен для быстрого NRT-сигнала;
- `5m` показывает более устойчивый фон.

### 13. Какой последний `p95Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_p95_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Что делает:

- показывает верхний хвост confidence distribution на последнем эмитированном окне.

Как читать:

- рост `p95Confidence` при стабильном `p50Confidence` означает усиление опасного хвоста;
- это особенно важно для `PROMPT_INJECTION`.

### 14. Какой последний `triggeredP50Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_triggered_p50_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Что делает:

- показывает типичный confidence только по findings с `triggered=true`.

Как читать:

- если `triggeredP50Confidence` близок к threshold, detector работает на границе;
- если метрика стабильно высокая, triggered-события действительно сильные.

### 15. Какой последний `triggeredP95Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_triggered_p95_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Что делает:

- показывает верхний хвост уже реально triggered findings.

Как читать:

- помогает быстро увидеть всплески очень уверенных инъекций или токсичности;
- особенно полезно на `attack` и `mixed` сценариях.

## Шаг 13. Важное замечание про окно 5m

При replay:

```bash
bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
```

event-time диапазон составляет примерно 4 минуты.

Следствие:

- окно `1m` закрывается и публикует aggregates;
- окно `5m` может ещё не закрыться;
- отсутствие output для `5m` в этом сценарии является ожидаемым поведением.

Чтобы увидеть `5m` aggregates:

```bash
bash tools/scripts/run-replay.sh --scenario mixed --requests 180 --sessions 12 --agent-id agent-risk-01
```

## Шаг 14. Если что-то не работает

### Если job не появилась в UI

Смотрите:

```bash
docker compose -f deployment/local/docker-compose.yml logs --tail=200 jobmanager
```

### Если job появилась, но Kafka output пустой

Проверяйте:

- source читает записи;
- aggregate vertex получает `read-records`;
- aggregate vertex пишет `write-records`;
- есть ли completed checkpoints;
- нет ли exceptions в UI.

### Если job упала

Смотрите:

```bash
docker compose -f deployment/local/docker-compose.yml logs --tail=200 taskmanager
docker compose -f deployment/local/docker-compose.yml logs --tail=200 jobmanager
```

### Если нужен чистый повторный прогон

Запускайте:

```bash
bash tools/scripts/cleanup-local.sh
bash tools/scripts/start-local.sh
./tools/scripts/init-topics.sh
bash tools/scripts/build-job.sh
bash tools/scripts/submit-job.sh
bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
```

## Шаг 15. Короткий happy path

Если нужен минимальный сценарий ручной проверки:

```bash
bash tools/scripts/cleanup-local.sh
bash tools/scripts/start-local.sh
./tools/scripts/init-topics.sh
bash tools/scripts/build-job.sh
bash tools/scripts/submit-job.sh
bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
bash tools/scripts/check-output-topics.sh
```

## Что вы получаете после этого walkthrough

После ручного прогона вы сможете сами увидеть:

- как Flink job стартует и исполняется;
- как данные проходят по Kafka topics;
- как выглядят нормализованные события;
- как выглядят оконные агрегаты по сработкам гардрейлов;
- где смотреть состояние pipeline;
- как повторно привести стенд к чистому начальному состоянию.
