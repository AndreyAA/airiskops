# Локальный Walkthrough: как руками проверить AISafetyOps Flink MVP

Дата актуальности: 2026-08-27

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
cd /home/bob/old_bob/IdeaProjects/flink
```

Если хотите начать с полностью чистого состояния:

```bash
bash scripts/cleanup-local.sh
```

Что делает cleanup:

- останавливает и удаляет локальные контейнеры;
- удаляет Docker volumes и сеть проекта;
- очищает локальные replay-файлы;
- очищает runtime policy snapshots;
- удаляет собранный `target/`.

## Шаг 1. Поднять локальный контур

```bash
bash scripts/start-local.sh
```

Что делает:

- поднимает `kafka`;
- поднимает `jobmanager`;
- поднимает `taskmanager`;
- поднимает `prometheus`.
- поднимает `grafana`.

Что проверить:

```bash
docker compose ps
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
./scripts/init-topics.sh
```

Что создаётся:

- `agent-requests`
- `agent-responses`
- `guardrail-findings`
- `normalized-events`
- `invalid-events`
- `late-events`
- `guardrail-aggregates`

Проверка:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

## Шаг 3. Собрать job

```bash
bash scripts/build-job.sh
```

Что делает:

- запускает Maven;
- прогоняет тесты;
- собирает shaded jar.

Что должно появиться:

- `target/flink-aisafetyops-1.0.0-SNAPSHOT-all.jar`

Проверка:

```bash
ls -l target
```

Важно:

- `target/` не хранится в git;
- после cleanup его нужно собрать заново.

## Шаг 4. Отправить job в Flink

```bash
bash scripts/submit-job.sh
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
- sink в `guardrail-aggregates`.

Что это означает:

- основная ветка отвечает за intake и нормализацию;
- отдельная ветка отвечает за risk visibility по сработкам гардрейлов.

## Шаг 6. Загрузить replay dataset

```bash
bash scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
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
bash scripts/check-output-topics.sh
```

Что показывает скрипт:

- offsets по output topics;
- sample из `normalized-events`;
- sample из `guardrail-aggregates`.

Как интерпретировать:

- если `normalized-events` не пустой, intake/validation/main flow работает;
- если `guardrail-aggregates` не пустой, Stage 2 оконной агрегации работает.

## Шаг 8. Посмотреть Kafka output вручную

### Нормализованные события

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic normalized-events \
  --partition 0 \
  --offset 0 \
  --max-messages 5
```

### Агрегаты гардрейлов

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
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
- `minDetectorLatencyMs`
- `avgDetectorLatencyMs`
- `maxDetectorLatencyMs`
- `detectorErrorCount`

## Шаг 9. Как читать бизнес-смысл агрегатов

Примеры интерпретации:

- `PROMPT_INJECTION`, `windowName=1m`, `triggeredCount=7`
  - за минуту агент дал 7 сработок по инъекциям;
- `TOXICITY`, `avgConfidence=0.81`
  - средняя уверенность детектора токсичности высокая;
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

## Шаг 13. Важное замечание про окно 5m

При replay:

```bash
bash scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
```

event-time диапазон составляет примерно 4 минуты.

Следствие:

- окно `1m` закрывается и публикует aggregates;
- окно `5m` может ещё не закрыться;
- отсутствие output для `5m` в этом сценарии является ожидаемым поведением.

Чтобы увидеть `5m` aggregates:

```bash
bash scripts/run-replay.sh --scenario mixed --requests 180 --sessions 12 --agent-id agent-risk-01
```

## Шаг 14. Если что-то не работает

### Если job не появилась в UI

Смотрите:

```bash
docker compose logs --tail=200 jobmanager
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
docker compose logs --tail=200 taskmanager
docker compose logs --tail=200 jobmanager
```

### Если нужен чистый повторный прогон

Запускайте:

```bash
bash scripts/cleanup-local.sh
bash scripts/start-local.sh
./scripts/init-topics.sh
bash scripts/build-job.sh
bash scripts/submit-job.sh
bash scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
```

## Шаг 15. Короткий happy path

Если нужен минимальный сценарий ручной проверки:

```bash
bash scripts/cleanup-local.sh
bash scripts/start-local.sh
./scripts/init-topics.sh
bash scripts/build-job.sh
bash scripts/submit-job.sh
bash scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01
bash scripts/check-output-topics.sh
```

## Что вы получаете после этого walkthrough

После ручного прогона вы сможете сами увидеть:

- как Flink job стартует и исполняется;
- как данные проходят по Kafka topics;
- как выглядят нормализованные события;
- как выглядят оконные агрегаты по сработкам гардрейлов;
- где смотреть состояние pipeline;
- как повторно привести стенд к чистому начальному состоянию.
