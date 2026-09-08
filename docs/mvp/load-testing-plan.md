# AIRiskOps Flink MVP: план нагрузочного тестирования

Дата актуальности: 2026-09-05

## Назначение

Этот документ фиксирует текущую практическую идею нагрузочного тестирования локального `AIRiskOps Flink MVP`.

Цель НТ для текущего этапа не в том, чтобы назвать абстрактный максимальный `RPS`, а в том, чтобы определить:

- при какой нагрузке локальный стенд перестает держать согласованный `NRTP` runtime contract;
- где именно начинается деградация: source, windowing, state/checkpointing, incident layer или observability;
- какие метрики надо сравнивать между baseline и stress-прогонами;
- как различать деградацию самой Flink job и деградацию локального генератора нагрузки.

## 1. Что именно тестируем

Текущий MVP состоит из таких runtime-веток:

- Kafka source для `agent-requests`, `agent-responses`, `guardrail-findings`;
- parse/validate boundary;
- side outputs для `invalid-events` и `late-events`;
- event-time window aggregates `1m` и `5m` по `GUARDRAIL_FINDING`;
- quality branch;
- incident branch с `keyBy(agentId + sessionId)` и session state;
- Kafka sinks для normalized, invalid, late, aggregates, quality и incidents;
- Prometheus/Grafana observability.

Значит НТ должно проверять не только throughput, но и:

- watermark progress;
- late-event pressure;
- checkpoint stability;
- state pressure;
- skew по session keys;
- задержку появления aggregate и incident outputs.

## 2. Ограничения локального MVP

Нужно честно фиксировать ограничения текущего стенда:

- single-node Docker environment;
- один `TaskManager` с `2` slots;
- один ноутбук как Kafka, Flink, Prometheus, Grafana и генератор нагрузки одновременно;
- Kafka topics локального MVP создаются с `3` partitions;
- live-нагрузка сейчас подается через `Python` generator и `kafka-console-producer`.

Практический вывод:

- при высоком `RPS` можно упереться не только в Flink job, но и в локальный способ генерации;
- поэтому результаты надо интерпретировать как поведение всего локального MVP-стенда;
- для первого этапа это допустимо, потому что цель MVP сейчас инженерно-прикладная, а не production-grade benchmark.

## 3. Главный критерий результата

Базовая итоговая метрика для сравнения прогонов:

- `max sustainable RPS`

Под ней понимается максимальный `RPS`, при котором в течение согласованного окна наблюдения, например `10` минут:

- watermark продолжает двигаться;
- `Completed Checkpoints` монотонно растет;
- `Failed Checkpoints` не начинают системно расти;
- `Backpressured Time` не становится устойчиво высоким на критическом hot path;
- доля `late-events` не выходит за согласованный operational threshold;
- `guardrail-aggregates` и `basic-incidents` продолжают публиковаться без явного зависания.

## 4. Сценарии прогона

### 4.1 Baseline ramp

Цель:

- найти первую точку деградации на чистом сценарии без chaos.

Рекомендуемая сетка:

- `10 RPS`
- `20 RPS`
- `40 RPS`
- `60 RPS`
- `80 RPS`
- `120 RPS`

Каждую ступень желательно держать `5-10` минут.

Фактически выполненная exploratory short-run сетка расширена до `200`, `400`
и `600 RPS` для DEFAULT profile и до `50`, `100`, `200`, `400`, `600 RPS` для
RocksDB. Эти 60-секундные результаты приведены в разделе 9; они уточняют
границы для следующего длительного теста, но не заменяют каноничный ramp.

Команда:

```bash
bash tools/scripts/run-live-generator.sh \
  --scenario mixed \
  --mode baseline \
  --duration-seconds 600 \
  --requests-per-second 20
```

Каноничный wrapper для первого прогона:

```bash
bash tools/scripts/run-nt-baseline.sh
```

Wrapper сохраняет per-run Markdown report, raw metrics JSON и generator log в
`runtime/load-tests/`. Отчёт привязан к точному Flink `job_id` и содержит
generator summary, E2E, busy/backpressure, JVM heap/CPU, checkpoints,
watermark, Kafka lag в трёх точках (`generator end`, `after settle`,
`after recovery`), уменьшение lag и catch-up rate для обоих интервалов и всего
периода, а также команды просмотра Docker logs. Пики runtime-метрик считаются
по всему интервалу теста и recovery, а checkpoint failures -- как дельта
счётчика данной job за этот интервал.

### 4.2 Burst resilience

Цель:

- проверить устойчивость к всплескам, а не только к ровному потоку.

Команда:

```bash
bash tools/scripts/run-live-generator.sh \
  --scenario prompt_injection_burst \
  --mode baseline \
  --duration-seconds 900 \
  --min-requests-per-second 20 \
  --max-requests-per-second 80
```

### 4.3 Disorder and late stress

Цель:

- проверить деградацию event-time path при росте late и too-late traffic.

Команда:

```bash
bash tools/scripts/run-live-generator.sh \
  --scenario mixed \
  --mode late-events \
  --duration-seconds 600 \
  --requests-per-second 30 \
  --late-share 0.20 \
  --too-late-share 0.10
```

### 4.4 State and skew stress

Цель:

- проверить incident branch под hot keys и неравномерной session-нагрузкой.

Команда:

```bash
bash tools/scripts/run-live-generator.sh \
  --scenario attack \
  --mode baseline \
  --duration-seconds 600 \
  --requests-per-second 30 \
  --sessions 1
```

### 4.5 Runtime profile comparison

Цель:

- сравнить heap/default backend и `RocksDB` profile на одинаковой нагрузке.

Сравнивать:

- `config/job/local-job.yaml`
- `config/job/local-rocksdb.yaml`

Что анализировать:

- checkpoint duration;
- backpressure;
- state pressure;
- e2e latency для aggregate и incident outputs.

## 5. Метрики анализа

### 5.1 Runtime health

- `Running Jobs`
- `Records In Per Task`
- `Records Out Per Task`
- `Busy Time`
- `Backpressured Time`
- `Idle Time`
- `Current Input Watermark By Task`
- `Completed Checkpoints`
- `Failed Checkpoints`
- `Last Checkpoint Duration`
- `Mailbox Latency Samples By Task`

### 5.2 Domain health

- `valid_events_total`
- `invalid_events_total`
- `on_time_events_total`
- `late_events_total`
- `aggregates_emitted_total`
- `incidents_emitted_total`
- `open_sessions`
- `last_avg_detector_latency_ms`
- `last_max_detector_latency_ms`
- `last_detector_error_rate`

### 5.3 E2E latency

С 2026-09-04 в MVP добавлены первые runtime-метрики end-to-end latency без изменения Kafka output contracts.

Для aggregate layer:

- `last_e2e_latest_event_to_emit_ms`
  - сколько прошло между самым поздним `eventTime` внутри aggregate и фактическим processing-time emission этого aggregate;
- `last_e2e_window_end_to_emit_ms`
  - сколько прошло между `windowEndMillis` и фактическим emission aggregate.

Текущий Prometheus scope для этих aggregate latency gauge:

- `flink_taskmanager_job_task_operator_airiskops_quality_window_guardrail_last_e2e_latest_event_to_emit_ms`
- `flink_taskmanager_job_task_operator_airiskops_quality_window_guardrail_last_e2e_window_end_to_emit_ms`

Для incident layer:

- `last_e2e_latest_event_to_emit_ms`
  - сколько прошло между последним `eventTime` в текущем session snapshot и emission incident.

Что важно:

- это runtime approximation, а не абсолютная бизнес-истина;
- метрика считает `processing_time_now - domain event time`;
- она уже полезна для baseline/stress-сравнений и для поиска точки деградации hot path;
- позже можно отдельно добавить более строгий e2e benchmark по Kafka ingest timestamp или внешнему probe.

## 6. Как интерпретировать деградацию

MVP можно считать деградировавшим, если выполняется хотя бы одно из условий:

- watermark перестает двигаться или уходит в устойчивое отставание;
- `Records In` растет, а `Records Out` на критических операторах перестает расти;
- `Failed Checkpoints` начинают расти;
- `Last Checkpoint Duration` приближается к `checkpointInterval`;
- `late_events_total` резко растет в baseline-сценарии;
- e2e latency по aggregate или incident layer переходит из кратковременных пиков в устойчивый рост.

## 7. Минимальная матрица первого цикла НТ

Для первого цикла достаточно такой матрицы:

1. Baseline `20 RPS`, `10m`, default state backend.
2. Baseline `40 RPS`, `10m`, default state backend.
3. Burst `20..80 RPS`, `15m`, default state backend.
4. Late stress `30 RPS`, `10m`, default state backend.
5. Hot session skew `30 RPS`, `10m`, default state backend.
6. Повтор пункта `2` и `5` на `RocksDB` profile.

После результатов short-run baseline приоритет следующего цикла уточнён:

1. DEFAULT `400 RPS`, `10m`, затем измерение полного catch-up.
2. RocksDB `50 RPS`, `10m`, затем измерение полного catch-up.
3. Только после этих выдержек продолжать burst, late-event и hot-session skew.

## 8. Шаблон фиксации результатов

Для каждого прогона удобно фиксировать хотя бы такие поля:

| Date | Profile | Scenario | Mode | RPS | Duration | Watermark OK | Checkpoints OK | Backpressure | Aggregate E2E ms | Incident E2E ms | Late share | Notes |
|---|---|---|---|---:|---:|---|---|---|---:|---:|---:|---|
| 2026-09-04 | default | mixed | baseline | 20 | 600 | yes | yes | low | 0-5000 | 0-3000 | <1% | baseline stable |

Где:

- `Watermark OK`
  - watermark двигается без устойчивого зависания;
- `Checkpoints OK`
  - completed checkpoints растут, failed checkpoints не накапливаются;
- `Backpressure`
  - краткая оценка `low`, `medium`, `high`;
- `Aggregate E2E ms`
  - ориентир по `last_e2e_latest_event_to_emit_ms` и `last_e2e_window_end_to_emit_ms`;
- `Incident E2E ms`
  - ориентир по `last_e2e_latest_event_to_emit_ms` incident layer;
- `Late share`
  - доля late events относительно valid/on-time потока для данного сценария.

## 9. Результаты short-run baseline

### 9.1 Условия измерения

Все результаты ниже получены 2026-09-04--2026-09-05 на локальном Docker
стенде: один TaskManager, `2` Flink task slots, `8` доступных vCPU без Docker
CPU quota, Kafka с `3` partitions на topic. Сценарий: `mixed`, `baseline`,
`12` sessions, seed `42`, длительность каждой ступени `60` секунд.

Серии `200/400/600 RPS` для каждого профиля начинались с очищенных input
topics (`agent-requests`, `agent-responses`, `guardrail-findings`) и новой
Flink job; внутри серии ступени шли последовательно. RocksDB `50/100 RPS`
были отдельной свежей серией, также с чистыми input topics. Output topics и
policy topics не очищались. Значения `Busy`, `BP`, heap, CPU и checkpoint --
максимумы в 90-секундном окне, снятом после прогона. Kafka lag снят отдельным
snapshot consumer group `airiskops-mvp` после той же ступени.

Это capacity smoke, а не доказательство `max sustainable RPS`: для последнего
нужна выдержка 5--10 минут и проверка восстановления backlog.

### 9.2 DEFAULT state backend

Профиль: `config/job/local-job.yaml`. Секция `runtimeState` отсутствует,
поэтому используется Flink `DEFAULT` (in-memory/HashMap) state backend.

| RPS | Requests / findings | Aggregate E2E event / window | Busy / BP | Heap max | JVM CPU peak / eq. vCPU | Checkpoint max / failed | Watermark lag | Kafka lag |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 200 | 12k / 48k | 15.0 / 15.0 s | 256 / 0 ms/s | 394.6 MiB | 40.5% / 3.24 | 81 ms / 0 | 34.9 s | 0 |
| 400 | 24k / 96k | 8.4 / 8.3 s | 1 / 0 ms/s | 443.1 MiB | 2.7% / 0.22 | 122 ms / 0 | 154.0 s | 0 |
| 600 | 36k / 144k | 22.8 / 22.7 s | 765 / 725 ms/s | 463.3 MiB | 20.1% / 1.61 | 1.70 s / 0 | 8.7 s | 101,370 |

### 9.3 RocksDB state backend

Профиль: `config/job/local-rocksdb.yaml`; `runtimeState.backendType=rocksdb`,
incremental checkpoints включены. Применение подтверждено runtime metric
`state_backend_code=1`.

| RPS | Requests / findings | Aggregate E2E event / window | Busy / BP | Heap max | JVM CPU peak / eq. vCPU | Checkpoint max / failed | Watermark lag | Kafka lag |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 | 3k / 12k | 26.9 / 26.8 s | 1000 / 0 ms/s | 400.7 MiB | 33.4% / 2.67 | 261 ms / 0 | 42.5 s | 0 |
| 100 | 6k / 24k | 22.0 / 22.0 s | 527 / 59 ms/s | 370.2 MiB | 25.5% / 2.04 | 226 ms / 0 | 36.6 s | 4,802 |
| 200 | 12k / 48k | 18.9 / 18.8 s | 893 / 648 ms/s | 446.7 MiB | 53.6% / 4.28 | 990 ms / 1 | 33.5 s | 9,880 |
| 400 | 24k / 96k | 56.7 / 56.7 s | 1000 / 886 ms/s | 438.8 MiB | 26.1% / 2.09 | 1.51 s / 1 | 24.4 s | 75,908 |
| 600 | 36k / 144k | 115.9 / 102.7 s | 1000 / 956 ms/s | 442.4 MiB | 27.2% / 2.18 | 2.18 s / 1 | 24.2 s | 265,364 |

### 9.4 Сравнение и детальный анализ

На этом конкретном локальном стенде DEFAULT state backend заметно устойчивее
RocksDB в коротком `mixed` baseline:

- DEFAULT разобрал `200` и `400 RPS` без Kafka lag и backpressure. На `600 RPS`
  появились `101,370` необработанных сообщений, `725 ms/s` backpressure и
  checkpoint `1.70 s`; эта ступень неустойчива.
- RocksDB при `50 RPS` не оставил Kafka lag, но один hot task достиг
  `1000 ms/s` busy time. На `100 RPS` уже появились backpressure и остаточный
  lag `4,802`; это первая предварительная точка деградации RocksDB профиля.
- На RocksDB `200 RPS` уже есть failed checkpoint, `648 ms/s` backpressure и
  `9,880` сообщений lag. На `400` и `600 RPS` job насыщена: busy time равен
  `1000 ms/s`, backpressure достигает `886--956 ms/s`, а lag растет до
  `75,908` и `265,364` соответственно.
- Рост E2E для RocksDB на `400/600 RPS` до `56.7/115.9 s` согласуется с
  накоплением backlog. Для DEFAULT E2E на `600 RPS` остается около `22.8 s`,
  но Kafka lag уже доказывает, что минутного интервала недостаточно для
  устойчивой обработки.
- JVM CPU peak не растет монотонно с RPS и не является главным признаком
  насыщения: `Busy`, `BP` и Kafka lag фиксируют время ожидания downstream и
  state/checkpoint path, которое не обязано потреблять CPU. Низкий JVM CPU
  одновременно с высоким backpressure указывает на blocking/I/O либо
  scheduling bottleneck, но не доказывает конкретную причину.
- Результат не означает, что RocksDB всегда хуже. В этом стенде checkpoint
  storage и RocksDB local dir расположены на локальной файловой системе,
  параллелизм ограничен двумя slots, а Kafka, Flink и генератор делят один
  хост. Для production сравнение требует отдельного измерения disk I/O,
  RocksDB native memory/compaction, CPU time и state size при realistic
  parallelism.

Практический вывод: для текущего локального MVP short-run нижняя граница
DEFAULT профиля -- `400 RPS`, а RocksDB требует проверки уже на `50 RPS`.
Это не `max sustainable RPS`; ближайший обязательный тест -- отдельные
`10m` прогоны DEFAULT `400 RPS` и RocksDB `50 RPS` с измерением времени
полного восстановления Kafka lag после остановки генератора.

### 9.5 Расшифровка метрик таблиц

| Метрика | Единица и источник | Как интерпретировать |
|---|---|---|
| `Aggregate E2E event / window` | секунды; `last_e2e_latest_event_to_emit_ms` / `last_e2e_window_end_to_emit_ms` | Время от последнего event time или конца event-time окна до emission aggregate. Включает ожидание watermark, поэтому это не только CPU latency. |
| `Busy` | ms/s; Flink `busyTimeMsPerSecond` | Максимальная полезная занятость task. `1000 ms/s` означает, что хотя бы один task был занят весь наблюдаемый интервал. |
| `BP` | ms/s; Flink `backPressuredTimeMsPerSecond` | Максимальное ожидание downstream. Устойчиво высокий BP -- признак того, что hot path не успевает передавать записи. |
| `Heap max` | MiB; `Status.JVM.Memory.Heap.Used` | Максимально занятый Java heap TaskManager. Не включает RocksDB native/off-heap memory; JVM heap limit равен `512 MiB`. |
| `JVM CPU peak / eq. vCPU` | % и vCPU; `Status.JVM.CPU.Load` | Пиковая загрузка JVM; эквивалент vCPU = `CPU load x 8` доступных vCPU. Это не affinity и не число физических ядер, занятых job. |
| `Checkpoint max / failed` | ms / count; JobManager checkpoint metrics | Максимальная длительность completed checkpoint в snapshot и накопленный счетчик failed checkpoints. Рост duration к checkpoint interval или увеличение failed count означает риск потери recoverability. |
| `Watermark lag` | секунды; `now - max(currentInputWatermark)` | Ориентир по event-time progress. После завершения потока idle partitions могут исказить значение; Kafka lag остается более надежной метрикой фактического backlog. |
| `Kafka lag` | сообщений; `kafka-consumer-groups --describe` | Сумма `LOG-END-OFFSET - CURRENT-OFFSET` по трем input topics. Ненулевой lag после завершения генератора означает, что pipeline не успел обработать ступень. |

## 10. Business value

После такого НТ команда получает:

- фактическую границу устойчивости локального MVP;
- понимание, где first bottleneck находится именно сейчас;
- базовую методику сравнения `default` и `RocksDB`;
- наблюдаемую метрику деградации не только по throughput, но и по latency;
- основу для следующего шага, если понадобится выделенный load harness или более строгий benchmark.
