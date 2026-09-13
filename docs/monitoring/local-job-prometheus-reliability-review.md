# Анализ надёжности `local-job` и Prometheus

Дата актуальности: 2026-09-13

## Назначение

Документ фиксирует текущее состояние локального профиля
[`config/job/local-job.yaml`](../../config/job/local-job.yaml), его влияние на
надёжность Flink job и ограничения наблюдаемости через Prometheus/Grafana.
Профиль предназначен для local MVP, demo и нагрузочных прогонов, а не для
production deployment.

## Краткий вывод

Профиль пригоден для функциональных E2E и baseline НТ, но не является
production-like профилем надёжности. Основные ограничения:

- не задан `runtimeState`, поэтому используется default state backend;
- checkpointing включён, но storage и recovery policy не заданы явно;
- Kafka работает в single-node режиме с одной replica;
- `AT_LEAST_ONCE` допускает дубликаты после recovery;
- Prometheus не имеет постоянного storage и alerting rules;
- часть dashboard использует нормализованный `job_name`, а checkpoint exporter
  публикует имя job с пробелами;
- JVM CPU load не показывает фактическое потребление host/container CPU в ядрах.

## Надёжность `local-job`

### State backend и checkpoints

В профиле отсутствует секция `runtimeState`. В результате применяется default
конфигурация `RuntimeStateConfig.defaults()`: state backend не переводится на
RocksDB, incremental checkpoints выключены, а внешний checkpoint storage явно не
настраивается.

При этом job вызывает `enableCheckpointing` с интервалом 30 секунд. Интервал
определяет частоту попыток checkpoint, но сам по себе не гарантирует сохранность
state или успешный recovery.

Для reliability-прогона следует использовать
[`config/job/local-rocksdb.yaml`](../../config/job/local-rocksdb.yaml) либо явно
добавить в профиль:

```yaml
runtimeState:
  backendType: rocksdb
  incrementalCheckpointsEnabled: true
  checkpointsDir: file:///opt/flink/state/checkpoints
  savepointsDir: file:///opt/flink/state/savepoints
  rocksdbLocalDir: /opt/flink/state/rocksdb
```

До production-like тестов также нужно явно определить checkpoint timeout,
минимальный интервал между checkpoint, допустимое число failed checkpoints,
restart strategy и политику externalized checkpoints.

### Kafka и delivery semantics

`deliveryGuarantee: AT_LEAST_ONCE` обеспечивает повторную доставку после
recovery, но допускает дубликаты в выходных Kafka topics. Поэтому в НТ нужно
разделять:

- число обработанных records;
- число уникальных бизнес-событий;
- число повторно опубликованных records.

Локальный Kafka в
[`deployment/local/docker-compose.yml`](../../deployment/local/docker-compose.yml)
работает с одним broker, одной replica и `min ISR = 1`. Это упрощает запуск, но
не даёт HA и не позволяет оценивать отказоустойчивость Kafka.

### Replay и изоляция прогонов

`groupId: airiskops-mvp` вместе с `startFromEarliest: true` удобны для replay,
но могут смешать новый прогон с retained messages предыдущих прогонов. Это
искажает Kafka lag, throughput и end-to-end latency.

Для чистого НТ необходимо использовать уникальную consumer group на каждый
прогон либо очищать входные topics перед стартом. В отчёте нужно фиксировать
начальный backlog и отличать его от lag, созданного текущей нагрузкой.

### Event time и late events

Текущие значения:

| Параметр | Значение | Влияние |
|---|---:|---|
| `outOfOrdernessSeconds` | 30 | Допуск нарушения порядка событий до продвижения watermark |
| `idleTimeoutMinutes` | 1 | Время, после которого idle partition перестаёт удерживать global watermark |
| `lateToleranceMinutes` | 5 | Период приёма late events после watermark |
| `autoWatermarkIntervalSeconds` | 5 | Частота эмиссии watermark |

Настройки подходят для MVP, но задержка бизнес-результата может включать
длительность окна, 30 секунд disorder budget, late tolerance, backpressure и
планирование Flink. Поэтому в отчёте нужно отдельно показывать:

- `latest_event_to_emit`;
- `window_end_to_emit`;
- watermark lag;
- Kafka consumer lag.

### Similarity branch и policy

В `local-job` similarity включён, но provider имеет значение `deterministic`.
Это не требует загрузки ONNX artifact и хорошо подходит для воспроизводимых
локальных прогонов, однако branch добавляет state и вычислительную нагрузку.

`policyRequireBootstrap: false` позволяет job стартовать без bootstrap policy.
Для контролируемого reliability-теста рекомендуется включить `true`, чтобы
ошибка отсутствующего policy завершала запуск, а не меняла semantics теста
незаметно.

## Prometheus и Grafana

### Текущая схема scrape

[`observability/prometheus/prometheus.yml`](../../observability/prometheus/prometheus.yml)
собирает metrics с трёх targets:

| Target | Port | Содержание |
|---|---:|---|
| Flink JobManager | `9249` | Lifecycle job и coordinator-level metrics |
| Flink TaskManager | `9250` | Throughput, busy time, backpressure, idle time и operator metrics |
| Checkpoint exporter | `9261` | Checkpoint/recovery metrics через Flink REST API |

`scrape_interval: 5s` подходит для локального НТ и позволяет видеть деградацию
почти в реальном времени. Для длительного или масштабного стенда этот интервал
нужно согласовывать с числом targets, series и нагрузкой самого Prometheus.

### Ограничения текущей реализации

1. **История metrics эфемерна.** У Prometheus нет постоянного volume для
   `/prometheus`, поэтому пересоздание container удаляет историю. Для сравнения
   прогонов нужен persistent volume или внешний Prometheus.

2. **Нет alerting rules.** Сейчас Prometheus только собирает и хранит metrics.
   Не формируются автоматические alerts на target down, failed checkpoints,
   рост lag, устойчивый backpressure или отсутствие job.

3. **Разный формат `job_name`.** Flink Prometheus reporter использует
   нормализованное имя `AIRiskOps_MVP_Increment_1`, а checkpoint exporter может
   публиковать `AIRiskOps MVP Increment 1`. Из-за этого dashboard-запросы могут
   находить Flink metrics, но не находить metrics exporter.

4. **Exporter может выбрать неправильную job.** Если job с заданным именем не
   найдена, exporter выбирает первую RUNNING job. При нескольких job checkpoint
   metrics могут относиться к другому pipeline.

5. **CPU оценивается неполно.**
   `flink_taskmanager_Status_JVM_CPU_Load` показывает нагрузку JVM, но не даёт
   точного ответа, сколько host/container CPU cores потребляет job. Для этого
   нужны container metrics, например cAdvisor
   `container_cpu_usage_seconds_total`, CPU limits и число доступных cores.

## Рекомендуемые доработки

### Приоритет P1

- унифицировать `job_name` в Flink metrics, exporter и Grafana;
- убрать fallback exporter на первую RUNNING job и возвращать `job_present = 0`,
  если target job не найдена;
- добавить постоянное хранилище Prometheus для сравнительных НТ;
- явно задать checkpoint storage и recovery policy для reliability-профиля;
- добавить recording/alerting rules для failed checkpoints, lag,
  backpressure, target availability и отсутствия job.

### Приоритет P2

- добавить cAdvisor или другой источник container CPU/memory metrics;
- разделить в отчёте initial backlog и lag текущего прогона;
- добавить метрику количества уникальных событий и контроль дубликатов;
- явно фиксировать и проверять bootstrap policy перед прогоном;
- добавить dashboards для container CPU, heap, RSS, GC и disk pressure RocksDB.

## Минимальный критерий готовности Prometheus

Перед reliability НТ должно быть подтверждено:

```promql
up{job="flink-jobmanager"} == 1
up{job="flink-taskmanager"} == 1
up{job="airiskops-checkpoint-exporter"} == 1
```

Кроме этого, для текущего `job_id` должны возвращаться значения для:

- `flink_taskmanager_job_task_busyTimeMsPerSecond`;
- `flink_taskmanager_job_task_backPressuredTimeMsPerSecond`;
- `flink_taskmanager_job_task_currentInputWatermark`;
- `flink_jobmanager_job_lastCheckpointDuration`;
- `flink_jobmanager_job_numberOfFailedCheckpoints`;
- `airiskops_flink_checkpoint_count`.

Если хотя бы один target недоступен или metric отсутствует, результат прогона
нельзя считать полноценным reliability verdict.
