# Анализ high-load режима: 50K RPS

Дата актуальности: 2026-09-13

## Назначение и границы

Документ фиксирует инженерный анализ нагрузки порядка `50 000 RPS` для текущего
AIRiskOps Flink MVP. Под `RPS` здесь понимается поток входных запросов; если
один запрос порождает request, response и guardrail finding, фактический
throughput Kafka будет выше.

Текущий local MVP нельзя использовать как production benchmark: в нём один
Kafka broker, три partitions и один TaskManager с двумя slots. Результат такого
прогона характеризует весь локальный стенд, а не предельную производительность
Flink-кластера.

## Главный вывод

При 50K RPS bottleneck возникает не только в вычислении окон. Нужно оценивать
всю цепочку:

```text
Kafka -> source -> parse/validate -> event-time -> keyed state -> windows
      -> incident/similarity -> sinks -> observability
```

Переход на RocksDB может уменьшить heap pressure, но одновременно увеличить
latency доступа к state. Если оператор после перехода обрабатывает меньше
events/sec, возникает следующая цепочка:

```text
RocksDB latency
-> operator не успевает обрабатывать вход
-> backpressure
-> растёт Kafka lag и время обработки backlog
-> state и окна живут дольше
-> растёт heap/native/disk pressure
-> возможен OOM или падение контейнера
```

Поэтому RocksDB не является автоматическим решением для 50K RPS. Сначала нужно
определить, является ли bottleneck CPU, state, checkpointing, I/O или skew.

## Что становится критичным

### Kafka partitions и throughput

Текущие три partitions не подходят для 50K RPS как capacity profile:

- source parallelism ограничен числом partitions;
- одна hot partition может ограничить весь pipeline;
- request/response/findings должны распределяться предсказуемо;
- один broker становится одновременно bottleneck сети, диска и producer I/O.

Нужно подобрать число partitions измерением throughput одной partition, а не
только пропорционально RPS. При выборе key необходимо сохранить нужную
корреляцию событий и одновременно избежать hot keys.

Если средний размер события равен `2 KB`, то только входной поток 50K RPS
составляет около `100 MB/s` без учёта replication, headers и выходных topics.
Если один запрос даёт несколько Kafka records, рассчитывать нужно полный
record rate и полный byte rate.

### Flink parallelism и slots

Двух TaskManager slots недостаточно для meaningful high-load теста. Нужно
настраивать и проверять parallelism для source, parsing, aggregation, incident
branch, embedding и sinks отдельно.

Недостаточно увеличить только global parallelism. Stateful operators должны
масштабироваться вместе с `keyBy`, иначе появляются:

- перегруженные hot subtasks;
- растущий backpressure на одном operator;
- uneven checkpoint duration;
- простаивающие slots рядом с перегруженными.

Для каждого оператора нужно измерить:

```text
capacity_per_subtask = устойчивый records/sec одной subtask
```

Минимальный parallelism оценивается как `target rate / capacity per subtask`,
но в production-like профиле необходим запас на skew, checkpointing и recovery.

## Окна и state при 50K RPS

### Текущая конфигурация

В `local-job` настроены tumbling event-time окна `1m` и `5m`,
`outOfOrdernessSeconds: 30` и `lateToleranceMinutes: 5`.

При 50K RPS это означает ориентировочно:

- до 3 млн входных событий за минуту;
- до 15 млн событий за пять минут;
- возможное удержание состояния 5-минутного окна плюс период late tolerance;
- дополнительное обновление уже выпущенных окон при late events.

Количество событий не обязательно равно размеру state: текущая агрегация
использует incremental accumulator. Но state создаётся для каждой комбинации:

```text
agentId × guardrailName × guardrailVersion × policyVersion × modelName × window
```

При высокой cardinality размер state становится сопоставимым не с числом
агентов, а с числом активных key/window combinations.

### Как выбирать окно

Окно нужно выбирать по business latency и state budget, а не только по удобному
календарному периоду. Рекомендуемая схема:

1. короткое operational window `10-30s` для быстрой реакции;
2. `1m` как основной near-real-time aggregate;
3. `5m` только для правил, которым действительно нужна такая корреляция;
4. долгую историю хранить downstream, а не в Flink state.

Нельзя без измерений увеличивать окно до 10-15 минут. Это одновременно
увеличивает активный state, checkpoint size, recovery work и задержку результата.

### Что нужно ограничивать

- число активных keys;
- session TTL;
- `lateTolerance`;
- размер accumulator;
- число request IDs и evidence samples;
- число similarity clusters;
- размер одного state value.

Не следует хранить в state полную историю событий окна или публиковать
`requestId`/`sessionId` как Prometheus labels.

## RocksDB: выигрыш и риск

### Что RocksDB решает

RocksDB уменьшает зависимость state от JVM heap, снижает риск больших Java object
graphs и позволяет использовать incremental checkpoints. Это полезно, когда
heap backend упирается в GC, heap size или полный checkpoint большого state.

### Что RocksDB ухудшает

Доступ к state становится дороже из-за serialization, JNI, native memory и
возможного disk I/O. При высокой частоте random state updates это может снизить
records/sec одной subtask.

RocksDB также расходует не только heap:

- native memory;
- block cache и write buffers;
- локальный disk;
- CPU на compaction;
- memory на Flink network buffers и serialization.

Поэтому возможен не только JVM `OutOfMemoryError`, но и container OOM по RSS,
native memory или превышению memory limit.

### Когда RocksDB ухудшит ситуацию

Если причина деградации находится в CPU или тяжёлом операторе, переход на RocksDB
может уменьшить throughput и усилить backpressure. Backlog при этом не обязан
полностью находиться в heap Flink: он остаётся в Kafka, но увеличиваются время
жизни state, очереди, in-flight buffers и количество временных объектов.

Решение должно приниматься по измерениям:

- records/sec per subtask;
- p99 operator latency;
- busy/backpressured/idle time;
- Kafka lag;
- active keys/windows;
- JVM heap и GC pause;
- process RSS и native memory;
- RocksDB compaction и disk I/O;
- checkpoint duration и checkpoint size;
- restore duration.

## Similarity branch

Similarity branch особенно чувствительна к high-load:

- embedding выполняется на hot path;
- vector dimension равна `384`;
- synchronous ONNX inference может ограничить throughput;
- поиск cluster выполняется линейно по активным clusters;
- state содержит clusters, request IDs, sessions и evidence samples.

Для 50K событий в секунду один проход по vector dimension 384 уже даёт около
19 млн операций умножения/сложения в секунду без учёта allocation, state access
и serialization. В worst case при линейном поиске стоимость приближается к:

```text
events × active_clusters × embedding_dimension
```

Рекомендуется:

- выполнять pre-filter до embedding;
- использовать batching;
- ограничить долю событий, попадающих в similarity branch;
- жёстко ограничить число clusters и размер samples;
- рассмотреть approximate nearest-neighbor index;
- вынести similarity в отдельный pipeline или capacity pool;
- измерять embedding latency отдельно от общей E2E latency.

ONNX inference внутри TaskManager нельзя считать подходящим для 50K RPS без
отдельного capacity-теста и оптимизации.

## Checkpointing и recovery

При интенсивном state churn условие надёжности выглядит так:

```text
checkpoint duration << checkpoint interval
```

Если duration приближается к `checkpointIntervalSeconds: 30`, checkpoint начинают
конкурировать с обработкой данных, растут backpressure и recovery risk.

Для high-load profile обязательны:

- RocksDB или другой подходящий state backend;
- incremental checkpoints;
- durable external checkpoint storage;
- быстрый local disk для RocksDB;
- явные timeout и restart strategy;
- контроль checkpoint alignment;
- контроль restore duration;
- capacity reserve для recovery, а не только для steady state.

Локальный `file:///opt/flink/state` подходит для MVP, но не для проверки
отказоустойчивости production-класса.

## Sinks и fan-out

Один входной event может породить несколько выходных records: normalized, late,
aggregate, quality и incident outputs. Поэтому capacity нужно считать по
выходному fan-out, а не только по input RPS.

Нужно контролировать:

- Kafka producer batching и compression;
- producer buffer memory и retries;
- размер incident payload;
- broker disk/network I/O;
- backpressure на каждом sink;
- дубликаты при `AT_LEAST_ONCE`.

## Prometheus и observability

Prometheus не должен находиться в hot path, но на 50K RPS нужно контролировать
cardinality и стоимость запросов. Metrics должны быть на уровне job/operator/task,
а не отдельного request.

Критичные сигналы:

- Kafka lag по partition;
- records in/out per second;
- busy/backpressured/idle time по task;
- watermark lag по subtask;
- p95/p99 E2E latency;
- checkpoint duration, size и failures;
- JVM heap, GC, process RSS и native memory;
- RocksDB compaction и disk usage;
- Prometheus scrape duration и target availability.

Запросы только через `max(...)` могут скрыть skew. Для анализа нужны разрезы по
`task_name`, `subtask` и operator, а high-cardinality identifiers должны оставаться
в Kafka/logs/storage.

`flink_taskmanager_Status_JVM_CPU_Load` не показывает число занятых host cores.
Для этого нужны container metrics, например cAdvisor
`container_cpu_usage_seconds_total`, CPU limits и количество доступных cores.

## Критерии успешного прогона

Прогон 50K RPS нельзя считать успешным только по факту отсутствия OOM. Должны
одновременно выполняться условия:

- Kafka lag не растёт монотонно;
- input и output throughput соответствуют ожидаемому fan-out;
- watermark продолжает двигаться;
- p99 E2E latency не растёт ступень за ступенью;
- нет устойчивого backpressure на source, state и sinks;
- checkpoint duration существенно меньше interval;
- failed checkpoints не растут;
- state size стабилизируется;
- нет перегруженной hot subtask;
- heap, native memory, RSS и disk ниже установленных лимитов;
- Prometheus targets доступны и metrics не пропадают;
- после recovery число уникальных событий корректно, а дубликаты ожидаемы и измерены.

## Последовательность high-load проверки

1. Измерить throughput одной Kafka partition и одной Flink subtask.
2. Проверить source/parse/sink без similarity branch.
3. Проверить default backend на малом state только для сравнения latency.
4. Проверить RocksDB с тем же workload и полным memory profile.
5. Увеличивать parallelism и partitions ступенчато.
6. Добавить окна `1m` и `5m`, измеряя state cardinality и checkpoint duration.
7. Отдельно включить similarity branch.
8. Провести recovery при steady-state нагрузке.
9. Проверить Prometheus/Grafana и полноту итогового отчёта.

Главный критерий capacity: не максимальный краткосрочный RPS, а максимальный
устойчивый RPS, при котором сохраняются latency SLO, контролируемый state,
завершающиеся checkpoints и отсутствие неограниченного backlog.
