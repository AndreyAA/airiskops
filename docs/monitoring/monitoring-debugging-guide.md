# Flink для AISafetyOps: мониторинг, наблюдаемость и отладка

## Глоссарий

- **Apache Flink** — distributed stream processing engine для потоковой и bounded-обработки данных в одном runtime.
- **Job** — логический пайплайн Flink, который собирается в коде и отправляется на выполнение в кластер.
- **JobManager** — координатор Flink job: планирование, recovery, checkpoint coordination, orchestration.
- **TaskManager** — worker-процесс Flink, который исполняет subtasks и хранит state.
- **Task** — исполняемая часть job graph на уровне runtime, обычно связанная с конкретным operator vertex.
- **Lag** — отставание обработки относительно источника или event time.
- **Slot** — единица выделения ресурсов внутри TaskManager, в которой может выполняться subtask.
- **Parallelism** — число параллельных экземпляров оператора или job.
- **Watermark lag** — разница между ожидаемым прогрессом event time и текущим watermark.
- **Checkpoint** — согласованный snapshot состояния job, нужный для fault tolerance и восстановления после сбоя.
- **Checkpoint barrier** — специальная служебная метка в потоке, по которой Flink координирует snapshot state между операторами.
- **Backpressure** — upstream не может быстро отдать данные, потому что downstream не успевает их потреблять.
- **Savepoint** — управляемый snapshot для ручных операций: обновлений, миграций, переноса и controlled restart.
- **State** — данные, которые оператор хранит между событиями: counters, session context, correlation buffers, dedup maps.
- **Keyed State** — state, изолированный по ключу после `keyBy`, например отдельно по `agentId` или `sessionId`.
- **Operator State** — state, принадлежащий экземпляру оператора, а не конкретному ключу.
- **State backend** — механизм хранения и восстановления state, например heap-based backend или RocksDB/ForSt-подобный backend.
- **Watermark** — оценка того, что события с timestamp меньше некоторой границы уже в основном поступили в систему.
- **Event Time** — обработка по бизнес-времени события, пришедшему из payload, а не по локальным часам машины.
- **Processing Time** — обработка по системному времени узла, на котором исполняется subtask.
- **Window** — группа событий, которую Flink агрегирует вместе по времени или по count-based правилам.
- **Allowed Lateness** — интервал, в течение которого опоздавшее событие всё ещё может обновить уже закрытое окно.
- **Operator chain** — несколько совместимых операторов, которые Flink исполняет в одном thread для снижения overhead.
- **Shuffle** — перераспределение данных между subtasks, например после `keyBy`, `rebalance` или `rescale`.
- **KeyBy** — логическое разбиение потока по ключу, после которого все события с одним ключом попадают в один logical state shard.
- **Busy time** — доля времени, когда subtask реально выполнял работу.
- **Idle time** — доля времени, когда subtask ждал входных данных.
- **Checkpoint alignment** — фаза согласования checkpoint barriers между потоками.
- **Checkpoint duration** — общее время выполнения snapshot.
- **Failed checkpoint** — checkpoint, который не завершился успешно.
- **Restart strategy** — политика перезапуска job после сбоя.
- **Dead letter** — поток проблемных событий, которые нельзя корректно обработать в основном pipeline.
- **Hot key** — ключ, на который попадает непропорционально большой объем событий.
- **Skew** — неравномерное распределение данных или работы по subtasks.
- **Subtask** — один параллельный экземпляр оператора.
- **History Server** — сервис Flink для просмотра завершенных job и их архивированной метаинформации.
- **Finding** — одно сырое событие срабатывания гардрейла по конкретному запросу или ответу агента.
- **Aggregate emission** — один выходной агрегат, который Flink опубликовал downstream по завершению или переиспуску окна.

## 1. Назначение документа

Этот файл дополняет основной manual и отвечает на три практических вопроса:

1. Что именно мониторить в Flink job под AISafetyOps.
2. Как понять, на каком этапе пайплайна проблема.
3. Как отлаживать ошибки и деградации без гадания по логам.

Фокус на кейсе:

- потоковые логи LLM-агентов;
- guardrail findings;
- stateful correlation;
- event-time windows;
- enrichment;
- incident routing;
- deployment в Kubernetes.

## 2. Основной принцип наблюдаемости

Flink нельзя эффективно мониторить только по одному сигналу.

Нормальная operational-модель всегда включает четыре уровня:

1. **Job-level health**
   - job running/failed/restarting;
   - end-to-end throughput;
   - restart count;
   - checkpoint success rate.
2. **Stage/operator-level health**
   - busy/idle/backpressure;
   - records in/out;
   - watermark progress;
   - state size;
   - async wait time.
3. **Infrastructure-level health**
   - CPU, memory, disk, network;
   - pod restarts;
   - object storage latency;
   - Kafka lag.
4. **Business-level health**
   - guardrail hit rate;
   - incident emission rate;
   - per-tenant volume;
   - доля invalid/late/dropped events.

Если мониторить только JVM/CPU, вы пропустите ошибки event-time. Если мониторить только guardrail hit rate, вы пропустите деградацию checkpointing. Если смотреть только на Flink UI, можно не заметить, что upstream source уже отстает на десятки минут.

## 3. Чем пользоваться в первую очередь

Базовый operational stack для Flink обычно такой:

- **Flink Web UI**
  - смотреть job graph;
  - backpressure;
  - metrics subtasks;
  - checkpoints;
  - exceptions.
- **Flink logs**
  - JobManager logs;
  - TaskManager logs;
  - operator exceptions;
  - serialization/classloading/state restore errors.
- **Metrics backend**
  - Prometheus/Grafana или совместимая система;
  - агрегаты по job/operator/subtask.
- **Kubernetes observability**
  - `kubectl logs`;
  - `kubectl describe pod`;
  - pod restarts;
  - resource throttling;
  - node pressure.
- **Source/sink observability**
  - Kafka consumer lag;
  - sink write latency;
  - error rate во внешних API;
  - object storage latency для checkpoints/savepoints.
- **History Server**
  - анализ завершенных и упавших job после остановки кластера.

Для локального single-node MVP добавить важное правило:

- для проверки Kafka sink сначала смотреть topic offsets и Flink metrics;
- `console-consumer` на локальном стенде может давать ложный `TimeoutException`, даже когда сообщения уже записаны в topic.

## 3.1 Локальная Grafana для AISafetyOps

В локальном контуре Grafana поднимается вместе с остальными сервисами.

URL:

- `http://localhost:3000`

Доступ:

- login: `admin`
- password: `admin`

Что уже настроено:

- datasource `Prometheus`;
- dashboard `AISafetyOps Flink Overview`;
- dashboard `AISafetyOps Business Metrics`;
- dashboard `AISafetyOps Capacity And Performance`;
- dashboard `AISafetyOps Detector Quality`;
- папка dashboard: `AISafetyOps`.

Что смотреть в dashboard в первую очередь:

- `Running Jobs`
  - есть ли живая Flink job;
- `Completed Checkpoints`
  - проходят ли checkpoints;
- `Last Checkpoint Duration`
  - не деградирует ли snapshot path;
- `Failed Checkpoints`
  - нет ли проблем со state/checkpointing;
- `Records In Per Task`
  - какие task реально получают поток;
- `Records Out Per Task`
  - какие task реально выпускают результат;
- `Current Input Watermark By Task`
  - движется ли event time;
- `Guardrail Aggregate Emissions`
  - публикуются ли `1m` и `5m` aggregates;
- `AISafetyOps Domain Counters`
  - растут ли `valid`, `invalid`, `late`, `on_time`.
- `Runtime Contract Info`
  - какой window type, delivery guarantee и набор aggregate windows реально активны.
- `Busy, Backpressured, Idle Time By Task`
  - видно ли, где pipeline уже упирается в вычисления или downstream.

Как читать `Emissions` и `Findings` в business/dashboard панелях:

- `Findings`
  - это число сырых событий `GUARDRAIL_FINDING`, которые pipeline получил и включил в оконную обработку;
  - одно пользовательское действие обычно даёт до 4 findings, потому что у нас 4 guardrail-а;
  - рост `Findings` означает, что входной поток есть и guardrail detectors реально производят результаты.
- `Emissions`
  - это число агрегатных записей, которые оконный оператор Flink выпустил в topic `guardrail-aggregates`;
  - emission не равен числу запросов и не равен числу уникальных окон;
  - одно окно может дать больше одного emission, если разрешены late events и окно переизлучается с обновлёнными значениями.

Практический смысл:

- `Findings` отвечают на вопрос: "сколько сырых guardrail-срабатываний мы наблюдаем";
- `Emissions` отвечают на вопрос: "сколько раз Flink уже пересчитал и опубликовал агрегированный результат";
- если `Findings` растут, а `Emissions` долго стоят на нуле, обычно проблема в watermark/window timing;
- если `Emissions` растут, а `Findings` нет, значит вы смотрите на догоняющие окна или late replay старых данных.

Практическая интерпретация:

- `Running Jobs = 0`
  - job не стартовала или уже упала;
- `Completed Checkpoints` не растёт
  - checkpointing не работает или job не выполняется стабильно;
- `Records In` растёт, `Records Out` не растёт
  - bottleneck или логическая проблема в конкретном task;
- watermark не движется
  - event-time окна не будут закрываться;
- `late` резко растёт
  - события приходят позже ожидаемого окна disorder/tolerance;
- `invalid` растёт
  - upstream schema drift, parse error или валидационный дефект;
- `Guardrail Aggregate Emissions` растёт только для `1m`
  - короткие окна уже закрываются, а `5m` ещё нет.

## 3.2 Что показывает каждый dashboard в Grafana

Ниже описание всех готовых dashboards и панелей, которые уже provisioned в локальном стенде.

### Dashboard `AISafetyOps Flink Overview`

Это operational dashboard для ответа на вопрос:

- жива ли Flink job;
- принимает ли она поток;
- движется ли event time;
- не деградирует ли runtime.

Панели:

- `Running Jobs`
  - показывает число job в статусе `RUNNING`;
  - для локального MVP обычно ожидается `1`;
  - `0` означает, что сначала надо смотреть Flink UI, submit и logs JobManager.
- `Completed Checkpoints`
  - показывает общее число успешно завершённых checkpoints;
  - число должно монотонно расти;
  - если метрика застыла, fault tolerance path не отрабатывает.
- `Last Checkpoint Duration`
  - показывает длительность последнего checkpoint в миллисекундах;
  - рост метрики часто означает проблемы со state backend, диском или network path.
- `Failed Checkpoints`
  - показывает число неуспешных checkpoints;
  - в здоровом локальном стенде должно быть `0`;
  - рост требует проверки logs, storage и timeout-настроек.
- `Records In Per Task`
  - показывает входной throughput по task;
  - нужен для локализации участка pipeline, который реально получает события;
  - если source растёт, а downstream task пустой, проблема между ними.
- `Records Out Per Task`
  - показывает выходной throughput по task;
  - помогает быстро увидеть bottleneck;
  - если `In` есть, а `Out` почти нет, оператор либо фильтрует всё, либо зависает, либо ошибается.
- `Mailbox Latency Samples By Task`
  - показывает samples внутренней latency исполнения task mailbox;
  - полезно как ранний сигнал перегрузки, GC pauses или тяжёлого пользовательского кода;
  - резкий рост при стабильном входном трафике требует проверки operator logic и ресурсов TaskManager.
- `Current Input Watermark By Task`
  - показывает текущий прогресс event time по task;
  - критична для окон и allowed lateness;
  - если watermark не движется, окна не будут эмитить результаты вовремя.
- `Guardrail Aggregate Emissions`
  - показывает, сколько агрегатов Flink уже выпустил по окнам `1m` и `5m`;
  - полезна для ответа на вопрос, закрываются ли окна вообще;
  - если `1m` растёт, а `5m` нет, это нормально на коротком горизонте наблюдения.
- `AISafetyOps Domain Counters`
  - показывает доменные counters по типам обработки: `valid`, `invalid`, `late`, `on_time`;
  - рост `invalid` означает проблемы со schema/validation;
  - рост `late` означает слишком поздние события или неверные ожидания по event time.

### Dashboard `AISafetyOps Business Metrics`

Это business/analytical dashboard для ответа на вопрос:

- сколько guardrail-событий реально приходит;
- сколько из них срабатывает;
- как это распределено по guardrail-ам;
- сколько токенов и ошибок связано с каждым guardrail-ом.

Панели:

- `1m Aggregate Emissions`
  - показывает количество emitted aggregates по минутному окну;
  - это не число raw events, а число опубликованных агрегатов;
  - полезно для проверки, что окно `1m` живое и downstream получает агрегаты.
- `1m Triggered Findings`
  - показывает количество findings за минутное окно, где `triggered=true`;
  - это основной бизнес-сигнал, сколько реальных сработок даёт pipeline;
  - рост во время `attack`-сценария ожидаем.
- `1m Detector Errors`
  - показывает количество ошибок detector processing, попавших в aggregate metrics;
  - в MVP обычно должно быть `0`;
  - любое ненулевое значение стоит расследовать как quality issue detector-а или нормализации.
- `1m Findings In Aggregates`
  - показывает общее число сырых findings, попавших в минутные агрегаты;
  - метрика отвечает на вопрос, сколько входного risk-signal реально было учтено окнами;
  - если findings есть во входе, а тут пусто, вероятна проблема с windowing/watermark.
- `Triggered Findings By Guardrail 1m`
  - показывает triggered findings в разрезе `PROMPT_INJECTION`, `TOXICITY`, `LOOPING`, `SYSTEM_PROMPT_LEAKAGE`;
  - удобна для сравнения профиля рисков между guardrail-ами;
  - если один guardrail резко доминирует, нужно проверить либо реальный инцидент, либо bias правил.
- `All Findings By Guardrail 1m`
  - показывает все findings по guardrail-ам, включая `triggered=false`;
  - помогает отделить объём анализа от объёма реальных срабатываний;
  - если общий поток высок, а triggered низкий, значит детектор видит много событий, но пороги отсекают большинство.
- `Triggered Share By Guardrail 1m`
  - показывает долю triggered findings относительно всех findings по каждому guardrail;
  - это proxy-метрика чувствительности guardrail-а;
  - слишком высокая доля может означать noisy detector или слишком низкий threshold.
- `Detector Errors By Guardrail 1m`
  - показывает ошибки по guardrail-ам в разрезе типов детектора;
  - помогает понять, ломается ли конкретный detector, а не весь pipeline;
  - особенно полезна после rollout новой версии правила или parser-а.
- `Input Tokens By Guardrail 1m`
  - показывает суммарный `inputTokens`, пришедший в findings данного guardrail-а;
  - полезна для оценки нагрузки detector-а и объёма анализируемого контента;
  - помогает замечать рост сложных или длинных пользовательских запросов.
- `Output Tokens By Guardrail 1m`
  - показывает суммарный `outputTokens`, связанных с findings данного guardrail-а;
  - нужна для оценки рисков на стороне ответа модели;
  - особенно полезна для анализа leakage и toxic response patterns.
- `Last Emitted Confidence P50 By Guardrail Window`
  - показывает последнее эмитированное `p50Confidence` по окнам `1m` и `5m` для `PROMPT_INJECTION` и `TOXICITY`;
  - это хороший индикатор типичной силы сигнала на последнем агрегате;
  - полезна для быстрой оценки того, смещается ли baseline confidence вверх.
- `Last Emitted Confidence P95 By Guardrail Window`
  - показывает последнее эмитированное `p95Confidence`;
  - помогает видеть верхний хвост confidence distribution;
  - особенно полезна при всплесках инъекций и токсичных взаимодействий.
- `Last Emitted Triggered Confidence P50 By Guardrail Window`
  - показывает типичный confidence только для findings с `triggered=true`;
  - удобна для контроля того, насколько уверенными остаются уже реальные сработки;
  - если метрика падает, threshold или detector quality стоит пересмотреть.
- `Last Emitted Triggered Confidence P95 By Guardrail Window`
  - показывает хвост самых сильных triggered findings;
  - помогает быстро замечать агрессивные пики по `PROMPT_INJECTION` и `TOXICITY`;
  - на коротком окне `1m` даёт быстрый operational signal.

Как использовать оба dashboard вместе:

- сначала открыть `AISafetyOps Flink Overview`
  - проверить, что job жива и метрики runtime выглядят здоровыми;
- затем открыть `AISafetyOps Business Metrics`
  - убедиться, что бизнес-сигналы действительно текут и агрегируются;
- если business dashboard пустой, а overview живой
  - обычно проблема в topic data, watermark, окнах или том, что вы смотрите слишком рано;
- если business dashboard растёт, а overview показывает failed checkpoints или latency spike
  - данные пока идут, но runtime уже деградирует и может скоро упасть.

### Dashboard `AISafetyOps Capacity And Performance`

Это dashboard для ответа на вопросы:

- какой runtime contract сейчас реально активен;
- где pipeline начинает тормозить;
- не приближаемся ли мы к saturation по task-level сигналам.

Панели:

- `Runtime Contract Info`
  - таблично показывает `window_type`, `delivery_guarantee`, `analysis_mode`, `aggregate_windows`;
  - нужна для быстрой проверки фактического runtime contract без чтения кода и YAML.
- `Out Of Orderness`
  - текущая настройка bounded disorder в секундах;
  - влияет на watermark progress и восприимчивость к reorder на входе.
- `Late Tolerance`
  - сколько секунд окно ещё принимает late events после nominal close;
  - влияет на повторные emissions и side output `late-events`.
- `Checkpoint Interval`
  - как часто job пытается делать snapshot state;
  - помогает быстро увидеть, с каким operational профилем сейчас идёт запуск.
- `Auto Watermark Interval`
  - как часто runtime эмитит watermark ticks;
  - влияет на реакцию event-time окон.
- `Configured Aggregate Windows`
  - таблица с реально активными окнами, например `1m` и `5m`;
  - если окно отсутствует здесь, downstream `No Data` надо искать в конфиге.
- `Open Incident Sessions`
  - текущий объём активного keyed session state в incident layer.
- `Last Checkpoint Duration`
  - быстрый индикатор здоровья snapshot path.
- `Failed Checkpoints`
  - показывает, не деградирует ли fault tolerance.
- `Records In Per Second By Task`
  - где реально есть входной throughput.
- `Records Out Per Second By Task`
  - где поток перестаёт выходить дальше по graph.
- `Busy, Backpressured, Idle Time By Task`
  - основной saturation-график:
  - `busy` показывает вычислительную загрузку;
  - `backpressured` показывает, что downstream не успевает;
  - `idle` показывает нехватку входного трафика.
- `Current Input Watermark By Task`
  - нужен для понимания, движется ли event time по веткам job.

### Dashboard `AISafetyOps Detector Quality`

Это dashboard для ответа на вопрос:

- растёт ли реальный risk signal;
- или деградирует качество самих guardrail detectors.

Панели:

- `1m Quality Emissions`
  - сколько quality snapshots было выпущено за минутное окно;
- `Max Detector Error Rate 1m`
  - максимальная доля detector errors по guardrail-ам в последнем 1m срезе;
- `Max Missing Confidence Rate 1m`
  - максимальная доля отсутствующего `confidence` для confidence-based guardrail-ов;
- `Max Detector Latency 1m`
  - верхний хвост detector latency по последнему минутному quality snapshot;
- `Last Detector Error Rate By Guardrail Window`
  - quality деградация по каждому guardrail и окну;
- `Last Trigger Rate By Guardrail Window`
  - помогает отличать всплеск атак от системной деградации;
- `Last Confidence Coverage Rate By Guardrail Window`
  - показывает полноту confidence telemetry;
- `Detector Latency By Guardrail Window`
  - средняя и максимальная latency детекторов по каждому окну.

Практический смысл:

- если `triggerRate` растёт, а `detectorErrorRate` и `missingConfidenceRate` низкие
  - вероятнее всего, вы видите реальный риск-сигнал;
- если одновременно растут `detectorErrorRate`, `missingConfidenceRate` или latency
  - сначала надо проверить качество detector-а и ingest path.

## 4. Что мониторить всегда

### 4.1 Job-level метрики

Минимальный набор:

- job state: `RUNNING`, `RESTARTING`, `FAILING`, `FAILED`;
- records in/out per second;
- uptime;
- restart count;
- last checkpoint status;
- number of failed checkpoints;
- end-to-end processing delay;
- watermark lag.

Что должно настораживать:

- job часто переходит в `RESTARTING`;
- throughput резко просел без снижения входного трафика;
- checkpoints стали длиннее или чаще падать;
- watermark перестал двигаться;
- incidents/output events внезапно стали почти нулевыми.

### 4.4 Минимальный набор Prometheus-запросов для AISafetyOps

Ниже приведён набор запросов, которые стоит запускать вручную даже при наличии Grafana.

#### Жива ли job

```promql
flink_jobmanager_numRunningJobs
```

Показывает:

- сколько Flink jobs сейчас находится в `RUNNING`.

Использование:

- `1` для локального MVP означает, что job работает;
- `0` означает, что сначала надо смотреть `submit`, `jobmanager logs` и `Flink UI`.

#### Сколько checkpoint завершилось

```promql
flink_jobmanager_job_numberOfCompletedCheckpoints{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- число успешно завершённых checkpoint для нашей job.

Использование:

- значение должно монотонно расти;
- если не растёт, state consistency path не работает как ожидалось.

#### Есть ли failed checkpoints

```promql
flink_jobmanager_job_numberOfFailedCheckpoints{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- число checkpoint, завершившихся ошибкой.

Использование:

- в норме `0`;
- рост часто указывает на storage, state pressure, timeout или runtime exception.

#### Сколько длился последний checkpoint

```promql
flink_jobmanager_job_lastCheckpointDuration{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- длительность последнего completed checkpoint в миллисекундах.

Использование:

- помогает рано заметить деградацию state backend или I/O.

#### Какие task получают входной поток

```promql
sum by (task_name) (
  rate(flink_taskmanager_job_task_numRecordsInPerSecond{job_name="AISafetyOps_MVP_Increment_1"}[1m])
)
```

Показывает:

- входной throughput по task.

Использование:

- помогает понять, где поток реально присутствует, а где уже нет.

#### Какие task отдают данные дальше

```promql
sum by (task_name) (
  rate(flink_taskmanager_job_task_numRecordsOutPerSecond{job_name="AISafetyOps_MVP_Increment_1"}[1m])
)
```

Показывает:

- выходной throughput по task.

Использование:

- если `In` есть, а `Out` нет, проблема локализуется на этом task.

#### Движется ли watermark

```promql
flink_taskmanager_job_task_currentInputWatermark{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- текущий input watermark по task.

Использование:

- если watermark застыл, event-time окна не будут эмитить агрегаты.

#### Какой runtime contract реально активен

```promql
flink_taskmanager_job_task_operator_aisafetyops_runtime_contract_info{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- служебную метрику со значением `1` и labels:
  - `window_type`
  - `delivery_guarantee`
  - `analysis_mode`
  - `aggregate_windows`

Использование:

- проверить, что job действительно поднята с ожидаемым runtime contract;
- быстро найти ситуацию, когда локально вы поменяли YAML, но работает старый submit/profile.

#### Какие окна реально настроены

```promql
flink_taskmanager_job_task_operator_aisafetyops_runtime_contract_window_size_seconds{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- активные окна и их размер в секундах через label `window`.

Использование:

- проверить, что ожидаемые `1m` и `5m` действительно существуют;
- если окно отсутствует, искать проблему в конфиге, а не в Grafana.

#### Какой bounded disorder и late tolerance сейчас активны

```promql
max(flink_taskmanager_job_task_operator_aisafetyops_runtime_contract_out_of_orderness_seconds{job_name="AISafetyOps_MVP_Increment_1"})
```

```promql
max(flink_taskmanager_job_task_operator_aisafetyops_runtime_contract_late_tolerance_seconds{job_name="AISafetyOps_MVP_Increment_1"})
```

Показывает:

- ключевые event-time параметры, влияющие на watermarking и late routing.

Использование:

- понять, почему late scenario попадает в окно или уходит в `late-events`;
- объяснить observed delay между поступлением finding и emission aggregate.

#### Публикуются ли оконные guardrail aggregates

```promql
flink_taskmanager_job_task_operator_guardrail_aggregate_records_total_1m{job_name="AISafetyOps_MVP_Increment_1"}
```

```promql
flink_taskmanager_job_task_operator_guardrail_aggregate_records_total_5m{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- число эмитированных aggregate-records по `1m` и `5m` окнам.

Использование:

- прямой operational сигнал для Stage 2;
- если `1m=0`, а `GUARDRAIL_FINDING` точно приходят, проблема в event-time, lateness или ветке aggregation.

#### Растут ли бизнес-счётчики pipeline

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

Показывает:

- доменные counters нашего пайплайна intake/validation/timeliness.

Использование:

- `valid` растёт, `on_time` растёт — основной path работает;
- `invalid` растёт — schema drift или parse/validation defect;
- `late` растёт — data ordering или event-time issue.

#### Жив ли Kafka sink path

```promql
flink_taskmanager_job_task_operator_KafkaProducer_select_rate{job_name="AISafetyOps_MVP_Increment_1"}
```

Показывает:

- активность Kafka producer внутри Flink sink.

Использование:

- полезно, когда надо отделить проблему логики operator от проблемы записи в Kafka.

#### Какой последний `p50Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_p50_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Показывает:

- последнее эмитированное значение `p50Confidence` по окнам `1m` и `5m`.

Использование:

- это типичный confidence на последнем агрегате, а не percentile по всей исторической выборке;
- полезно для быстрого контроля baseline по confidence-based гардрейлам.

#### Какой последний `p95Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_p95_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Показывает:

- верхний хвост confidence distribution на последнем эмитированном окне.

Использование:

- полезно для обнаружения усиления опасного хвоста даже при стабильном `p50Confidence`.

#### Какой последний `triggeredP50Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_triggered_p50_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Показывает:

- типичный confidence только по findings с `triggered=true`.

Использование:

- помогает оценивать, насколько уверенными остаются уже реальные сработки.

#### Какой последний `triggeredP95Confidence` по `PROMPT_INJECTION` и `TOXICITY`

```promql
max by (guardrail, window) (
  flink_taskmanager_job_task_operator_aisafetyops_window_guardrail_last_triggered_p95_confidence{
    job_name="AISafetyOps_MVP_Increment_1",
    guardrail=~"PROMPT_INJECTION|TOXICITY"
  }
)
```

Показывает:

- верхний хвост уже triggered findings.

Использование:

- особенно полезно на `attack` и `mixed` нагрузке, когда надо быстро увидеть очень сильные срабатывания.

### 4.2 Operator-level метрики

На уровне operator/subtask смотреть:

- `busyTimeMsPerSecond`;
- `idleTimeMsPerSecond`;
- `backPressureTimeMsPerSecond`;
- `numRecordsIn`;
- `numRecordsOut`;
- `currentInputWatermark`;
- `currentOutputWatermark`, если доступен;
- state size;
- async pending requests;
- garbage collection pauses;
- serialization/deserialization failures.

Интерпретация:

- высокий `busyTime`, низкий backpressure — оператор загружен compute work;
- высокий backpressure upstream — downstream узкое место;
- высокий idle time — нет входных данных или broken upstream;
- watermark стоит на месте — один из upstream partitions тормозит event time.

### 4.3 Infra-level метрики

Для Kubernetes и JVM:

- CPU saturation;
- memory usage;
- heap/off-heap pressure;
- container restarts;
- OOMKilled;
- disk IO;
- network retransmits;
- object storage request latency;
- Kafka broker/client errors.

Важно:

- Flink-проблема не всегда Flink-проблема;
- падение checkpoint throughput часто вызвано storage/network;
- idle operators иногда означают не "все хорошо", а "source не читает".

## 5. Как мониторить по этапам пайплайна

## 5.1 Этап Source / ingest

Что происходит:

- source читает данные из Kafka или файлов;
- разбивает их на partitions/splits;
- отдает в downstream operators.

Что мониторить:

- consumer lag по Kafka;
- records in per source subtask;
- idle time;
- source errors;
- rebalance/repartition behavior;
- file discovery lag для `FileSource`, если используется continuous reading.

Типовые симптомы:

- входной трафик есть, а source почти idle;
- один source subtask перегружен, остальные пустуют;
- lag растет, хотя downstream почти не занят;
- source падает по auth/network errors.

Как отлаживать:

1. Проверить внешний источник, а не только Flink UI.
2. Сравнить lag по partitions.
3. Проверить, сколько partitions реально читает job и соответствует ли это parallelism.
4. Проверить ошибки auth, TLS, ACL, network policy.
5. Проверить, не упирается ли source в slow deserialization.

Частые ошибки:

- неверные credentials или ACL;
- слишком маленький parallelism относительно числа partitions;
- heavy JSON parsing внутри source path;
- schema drift в входных сообщениях.

### 5.2 Этап Parse / schema validation / normalization

Что происходит:

- сырое сообщение превращается в доменный объект;
- валидируются обязательные поля;
- брак уходит в dead-letter или side output.

Что мониторить:

- parse error rate;
- invalid event rate;
- dead-letter throughput;
- долю событий без обязательных полей;
- latency на этапе нормализации.

Типовые симптомы:

- после релиза upstream schema incidents резко падают до нуля;
- invalid events внезапно растут;
- оператор normalization стал busy при прежнем трафике.

Как отлаживать:

1. Проверить sample raw messages.
2. Сравнить фактическую schema с ожидаемой.
3. Вытащить причины reject в структурированный error code, а не только exception text.
4. Проверить, не потерялись ли timestamps, `tenantId`, `requestId`, `sessionId`.
5. Убедиться, что side output для invalid data реально читается и наблюдается.

Практический совет:

- не ограничивайтесь `Exception in operator`; логируйте компактный доменный reason code.

### 5.3 Этап timestamp assignment и watermarking

Что происходит:

- событию назначается event timestamp;
- Flink вычисляет watermark progress.

Что мониторить:

- `currentInputWatermark`;
- watermark lag по source и downstream;
- долю late events;
- долю dropped-too-late events;
- idleness по partitions.

Типовые симптомы:

- окна не закрываются;
- окна закрываются слишком поздно;
- почти все события внезапно "late";
- у разных subtasks watermark сильно расходится.

Как отлаживать:

1. Проверить, что timestamp берется из доменного event, а не из processing time.
2. Проверить timezone/format parsing.
3. Проверить реальное распределение `ingest_time - event_time`.
4. Проверить, не висит ли один idle partition без `withIdleness(...)`.
5. Проверить, не отправляет ли upstream события с неправильными часами.

Ключевая метрика:

- `currentInputWatermark` — официальный и самый полезный индикатор event-time progress в task metrics.

### 5.4 Этап window aggregations

Что происходит:

- данные группируются по ключу и времени;
- рассчитываются счетчики, average, percentile-подобные метрики или агрегаты.

Что мониторить:

- records in/out;
- watermark progress;
- state size;
- window firing rate;
- late events rate;
- operator busy time.

Типовые симптомы:

- окно не срабатывает;
- окно выдает слишком мало данных;
- одно окно задерживает целый downstream;
- state size растет без ограничения.

Как отлаживать:

1. Проверить watermark и allowed lateness.
2. Проверить window assigner и keying.
3. Проверить, нет ли слишком крупных ключей.
4. Проверить, не делает ли window function тяжелые allocations.
5. Проверить, не складываете ли вы полный набор событий вместо incremental aggregate.

Частая ошибка:

- разработчик думает, что проблема в window, а реальная проблема в том, что watermark не продвигается.

### 5.5 Этап keyed state и correlation

Что происходит:

- оператор хранит историю по `sessionId`, `requestId` или другому ключу;
- принимает решения по предыдущим событиям;
- ставит timers.

Что мониторить:

- state size;
- number of registered timers;
- throughput на subtask;
- skew по ключам;
- restore time после restart;
- TTL behavior косвенно через state growth.

Типовые симптомы:

- один subtask перегружен сильнее остальных;
- state продолжает расти;
- restore после restart стал очень долгим;
- repeated duplicate alerts;
- suppress logic перестала работать.

Как отлаживать:

1. Проверить ключ: `sessionId`, `requestId`, `tenantId + agentId` и т.д.
2. Проверить наличие hot keys.
3. Проверить TTL и cleanup strategy.
4. Проверить, что timers удаляются или переиспользуются корректно.
5. Проверить логику dedup/suppression на replay.

Что обычно ломает keyed state:

- плохой выбор ключа;
- слишком длинный TTL;
- хранение больших payload'ов вместо compact state;
- несогласованность между `processElement()` и `onTimer()`.

### 5.6 Этап Async I/O и enrichment

Что происходит:

- job ходит в справочник, cache или policy service;
- ожидает async response;
- продолжает обработку.

Что мониторить:

- pending async requests;
- async timeout rate;
- async error rate;
- latency p50/p95/p99;
- retry volume;
- correlation между async latency и backpressure.

Типовые симптомы:

- throughput падает без роста CPU;
- backpressure начинает распространяться upstream;
- checkpoints деградируют;
- enrichment results приходят слишком поздно.

Как отлаживать:

1. Проверить внешний сервис отдельно от Flink.
2. Проверить concurrency limits.
3. Проверить timeout settings.
4. Проверить retry storm.
5. Проверить cache hit ratio, если есть cache.

Правило:

- если Async I/O стал bottleneck, лечить надо не только Flink-конфиг, а весь контракт с внешним сервисом.

### 5.7 Этап Broadcast State и dynamic rules

Что происходит:

- поток правил раздается всем subtasks;
- основной event stream использует эти правила при классификации.

Что мониторить:

- rate обновлений правил;
- размер broadcast state;
- время применения новых правил;
- mismatch между `policyVersion` во входном и выходном событии;
- число событий, обработанных fallback policy.

Типовые симптомы:

- часть инцидентов классифицируется по старой версии policy;
- после обновления правил output резко меняется;
- subtasks долго догоняют поток rules.

Как отлаживать:

1. Проверить ordering и versioning policy updates.
2. Проверить, что `policyVersion` проходит до sinks.
3. Проверить, нет ли oversized broadcast payload.
4. Проверить rollback scenario на replay.

### 5.8 Этап CEP и сложные паттерны

Что происходит:

- Flink ищет последовательности событий в пределах окна времени.

Что мониторить:

- throughput CEP operator;
- state size CEP;
- watermark lag;
- число matched patterns;
- число partially matched patterns;
- late event impact.

Типовые симптомы:

- ожидаемые паттерны не находятся;
- совпадений слишком много;
- CEP state быстро растет;
- после late events результат нестабилен.

Как отлаживать:

1. Проверить event order assumptions.
2. Проверить watermark strategy.
3. Проверить временные границы паттерна.
4. Проверить, не надо ли заменить CEP на более простой keyed-state алгоритм.

### 5.9 Этап Sink

Что происходит:

- результаты уходят в Kafka, lakehouse, SIEM, case-management или dead-letter storage.

Что мониторить:

- sink throughput;
- sink error rate;
- write latency;
- retries;
- transaction/commit failures;
- external system saturation.

Типовые симптомы:

- upstream backpressure нарастает от sink к source;
- incidents вычислены, но не доходят до внешней системы;
- checkpoints failing из-за sink commit path;
- дубли в downstream.

Как отлаживать:

1. Проверить внешнюю систему отдельно.
2. Проверить semantics sink: exactly-once, at-least-once, idempotent writes.
3. Проверить commit/flush behavior.
4. Проверить throttling, quotas и rate limits.
5. Проверить, не перепутаны ли временные технические сбои и логические дубли.

## 6. Как искать узкое место по Flink UI

Практический порядок анализа:

1. Открыть job graph.
2. Найти оператор с максимальным backpressure или busy time.
3. Сравнить metrics по subtasks, а не только aggregate.
4. Проверить `currentInputWatermark`.
5. Открыть checkpoints tab.
6. Проверить exceptions tab.
7. Сопоставить это с логами TaskManager и внешними системами.

Интерпретация цветов и метрик backpressure полезна, но не должна использоваться изолированно:

- `HIGH backpressure` на source почти всегда означает, что bottleneck downstream;
- `HIGH busy` на одном subtask и нормальные соседние subtasks обычно намекает на skew;
- высокий idle на всей ветке может означать не "система свободна", а "данные не приходят".

## 7. Как читать checkpoints

Что смотреть:

- интервал checkpointing;
- duration;
- end-to-end duration;
- alignment time;
- bytes persisted;
- failed vs completed checkpoints.

Типовые симптомы:

- duration растет от релиза к релизу;
- checkpoints периодически timeout;
- unaligned or aligned behavior меняется под нагрузкой;
- restore после падения занимает слишком долго.

Как отлаживать:

1. Проверить state growth.
2. Проверить object storage latency.
3. Проверить backpressure.
4. Проверить slow sink и async bottlenecks.
5. Проверить размер state у конкретных operators.

Если checkpointing деградирует, почти всегда страдает не только recovery, но и вся практическая устойчивость production job.

## 8. Как отлаживать падения и exceptions

### 8.1 Первый вопрос: это код, данные или инфраструктура

Большинство инцидентов укладываются в три класса:

1. **Code/logic issue**
   - null handling;
   - serialization issue;
   - wrong assumptions about event ordering;
   - state/timer bug.
2. **Data issue**
   - schema drift;
   - invalid payload;
   - corrupted timestamp;
   - missing key fields.
3. **Infra/dependency issue**
   - Kafka/network/auth failure;
   - object storage slowdown;
   - sink outage;
   - pod OOM/restart.

### 8.2 Минимальный порядок разбора exception

1. Найти failing operator name и subtask.
2. Определить, на каком типе входных данных он падает.
3. Проверить, повторяется ли падение после restart на том же месте.
4. Проверить state restore logs.
5. Проверить внешние зависимости этого оператора.

### 8.3 Что нужно обязательно логировать в коде

Для stateful AISafetyOps operators полезно логировать:

- operator name;
- tenantId;
- requestId;
- sessionId;
- policyVersion;
- compact error code;
- stage name;
- timestamp события;
- correlation id внешнего enrichment запроса.

Не надо:

- писать целиком системные промпты или пользовательские prompts в открытые логи;
- логировать полные PII payload'ы;
- превращать TaskManager logs в дамп всех событий.

## 9. Как отличать логическую ошибку от data skew

Признаки логической ошибки:

- job стабильно падает на одном классе данных;
- поведение не зависит от нагрузки;
- падение воспроизводится на replay;
- исключение связано с бизнес-логикой или сериализацией.

Признаки skew:

- job не падает, но один или несколько subtasks значительно тяжелее;
- высокий busy time только у части subtasks;
- один ключ или tenant доминирует по объему;
- задержка нарастает без явной exception.

Как проверять skew:

- выводить sampled distribution по ключам;
- сравнивать records in/out по subtasks;
- смотреть state size per subtask;
- сравнивать watermark progress на разных ветках.

## 10. Как отлаживать late data и неверную event-time логику

Симптомы:

- инциденты формируются позже ожидаемого;
- окна пустые или неполные;
- CEP не находит паттерны;
- часть событий систематически улетает в late side output.

Порядок разбора:

1. Проверить timestamp extraction.
2. Проверить фактический lag distribution по источникам.
3. Проверить `withIdleness`.
4. Проверить allowed lateness.
5. Проверить, нет ли у upstream некорректных часов или timezone shifts.

Очень частая ошибка:

- разработчик отлаживает windows, хотя реальная причина в неправильном `eventTime` поле или в том, что часть источников присылает локальное время без UTC нормализации.

## 11. Как отлаживать проблемы после релиза

После rollout нового job version проверяйте в первые часы:

- restart count;
- checkpoint duration;
- failed checkpoints;
- watermark movement;
- invalid/dead-letter rate;
- throughput per sink;
- incident volume against baseline;
- policyVersion distribution;
- state size growth.

Если после релиза business metrics резко изменились, а технические метрики нормальны, это часто означает:

- поломали бизнес-логику;
- поломали mapping severity;
- поменяли thresholds;
- изменили keying;
- сломали correlation.

Если технические метрики тоже деградировали, чаще причина в:

- более тяжелом state;
- новом внешнем enrichment;
- дополнительном shuffle;
- возросшем числе timers;
- sink bottleneck.

## 12. Recommended runbooks

### 12.1 Job stuck, но не падает

Проверить:

1. watermark progress;
2. backpressure;
3. busy/idle time;
4. checkpoint tab;
5. sink latency;
6. async pending requests.

### 12.2 Job часто рестартует

Проверить:

1. последние exceptions;
2. repeating failing operator;
3. state restore logs;
4. OOMKilled и pod events;
5. schema changes upstream;
6. recent policy/rules changes.

### 12.3 Инциденты перестали появляться

Проверить:

1. source records in;
2. invalid/dead-letter rate;
3. watermark progress;
4. policy updates;
5. sink delivery;
6. не ушел ли весь output в side outputs.

### 12.4 Checkpoints стали падать

Проверить:

1. state growth;
2. object storage health;
3. sink pressure;
4. backpressure;
5. recent code changes that increased state or timers.

## 13. Что закладывать в код заранее ради отладки

Чтобы job было легче сопровождать, полезно заранее сделать:

- явные имена operators;
- стабильные `uid()` для stateful stages;
- side outputs для invalid и late events;
- компактные structured error codes;
- operator-specific metrics;
- отдельные counters для dropped/suppressed events;
- traceable `policyVersion`;
- sampled diagnostic logging без утечки чувствительных данных.

Для AISafetyOps особенно полезны отдельные счетчики:

- `prompt_injection_hits_total`;
- `toxicity_hits_total`;
- `looping_hits_total`;
- `system_prompt_leakage_hits_total`;
- `incident_emitted_total`;
- `incident_suppressed_total`;
- `invalid_events_total`;
- `late_events_total`.

## 14. Практический чеклист дежурного инженера

Если пришел алерт по Flink job, порядок проверки такой:

1. Job state в Flink UI.
2. Последние exceptions.
3. Checkpoints status.
4. Backpressure view.
5. Watermark progress.
6. Records in/out на подозрительных operators.
7. TaskManager logs.
8. Kafka lag или status источника.
9. Sink/API/object storage status.
10. Изменения релиза, policy updates и конфигурации.

## 15. Краткий инженерный вывод

Мониторинг Flink надо строить не вокруг "жива ли JVM", а вокруг трех осей:

- движется ли data plane;
- движется ли event time;
- сохраняется ли state без деградации recovery.

Если упростить до одного правила:

**Любую проблему во Flink надо сначала локализовать по этапу пайплайна, и только потом лечить конфиг или код.**

## 16. Ссылки на документацию

Официальные источники, на которые стоит опираться:

- Flink Operations overview: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/
- Flink Metrics: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/
- Flink Monitoring Back Pressure: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/
- Flink Monitoring Checkpointing: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/checkpoint_monitoring/
- Flink Debugging Windows and Event Time: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/debugging/debugging_event_time/
- Flink Logging: https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/advanced/logging/
- Flink History Server: https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/advanced/historyserver/
- Flink REST API: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/
- Flink Checkpointing: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/
- Flink Task Failure Recovery: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/task_failure_recovery/
- Flink Kubernetes deployment: https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/native_kubernetes/
- Flink Kubernetes Operator docs: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/
