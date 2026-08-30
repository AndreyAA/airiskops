# AISafetyOps Flink MVP: ближайшие планы развития

Дата актуальности: 2026-08-30

## Назначение

Этот документ фиксирует ближайшие возможные улучшения текущего AISafetyOps Flink MVP с точки зрения:

- business value для подразделения операционных рисков;
- технической целесообразности;
- последовательности внедрения;
- готовности к переносу из локального Docker-контура в банковскую инфраструктуру.

Документ не заменяет [mvp-spec.md](/home/bob/old_bob/IdeaProjects/flink/docs/mvp/mvp-spec.md), а дополняет её как список следующих практических шагов после уже собранного MVP.

## 1. Текущая ценность проекта

На текущем этапе проект уже даёт измеримую пользу:

- принимает события LLM-агентов и guardrail detectors через Kafka;
- нормализует и валидирует поток;
- отделяет `invalid` и `late` события;
- строит NRT-агрегаты по окнам `1m` и `5m`;
- публикует результаты в Kafka;
- показывает runtime и business-метрики в Prometheus и Grafana;
- позволяет руками воспроизводить сценарии через replay и live generator.

Это уже достаточная основа для:

- демонстрации работоспособности пайплайна;
- проверки базового качества guardrail telemetry;
- обсуждения production rollout с platform и risk-командами;
- локальной инженерной разработки с контролируемым regression cycle.

## 2. Главный вывод по business value

Сейчас проект хорошо отвечает на вопрос:

- "идёт ли поток и что в нём происходит".

Следующий этап зрелости должен перевести систему к вопросам:

- "какие risk-cases реально требуют внимания";
- "насколько качественно работают сами guardrail-ы";
- "можно ли быстро расследовать и объяснить конкретный сигнал";
- "можно ли безопасно менять policy и detector rules без потери контроля".

Именно поэтому следующие улучшения ниже приоритизированы не по технической красоте, а по росту практической пользы для Operational Risk.

## 3. Приоритетный список ближайших улучшений

### 3.1 Добавить incident layer поверх aggregate layer

#### Что улучшить

Сейчас система хорошо показывает findings и aggregates, но для работы риск-команды этого недостаточно. Нужна сущность уровня `incident`, с которой можно работать как с операционным сигналом.

#### Что предлагается

- ввести новый доменный контракт `BasicIncident`;
- добавить отдельный output topic, например `basic-incidents`;
- собирать incident по `agentId + sessionId`;
- вычислять базовую `severity`;
- включать в incident:
  - `agentId`;
  - `sessionId`;
  - список связанных `requestId`;
  - типы guardrail-ов;
  - краткое explanation summary;
  - `policyVersion`;
  - `guardrailVersion`.

#### Business value

После этого этапа команда получает:

- не просто метрики, а операционную сущность для разбора;
- основу для последующей интеграции с case-management, SOC или risk tooling;
- быстрый переход от "видим всплеск" к "вот конкретная подозрительная сессия".

#### Почему это важно

Без incident layer Grafana и Kafka остаются в основном инженерной витриной. С incident layer система начинает говорить на языке риска и расследований.

### 3.2 Сделать policy management частью реального runtime path

#### Что улучшить

Сейчас `load-policies.sh` создаёт полезный локальный snapshot, но policy ещё не участвует в runtime как живой управляющий поток.

#### Что предлагается

- минимально:
  - явно читать active policy при старте job;
  - записывать её `policyVersion` во все derived outputs;
- следующим шагом:
  - завести `policy-updates` topic;
  - раздавать policy через broadcast state;
  - применять threshold и severity rules без изменения кода job.

#### Business value

После этого этапа команда получает:

- управляемое изменение порогов и policy rules;
- лучший контроль релизов guardrail-ов;
- меньше ручных redeploy-действий ради изменения логики риска;
- наблюдаемую связь между policy version и результатами пайплайна.

#### Почему это важно

Для подразделения, которое само разрабатывает guardrail-ы, управляемость правил почти так же важна, как и сами метрики.

### 3.3 Разделить observability по двум вопросам: risk signal и detector quality

#### Что улучшить

Сейчас часть метрик смешивает два разных смысла:

- что реально делают пользователи и агенты;
- насколько качественно работает сам guardrail detector.

#### Что предлагается

- задействовать `guardrail-quality-metrics` как реальный output;
- ввести отдельные quality-сигналы:
  - `invalid_rate`;
  - `late_rate`;
  - `detector_error_rate`;
  - `missing_confidence_rate`;
  - `trigger_rate`;
  - `version_skew`;
- сделать отдельный dashboard:
  - `AISafetyOps Detector Quality`.

#### Business value

После этого этапа команда получает:

- возможность отделить всплеск атаки от деградации detector-а;
- более честную картину качества собственных контролей;
- основу для rollout/shadow/quality review новых версий правил.

#### Почему это важно

Для вашей команды guardrail является не только инструментом наблюдения, но и самостоятельным объектом контроля качества.

### 3.4 Добавить session-oriented correlation, а не только window aggregates

#### Что улучшить

Окна `1m` и `5m` полезны для NRT-видимости, но operational risks часто проявляются как паттерн внутри пользовательской сессии, а не только как рост count внутри окна.

#### Что предлагается

- keyed state по `agentId + sessionId`;
- краткоживущий TTL state;
- накопление findings внутри сессии;
- простые correlation rules без тяжёлого CEP на первом шаге.

Примеры правил:

- несколько `PROMPT_INJECTION` подряд в одной сессии;
- сочетание `PROMPT_INJECTION` и `SYSTEM_PROMPT_LEAKAGE`;
- рост `TOXICITY` после серии безопасных ответов;
- повторяющийся `LOOPING` в рамках одной сессии.

#### Business value

После этого этапа команда получает:

- более реалистичные risk signals;
- лучшую explainability для расследований;
- шаг от "вот всплеск на дашборде" к "вот паттерн поведения в конкретной сессии".

#### Почему это важно

Operational risk почти всегда работает не только с isolated events, а с цепочками событий.

### 3.5 Усилить replay из технического smoke в бизнес-сценарный regression

#### Что улучшить

Сейчас replay уже полезен, но в первую очередь как инженерный инструмент. Следующая зрелость: превратить его в набор воспроизводимых бизнес-сценариев.

#### Что предлагается

- завести named scenarios:
  - `prompt_injection_burst`;
  - `toxicity_campaign`;
  - `looping_false_positive_check`;
  - `policy_regression_case`;
- для каждого сценария фиксировать expected outcomes:
  - expected findings;
  - expected incidents;
  - expected aggregates;
  - expected dashboard shifts.

#### Business value

После этого этапа команда получает:

- демонстрируемую воспроизводимость поведения системы;
- более сильную защиту от регрессий при изменении правил;
- понятный сценарный язык для бизнеса, QA и аудита.

#### Почему это важно

Для банка и риск-функции воспроизводимость и объяснимость обычно важнее, чем просто “оно работает”.

#### Что дополнительно нужно усилить

Replay должен моделировать не только разные бизнес-паттерны, но и разные режимы доставки и качества данных.

Нужны две отдельные оси сценариев:

- business scenario:
  - `normal`;
  - `attack`;
  - `mixed`;
  - `prompt_injection_burst`;
  - `toxicity_campaign`;
  - `looping_false_positive_check`;
- delivery and quality mode:
  - `baseline`;
  - `late-events`;
  - `invalid-events`;
  - `detector-errors`;
  - `combined-chaos`.

Это позволит запускать сценарии вида:

- обычный поток без аномалий;
- всплеск одного конкретного паттерна;
- нормальный бизнес-сценарий, но с загрязнением invalid events;
- attack-сценарий с late findings;
- mixed-сценарий с деградацией detector-а.

#### Что должно появиться в tooling

Нужно расширить:

- [generate_events.py](/home/bob/old_bob/IdeaProjects/flink/tools/generators/generate_events.py);
- [stream_live_events.py](/home/bob/old_bob/IdeaProjects/flink/tools/generators/stream_live_events.py);
- [run-replay.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/run-replay.sh);
- [run-live-generator.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/run-live-generator.sh).

Минимальный набор новых флагов:

- `--mode baseline|late-events|invalid-events|detector-errors|combined-chaos`;
- `--late-share`;
- `--too-late-share`;
- `--invalid-share`;
- `--error-share`;
- `--burst-start-second`;
- `--burst-duration-seconds`;
- `--burst-multiplier`;
- `--detector-latency-multiplier`;
- `--out-of-orderness-seconds`.

#### Что должны уметь сценарии

`prompt_injection_burst`

- на коротком интервале резко повышать долю и confidence `PROMPT_INJECTION`;
- давать заметный всплеск `triggeredP95Confidence`;
- быть хорошо видимым в `1m` окне.

`toxicity_campaign`

- повышать долю `TOXICITY`;
- давать sustained нагрузку в нескольких окнах подряд;
- быть хорошо видимым на `1m` и `5m`.

`late-events`

- часть finding-ов должна приходить в пределах bounded out-of-orderness;
- часть должна переэмитить окно в пределах allowed lateness;
- часть должна уходить в `late-events`.

`invalid-events`

- часть сообщений должна ломать обязательный контракт:
  - отсутствует `requestId`;
  - отсутствует `sessionId`;
  - отсутствует `guardrailName`;
  - отсутствует `confidence` для confidence-based guardrail;
  - отсутствует `triggered` для `GUARDRAIL_FINDING`.

`detector-errors`

- часть finding-ов должна иметь `detectorStatus != OK`;
- часть должна иметь повышенный `detectorLatencyMs`;
- сценарий должен быть виден на detector quality metrics.

#### Business value

После такого расширения replay команда получает:

- воспроизводимые негативные сценарии, а не только happy-path генератор;
- возможность руками показать, как система реагирует на burst, late, invalid и detector degradation;
- основу для реального regression-suite уровня риска и эксплуатации.

## 4. Улучшения второй очереди

### 4.1 Формализовать versioned event contract

#### Что улучшить

Сейчас event contract уже описан, но ещё не оформлен как полноценно versioned schema discipline.

#### Что предлагается

- добавить `schemaVersion` в события;
- фиксировать обязательные и optional поля по версиям;
- считать parse/validation ошибки в разрезе schema version;
- подготовить migration path `JSON -> Avro/Protobuf`.

#### Business value

- меньше рисков schema drift при интеграции с реальными upstream системами;
- проще масштабировать число producers;
- легче объяснять несовместимости и отклонения в потоке.

### 4.2 Усилить observability до SRE-ready уровня

#### Что улучшить

Сейчас observability уже полезна, но ещё не полностью покрывает production-like эксплуатацию.

#### Что предлагается

- добавить consumer lag dashboard;
- добавить restart/recovery panels;
- добавить alert rules на:
  - watermark stall;
  - рост `invalid`;
  - рост `late`;
  - spike в `triggered share`;
  - detector error spikes;
- добавить health-summary view уровня `green/yellow/red`.

#### Business value

- меньше времени на ручную диагностику;
- лучше видны ранние симптомы деградации;
- проще передавать систему в эксплуатацию.

#### Что обязательно добавить в этот инкремент

##### Runtime contract visibility

Это отдельная capability, а не просто кусок документации.

Под `runtime contract visibility` здесь понимается явная фиксация и наблюдаемость правил, по которым job реально работает во времени и при сбоях:

- какой тип окна используется;
- по какому времени работает окно;
- какой допуск на out-of-order события;
- сколько late events ещё могут обновить окно;
- какие delivery guarantees даёт sink;
- как часто делаются checkpoints;
- где эти настройки меняются и чем это грозит downstream.

Иными словами, это ответ на вопрос:

- "что именно Flink обещает нам как runtime-платформа в текущей конфигурации".

##### Что нужно явно зафиксировать

Для текущего MVP надо в одном месте собрать такую таблицу:

- `window type`
  - текущее значение: `Tumbling Event-Time Window`;
  - где задаётся: [IncrementOneTopologyBuilder.java](/home/bob/old_bob/IdeaProjects/flink/flink-job/src/main/java/com/bank/aisafetyops/app/usecase/IncrementOneTopologyBuilder.java);
  - на что влияет: как группируются события и как часто появляются aggregates.
- `window sizes`
  - текущее значение: `1m`, `5m`;
  - где задаётся: [IncrementOneTopologyBuilder.java](/home/bob/old_bob/IdeaProjects/flink/flink-job/src/main/java/com/bank/aisafetyops/app/usecase/IncrementOneTopologyBuilder.java);
  - на что влияет: скорость реакции и устойчивость aggregate signal.
- `outOfOrdernessSeconds`
  - текущее значение: `30`;
  - где задаётся: [local-job.yaml](/home/bob/old_bob/IdeaProjects/flink/config/job/local-job.yaml);
  - на что влияет: сколько event-time disorder job считает нормальным до продвижения watermark.
- `idleTimeoutMinutes`
  - текущее значение: `1`;
  - где задаётся: [local-job.yaml](/home/bob/old_bob/IdeaProjects/flink/config/job/local-job.yaml);
  - на что влияет: когда молчащий source partition перестаёт тормозить watermark всего потока.
- `lateToleranceMinutes`
  - текущее значение: `5`;
  - где задаётся: [local-job.yaml](/home/bob/old_bob/IdeaProjects/flink/config/job/local-job.yaml);
  - на что влияет: как долго позднее событие ещё может обновить уже эмитированное окно.
- `checkpointIntervalSeconds`
  - текущее значение: `30`;
  - где задаётся: [local-job.yaml](/home/bob/old_bob/IdeaProjects/flink/config/job/local-job.yaml);
  - на что влияет: частота recovery snapshots и стоимость fault tolerance.
- `autoWatermarkIntervalSeconds`
  - текущее значение: `5`;
  - где задаётся: [local-job.yaml](/home/bob/old_bob/IdeaProjects/flink/config/job/local-job.yaml);
  - на что влияет: как часто runtime публикует watermark progress.
- `delivery guarantee`
  - текущее значение: `AT_LEAST_ONCE`;
  - где задаётся: [KafkaSinkFactory.java](/home/bob/old_bob/IdeaProjects/flink/flink-job/src/main/java/com/bank/aisafetyops/infra/sink/KafkaSinkFactory.java);
  - на что влияет: downstream должен быть готов к duplicate emissions после recovery и late updates.

##### Почему это важно для бизнеса и эксплуатации

Без этой фиксации бизнес и эксплуатация легко делают неверные выводы:

- считают, что `5m` окно должно эмитить результат так же быстро, как `1m`;
- считают, что late event обязан попасть в основную витрину без ограничений;
- считают, что Kafka output не может содержать дубликаты;
- считают, что отсутствие aggregate сразу означает ошибку, хотя причина может быть в watermark или окне.

##### Что надо добавить в observability

Нужно не только описать contract, но и показать его в мониторинге:

- текущий watermark progress;
- late event volume;
- invalid event volume;
- aggregate emissions by window;
- checkpoint success/failure;
- restart count;
- source inactivity или consumer lag.

##### Что надо добавить для мониторинга производительности и ресурсных пределов

Нужно отдельно видеть не только correctness потока, но и то, где pipeline начинает тормозить и где ресурсы подходят к пределу.

Минимальный performance-observability набор:

- input throughput по source/operator;
- output throughput по source/operator;
- busy time по operator/subtask;
- idle time по operator/subtask;
- backpressure signals;
- mailbox latency;
- checkpoint duration trend;
- checkpoint failure trend;
- Kafka consumer lag;
- JVM heap usage;
- container memory usage;
- CPU saturation;
- container restarts;
- network и disk symptoms, если доступны в локальном или production-like стеке.

##### На какие вопросы это отвечает

Этот блок observability должен отвечать на практические вопросы:

- где именно pipeline стал узким местом;
- source тормозит сам или его тормозит downstream;
- проблема в event-time/windowing или в compute/load;
- упираемся ли мы в CPU;
- упираемся ли мы в heap/off-heap memory;
- не деградирует ли checkpointing;
- не растёт ли lag быстрее, чем job успевает вычитывать поток.

##### Как интерпретировать bottleneck

Типовые сигналы:

- высокий `busyTime` и высокий throughput
  - оператор занят полезной работой, но пока справляется;
- высокий `busyTime` и растущий backpressure
  - оператор становится bottleneck;
- низкий CPU, но растущий lag
  - проблема может быть в source config, network, serialization или downstream blocking;
- растущий checkpoint duration
  - возможны проблемы со state size, storage или alignment под backpressure;
- высокий memory usage и restart/OOM symptoms
  - ресурсы на исходе, надо смотреть state footprint, slots и parallelism;
- watermark стоит, а throughput есть
  - проблема скорее в event-time progression или одном зависшем upstream partition.

##### Что стоит добавить в dashboards

В observability стоит выделить отдельный runtime/performance экран или расширить `AISafetyOps Flink Overview` такими панелями:

- `Kafka Consumer Lag By Topic`;
- `Operator Busy Time`;
- `Operator Backpressure Proxy`;
- `Checkpoint Duration Trend`;
- `Checkpoint Failures Trend`;
- `TaskManager CPU`;
- `TaskManager Memory`;
- `TaskManager Restarts`;
- `Source Throughput vs Aggregate Emissions`.

##### Что особенно важно для переноса в банковскую инфраструктуру

При переходе с локального Docker-стенда на банковский контур performance-monitoring должен помочь ответить на:

- сколько partitions реально нужно под ожидаемые сотни и тысячи RPS;
- какой parallelism нужен под окна `1m/5m`;
- где начинается pressure по memory/state;
- как влияет рост late events на re-emission и checkpoint cost;
- какой headroom остаётся до деградации runtime.

##### Business value

После этого этапа команда получает:

- не только понимание "работает/не работает", но и понимание "где именно уже начинает болеть";
- раннее обнаружение деградации до пользовательского инцидента;
- основу для capacity planning и production rollout discussion с platform team.

##### Где это должно жить

Нужно завести отдельный runtime contract doc или явно выделенный раздел в:

- [mvp-runbook.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/mvp-runbook.md);
- [local-walkthrough.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/local-walkthrough.md);
- [monitoring-debugging-guide.md](/home/bob/old_bob/IdeaProjects/flink/docs/monitoring/monitoring-debugging-guide.md).

##### Business value

После этого этапа команда получает:

- единое объяснение runtime-поведения без чтения Java-кода;
- более корректную интерпретацию Grafana и Kafka outputs;
- более качественный диалог с platform team, SRE и risk stakeholders.

#### Какие числовые параметры надо сделать явно настраиваемыми

Для этого инкремента стоит усилить конфигурацию и не оставлять важные operational числа размазанными по скриптам и генераторам.

Минимальный список:

- `outOfOrdernessSeconds`
- `idleTimeoutMinutes`
- `lateToleranceMinutes`
- `checkpointIntervalSeconds`
- `autoWatermarkIntervalSeconds`
- `burstStartSecond`
- `burstDurationSeconds`
- `burstMultiplier`
- `lateShare`
- `tooLateShare`
- `invalidShare`
- `errorShare`
- `detectorLatencyMultiplier`
- `minRequestsPerSecond`
- `maxRequestsPerSecond`
- `replayRequestOffsetSeconds`

##### Где их настраивать

- runtime job semantics:
  - через [local-job.yaml](/home/bob/old_bob/IdeaProjects/flink/config/job/local-job.yaml) и `JobConfig`;
- generator semantics:
  - через CLI аргументы скриптов и, при необходимости, отдельный YAML profile для replay scenarios;
- business scenario presets:
  - через именованные preset-конфиги, чтобы один и тот же сценарий можно было повторить без ручного ввода десятка чисел.

##### Почему это важно

Без числовой параметризации replay и observability быстро превращаются в набор hardcoded demo-паттернов.

С параметризацией команда получает:

- повторяемость экспериментов;
- быстрое изменение интенсивности burst-сценариев;
- возможность показывать систему под разной тяжестью late/invalid/error traffic;
- лучшую подготовку к нагрузочному и production-like тестированию.

### 4.3 Формализовать metric semantics как проектное правило

#### Что улучшить

Уже был найден важный класс ошибок: если панель называется `1m`, она не должна показывать cumulative counter с начала жизни job.

#### Что предлагается

- закрепить правило в документации и AGENTS:
  - `1m` и `5m` в названии означают метрику за этот интервал;
  - cumulative panels должны содержать `Total` или `Since Start`;
  - last aggregate gauges должны содержать `Last Emitted`.

#### Business value

- меньше риска неверной интерпретации дашбордов;
- меньше ложных управленческих выводов;
- более качественная эксплуатационная коммуникация.

## 5. Production-readiness темы, которые стоит начинать заранее

### 5.1 Upgrade and recovery discipline

Что стоит делать заранее:

- удерживать стабильные `uid()` у stateful operators;
- заранее продумать savepoint strategy;
- документировать upgrade path;
- оформить rollback procedure.

Business value:

- меньше риска при переносе в банковскую инфраструктуру;
- ниже стоимость первой production migration.

### 5.2 Infra capacity assumptions

Что стоит делать заранее:

- план partitions vs parallelism;
- checkpoint storage strategy;
- secrets/config separation;
- classpath/dependency policy;
- resource profile для future production.

Business value:

- меньше сюрпризов при первом нагрузочном POC;
- проще согласование с platform team.

## 6. Python: когда добавлять, а когда нет

На текущем этапе Python не нужен в critical path runtime.

Рекомендация:

- не делать PyFlink частью основного production path без сильной причины;
- использовать Python как generator/replay/tooling слой;
- рассматривать Python runtime только если:
  - нужен reuse готового NLP/ML scoring;
  - есть существующий код, который экономически нецелесообразно переписывать на Java.

Business value:

- сохраняется простота JVM-first runtime;
- уменьшается сложность эксплуатации;
- снижается риск runtime fragmentation.

## 7. Рекомендуемая ближайшая последовательность

Если выбирать самый прагматичный порядок, я бы рекомендовал:

1. `BasicIncident` stream.
2. Реальный `policyVersion` и policy runtime control.
3. Detector quality dashboard и quality stream.
4. Session-based correlation.
5. Scenario-based replay regression.

## 8. Ожидаемый эффект после выполнения этого плана

После выполнения этих ближайших шагов система перестанет быть только observability MVP и станет:

- первичной operational risk platform для guardrail monitoring;
- инструментом расследования по агентам и пользовательским сессиям;
- контролируемой средой для развития самих guardrail-ов;
- основой для переноса в банковский production contour.
