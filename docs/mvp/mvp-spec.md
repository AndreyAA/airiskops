# Flink для AIRiskOps: MVP и детальная спецификация реализации

## Глоссарий

- **AIRiskOps** — операционная функция наблюдения, контроля и улучшения безопасности AI-агентов и LLM workflows.
- **Apache Flink** — distributed stream processing engine для обработки событий в near real time и bounded/replay-сценариях.
- **Guardrail** — детектор или правило, которое оценивает запрос, ответ, диалог или поведение агента на предмет риска.
- **Finding** — отдельный результат срабатывания guardrail-а.
- **Incident** — агрегированный операционный сигнал, с которым уже работает риск-команда, SOC или аналитик.
- **Shadow mode** — режим, когда новая версия детектора считает результат параллельно, но не влияет на основной production outcome.
- **Canonical event** — единый нормализованный формат событий внутри пайплайна.
- **Job** — логический Flink-пайплайн, который собирается в коде и отправляется на выполнение.
- **JobManager** — компонент Flink, который координирует выполнение job, recovery и checkpoints.
- **TaskManager** — worker-процесс Flink, который исполняет операторы и держит runtime/state.
- **Parallelism** — число параллельных экземпляров оператора или job.
- **Subtask** — один параллельный экземпляр оператора.
- **Slot** — единица вычислительных ресурсов внутри TaskManager, в которой может исполняться subtask.
- **Checkpoint** — согласованный snapshot state, используемый для fault tolerance и восстановления после сбоя.
- **Savepoint** — управляемый snapshot для ручных операций: миграции, обновления, controlled restart.
- **State** — локально хранимая оператором история по ключу или по экземпляру оператора.
- **Keyed State** — state, изолированный по ключу, например по `agentId` или `sessionId`.
- **Operator State** — state, принадлежащий экземпляру оператора, а не конкретному ключу.
- **Watermark** — механизм event-time прогресса во Flink.
- **Event Time** — время самого бизнес-события из payload, а не локальное время машины.
- **Processing Time** — локальное системное время worker-узла.
- **Window** — группа событий, которую Flink обрабатывает совместно по time/count rule.
- **Allowed Lateness** — интервал, в который позднее событие ещё может обновить уже закрытое окно.
- **KeyBy** — логическое разбиение потока по ключу, после которого все события одного ключа попадают в один state shard.
- **Operator chain** — несколько совместимых операторов, которые Flink может выполнить в одном thread.
- **Shuffle** — перераспределение данных между subtasks, например после `keyBy()`.
- **Broadcast rules** — компактные динамические правила, раздаваемые всем параллельным экземплярам оператора.
- **Drill-down** — возможность пройти от агрегата или incident назад к исходным событиям.
- **Replay** — повторное проигрывание исторических событий для проверки логики.

## 1. Цель документа

Этот документ выделяет и переупаковывает MVP из основного manual в самостоятельную спецификацию, ориентированную на внедрение в подразделении операционных рисков, которое:

- само разрабатывает guardrails;
- само отвечает за их качество и эксплуатационную полезность;
- использует Flink как streaming runtime для near-real-time обработки логов LLM-агентов и сработок guardrail-ов.

Ключевое требование этого документа:

- **после каждого этапа должен быть достигнут конкретный business value**, который можно показать стейкхолдерам и проверить руками.

## 2. Продуктовая цель MVP

MVP должен дать не просто поток метрик, а **первую рабочую систему контроля качества guardrail-ов и первичного incidenting**.

На выходе MVP подразделение должно уметь:

- видеть поток сработок по всем ключевым guardrail-ам;
- понимать, какие агенты, пользовательские сессии и отдельные запросы реально подозрительны;
- проверять качество и стабильность собственных guardrail-ов;
- отличать рост риска от деградации самого detection layer;
- быстро расследовать конкретный `requestId`, `sessionId` и `agentId`;
- менять базовые thresholds без полного переписывания пайплайна.

## 3. Scope MVP

В MVP входят только четыре guardrail-а:

- `PROMPT_INJECTION` с `confidence: double`;
- `TOXICITY` с `confidence: double`;
- `LOOPING` с `boolean`;
- `SYSTEM_PROMPT_LEAKAGE` с `boolean`.

В MVP не входят:

- full-scale CEP для сложных многошаговых атак;
- тяжелый внешний enrichment с медленными API;
- полноценный ML scoring;
- PyFlink как основной execution path;
- автоматическое обучение threshold-ов;
- сложная cross-job orchestration.

## 4. Корректировка MVP под операционные риски

Если смотреть только на исходный MVP из основного manual, он слишком аналитический: счетчики, confidence и токены. Для подразделения операционных рисков этого недостаточно.

Поэтому MVP должен быть усилен пятью обязательными capability:

1. **Version-aware guardrail monitoring**
   - каждое finding должно знать `guardrailVersion` и `policyVersion`.
2. **Detector health metrics**
   - надо уметь видеть, не сломался ли сам detector.
3. **Basic incident stream**
   - нужны не только агрегаты, но и операционные incident signals.
4. **Drill-down**
   - нужна возможность быстро открыть цепочку исходных событий.
5. **Shadow evaluation hook**
   - даже если полноценно не включать shadow mode сразу, схема событий и sinks должны быть готовы к нему.

## 5. Целевая разбивка MVP на этапы

Ниже этапы построены так, чтобы **каждый этап сам по себе давал измеримый business value**, а не был только технической подготовкой к следующему.

## 5.0 Зафиксированные договоренности и предпосылки

Ниже перечислены договоренности, на которых основана эта спецификация.

### Контекст внедрения

- первый MVP разрабатывается и запускается локально на ноутбуке;
- execution environment первого этапа: Docker-based локальный стенд;
- перенос в банковскую инфраструктуру рассматривается как следующий этап после локального MVP.

### Режим обработки

- Flink используется для **NRTP** processing;
- окно анализа для MVP: **1-5 минут**;
- целевое время реакции на значимый trigger: **не более 2-3 минут**;
- допустимо, что агрегаты и incident visibility появляются не мгновенно, а в пределах этого операционного интервала;
- MVP не ориентирован на sub-second stream processing.

### Бизнес-ключи и модель корреляции

- `requestId` — это отдельный атомарный запрос;
- `sessionId` — это набор связанных запросов одного пользователя;
- `agentId` — основной business scope для анализа проблем по агенту;
- на раннем MVP основная operational корреляция должна идти вокруг `agentId` и `sessionId`;
- `requestId` остается обязательным для drill-down, explainability и атомарной корреляции внутри одной сессии;
- текущее допущение: отдельного независимого `tenantId` в MVP нет, и его роль фактически близка к `agentId`;
- при этом поле `tenantId` лучше сохранить в canonical schema как reserved field для будущего переноса в корпоративную инфраструктуру.

### Источники и управление правилами

- на старте предполагается локальный поток событий и тестовая нагрузка;
- для локального MVP фиксируется single-node **Apache Kafka** в Docker как основной event transport;
- формат событий для MVP фиксируется как **JSON**;
- схема JSON должна проектироваться так, чтобы переход на `Avro` или `Protobuf` был локализован на boundary serialization layer;
- policy updates в MVP приходят из YAML-файла;
- как следующий шаг возможен REST endpoint, который публикует policy updates в поток правил;
- для раннего MVP file-driven policy management считается достаточным;
- конфигурация job и policy defaults тоже должны храниться в YAML.

### Incident output

- на MVP incidents пока не обязаны интегрироваться в SIEM или case-management;
- достаточно structured logging и вывода в метрики;
- при необходимости можно параллельно писать incidents в debug sink или локальный Kafka topic.

### Detector health

- отдельных detector health events на старте нет;
- health и quality guardrail-ов в MVP считаются через proxy-метрики: hit rate, detector latency, detector errors, invalid events, late events, version drift.

### Нагрузочный профиль

- локальный функциональный прогон: `10-20 RPS`;
- локальный stress-lite режим желательно предусмотреть выше этого уровня;
- ожидаемая production-нагрузка в будущем: от сотен RPS до пиков порядка `3000 RPS`.

### Технологические ограничения MVP

- MVP должен быть Java-first;
- PyFlink и Python workers не входят в critical path;
- Python допускается только как позднее расширение после стабилизации базового MVP.
- operational scripts допустимы на `bash`;
- генератор replay dataset допустим на `Python`, но это не часть runtime path Flink job.

## 5.1 Этап 1. Trusted Event Foundation

### Цель

Собрать поток, которому можно доверять как источнику правды.

### Что реализуем

- ingest raw agent logs и raw guardrail findings из Kafka;
- canonical normalization в единый `SafetyEvent`;
- schema validation;
- timestamp assignment;
- watermark strategy;
- side outputs для invalid и too-late events;
- raw sink для расследований;
- базовые operational metrics.

### Business value

После этапа 1 подразделение получает:

- прозрачность по тому, какие guardrail-данные реально приходят;
- контроль качества входного потока;
- подтверждение, что telemetry pipeline не теряет критичные поля;
- возможность разбирать реальные события по `agentId`, `sessionId` и `requestId`.

Это первый полезный результат, потому что до него невозможно доказать ни корректность detection, ни полноту аналитики.

### Как это можно посмотреть и пощупать

- открыть raw normalized stream по конкретному `requestId`;
- открыть raw normalized stream по конкретным `agentId` и `sessionId`;
- построить dashboard по volume событий, invalid rate, late rate;
- сравнить raw входы upstream и normalized outputs;
- руками убедиться, что у событий есть `tenantId`, `agentId`, `sessionId`, `requestId`, `eventTime`.

### Критерий готовности

- не менее 99% валидных событий успешно нормализуются;
- invalid events не теряются и идут в отдельный sink;
- watermark двигается стабильно;
- для выбранного `requestId` можно найти полную цепочку normalized events.

## 5.2 Этап 2. Guardrail Visibility MVP

### Цель

Получить первую рабочую витрину эффективности guardrail-ов и их качества в режиме NRTP.

### Что реализуем

- windowed aggregates по `agent`, `session`, `model`, `guardrailType`;
- окна агрегации `1m` и `5m` для NRTP-витрин;
- counts по сработкам;
- confidence distribution для `PROMPT_INJECTION` и `TOXICITY`;
- counts по `LOOPING=true` и `SYSTEM_PROMPT_LEAKAGE=true`;
- токены `input/output`;
- detector latency и detector error counters;
- `guardrailVersion` и `policyVersion` в агрегатах.

### Business value

После этапа 2 подразделение получает:

- первую рабочую observability-витрину по собственным guardrail-ам;
- ответ на вопрос, какие guardrail-ы шумят, молчат или деградируют и в каких агентных сессиях это проявляется;
- базовую основу для оперативного контроля релизов новых правил и детекторов.

Это уже ценно для operational risk, потому что позволяет проверять не только поведение агентов, но и качество собственных контрольных механизмов.

### Как это можно посмотреть и пощупать

- dashboard по каждому guardrail: hit rate, confidence, latency, errors;
- таблицы по `guardrailVersion`;
- сравнение срабатываний по `agentId`, `sessionId` и `model`;
- сравнение одних и тех же сигналов на окнах `1m` и `5m`;
- алерт на аномально высокий detector error rate.

### Критерий готовности

- по каждому из четырех guardrail-ов строятся отдельные метрики;
- в каждой агрегированной записи видны `guardrailVersion` и `policyVersion`;
- latency и error counters доступны в observability stack;
- команда может показать конкретный рост/падение hit rate после controlled replay.

## 5.3 Этап 3. Basic Incident MVP

### Цель

Перейти от голых сработок к минимально полезным incident signals на уровне агента и пользовательской сессии.

### Что реализуем

- `keyBy(agentId + sessionId)` для корреляции findings в пределах агентной пользовательской сессии;
- короткоживущий keyed state по сессии;
- накопление и merge findings внутри сессии с drill-down до отдельных `requestId`;
- расчет базовой severity;
- выпуск `BasicIncident`;
- suppression дублей в окне, согласованном с анализом `1-5 минут`;
- structured logging и вывод в метрики, с опциональным debug sink.

### Business value

После этапа 3 подразделение получает:

- не просто счетчики guardrail-ов, а список конкретных подозрительных запросов;
- снижение шума за счет объединения нескольких findings в один session/agent incident;
- первый операционный артефакт, с которым можно работать в расследовании.

Это первый этап, где Flink начинает приносить value не только как аналитический движок, но и как incident engine.

### Как это можно посмотреть и пощупать

- открыть incident stream;
- для конкретного incident увидеть `agentId`, `sessionId`, связанный набор `requestId`, `guardrailsTriggered`, `highestSeverity`, `reasonCodes`;
- сравнить число raw findings и число consolidated incidents;
- показать кейс, где 2-3 findings из одной сессии объединяются в один incident.

### Критерий готовности

- incidents стабильно логируются и отражаются в метриках;
- дубли по одной сессии заметно сокращены;
- для каждого incident есть drill-down к исходным finding events;
- базовая severity понятна и объяснима аналитикам риска.

## 5.4 Этап 4. Dynamic Policy MVP

### Цель

Дать подразделению возможность менять operational thresholds без redeploy.

### Что реализуем

- broadcast stream правил;
- per-tenant thresholds;
- severity mapping;
- suppress rules;
- emergency toggles;
- `policyVersion` propagation во все output events;
- fallback behavior при отсутствии policy.

### Business value

После этапа 4 подразделение получает:

- быстрый operational control над собственными guardrail-ами;
- возможность быстро реагировать на всплески false positives или missed detections;
- сокращение time-to-change для порогов и routing logic.

Для подразделения, которое само делает guardrails, это один из самых важных value jumps.

### Как это можно посмотреть и пощупать

- изменить threshold для тестового tenant и увидеть изменение incident output без redeploy;
- проверить `policyVersion` в raw findings, aggregates и incidents;
- провести controlled demo “до/после изменения порога”.

### Критерий готовности

- изменение policy отражается в output без перезапуска job;
- policy changes трассируются через `policyVersion`;
- fallback policy не ломает pipeline;
- можно показать бизнесу controlled tuning case.

## 5.5 Этап 5. Guardrail Quality Control MVP

### Цель

Добавить контроль качества собственных guardrail-ов, а не только обработку их output.

### Что реализуем

- quality metrics per guardrail version;
- detector health metrics;
- disagreement stream для будущего shadow mode;
- feedback-ready schema для ручной разметки `confirmed_risk`, `false_positive`, `benign_test`, `needs_review`;
- базовые витрины drift по версиям.

### Business value

После этапа 5 подразделение получает:

- первую operational систему контроля качества guardrail-ов;
- возможность измерять не только события риска, но и качество собственных релизов;
- основу для canary/shadow rollout новых версий detector-ов.

Этот этап особенно важен именно для вашей функции, потому что вы не внешний потребитель guardrail-сработок, а владелец самих guardrail-ов.

### Как это можно посмотреть и пощупать

- dashboard `guardrail_version A vs B`;
- расхождения по confidence и trigger decisions;
- detector health panel;
- отдельный stream расхождений и suspicious regressions.

### Критерий готовности

- доступна отдельная витрина по quality metrics guardrail-ов;
- новые версии detector-ов можно сравнивать на одном и том же потоке;
- техническая деградация detector-а не маскируется под снижение риска в бизнес-потоке.

## 6. Итоговая MVP-структура

Если свернуть все в одну фразу:

**MVP для AIRiskOps в операционных рисках — это trusted NRTP pipeline, который не только считает сработки guardrail-ов, но и дает version-aware visibility, session-aware basic incidenting и контроль качества самих guardrail-ов.**

## 7. Детальная спецификация реализации

## 7.1 Архитектурный принцип

Реализация должна быть **Java-first** на `DataStream API`.

Причины:

- команда ориентирована на Java;
- основная stateful и operational logic проще и надежнее обслуживается в JVM;
- PyFlink сейчас не нужен для core MVP path.

Python допускается только как позднее расширение, не входящее в MVP.

## 7.2 Источники данных

Целевые production sources:

- Kafka topic `agent-requests`;
- Kafka topic `agent-responses`;
- Kafka topic `guardrail-findings`;
- Kafka topic `guardrail-health`, если такие heartbeat/health events уже есть;
- Kafka topic `policy-updates`, начиная с этапа 4.

Нефункциональное правило:

- `FileSource` допускается только для replay, тестов и демонстраций.

Для MVP на локальном ноутбуке:

- фиксируется Docker-based стенд с single-node Apache Kafka;
- pipeline должен быть воспроизводим локально без банковской инфраструктуры;
- deployment assumptions Kubernetes и enterprise object storage на этом этапе не являются обязательными.

## 7.2.1 Формат событий и стратегия миграции формата

Стартовый формат MVP:

- JSON events в Kafka topics;
- JSON Lines для локального replay dataset и отладки.

Чтобы потом безболезненно перейти на другой формат:

- доменные модели должны быть отделены от транспорта;
- serialization/deserialization должны быть инкапсулированы в отдельном слое;
- бизнес-логика operators не должна зависеть от конкретного wire format;
- schema evolution rules нужно документировать уже на уровне полей canonical event.

Практический вывод:

- для MVP JSON дает максимальную скорость старта и удобство отладки;
- при переносе в банковскую инфраструктуру boundary layer можно заменить на Avro/Protobuf без переделки всей pipeline logic.

## 7.2.2 Replay dataset

Replay dataset входит в MVP как обязательный operational capability.

Назначение replay dataset:

- воспроизводить demo и тестовые сценарии;
- проверять поведение job после изменения thresholds и policy;
- выполнять локальный regression на одном и том же наборе событий;
- разбирать инциденты и сложные сессии без ожидания живого трафика;
- готовить deterministic нагрузочные и функциональные сценарии.

Минимальные replay profiles:

- `normal`;
- `attack`;
- `mixed`.

Требование:

- replay dataset должен генерироваться воспроизводимо по `seed`;
- формат вывода — JSON Lines;
- генератор должен уметь выпускать данные отдельно по Kafka topics.

## 7.3 Каноническая модель событий

### SafetyEvent

```java
public record SafetyEvent(
    String tenantId,
    String environment,
    String agentId,
    String sessionId,
    String requestId,
    String turnId,
    Instant eventTime,
    EventType eventType,
    String modelName,
    String userId,
    String channel,
    int inputTokens,
    int outputTokens,
    String guardrailName,
    String guardrailVersion,
    String policyVersion,
    Double confidence,
    Boolean triggered,
    Boolean loopingDetected,
    Boolean systemPromptLeakageDetected,
    Long detectorLatencyMs,
    String detectorStatus,
    Map<String, String> tags
) {}
```

### GuardrailFinding

```java
public record GuardrailFinding(
    String tenantId,
    String agentId,
    String sessionId,
    String requestId,
    Instant eventTime,
    GuardrailType type,
    String guardrailVersion,
    String policyVersion,
    Double confidence,
    boolean triggered,
    SeverityBand severity,
    Long detectorLatencyMs,
    String detectorStatus,
    String reasonCode
) {}
```

### BasicIncident

```java
public record BasicIncident(
    String incidentId,
    String tenantId,
    String agentId,
    String sessionId,
    Instant incidentTime,
    List<String> requestIds,
    List<GuardrailType> guardrailsTriggered,
    SeverityBand highestSeverity,
    String policyVersion,
    List<String> reasonCodes,
    boolean suppressedDuplicate
) {}
```

## 7.4 Правила нормализации

Обязательные поля:

- `tenantId`;
- `agentId`;
- `sessionId`;
- `requestId`;
- `eventTime`;
- `eventType`.

Семантика идентификаторов:

- `agentId` — основной business key MVP;
- `sessionId` — обязательный ключ пользовательской сессии;
- `requestId` — обязательный ключ атомарного запроса внутри сессии.

Для confidence-based guardrail-ов:

- `confidence` обязателен, если `guardrailName` равен `PROMPT_INJECTION` или `TOXICITY`.

Для boolean guardrail-ов:

- `triggered` обязателен;
- boolean signal должен нормализоваться в единый `triggered`.

Ошибочные события:

- не выбрасываются молча;
- уходят в side output `invalid-events`.

## 7.5 Event time и watermarking

Базовая стратегия:

```java
WatermarkStrategy<SafetyEvent> watermarkStrategy =
    WatermarkStrategy
        .<SafetyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((event, ts) -> event.eventTime().toEpochMilli())
        .withIdleness(Duration.ofMinutes(1));
```

Требование:

- финальное значение lateness должно уточняться по фактическому лагу источников на тестовом потоке;
- настройки watermarks, окон и timers должны соответствовать окну анализа `1-5 минут` и времени реакции не более `2-3 минут`.

Output categories:

- normal flow;
- late side output;
- invalid side output.

## 7.6 Логические стадии пайплайна

### Stage A. Ingest and normalization

Вход:

- raw Kafka records.

Выход:

- `DataStream<SafetyEvent>`;
- side output `invalid-events`.

### Stage B. Primary findings extraction

Вход:

- `SafetyEvent`.

Выход:

- `DataStream<GuardrailFinding>`.

Функция:

- привести все четыре guardrail-а к единому типу findings;
- назначить severity по базовым правилам;
- сохранить `guardrailVersion`, `policyVersion`, `reasonCode`.

### Stage C. Aggregates

Вход:

- `GuardrailFinding`.

Выход:

- windowed aggregate events.

Ключи:

- `agentId`;
- `sessionId`, если нужен session slice;
- `guardrailType`;
- `guardrailVersion`;
- `policyVersion`.

Окна:

- стартово `1m` и `5m` windows для operational NRTP views;
- выбор финального окна зависит от желаемого компромисса между скоростью появления сигнала и устойчивостью к late events.

### Stage D. Agent/session-level incidenting

Вход:

- `GuardrailFinding`.

Механика:

- `keyBy(agentId + sessionId)`;
- аккумулировать findings в пределах агентной пользовательской сессии;
- хранить компактный набор связанных `requestId`;
- timer на финализацию session-level incident;
- deduplication в коротком окне.

Выход:

- `DataStream<BasicIncident>`.

### Stage E. Policy application

Вход:

- `GuardrailFinding` или `BasicIncident`;
- broadcast `TenantPolicy`.

Функция:

- переопределить thresholds;
- переназначить severity;
- применить suppress rules.

### Stage F. Quality control

Вход:

- findings, incidents, detector health events, позже feedback stream.

Выход:

- quality metrics stream;
- disagreement/debug stream.

## 7.7 Stateful design

### Agent/session-level state

Назначение:

- собрать findings по одной пользовательской сессии внутри конкретного агента.

Тип:

- `MapState<String, GuardrailFinding>` по `requestId` или компактная custom структура для session bucket.

TTL:

- 15-30 минут стартово, в зависимости от длительности пользовательских сессий, окна анализа `1-5 минут` и требований к подавлению дублей.

### Dedup state

Назначение:

- не выпускать повторный incident на тот же `requestId` в suppression window.
- не выпускать повторный incident на ту же `agentId + sessionId` корреляцию в suppression window.

Тип:

- `ValueState<Instant>` или `ValueState<String>`.

TTL:

- 10 минут стартово.

### Почему полная длинная session history не входит в ранний MVP

`sessionId` уже входит в ранний MVP как основной корреляционный ключ, но полная длинная история всей сессии не входит:

- слишком длинная история увеличивает state;
- усложняет cleanup и TTL;
- повышает риск noisy correlation logic.

В MVP нужна компактная session-level корреляция в пределах NRTP окна, а не полное долговременное досье по сессии.

## 7.8 Правила severity

Стартовая модель:

- `PROMPT_INJECTION`:
  - `confidence >= 0.90` -> `CRITICAL`
  - `confidence >= 0.75` -> `HIGH`
  - `confidence >= 0.55` -> `MEDIUM`
  - иначе `LOW`
- `TOXICITY`:
  - `confidence >= 0.90` -> `HIGH`
  - `confidence >= 0.70` -> `MEDIUM`
  - иначе `LOW`
- `LOOPING=true` -> `MEDIUM`
- `SYSTEM_PROMPT_LEAKAGE=true` -> `CRITICAL`

Это только стартовые defaults. Начиная с этапа 4 они должны вытесняться policy-driven mapping.

## 7.9 Sink strategy

Обязательные sinks MVP:

- `raw-normalized-events`;
- `invalid-events`;
- `late-events`;
- `guardrail-aggregates`;
- `basic-incidents`;
- `guardrail-quality-metrics`.

Для раннего MVP:

- `basic-incidents` достаточно логировать structured logs и отражать в метриках;
- отдельный Kafka sink для incidents может быть добавлен как debug или post-MVP capability.
- replay dataset и generated JSON Lines должны храниться как локальные артефакты для повторного прогона regression.

Желательные downstream consumers:

- Grafana/Prometheus stack;
- SIEM или incident topic;
- OLAP/lakehouse для исторического анализа.

## 7.10 Мониторинг и метрики MVP

Минимальные технические метрики:

- throughput;
- busy/idle/backpressure;
- watermark lag;
- invalid events count;
- late events count;
- checkpoint duration;
- failed checkpoints;
- restart count.

Минимальные бизнес-метрики:

- hit rate per guardrail;
- confidence distribution;
- incidents per agent;
- incidents per session;
- incidents per guardrail version;
- detector latency p95;
- detector error rate;
- suppressed incident count.

## 7.11 Требования к логированию

В логах должны присутствовать:

- operator name;
- `tenantId`;
- `agentId`;
- `requestId`;
- `sessionId`;
- `guardrailVersion`;
- `policyVersion`;
- compact `reasonCode`.

В логах не должны появляться:

- полный system prompt;
- полный пользовательский prompt;
- полный PII payload;
- длинные неструктурированные stack traces без контекста события.

## 7.12 Нефункциональные требования

- checkpointing включен с первого production-like запуска;
- stateful operators имеют стабильные `uid()`;
- pipeline переживает restart без потери корректности state;
- invalid и late events не теряются;
- output events трассируются до `agentId`, `sessionId` и исходных `requestId`;
- policy changes не требуют redeploy, начиная с этапа 4.

## 7.13 Acceptance criteria по всему MVP

MVP считается реализованным, если:

1. поток raw событий стабильно нормализуется в canonical schema;
2. по всем четырем guardrail-ам строятся агрегаты и quality metrics;
3. по `agentId + sessionId` выпускается `BasicIncident` с drill-down до `requestId`;
4. thresholds и severity можно менять через policy stream;
5. видны `guardrailVersion` и `policyVersion` на всем основном пути;
6. команда может показать controlled demo “поток -> finding -> incident -> dashboard -> drill-down”.

## 8. Что сознательно не включено в MVP

- session-level long history;
- CEP;
- interval join нескольких независимых больших потоков;
- тяжелый external async enrichment;
- автоматический feedback learning;
- PyFlink и Python workers в production path.

Эти возможности следует рассматривать только после того, как MVP стабильно создает measurable business value.

## 9. Что можно добавить сразу после MVP

Первый разумный post-MVP пакет:

- session correlation;
- richer suppression logic;
- shadow evaluation новой версии guardrail;
- feedback stream от расследований;
- detector comparison `version A vs version B`;
- replay framework для regression checks.

## 10. Python как позднее расширение

Python действительно имеет смысл как один из последних этапов, но только при четком use case:

- reuse существующих NLP/ML guardrail-моделей;
- shadow scoring новой версии detector-а;
- offline/online comparison результатов;
- экспериментальные эвристики, которые еще не готовы переноситься в Java.

Рекомендованный порядок:

1. Сначала Java-only MVP.
2. Потом внешний Python scoring service или batch shadow evaluation.
3. И только затем, если нужно, PyFlink или mixed runtime path.

Причина простая:

- Python увеличивает packaging, debugging и operational complexity;
- для раннего operational-risk MVP этот trade-off обычно невыгоден.

## 11. Что уже зафиксировано и что осталось открытым

### Уже зафиксировано

1. Первый MVP делается локально на ноутбуке через Docker.
2. `requestId` существует и обозначает отдельный запрос.
3. `sessionId` нужно использовать как контейнер набора запросов одного пользователя.
4. Основной business scope для анализа проблем — `agentId`.
5. Flink используется в режиме NRTP с ожидаемой задержкой порядка `1-5 минут`.
6. Окно анализа составляет `1-5 минут`.
7. Время реакции на значимый trigger должно быть не более `2-3 минут`.
8. Для локального MVP фиксируется single-node Apache Kafka в Docker.
9. Отдельных detector health events пока нет.
10. Первый incident output достаточно логировать и выводить в метрики.
11. Policy updates на MVP приходят из YAML-файла; позже допустим REST endpoint.
12. На старте `tenantId` фактически близок к `agentId`, но поле лучше сохранить в схеме.
13. Локальная тестовая нагрузка: `10-20 RPS`; в будущем production profile — до `3000 RPS`.
14. Python не входит в ранний critical path и рассматривается как позднее расширение.

### Осталось открытым

1. Какие именно Kafka topics уже существуют, и в каком формате лежат raw events: JSON, Avro, Protobuf?
2. Нужен ли уже на MVP локальный replay dataset из реальных или синтетических guardrail событий?

## 12. Следующий шаг

После ответа на открытые вопросы следующая практическая итерация должна быть такой:

1. Зафиксировать event schema.
2. Зафиксировать topics и sink contracts.
3. Зафиксировать thresholds defaults.
4. Зафиксировать KPI MVP и demo scenario.
5. После этого разложить реализацию на Java-модули и operator classes.

## 13. Инкрементная реализация и регресс

Реализацию MVP нужно вести инкрементами.

Обязательное правило:

- после каждого инкремента добавляется локальный или интеграционный тест на новую функциональность;
- после каждого инкремента прогоняется полный локальный regression suite;
- новые сценарии replay dataset должны дополнять regression, а не заменять его.

Минимальный практический набор:

- unit/smoke tests для генератора replay dataset;
- shell syntax checks для operational scripts;
- integration checks для локальной подготовки данных и Kafka topic initialization;
- controlled replay сценарии `normal`, `attack`, `mixed`.
