# Flink для AIRiskOps: подробный manual по внедрению и эксплуатации

## Глоссарий

- **Apache Flink** — distributed stream processing engine для обработки событий в реальном времени и пакетных прогонов на одном runtime.
- **DataStream API** — основной Java API Flink для построения потоковых пайплайнов из операторов.
- **Job** — логический пайплайн, который вы собираете в коде и отправляете на выполнение.
- **JobGraph / ExecutionGraph** — внутренние представления job: сначала логический граф, затем исполняемый граф задач.
- **JobManager** — процесс координации job: планирование, recovery, checkpoint coordination, resource orchestration.
- **TaskManager** — worker-процесс Flink, который исполняет операторы и держит state.
- **Task** — исполняемая runtime-единица в графе выполнения Flink, обычно связанная с конкретным vertex/operator stage.
- **Slot** — единица выделения ресурсов внутри TaskManager. Несколько subtasks могут делить один процесс TaskManager через разные slots.
- **Parallelism** — число параллельных экземпляров оператора.
- **Subtask** — один параллельный экземпляр оператора.
- **Operator chain** — несколько совместимых операторов, которые Flink может исполнять в одном thread для снижения serialization/network overhead.
- **Shuffle** — перераспределение данных между subtasks, например после `keyBy()`.
- **KeyBy** — логическое разбиение потока по ключу, после которого все события одного ключа направляются в один logical shard state.
- **Keyed State** — state, привязанный к ключу потока.
- **Operator State** — state, привязанный к экземпляру оператора, а не к ключу.
- **State backend** — механизм хранения и восстановления state, например heap-based backend или RocksDB/ForSt-подобный backend.
- **Checkpoint** — согласованный snapshot state для восстановления после сбоя.
- **Checkpoint barrier** — служебная метка в потоке, по которой Flink координирует создание snapshot между операторами.
- **Savepoint** — управляемый snapshot для обновлений, миграций и ручных операций.
- **Watermark** — оценка того, что события с timestamp меньше некоторой границы уже в основном пришли.
- **Event Time** — обработка по времени события, а не по системному времени машины.
- **Processing Time** — обработка по локальному времени worker-узла.
- **Window** — группа событий, которую Flink обрабатывает совместно по временным или count-based правилам.
- **Allowed Lateness** — окно допустимого опоздания событий после формального закрытия окна.
- **Backpressure** — состояние, когда downstream не успевает принимать данные и тормозит upstream.
- **State TTL** — срок жизни записей в state.
- **Async I/O** — неблокирующее обогащение события внешним источником.
- **Broadcast State** — механизм доставки небольшого набора правил всем параллельным экземплярам оператора.
- **CEP** — Complex Event Processing, библиотека Flink для поиска паттернов событий.
- **Exactly-once** — семантика, при которой состояние и совместимые sinks восстанавливаются без дублирования итогового эффекта.
- **At-least-once** — семантика, при которой дубли возможны, но данные не теряются при корректной настройке.
- **FileSource** — встроенный source Flink для файлов и директорий; подходит для bootstrap, replay, тестов и bounded jobs.
- **Kafka Source** — production-grade source для непрерывного ingest событий в streaming-сценарии.
- **Pekko RPC** — RPC subsystem в актуальных ветках Flink 2.x; исторически в старых ветках использовалась Akka.
- **Finding** — одно сырое guardrail-событие по конкретному действию агента, например результат детекции prompt injection для одного `requestId`.
- **Emission** — одно опубликованное вниз по pipeline агрегатное сообщение, например запись в `guardrail-aggregates` после закрытия окна.

## 1. Цель документа

Этот документ адаптирует базовую архитектуру Flink под банковский кейс **AIRiskOps**: потоковую обработку логов LLM-агентов, событий guardrail-детекторов и инцидентов безопасности.

Предполагается, что у вас есть:

- события запросов к LLM-агентам;
- сработки guardrails;
- служебные события агента;
- справочники правил и критичности;
- потребность в near-real-time аналитике, алертинге и расследовании.

Ваши guardrails:

- детекция prompt injection: `confidence: double`;
- детекция toxicity: `confidence: double`;
- детекция looping: `boolean`;
- детекция system prompt leakage: `boolean`.

Документ ориентирован на **технически сильного Java-разработчика**, который пока не глубоко знаком с потоковой обработкой, stateful operators, watermarking и production deployment Flink.

## 2. Scope, дата и версионная база

Дата сверки: **2026-08-26**.

Ниже важно разделять две вещи:

1. **Принципы Flink**, которые стабильны много лет.
2. **Версионно-зависимые детали runtime**, которые менялись между ветками.

Для этого manual разумно принять такую базу:

- архитектурные принципы: актуальны для современных Flink 1.20.x и 2.x;
- ссылки на официальные docs: в основном `flink-docs-stable`, а для некоторых библиотек и разделов могут использоваться release docs;
- критичная поправка к исходному конспекту: для актуальных веток Flink RPC больше не следует описывать как Akka-only; в современных 2.x документах и кодовой базе используется **Apache Pekko RPC**.

Инженерный вывод для банка:

- если нужен максимально консервативный production rollout, обычно сначала оценивают наиболее зрелую поддерживаемую ветку, которую готова поддерживать ваша platform team;
- если нужен greenfield на новых возможностях 2.x, это надо проверять отдельным POC с нагрузкой, recovery и upgrade rehearsal.

Без вашей внутренней платформенной матрицы поддержки нельзя честно зафиксировать единственно правильную версию. Поэтому ниже документ написан так, чтобы логика пайплайна и эксплуатации не была привязана к одному минорному релизу.

## 3. Executive summary

Для AIRiskOps Flink подходит хорошо, если задача выглядит так:

- поток высокий или средний, но постоянный;
- нужна stateful correlation по `session_id`, `agent_id`, `request_id`, `tenant_id`;
- важны event-time semantics, late events, recovery и replay;
- нужно в одном job сочетать агрегации, детекцию паттернов, enrichment и routing в разные sinks.

Flink подходит хуже, если:

- вся логика сводится к простому ETL без state и без real-time реакций;
- внешние вызовы медленные и доминируют над локальной обработкой;
- команда не готова поддерживать checkpointing, state migration и операционный runtime.

Для вашего кейса рекомендуемая целевая архитектура обычно такая:

1. Основной runtime и business logic писать на **Java DataStream API**.
2. Основной streaming ingest в production делать через **Kafka**, а не через `FileSource`.
3. `FileSource` оставить для replay, backfill, integration tests, sandbox и controlled bootstrap.
4. Базовую нормализацию событий, keying, watermarking, stateful correlation и alert routing держать в одном Flink job.
5. Правила критичности, пороги confidence и suppress/allow lists раздавать через **Broadcast State**.
6. Для обогащения данными case-management, CMDB, HR, catalog и policy registry использовать **Async I/O** или отдельный precomputed stream, а не синхронные JDBC-запросы из `map()`.

## 4. Перепроверка и корректировка исходных тезисов

### 4.1 Что верно по сути

Из исходного конспекта в целом корректны такие идеи:

- Flink job — это не набор HTTP-микросервисов, а единый исполняемый dataflow graph.
- Runtime сам режет job на задачи и распределяет их по worker-процессам.
- `parallelism > 1` не означает автоматически разные pod'ы.
- для `keyBy`, rebalance и других redistribution шагов есть network shuffle между TaskManager'ами.
- `FileSource` пригоден для прототипов, replay и bounded jobs.
- PyFlink реально позволяет включать Python UDF в общий pipeline.
- для late data, enrichment, CEP, interval join и dynamic rules Flink действительно предоставляет штатные механизмы.

### 4.2 Что нужно уточнить или поправить

#### RPC: уже не только Akka

Формулировка "JobManager общается с TaskManager через Akka RPC" для современного manual слишком узкая и устаревающая. Для актуальных версий корректнее писать так:

- управление задачами идет через **Flink RPC subsystem**;
- в современных 2.x ветках это **Pekko RPC**;
- в старых ветках исторически использовалась **Akka**.

Если в вашем внутреннем документе просто написать "Akka RPC", он быстро станет неточным.

#### Blocking shuffle в batch надо описывать осторожно

Идея "в batch есть blocking shuffle" правильная, но не стоит закреплять детали уровня "`sendfile` используется всегда". Корректнее:

- для bounded / batch-style execution Flink использует batch-oriented exchange strategies;
- данные могут materialize'иться на диск и потом читаться downstream;
- конкретные оптимизации зависят от версии, deployment mode и stack реализации shuffle.

#### PyFlink не должен становиться default-архитектурой для Java-команды

Для вашей аудитории PyFlink лучше описывать как опцию:

- ядро production job для банка разумнее вести на Java;
- Python оставлять только там, где без него реально нельзя, например для reuse существующего NLP/ML-кода;
- критичный stateful control flow лучше не размазывать между JVM и Python без веской причины.

#### Правило "если `ProcessFunction` больше 50 строк, разбиваем" полезно как эвристика, но это не инженерный критерий

Лучше заменить его на набор реальных признаков:

- в функции смешаны разные ключи и разные жизненные циклы state;
- внутри есть и enrichment, и anomaly logic, и routing;
- растет число timer'ов;
- становится трудно отдельно тестировать event-time semantics.

### 4.3 Что важно добавить для production, а не только для концепта

В исходном конспекте не хватало production-обязательных тем:

- schema discipline и versioning событий;
- operator `uid()` для upgrade/savepoint compatibility;
- checkpoint storage и recovery SLA;
- sink semantics;
- backpressure and memory tuning;
- observability;
- governance для правил и конфигурации;
- security и multi-tenant isolation.

## 5. Целевая бизнес-модель данных для AIRiskOps

### 5.1 Нормализованное событие

Практически всегда выгодно сначала привести разные события к единой envelope-модели, а потом уже строить stateful pipeline.

Пример:

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
    String locale,
    PromptInjectionSignal promptInjection,
    ToxicitySignal toxicity,
    Boolean loopingDetected,
    Boolean systemPromptLeakageDetected,
    int inputTokens,
    int outputTokens,
    String policyVersion,
    Map<String, String> tags
) {}
```

Где:

- `eventType` отличает запрос агента, ответ модели, guardrail hit, workflow event, escalation event;
- `eventTime` приходит из доменной системы, а не выставляется в Flink на входе;
- `policyVersion` помогает потом объяснять, почему инцидент был классифицирован именно так;
- `tenantId` обязателен, если дальше возможна multi-tenant обработка.

### 5.2 Нормализация guardrail-результатов

Ваши детекторы имеют разную природу: два confidence-based и два boolean-based. Поэтому полезно ввести derived-представление:

```java
public record GuardrailFinding(
    String tenantId,
    String agentId,
    String sessionId,
    String requestId,
    Instant eventTime,
    GuardrailType type,
    SeverityBand severityBand,
    Double confidence,
    boolean triggered,
    String policyVersion,
    String explanationCode
) {}
```

Рекомендация:

- всегда хранить и `triggered`, и исходный `confidence`, если он есть;
- для boolean-детекторов `confidence` можно оставлять `null`;
- severity лучше вычислять не хардкодом в коде job, а таблицей правил через broadcast stream.

### 5.3 Ключи потока

Типовые ключи:

- `tenantId + agentId + sessionId` — для диалоговой истории и паттернов в рамках сессии;
- `tenantId + requestId` — для корреляции одного запроса с несколькими детекторами;
- `tenantId + agentId` — для rate-based аномалий по агенту;
- `tenantId + modelName` — для деградации конкретной модели или rollout-а.

Правило: ключ выбирается не по удобству программиста, а по жизненному циклу state и бизнес-вопросу.

## 6. Рекомендуемая архитектура пайплайна

### 6.1 Этап 0: ingest

Для production:

- основной источник событий: Kafka или совместимый log-based broker;
- для replay и backfill: `FileSource`;
- для тестов: локальные файлы, synthetic generators, deterministic fixture streams.

Почему не `FileSource` как основной production source:

- он хорош для bounded data и controlled replay;
- он хуже соответствует постоянно живому event stream;
- вокруг него нет естественной модели consumer groups, offset ownership и независимого масштабирования как у Kafka.

### 6.2 Этап 1: schema validation и normalization

На этом этапе:

- парсите входной JSON/Avro/Protobuf;
- валидируете обязательные поля;
- назначаете event timestamp;
- раскладываете "сырые" поля guardrail-систем в нормализованный `SafetyEvent`.

Здесь же полезно:

- выносить мусорные события в side output `invalid-events`;
- считать базовые ingestion-метрики;
- фиксировать source metadata: topic, partition, offset, file path.

### 6.3 Этап 2: watermarking и strategy for late data

Для guardrail-логов late events обычны:

- агент отправил событие поздно;
- upstream batch-flush случился через несколько секунд;
- сеть или очередь временно лагала;
- enrichment stream пришел позже основного.

Обычно стартовая стратегия такая:

- event time;
- bounded out-of-orderness watermark;
- `withIdleness(...)`, если отдельные partitions иногда молчат;
- небольшой `allowedLateness`, только если downstream реально умеет переагрегировать.

Пример:

```java
WatermarkStrategy<SafetyEvent> wm =
    WatermarkStrategy
        .<SafetyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((event, ts) -> event.eventTime().toEpochMilli())
        .withIdleness(Duration.ofMinutes(1));
```

Почему это важно:

- без watermark окно может не закрываться;
- без idleness один "молчаливый" partition может тормозить весь watermark;
- слишком большой lateness раздувает state и задерживает завершение окон.

### 6.4 Этап 3: первичная агрегация по окнам

MVP для вашего кейса:

- число запросов;
- число сработок каждого guardrail;
- средний и p95 `confidence` по prompt injection;
- средний и p95 `confidence` по toxicity;
- число `loopingDetected=true`;
- число `systemPromptLeakageDetected=true`;
- суммарные `inputTokens` и `outputTokens`;
- hit rate по агенту, каналу, tenant и policy version.

Практический совет:

- для high-volume metrics используйте incremental aggregates, а не храните весь набор событий в window function;
- window result делайте отдельным typed event, а не сразу final JSON string.

### 6.5 Этап 4: session-aware correlation

Здесь появляется state:

- история последних N событий по `sessionId`;
- накопление guardrail findings в рамках `requestId`;
- корреляция "запрос -> сработали 2 и более guardrails -> модель все равно вернула чувствительный ответ".

Обычно это делается через `KeyedProcessFunction` или `ProcessWindowFunction`, а не через один гигантский `map()`.

Что хранить в state:

- последнее событие запроса;
- compact history последних K turns;
- counters по типам сработок;
- timestamp последней активности;
- флаги подавления повторных алертов.

### 6.6 Этап 5: enrichment

Типовые enrichment-источники:

- реестр агентов;
- owner/team/service catalog;
- справочник criticality;
- справочник политик и порогов;
- mapping `modelName -> provider / deployment ring / rollout batch`.

Для внешних вызовов есть два нормальных пути:

1. **Async I/O** к внешнему сервису/кэшу.
2. Отдельный поток справочников с последующей join/broadcast.

Что не надо делать:

- синхронный HTTP/JDBC вызов внутри `map()` или `processElement()`;
- тяжелый per-event REST round-trip в критическом hot path.

### 6.7 Этап 6: dynamic rules через Broadcast State

Это один из ключевых паттернов для AIRiskOps.

Что выносить в broadcast rules:

- пороги `confidence` по tenant;
- suppress rules;
- allow lists;
- severity mapping;
- признаки "эскалировать сразу";
- временные emergency rules во время инцидента.

Почему это лучше hardcode:

- не нужен redeploy job на каждый порог;
- появляется audit trail версий правил;
- проще делать controlled rollout.

Ограничение:

- broadcast state физически живет у каждого parallel instance;
- большие справочники туда класть нельзя;
- правила должны быть компактными и редко меняющимися.

### 6.8 Этап 7: anomaly detection и CEP

Для вашего кейса CEP подходит, если нужно ловить паттерны вида:

- несколько prompt injection hits подряд в одной сессии;
- escalation от toxicity low -> medium -> high за короткий интервал;
- looping + рост token usage + повторяющиеся retries;
- leakage indicator после серии system/tool prompts.

Но не надо тащить CEP во все подряд:

- если паттерн выражается обычным счетчиком и таймером, `KeyedProcessFunction` часто проще;
- CEP полезен там, где нужен порядок, временные ограничения и комбинация нескольких типов событий.

### 6.9 Этап 8: sinks

Типовые выходы:

- оперативные метрики в Kafka / аналитическую шину;
- инциденты в SIEM / case management;
- агрегаты в OLAP / lakehouse;
- side output для dead-letter и invalid events;
- расследовательский raw sink с retention для форензики.

Ключевой момент:

- exactly-once в Flink заканчивается там, где sink его больше не поддерживает;
- всегда фиксируйте семантику по каждому sink отдельно.

## 7. Как один job реально исполняется в кластере

### 7.1 Это не цепочка HTTP-микросервисов

Ваш Java-код строит логический dataflow. Дальше Flink:

- оптимизирует граф;
- разбивает его на subtasks;
- может chain'ить совместимые операторы в один execution unit;
- размещает subtasks по TaskManager slots;
- координирует state, backpressure и recovery.

Поэтому правильная ментальная модель:

- не "сервис A вызывает сервис B";
- а "операторы образуют execution graph, а runtime сам решает размещение и transport".

### 7.2 Parallelism, slots и pod'ы

Правильная интерпретация:

- `parallelism = 8` означает восемь subtasks для оператора;
- это **не** означает автоматически восемь pod'ов;
- сколько subtasks окажется в одном pod, зависит от числа slots на TaskManager и политики размещения.

Пример:

- если `taskmanager.numberOfTaskSlots: 2` и кластер дал один TaskManager pod, два subtasks могут жить в одном JVM-процессе;
- если у вас по одному slot на TaskManager, то для тех же двух subtasks обычно понадобятся два worker placement unit.

Для банковского production чаще удобнее мыслить так:

- slot — это ресурсная емкость worker-а;
- pod — это упаковка одного TaskManager процесса;
- параллелизм job надо соотносить не с количеством pod'ов, а с CPU, memory, network и state footprint.

### 7.3 Управляющий и data plane

Логически у Flink есть два плана:

- **control plane**: coordination, scheduling, heartbeats, checkpoint orchestration, failure handling;
- **data plane**: передача actual records между subtasks.

Для актуальных версий:

- control plane построен на Flink RPC subsystem, в современных ветках через **Pekko**;
- data plane использует сетевой transport stack Flink, основанный на Netty/TCP для межпроцессного обмена.

Практический смысл:

- если у вас проблемы с checkpoint alignment, backpressure и shuffle, это обычно не "RPC проблема";
- если JobManager стабилен, но растет latency на `keyBy`/join, нужно смотреть network buffers, serialization, skew и downstream saturation.

## 8. Event time, windows и late data для guardrail-логов

### 8.1 Почему processing time здесь недостаточен

В AIRiskOps вас интересует реальный порядок событий безопасности, а не момент, когда worker их увидел.

Примеры, где processing time ломает аналитику:

- детектор prompt injection записал событие позже основного request log;
- событие toxicity пришло из внешнего sidecar с задержкой;
- файловый replay или queue lag сместили время поступления.

Если вы используете processing time:

- окна будут отражать лаги инфраструктуры;
- корреляции будут нестабильны;
- инциденты будут "прыгать" между окнами.

### 8.2 Как выбирать watermark lag

Стартовая методика:

1. Измерьте распределение `ingest_time - event_time`.
2. Посмотрите p95/p99 лаг по каждому источнику.
3. Задайте watermark lag немного выше рабочего p99, а не "на глаз".

Если:

- p99 опоздания 6 секунд, начните с 10-15 секунд;
- p99 опоздания 40 секунд из-за batch flush, вероятно нужен 60 секунд лаг или переработка upstream.

### 8.3 Allowed lateness использовать умеренно

`allowedLateness` полезен, когда late events реально надо довносить в уже закрытые окна.

Но цена:

- дольше живет window state;
- сложнее reasoning по повторным обновлениям;
- downstream sink должен уметь корректно принимать corrections/upserts.

Для alerting-пайплайна часто лучше:

- маленький lateness;
- очень поздние события отправлять в side output;
- отдельно разбирать их в reconciliation/replay flow.

## 9. State: что хранить и как не утонуть

### 9.1 Когда state действительно нужен

State нужен, если логика зависит от истории:

- "это уже третий prompt injection hit за 5 минут";
- "этот `requestId` уже имел toxicity hit и leakage hit";
- "в этой сессии уже был looping, не шли второй identical alert";
- "за последние 20 turn confidence steadily рос".

Если история не нужна, не тяните keyed state без причины.

### 9.2 State TTL

TTL помогает не держать бесконечную историю. Для guardrail-сценариев TTL обычно задают по доменной логике:

- история сессии: часы или дни;
- request correlation: минуты или часы;
- suppress flags: минуты;
- tenant-level rolling counters: зависит от окна и SLA расследований.

Важный нюанс:

- TTL не заменяет полноценное доменное проектирование state;
- слишком длинный TTL резко увеличивает storage и recovery time;
- слишком короткий TTL ломает корреляцию и suppress logic.

### 9.3 State schema evolution

Для production это критично:

- задавайте стабильные `uid()` операторам;
- не меняйте бездумно типы state;
- тестируйте restore из savepoint/checkpoint на staging;
- ведите версионирование stateful изменений.

Если этого не делать, первый серьезный upgrade станет операционным инцидентом.

## 10. Async I/O, joins и enrichment strategy

### 10.1 Когда нужен Async I/O

Async I/O оправдан, когда:

- справочник нельзя заранее влить в stream;
- ответ приходит быстро и предсказуемо;
- есть локальный или сетевой cache;
- без enrichment нельзя принять решение.

Не оправдан, когда:

- каждый запрос идет в медленную систему расследований с p99 500+ ms;
- rate высокий и enrichment становится bottleneck;
- внешняя система не держит нужный QPS.

### 10.2 Join strategy для AIRiskOps

Нормальная матрица выбора:

| Задача | Подход |
|---|---|
| Корреляция событий одного запроса | `keyBy(requestId)` + keyed state |
| Соединение основного потока с редкими обновлениями правил | Broadcast State |
| Временная корреляция двух потоков событий | Interval Join |
| Большой reference dataset | отдельный stream/table pipeline или внешний cache |
| Персонализированный remote lookup | Async I/O |

### 10.3 Interval Join

Interval join полезен, если есть два потока:

- `agent_request_events`;
- `guardrail_finding_events`.

И надо связать события, пришедшие в интервале, например:

- finding между `request_time - 2s` и `request_time + 30s`.

Ограничение:

- нужен аккуратный event-time design;
- late data и watermarking напрямую влияют на полноту join.

## 11. CEP и паттерны безопасности

### 11.1 Когда CEP оправдан

Пример реального CEP-сценария:

- в одной сессии сначала 2 prompt-injection сигнала с `confidence >= 0.8`,
- потом `systemPromptLeakageDetected = true`,
- потом unusually high output tokens,
- все в пределах 3 минут.

Это уже не просто счетчик, а ordered sequence с time constraints.

### 11.2 Когда CEP избыточен

CEP не нужен, если задача такая:

- "больше трех toxicity hits за 10 минут";
- "looping=true дважды подряд";
- "confidence выше порога".

Это проще и дешевле делается через keyed state и timers.

### 11.3 Пример классификации паттернов

Типовые паттерны под ваш кейс:

- **Jailbreak escalation**: серия prompt injection hits с ростом confidence.
- **Leakage follow-up**: leakage flag после смены роли/system prompt event.
- **Runaway agent**: looping + рост token consumption + retry storm.
- **Multi-signal compromise**: toxicity + prompt injection + leakage в одном request/session scope.

## 12. Broadcast rules и управление политиками

### 12.1 Что выносить во внешние правила

Минимальный набор:

- per-tenant thresholds;
- severity mapping;
- suppression windows;
- routing destination;
- policy enable/disable flags;
- временные hotfix rules во время инцидента;
- allow lists для известных тестовых агентов.

### 12.2 Чего не делать

Не используйте broadcast state как замену полноценной БД:

- большие справочники там плохо масштабируются;
- каждый parallel task держит копию;
- frequent churn rules увеличивает operational complexity.

### 12.3 Governance

Для банка желательно:

- каждая версия правил имеет `policy_version`;
- есть audit trail кто и когда поменял порог;
- rollback правил возможен без redeploy;
- тестовые правила прогоняются на replay stream до prod rollout.

## 13. Deployment на Kubernetes

### 13.1 Что реально рекомендовать

Для крупной организации обычно разумно рассматривать **Flink Kubernetes Operator** или нативный Kubernetes deployment mode как стандартный путь управления lifecycle job.

Причины:

- декларативное управление job;
- удобнее rolling upgrades и restore;
- лучше automation вокруг savepoint/checkpoint;
- проще встроить в platform engineering и GitOps.

### 13.2 Session mode vs application mode

Для production AIRiskOps чаще лучше **application mode** или operator-managed application deployment, потому что:

- job и его lifecycle явно изолированы;
- меньше риск "общего кластера ради всего подряд";
- проще reasoning по зависимостям и blast radius.

Session cluster полезен:

- для dev/test;
- для shared sandbox;
- для коротких exploratory jobs.

### 13.3 Ресурсы

Стартовые принципы sizing:

- не считать pod count целевой метрикой;
- считать CPU, memory, state size, network shuffle и checkpoint duration;
- для stateful jobs осторожно относиться к oversubscription slots.

Практически:

- слишком много slots на один TaskManager усложняет GC, memory contention и noisy neighbor behavior;
- слишком мало slots увеличивает число pod'ов и orchestration overhead.

### 13.4 Черновой манифест deployment

Ниже только иллюстрация формы, а не финальная production-конфигурация:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: airiskops-guardrails
spec:
  image: registry.bank.local/flink/airiskops-guardrails:1.0.0
  flinkVersion: v2_1
  serviceAccount: flink
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.checkpoints.dir: s3://bank-flink-prod/checkpoints/airiskops
    state.savepoints.dir: s3://bank-flink-prod/savepoints/airiskops
    execution.checkpointing.interval: "30s"
    execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
  jobManager:
    resource:
      cpu: 1
      memory: "2048m"
  taskManager:
    resource:
      cpu: 2
      memory: "8192m"
  job:
    jarURI: local:///opt/flink/usrlib/airiskops-guardrails.jar
    parallelism: 8
    upgradeMode: savepoint
    state: running
```

Этот пример надо адаптировать под:

- ваш registry;
- ваш object storage;
- выбранную ветку Flink/operator;
- политику service accounts, network policies и secret management.

## 14. PyFlink: что нужно знать, но что не делать default-путем

### 14.1 Что из исходного конспекта верно

Верно следующее:

- PyFlink позволяет включать Python logic в Flink pipeline;
- core runtime Flink, state, checkpoints и orchestration живут в JVM;
- Python code исполняется через отдельный Python execution path, а не превращает Flink в "Python engine".

### 14.2 Что важно для вашего контекста

Для Java-команды банка базовая рекомендация такая:

- основной pipeline писать на Java;
- Python использовать только если есть сильная причина reuse, например существующий NLP/ML код или исследовательская библиотека;
- все критичные stateful части, alert routing и operational logic держать в JVM.

### 14.3 Почему это важно

Смешанный Java+Python стек означает:

- сложнее packaging;
- сложнее dependency management;
- сложнее debugging;
- сложнее performance reasoning;
- больше moving parts при incident response.

Поэтому PyFlink стоит внедрять как исключение, а не как default.

## 15. Production checklist для банка

### 15.1 Обязательные инженерные решения

- Выбрать canonical event schema и правила versioning.
- Зафиксировать event-time strategy и методику расчета watermark lag.
- Назначить стабильные `uid()` всем stateful операторам.
- Настроить durable checkpoint/savepoint storage.
- Проверить recovery из checkpoint и restore из savepoint на staging.
- Разделить sinks по гарантии доставки и зафиксировать их semantics.
- Ограничить и протестировать volume broadcast rules.
- Вынести thresholds и policy mappings из кода.
- Настроить backpressure, checkpoint, restart и lag monitoring.
- Описать runbook на случай late-data surge, external enrichment outage и savepoint restore failure.

### 15.2 Метрики, без которых нельзя

- input/output throughput;
- end-to-end latency;
- watermark lag;
- checkpoint duration;
- checkpoint failure count;
- backpressure;
- busy time per subtask;
- state size;
- async I/O latency and timeout rate;
- dead-letter volume;
- per-guardrail hit rate;
- per-tenant and per-agent anomaly rate.

### 15.3 Типовые антипаттерны

- Делать blocking HTTP call на каждое событие.
- Держать гигантский mutable object в `ProcessFunction`.
- Не задавать `uid()` stateful операторам.
- Использовать processing time там, где нужен event time.
- Завышать `allowedLateness` без бизнес-необходимости.
- Смешивать raw logs, агрегаты и инциденты в один sink без разных contracts.
- Пытаться хранить большой reference catalog в broadcast state.
- Считать, что `parallelism` напрямую равен числу pod'ов.
- Использовать `FileSource` как production streaming backbone.

## 16. Рекомендуемый поэтапный план внедрения

### Этап 1. MVP

- ingest raw request/response/guardrail events;
- normalization;
- watermarking;
- windowed aggregates;
- базовые per-tenant/per-agent alert counters;
- raw and aggregate sinks.

### Этап 2. Stateful correlation

- keyed session history;
- request-level guardrail merge;
- state TTL;
- side outputs для late/invalid data;
- alert suppression logic.

### Этап 3. Dynamic policy layer

- broadcast rules;
- per-tenant thresholds;
- severity routing;
- policy audit fields.

### Этап 4. Advanced detection

- CEP для multi-step patterns;
- interval join разных event streams;
- async enrichment;
- incident scoring.

### Этап 5. Production hardening

- savepoint-based upgrade drills;
- chaos/recovery drills;
- replay tests;
- performance tuning;
- security hardening;
- cost and capacity review.

## 17. Практические примеры под ваш кейс

### 17.1 Простая нормализация severity

```java
public SeverityBand classifyPromptInjection(double confidence, TenantPolicy policy) {
    if (confidence >= policy.promptInjectionCriticalThreshold()) {
        return SeverityBand.CRITICAL;
    }
    if (confidence >= policy.promptInjectionHighThreshold()) {
        return SeverityBand.HIGH;
    }
    if (confidence >= policy.promptInjectionMediumThreshold()) {
        return SeverityBand.MEDIUM;
    }
    return SeverityBand.LOW;
}
```

Это лучше вызывать из process logic с данными из broadcast rules, а не с hardcoded thresholds.

### 17.2 Корреляция нескольких guardrails по `requestId`

Идея:

- все findings одного `requestId` собираются в keyed state;
- ставится timer на короткое ожидание late siblings;
- по таймеру выпускается consolidated incident event.

Полезно для случаев:

- `prompt_injection confidence=0.91`;
- затем `toxicity confidence=0.77`;
- затем `system_prompt_leakage=true`.

Вместо трех несвязанных алертов вы выпускаете один incident summary.

### 17.3 Session-level anti-looping logic

Если `loopingDetected=true`, это редко стоит трактовать изолированно. Лучше связывать с:

- ростом token consumption;
- повторяющимися похожими prompts;
- отсутствием terminal state у agent workflow;
- числом retries/tool invocations.

То есть сам boolean-guardrail — это не всегда инцидент, а иногда только сильный признак для composite rule.

## 18. Детальная архитектура типичного Flink-приложения

Ниже описана типовая архитектура production Flink-приложения в том виде, в котором ее полезно понимать Java-разработчику.

### 18.1 Логические слои приложения

Почти любое серьезное Flink-приложение состоит из пяти логических слоев:

1. **Ingress layer**:
   - читает события из Kafka, файлов, CDC, очередей или сервисных логов;
   - отвечает за десериализацию, базовую валидацию и метаданные источника.
2. **Normalization layer**:
   - приводит разные входные форматы к единой доменной модели;
   - назначает timestamps и watermark strategy;
   - отбрасывает или маршрутизирует битые записи.
3. **Stateful processing layer**:
   - выполняет `keyBy`, windows, joins, timers, correlation, suppression, CEP;
   - здесь живут основные keyed state и operator state.
4. **Policy and enrichment layer**:
   - подключает внешние правила, справочники и dynamic configuration;
   - использует broadcast state, async I/O или отдельные side streams.
5. **Egress layer**:
   - публикует агрегаты, инциденты, сырые события и dead-letter outputs;
   - отвечает за совместимость с downstream semantics.

Для AIRiskOps эти пять слоев обычно соответствуют такому потоку:

```text
Kafka/FileSource
  -> parse + validate
  -> SafetyEvent normalization
  -> watermark assignment
  -> keyBy(sessionId/requestId/tenantId)
  -> stateful correlation / windows / CEP
  -> broadcast policy application
  -> enrichment
  -> incident routing / metrics / archival sinks
```

### 18.2 Внутренняя структура job

Типичное Flink-приложение в коде выглядит не как "один длинный метод", а как композиция этапов:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<RawEvent> raw = buildSource(env);

DataStream<SafetyEvent> normalized = raw
    .process(new ParseAndValidateFunction())
    .assignTimestampsAndWatermarks(buildWatermarkStrategy());

KeyedStream<SafetyEvent, SessionKey> bySession =
    normalized.keyBy(SafetyKeySelectors.bySession());

DataStream<IncidentCandidate> candidates = bySession
    .process(new SessionCorrelationFunction());

BroadcastStream<TenantPolicy> policies = buildPolicyStream(env)
    .broadcast(POLICY_STATE_DESCRIPTOR);

DataStream<Incident> incidents = candidates
    .connect(policies)
    .process(new PolicyAwareIncidentFunction());

writeSinks(incidents);

env.execute("airiskops-guardrails");
```

Что здесь важно:

- source, normalization, correlation, policy application и sinks лучше держать как отдельные явно именованные стадии;
- stateful операторы надо делать маленькими и предметными;
- `keyBy()` меняет физику исполнения, потому что почти всегда влечет shuffle;
- `connect(...).process(...)` с broadcast stream является стандартным production-паттерном для dynamic rules.

### 18.3 Что происходит после `env.execute()`

После сборки job Flink runtime делает примерно следующее:

1. Превращает код в **JobGraph**.
2. Определяет, какие операторы можно chain'ить.
3. Преобразует граф в физические execution vertices и subtasks.
4. Назначает subtasks в available slots.
5. Поднимает сетевые каналы между upstream и downstream subtasks.
6. Запускает processing loops на TaskManager'ах.
7. Координирует checkpoints и recovery.

Практический смысл для разработчика:

- даже маленькое изменение `keyBy`, `rebalance`, `rescale`, `union`, window или join может заметно поменять physical execution;
- код "выглядит линейно", а исполняется как параллельный распределенный граф.

### 18.4 Типичный hot path одного события

Один event в обычном stateful Flink pipeline проходит такой путь:

1. Source reader читает record из Kafka/file split.
2. Record десериализуется в JVM-объект.
3. Timestamp assigner извлекает event time.
4. Watermark strategy обновляет локальное представление прогресса времени.
5. Record проходит через chain локальных операторов без network hop, если chaining возможен.
6. После `keyBy()` или другого repartitioning record сериализуется и уходит в network stack.
7. Downstream subtask десериализует record, вычисляет key group и обращается к keyed state.
8. Operator обновляет state, может поставить timer, выпустить side output или основной результат.
9. Sink сериализует итоговое событие и отправляет наружу.

Где чаще всего живут реальные издержки:

- serialization/deserialization;
- shuffle между TaskManager'ами;
- state access under load;
- skew по ключам;
- медленный sink или enrichment;
- checkpoint barriers под backpressure.

### 18.5 Физическая архитектура типичного deployment

На Kubernetes типичное Flink-приложение состоит из:

- **JobManager pod**:
  - координирует job;
  - следит за execution state;
  - инициирует checkpoints;
  - принимает решения о recovery.
- **TaskManager pods**:
  - исполняют subtasks;
  - держат network buffers;
  - хранят working state и участвуют в snapshotting.
- **External durable storage**:
  - object storage или distributed filesystem для checkpoints/savepoints;
  - без него production recovery design неполон.
- **External sources and sinks**:
  - Kafka, SIEM, case management, lakehouse, operational DB, metrics systems.

Типовая схема:

```text
         +-------------------+
         |     JobManager    |
         | scheduling/RPC    |
         | checkpoints       |
         +---------+---------+
                   |
      +------------+-------------+
      |                          |
+-----v------+            +------v-----+
| TaskManager|            | TaskManager|
|  slots     |  <Netty>   |  slots     |
| subtasks   |            | subtasks   |
+-----+------+            +------+-----+
      |                          |
      +------------+-------------+
                   |
         +---------v---------+
         | durable snapshots |
         | checkpoints/save  |
         +-------------------+
```

### 18.6 Как thinking должен меняться у Java-разработчика

В обычном service backend вы часто думаете так:

- запрос пришел;
- код выполнился;
- ответ ушел;
- память объекта больше никому не нужна.

Во Flink мышление другое:

- поток бесконечный или большой bounded;
- operator живет долго;
- state переживает отдельные records;
- time semantics задаете вы;
- ошибка в key design или watermark strategy масштабируется на весь pipeline;
- upgrade влияет не только на код, но и на совместимость state.

То есть типичное Flink-приложение надо проектировать не как "метод обработки сообщения", а как "долгоживущую распределенную state machine".

### 18.7 Типовое разделение на несколько job

Не все надо запихивать в один giant job. Типовая production-разбивка выглядит так:

- **Job A: Raw ingest and normalization**
  - читает сырые логи;
  - валидирует schema;
  - публикует canonical `SafetyEvent`.
- **Job B: Real-time correlation and incidenting**
  - берет canonical stream;
  - делает stateful detection;
  - выпускает incidents и metrics.
- **Job C: Replay / reconciliation**
  - переигрывает late или corrected data;
  - сверяет инциденты и агрегаты.
- **Job D: Reference data preparation**
  - готовит компактные справочники или policy streams для downstream broadcast use.

Когда лучше один job:

- low operational overhead;
- мало независимых команд-владельцев;
- tight event-time correlation между всеми стадиями.

Когда лучше несколько job:

- разный SLA;
- разные owners;
- разные паттерны масштабирования;
- нужно изолировать blast radius;
- replay и online path должны жить отдельно.

### 18.8 Типовой packaging Java-приложения

Для вашей аудитории стандартный путь такой:

- Java/Scala code собирается в один application JAR;
- коннекторы и зависимости поставляются либо в fat JAR, либо через image/usrlib strategy;
- job конфигурация приходит через `flink-conf.yaml`, operator CRD, environment variables или args;
- deployment artifact лучше делать воспроизводимым и immutable.

Для production это обычно означает:

- versioned container image;
- versioned job artifact;
- versioned config;
- отдельный путь для savepoint-based upgrades.

### 18.9 Типовая схема отказов

С точки зрения эксплуатации типичное Flink-приложение должно проектироваться под такие отказы:

- падение TaskManager;
- временная недоступность sink;
- slow consumer и backpressure;
- spike late events;
- object storage slowdown для checkpointing;
- schema drift в upstream;
- rule misconfiguration в broadcast stream;
- сетевые лаги между worker nodes.

Хорошее Flink-приложение заранее отвечает на вопросы:

- что будет со state при restart;
- где лежат checkpoints/savepoints;
- какой restart strategy используется;
- как быстро job восстанавливается;
- что будет при partial sink outage;
- как replay влияет на downstream duplication semantics.

## 19. Краткий инженерный вердикт

Формула истины для вашего кейса:

**Flink стоит внедрять там, где guardrail-события нужно не просто пересылать, а коррелировать по времени, ключу и истории с гарантируемым recovery state.**

Decision rule:

- выбирайте Flink, если вам нужен stateful event-time pipeline с корреляцией и recovery;
- не выбирайте Flink как первую систему, если задача пока ограничивается простыми счетчиками без state и без строгой real-time логики;
- не делайте PyFlink default для Java-centric банковской команды;
- не используйте `FileSource` как основной production ingest, даже если для POC он удобен.

Предупреждение для стикера:

**Самая частая ошибка во Flink-проектах — недооценить не код обработки, а стоимость state, lateness и upgrade discipline.**

## 20. Неизвестные и спорные утверждения

- Без вашей внутренней матрицы поддержки нельзя зафиксировать единственную "правильную" версию Flink для production.
- Для конкретной рекомендации по RocksDB/ForSt state backend, memory fractions и checkpoint tuning нужен отдельный workload-based sizing.
- Для окончательного выбора между pure DataStream и Table/SQL hybrid нужен реальный список sinks, schemas и downstream consumers.
- Для precise deployment guidance на Kubernetes нужна ваша целевая модель: native Flink deployment, Flink K8s Operator, service mesh restrictions, object storage, secrets, observability stack.

## 21. Источники

Ниже указаны первоисточники, по которым сверялись ключевые утверждения:

1. Apache Flink Docs, Architecture Overview: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/
2. Apache Flink Docs, Task Lifecycle and Resources: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/#task-slots-and-resources
3. Apache Flink Docs, Event Time and Watermarks: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/
4. Apache Flink Docs, Windowing and Allowed Lateness: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/
5. Apache Flink Docs, Working with State and State TTL: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/
6. Apache Flink Docs, Broadcast State Pattern: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/broadcast_state/
7. Apache Flink Docs, Joins and Interval Join: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/joining/
8. Apache Flink Docs, FileSource: https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/filesystem/
9. Apache Flink Docs, Kubernetes Deployment: https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/native_kubernetes/
10. Apache Flink Kubernetes Operator Docs: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/
11. Apache Flink Docs, Debugging Backpressure: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/
12. Apache Flink Docs, Checkpoints and Fault Tolerance: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/
13. Apache Flink Docs, Checkpointing under Backpressure: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpointing_under_backpressure/
14. Apache Flink Docs, PyFlink Overview: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/overview/
15. Apache Flink Docs, Python Dependency Management: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/dependency_management/
16. Apache Flink Docs, Python UDF internals and execution mode references: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/udfs/python_udfs/
17. Apache Flink CEP Docs: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/libs/cep/

## 22. Список ссылок на документацию

Ниже отдельный список документации по категориям, чтобы manual можно было использовать как рабочий reference index.

### Базовая архитектура и runtime

- Flink Architecture Overview: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/
- Task Slots and Resources: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/#task-slots-and-resources
- State and Fault Tolerance Overview: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/
- Checkpointing: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/
- Checkpointing under Backpressure: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpointing_under_backpressure/

### Время, окна и joins

- Event Time and Watermarks: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/
- Windows: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/
- Joining: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/joining/
- Broadcast State: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/broadcast_state/
- CEP: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/libs/cep/

### Источники, sinks и deployment

- FileSystem / FileSource connector docs: https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/filesystem/
- Native Kubernetes Deployment: https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/native_kubernetes/
- Flink Kubernetes Operator Docs: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/
- Monitoring Back Pressure: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/

### PyFlink

- PyFlink Overview: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/overview/
- Python Dependency Management: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/dependency_management/
- Python UDFs: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/udfs/python_udfs/
