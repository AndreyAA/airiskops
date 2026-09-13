# Similar Prompt Injection Campaign: подробный план реализации

Дата актуальности: 2026-09-13

Связанная спецификация: [similar-prompt-injection-feature.md](similar-prompt-injection-feature.md)

Статус: основные этапы реализации выполнены; остаются event-time hardening,
расширенная диагностика и полноценный Docker E2E/recovery acceptance.

## 0. Фактический прогресс на 2026-09-04

| Этап | Статус | Что осталось |
|---|---|---|
| 0. Defaults | Частично | Calibration corpus и измерения на настоящей ONNX-модели не подготовлены. |
| 1. Event contract | Реализован | Нужен отдельный compatibility acceptance на внешних producers. |
| 2. Embedding abstraction и ONNX adapter | Реализован | Требуется эксплуатационная проверка длительного inference runtime и failure metrics. |
| 3. Similarity state | Частично | Есть keyed bounded clusters и дедупликация, но нет удаления отдельных устаревших findings и retention с allowed lateness. |
| 4. Incident contract и metrics | Частично | Incident и совместимые counters добавлены; не хватает полного набора detector diagnostics и acceptance для revisions/state limits. |
| 5. Topology | Реализован | Similarity branch topology-level отключается; savepoint compatibility требует отдельной проверки. |
| 6. Replay/live generators | Реализован частично | Replay fixtures добавлены; live progression требует отдельного длительного acceptance-прогона. |
| 7. Model artifact и deployment | Реализован | Custom image, local artifacts и manifest/checksum validation добавлены; startup smoke требует отдельной автоматизации. |
| 8. Grafana и observability | Частично | Панель и incident counters добавлены; отсутствуют embedding health metrics и фактическая проверка series на обновлённой job. |
| 9. End-to-end acceptance | Частично | Общий smoke и manual similarity replay описаны; не хватает автоматической проверки rule payload, negative-agent isolation, failure scenarios и checkpoint recovery. |

Изменение rolling window, watermark или allowed-lateness поведения выполняется
отдельным согласованным изменением согласно корневому `AGENTS.md`. До этого
текущий кластерный expiry нельзя считать нормативной реализацией разделов 5.3 и
11.3 этого плана.

## 1. Цель реализации

Добавить в AIRiskOps Flink MVP отдельную ветку, которая строит локальные embeddings для triggered prompt-injection evidence, группирует похожие атаки одного агента между разными сессиями в rolling event-time окне и выпускает наблюдаемый `SIMILAR_PROMPT_INJECTION_CAMPAIGN` incident.

Изменение затрагивает event contract, Java/Flink runtime, generator tooling, deployment модели, метрики, Grafana и документацию. Работы следует выполнять этапами, сохраняя проверяемость после каждого этапа.

## 2. Этап 0. Зафиксировать и проверить concept defaults

Реализация начинается со следующих согласованных значений:

- rolling event-time window: `5m`;
- cosine similarity threshold: `0.5`;
- minimum unique requests: `3`;
- minimum distinct sessions: `2`;
- severity: `HIGH`;
- при `incidentEmitUpdates=true` выпускаются revision updates;
- `BasicIncident` содержит до пяти representative evidence samples;
- одиночный `BasicIncident.sessionId` содержит triggering session;
- ONNX model/tokenizer/manifest включаются в custom Flink image;
- state limits используются из раздела 4.2 этого плана.

Порог `0.5` намеренно оптимизирован для демонстрации срабатывания, а не для production precision. До production-like настройки отдельно подготовить calibration corpus:

- positive pairs одной prompt-injection family;
- hard negative pairs разных attack families;
- русские, английские и смешанные варианты;
- варианты с изменённым регистром, пробелами и служебными вставками.

На закреплённом ONNX artifact сохранить распределения cosine similarity и рекомендованное будущее значение threshold. Этот анализ не меняет начальный concept default без отдельного решения.

Результат этапа:

- config keys и defaults однозначно записаны в spec и тестовых expectations;
- calibration corpus подготовлен либо заведён как явно отложенная production-hardening задача;
- в реализации не остаётся неявных TBD.

## 3. Этап 1. Расширить входной event contract

### 3.1 Domain model

Изменить:

- `../../flink-job/src/main/java/com/bank/airiskops/model/SafetyEvent.java`;
- при необходимости связанные test builders/fixtures.

Добавить `String evidenceSnippet` рядом с guardrail-specific полями. Поле не должно извлекаться из `rawPayload` downstream-кодом.

### 3.2 Parser

Изменить:

- `../../flink-job/src/main/java/com/bank/airiskops/infra/parser/SafetyEventParser.java`.

Добавить константу JSON field и optional parsing. Отсутствие поля не является parse error.

### 3.3 Parser и serde tests

Обновить:

- `SafetyEventParserTest`;
- `JsonSerdeTest`/`JsonSerializeFunctionTest`, если их assertions зависят от полного payload;
- все прямые вызовы конструктора `SafetyEvent` в unit/integration tests.

Проверить:

- поле читается без потери Unicode;
- отсутствие поля обратно совместимо;
- blank значение не делает общий finding invalid, но будет отфильтровано similarity-веткой;
- `rawPayload` по-прежнему хранит исходный JSON.

### 3.4 Документация контракта

Обновить:

- `../../docs/architecture/event-contracts.md`;
- `../../README.md`, если список ключевых полей приводится там;
- связанные MVP specifications и runbooks, где показан пример `GUARDRAIL_FINDING`.

Business value этапа: finding получает явный, машиночитаемый материал для semantic correlation.

Проверка этапа:

```bash
bash tools/scripts/run-regression.sh
```

## 4. Этап 2. Добавить embedding abstraction и локальный ONNX adapter

### 4.1 Dependencies

Изменить `../../flink-job/pom.xml`:

- добавить согласованную версию LangChain4j in-process embeddings;
- добавить только необходимые ONNX-related artifacts;
- проверить shade configuration и отсутствие конфликтов Jackson/SLF4J/native libraries.

Не добавлять vector database: для пятиминутного bounded state поиск выполняется внутри keyed operator.

### 4.2 Конфигурация

Добавить отдельный config record, например `SimilarPromptInjectionRuleConfig`, с полями:

- `enabled`;
- `window`;
- `similarityThreshold`;
- `minUniqueRequests`;
- `minDistinctSessions`;
- `severity`;
- `embeddingModelPath`;
- `embeddingTokenizerPath`;
- `embeddingModelVersion`;
- `embeddingInputPrefix`;
- `maxEvidenceLength`;
- `maxClustersPerKey`;
- `maxTrackedRequestIdsPerCluster`;
- `maxRequestIdsPerIncident`;
- `maxSessionIdsPerCluster`;
- `maxEvidenceSamplesPerCluster`;
- `maxStoredEvidenceSampleLength`;
- overflow policy.

Concept defaults:

```yaml
incidentSimilarPromptInjectionEnabled: true
incidentSimilarPromptInjectionWindowMinutes: 5
incidentSimilarPromptInjectionThreshold: 0.5
incidentSimilarPromptInjectionMinUniqueRequests: 3
incidentSimilarPromptInjectionMinDistinctSessions: 2
incidentSimilarPromptInjectionSeverity: HIGH
incidentSimilarPromptInjectionMaxClustersPerKey: 20
incidentSimilarPromptInjectionMaxTrackedRequestIdsPerCluster: 100
incidentSimilarPromptInjectionMaxRequestIdsPerIncident: 50
incidentSimilarPromptInjectionMaxSessionIdsPerCluster: 20
incidentSimilarPromptInjectionMaxEvidenceSamplesPerCluster: 5
incidentSimilarPromptInjectionMaxEvidenceLengthChars: 1000
incidentSimilarPromptInjectionMaxStoredEvidenceSampleLengthChars: 500
```

State retention вычисляется как `window + lateTolerance`; при текущих локальных значениях это `10m`.

Overflow policy:

- сначала удалить все истёкшие кластеры;
- если лимит 20 всё ещё достигнут, вытеснить кластер с минимальным `lastEventTimeMillis`, используя `clusterId` как deterministic tie-breaker;
- после 100 tracked request IDs не принимать новые уникальные findings в этот кластер и увеличить overflow counter;
- output lists обрезать независимо до 50 request IDs, 20 session IDs и пяти evidence samples.

Провести параметры через:

- `JobConfigOptions`;
- `JobConfig`;
- `../../config/job/local-job.yaml`;
- CLI/YAML config tests.

Не переиспользовать `incidentPiAndToxicWindowMinutes`: новое правило имеет собственную семантику и независимо настраивается.

### 4.3 Adapter boundary

Добавить небольшой интерфейс, пригодный для unit testing, например:

```java
public interface EvidenceEmbedder extends AutoCloseable {
    float[] embed(String evidenceSnippet);
    String modelVersion();
}
```

Реализацию разместить в `com.bank.airiskops.infra`, например `OnnxEvidenceEmbedder`.

Ответственность adapter-а:

- загрузить ONNX и tokenizer по локальным путям;
- добавить согласованный input prefix;
- выполнить synchronous inference;
- выполнить L2 normalization;
- проверить dimension и конечность компонент;
- корректно закрыть native resources, если API это требует;
- не логировать evidence text.

### 4.4 Flink lifecycle function

Добавить `RichMapFunction<SafetyEvent, EmbeddedGuardrailFinding>` в `com.bank.airiskops.app.functions`.

Требования:

- embedder field является `transient`;
- создаётся в `open()`;
- переиспользуется в `map()`;
- освобождается в `close()`;
- function не хранит model bytes в checkpoint state;
- failures учитываются отдельным low-cardinality counter и обрабатываются согласованным способом.

### 4.5 Модель результата

Добавить `EmbeddedGuardrailFinding` в `com.bank.airiskops.model`:

- identity fields;
- `eventTimeMillis`;
- `guardrailName`;
- `embeddingModelVersion`;
- `float[] embedding`;
- bounded/normalized evidence sample для последующего включения representative samples в incident payload.

Unit tests:

- adapter contract тестируется deterministic fake embedder-ом без ONNX artifact;
- отдельный integration/smoke test запускается с реальным model/tokenizer;
- malformed model path приводит к понятной startup error;
- blank/oversized evidence обрабатывается согласно config;
- нормализация и version propagation проверяются явно.

Business value этапа: JVM получает локальный воспроизводимый semantic representation без сетевой зависимости.

## 5. Этап 3. Реализовать similarity keyed state

### 5.1 Ключ

Добавить `SimilarAttackKey` и `SimilarAttackKeySelector`.

Ключ обязан включать:

```text
tenantId, agentId, guardrailName, embeddingModelVersion
```

`sessionId` остаётся атрибутом evidence внутри state.

### 5.2 Cluster state model

Добавить Flink-serializable state model, содержащий:

- стабильный `clusterId`;
- normalized centroid `float[]`;
- unique finding count;
- bounded unique `requestIds`;
- bounded unique `sessionIds`;
- до пяти representative evidence samples длиной до 500 символов;
- `firstEventTimeMillis`;
- `lastEventTimeMillis`;
- emission revision/status;
- embedding model version.

Проверить serializer compatibility и явно зафиксировать state descriptor name. Не переиспользовать существующий `session-incident-snapshot`, потому что его key и lifecycle не соответствуют межсессионной задаче.

### 5.3 Similarity function

Добавить чистые функции:

- cosine similarity;
- centroid update;
- best-cluster selection;
- time-window membership;
- unique request/session counting.

Требования:

- dimension mismatch не игнорируется;
- zero vector не принимается;
- `NaN`/infinite components не попадают во state;
- при равном score выбор кластера детерминирован;
- threshold comparison использует явно документированную включённую границу `>=`.

### 5.4 Keyed process function

Добавить отдельный `KeyedProcessFunction<SimilarAttackKey, EmbeddedGuardrailFinding, BasicIncident>` либо специальную внутреннюю incident candidate model с последующим mapper-ом.

На каждом finding:

1. Очистить evidence/clusters за пределами rolling event-time window.
2. Проверить dedup по `requestId`.
3. Найти ближайший centroid.
4. Присоединить finding либо создать кластер.
5. Зарегистрировать event-time cleanup timer.
6. Проверить `minUniqueRequests` и `minDistinctSessions`.
7. При `3` unique requests и `2` distinct sessions выпустить incident с `severity=HIGH`.
8. Если кластер уже emitted и `incidentEmitUpdates=true`, выпустить revision update на следующий уникальный finding; иначе не повторять emission.
9. В `BasicIncident.sessionId` записать session triggering finding.
10. Включить в incident bounded representative evidence samples.
11. Обновить bounded state.

`onTimer()` удаляет истёкшие элементы и очищает пустой keyed state.

### 5.5 Unit и harness tests

Покрыть как минимум:

- похожие attacks в двух сессиях одного агента создают incident;
- одна сессия не создаёт cross-session incident;
- разные агенты изолированы;
- разные tenants изолированы;
- разные model versions изолированы;
- ниже threshold создаются разные clusters;
- finding ровно на границе окна учитывается;
- finding за границей окна не учитывается;
- out-of-order finding обрабатывается согласно event-time semantics;
- duplicate `requestId` не увеличивает count;
- state cleanup timer освобождает state;
- max cluster limit и overflow policy;
- first emission, отсутствие update при `incidentEmitUpdates=false` и revision update при `incidentEmitUpdates=true`;
- checkpoint snapshot/restore сохраняет active clusters;
- invalid vector не повреждает operator state.

Business value этапа: похожие атаки разных сессий одного агента становятся единым детектируемым паттерном.

## 6. Этап 4. Расширить incident output

### 6.1 Rule name

Добавить `SIMILAR_PROMPT_INJECTION_CAMPAIGN` в `IncidentRuleNames`.

### 6.2 BasicIncident

Аддитивно расширить output contract:

- добавить `sessionIds`;
- добавить bounded `evidenceSamples`;
- добавить `embeddingModelVersion`;
- использовать `triggeredFindingsCount` как число уникальных request IDs в similarity cluster независимо от bounded output list;
- для нового rule сохранять в одиночном `sessionId` сессию triggering finding;
- для нового rule устанавливать `severity=HIGH`;
- сохранить существующие getters/setters для Flink/Jackson POJO serialization.

Не включать `float[] embedding` в incident payload.

### 6.3 Incident identity

Incident ID строить на уровне campaign cluster, а не отдельной сессии. Формула должна быть стабильной для повторной доставки и revision updates и не содержать raw evidence.

Все revision updates одного кластера используют тот же `incidentId`; `emissionRevision` увеличивается монотонно.

### 6.4 Metrics

Similarity detector должен увеличивать совместимые incident counters:

- общий `incidents_emitted_total`;
- per-rule counter с `rule=SIMILAR_PROMPT_INJECTION_CAMPAIGN`;
- severity counter;
- update counter при включённых revisions.

Допустимые дополнительные технические метрики:

- embeddings generated;
- embedding failures;
- findings skipped because evidence is missing;
- clusters created/expired;
- findings deduplicated;
- state limit drops/evictions.

Labels ограничить статическими значениями типа result/reason/model version. Не использовать agent/session/request/evidence labels.

Tests:

- JSON serialization нового incident;
- backward-compatible serialization прежних incidents;
- стабильный incident ID;
- корректные counters в operator harness.

Business value этапа: campaign доступна downstream consumers как самостоятельный операционный incident.

## 7. Этап 5. Подключить topology

Изменить `IncrementOneTopologyBuilder`:

1. От существующего validated/on-time `guardrailFindings` создать отдельную ветку.
2. Отфильтровать triggered prompt injection с evidence.
3. Выполнить embedding map operator.
4. Назначить стабильные operator names и `uid()`.
5. Выполнить `keyBy(SimilarAttackKeySelector)`.
6. Подключить similarity process function.
7. Объединить новый stream incidents с существующим stream `BasicIncident` перед существующим Kafka sink либо использовать отдельный serializer с тем же topic без изменения topic contract.

Предпочтителен единый sink после union, если это не требует смены существующего sink UID/state semantics. До изменения topology проверить последствия для savepoint compatibility.

Добавить topology tests:

- новый operator присутствует;
- старые names/UIDs не меняются;
- destination остаётся `basic-incidents`;
- evidence-less findings не идут в embedding branch;
- существующие aggregate и incident branches сохраняются.

Business value этапа: новая логика становится частью основного streaming pipeline без изменения Kafka topics.

## 8. Этап 6. Обновить replay и live generators

### 8.1 Общая генерация evidence

Изменить `../../tools/generators/generate_events.py`:

- добавить `evidenceSnippet` в prompt-injection findings там, где это соответствует сценарию;
- сохранить отсутствие evidence для части контрольных событий, чтобы проверить backward compatibility;
- добавить счётчик evidence-bearing findings в replay metadata/summary;
- не дублировать логику в live generator: использовать общий generation path.

### 8.2 Новый deterministic scenario

Добавить `similar_prompt_injection_campaign` в `SCENARIOS` и отдельную attack-family generation logic.

Набор должен содержать:

- не менее одной positive attack family с перефразированными русскими/английскими или смешанными snippets;
- минимум три уникальных request ID;
- минимум две сессии одного агента;
- timestamps внутри configured window;
- negative controls, не относящиеся к той же semantic family;
- достаточно высокую guardrail confidence/triggered status для попадания в similarity branch.

Тексты scenario fixtures должны быть статическими и reviewable; случайность выбирает варианты и порядок, но не генерирует неконтролируемый смысл.

### 8.3 Generator tests

Обновить `../../tools/tests/test_generate_events.py`:

- scenario доступен CLI parser-у;
- replay deterministic при одинаковом seed;
- positive family распределена по разным sessions;
- `requestId` уникальны;
- `evidenceSnippet` присутствует у campaign findings;
- negative controls присутствуют;
- timestamps укладываются в тестовое окно;
- live tick generation сохраняет campaign progression между ticks;
- старые scenarios продолжают работать.

Проверки:

```bash
python3 -m py_compile tools/generators/generate_events.py tools/generators/stream_live_events.py
python3 -m unittest tools.tests.test_generate_events
```

Business value этапа: фичу можно воспроизводимо продемонстрировать без ручной публикации Kafka JSON.

## 9. Этап 7. Model artifact и deployment

Модель включается в custom Flink image:

1. Добавить Dockerfile для локальных Flink JobManager/TaskManager на базе текущего `flink:1.20.2-scala_2.12-java17`.
2. Закрепить точную model revision и checksum ONNX/tokenizer.
3. На image build скопировать ONNX, tokenizer и manifest в неизменяемый каталог, например `/opt/flink/models/multilingual-e5-small/`.
4. В manifest записать model name, revision, vector dimension, pooling, input prefix и quantization.
5. Перевести оба сервиса `jobmanager` и `taskmanager` в `../../deployment/local/docker-compose.yml` на один custom image/build definition.
6. Указать model/tokenizer paths в `../../config/job/local-job.yaml`.
7. Запретить неявный download при старте job; отсутствие artifact должно завершать startup понятной ошибкой.
8. Проверить Linux native ONNX Runtime в фактическом TaskManager image.
9. Оценить память с учётом одного экземпляра модели на parallel subtask.
10. Задать явный parallelism embedding operator для локального MVP.
11. Ограничить внутренний executor модели, чтобы несколько subtasks не oversubscribe CPU.

Model artifacts не должны подменяться host volume из `../../runtime`: image является источником истины для inference runtime.

Добавить startup smoke test: model/tokenizer читаются, один fixture превращается в вектор ожидаемой dimension.

Business value этапа: локальный сценарий запускается воспроизводимо и не зависит от доступности внешнего model registry.

## 10. Этап 8. Grafana и observability

### 10.1 Dashboard

Изменить:

- `../../observability/grafana/dashboards/airiskops-incidents.json`.

Добавить stat/time-series panel:

```text
Similar Prompt Injection Campaign Incidents 5m
```

PromQL использует существующий per-rule metric и фильтр:

```promql
sum(increase(
  flink_taskmanager_job_task_operator_airiskops_incident_rule_incidents_emitted_total{
    job_name="AIRiskOps_MVP_Increment_1",
    rule="SIMILAR_PROMPT_INJECTION_CAMPAIGN"
  }[5m]
))
```

Проверить, что `Incidents By Rule 5m` показывает новый rule без отдельного query.

### 10.2 Monitoring docs

Обновить:

- `../../docs/monitoring/monitoring-debugging-guide.md`;
- `../../docs/runbooks/local-walkthrough.md`;
- `../../docs/runbooks/mvp-runbook.md`;
- `../../README.md` и `../../docs/README.md`, если добавляются новые документы или команды.

Описание панели должно отвечать:

- что измеряется;
- почему рост означает межсессионную похожую атаку;
- как отличить отсутствие атак от сбоя embedding operator;
- какие Kafka messages проверить для drill-down.

### 10.3 Observability checks

- dashboard JSON валиден;
- Prometheus видит новый rule counter;
- label cardinality не растёт от IDs или текста;
- replay/live scenario делает panel ненулевой;
- embedding failures наблюдаемы отдельно от отсутствия campaign incidents.

Business value этапа: результат виден risk/operations пользователю без чтения сырых Kafka сообщений.

## 11. Этап 9. End-to-end acceptance

### 11.1 Automated regression

Выполнить:

```bash
bash tools/scripts/run-regression.sh
bash tools/scripts/build-job.sh
python3 -m py_compile tools/generators/generate_events.py tools/generators/stream_live_events.py
```

### 11.2 Local scenario

1. Поднять stack по существующему local walkthrough.
2. Запустить replay нового scenario минимум с двумя сессиями:

```bash
bash tools/scripts/run-replay.sh \
  --scenario similar_prompt_injection_campaign \
  --requests 12 \
  --sessions 3 \
  --agent-id agent-risk-01
```

Generator scenario должен гарантировать, что при этих параметрах не менее трёх похожих атак одного агента распределены минимум по двум сессиям внутри пяти минут.

3. Прочитать `guardrail-findings` и подтвердить `evidenceSnippet`.
4. Прочитать `normalized-events` и подтвердить structured field.
5. Прочитать `basic-incidents` и найти `SIMILAR_PROMPT_INJECTION_CAMPAIGN`.
6. Проверить, что incident содержит минимум две sessions и ожидаемые requests.
7. Проверить Prometheus per-rule counter.
8. Открыть `AIRiskOps Incidents` и подтвердить dedicated panel и общую rule series.
9. Повторить с negative/control dataset и подтвердить отсутствие ложного объединения.
10. Повторить с одинаковыми атаками разных agents и подтвердить изоляцию.

### 11.3 Failure scenarios

Проверить:

- model file отсутствует;
- tokenizer отсутствует;
- evidence blank/oversized;
- embedding inference возвращает ошибку;
- vector dimension неожиданно изменилась;
- state limit достигнут;
- finding приходит out of order;
- duplicate Kafka message;
- TaskManager restart/checkpoint restore в середине активного кластера.

## 12. Рекомендуемая последовательность pull requests

Чтобы не смешивать большой refactoring и business feature:

1. **Contract + generator fixtures**
   - `evidenceSnippet`;
   - parser/tests/docs;
   - новый scenario без включения detector-а.
2. **Embedding adapter**
   - dependency/config/model lifecycle;
   - fake и real-model tests;
   - deployment artifact path.
3. **Similarity state + incident contract**
   - key/cluster/process function;
   - `sessionIds`;
   - harness tests.
4. **Topology + metrics**
   - branch wiring;
   - sink integration;
   - topology/regression tests.
5. **Grafana + runbooks + E2E**
   - panel;
   - PromQL;
   - local walkthrough;
   - recorded acceptance results.

После каждого PR должны быть указаны:

- что изменилось;
- команда проверки;
- business value;
- совместимость contracts/state/UIDs;
- оставшиеся production-hardening или calibration задачи.
