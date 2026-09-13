# SDD: Similar Prompt-Injection Campaign Detection

Статус: `Draft`  
Дата: `2026-09-12`  
Область: AIRiskOps Flink MVP  
Кодовая база: `feature/similar-prompt-injection-rocksDB-onyx`

Этот документ является нормативной спецификацией фичи в формате
Spec-Driven Development. Он описывает требуемое поведение, контракты,
инварианты и критерии приёмки. Фактический reverse-engineering текущей ветки
сохраняется отдельно в
`specs/similar-prompt-injection-branch-summary.md`.

Спецификация отдельного topology-level toggle вынесена в
`specs/similar-prompt-injection-topology-toggle-sdd.md`.

## 1. Problem And Outcome

Одиночный guardrail finding недостаточен для обнаружения распределённой атаки:
один и тот же агент может получать семантически похожие prompt injections из
разных пользовательских сессий. Система должна объединять такие findings в
ограниченную по времени кампанию и выдавать один операционный incident.

Ожидаемый результат:

- обнаруживать похожие `PROMPT_INJECTION` findings между сессиями;
- изолировать tenants, agents и версии embedding-модели;
- сохранять прежние incident rules без изменения их semantics;
- публиковать bounded incident payload для расследования;
- работать локально без сетевого inference в event-processing path;
- иметь измеримые runtime, state и failure signals.

## 2. Scope

### 2.1 In scope

- optional `evidenceSnippet` во входном `SafetyEvent`;
- локальное embedding evidence;
- cosine-similarity clustering;
- cross-session correlation одного tenant/agent;
- rolling event-time window;
- bounded managed keyed state;
- новый incident rule `SIMILAR_PROMPT_INJECTION_CAMPAIGN`;
- deterministic implementation для тестов;
- ONNX implementation для локального runtime;
- replay scenario и проверка output contract;
- counters и Grafana visibility.

### 2.2 Out of scope

- удалённый embedding service;
- изменение semantics существующих incident rules;
- добавление embedding-вектора во входной или выходной Kafka contract;
- обучение или fine-tuning модели;
- approximate nearest-neighbor index;
- глобальная корреляция между разными agents;
- автоматическое изменение similarity threshold по ходу runtime.

## 3. Terminology

| Термин | Определение |
|---|---|
| Finding | Валидный `GUARDRAIL_FINDING` с guardrail metadata и event time. |
| Evidence | Опциональный текстовый `evidenceSnippet`, объясняющий finding. |
| Embedding | Нормализованный `float[]`, построенный локальным embedder-ом. |
| Similarity cluster | Активный state object с centroid, request IDs, sessions и evidence samples. |
| Campaign | Cluster, достигший минимальных request и session thresholds. |
| Similarity key | `tenantId + agentId + guardrailName + embeddingModelVersion`. |
| Run | Одна конфигурация similarity detector и её event-time interval. |

## 4. Functional Requirements

### FR-01. Optional evidence contract

`SafetyEvent` MUST accept optional string field `evidenceSnippet`.

Если поле отсутствует, равно `null` или состоит только из whitespace:

- parser MUST preserve the event as valid, если остальные обязательные поля
  корректны;
- существующие aggregate и incident branches MUST continue processing the
  event;
- similarity branch MUST skip the event;
- embedding inference MUST NOT запускаться.

### FR-02. Similarity branch filter

Similarity branch MUST process only findings, для которых одновременно
выполнено:

```text
eventType = GUARDRAIL_FINDING
guardrailName = PROMPT_INJECTION
triggered = true
evidenceSnippet is non-null and non-blank
```

Другие guardrails, `triggered=false`, invalid events, late events, requests и
responses MUST NOT вызывать embedding inference этой ветки.

### FR-03. Evidence normalization

Перед inference evidence MUST быть trimmed и ограничен длиной
`maxEvidenceLengthChars`. Значение по умолчанию: `1000`.

Полный текст evidence MUST NOT попадать в metric labels. В incident разрешается
только bounded representative sample длиной не более `500` символов.

### FR-04. Embedding provider abstraction

Runtime MUST выбирать provider через конфигурацию. Provider MUST expose:

```text
embed(text) -> float[]
modelVersion() -> string
close()
```

Embedder MUST быть создан один раз на lifecycle `open()` Flink operator и
переиспользоваться событиями соответствующего subtask. Model artifacts MUST
быть локальными; event path MUST NOT зависеть от network access.

### FR-05. Deterministic provider

Для regression и replay MUST существовать deterministic provider с фиксированной
версией. Одинаковое normalized evidence при одной версии provider MUST давать
эквивалентный embedding в пределах floating-point tolerance.

Deterministic provider MUST быть явно обозначен model version-ом
`deterministic-hash-v1` и не должен маскироваться под production ONNX model.

### FR-06. ONNX provider

ONNX provider MUST использовать:

- LangChain4j `OnnxEmbeddingModel`;
- ONNX Runtime CPU;
- `multilingual-e5-small`;
- mean pooling;
- configured input prefix `query: `;
- expected dimension `384`;
- L2-normalized output.

При каждом inference provider MUST reject:

- vector другой размерности;
- non-finite component;
- zero или non-finite norm.

### FR-07. Artifact validation

До создания ONNX model provider MUST проверить:

- model file существует и является regular file;
- tokenizer file существует и является regular file;
- manifest читается;
- manifest version совпадает с runtime config;
- dimension, pooling, input prefix и max tokens совпадают с config;
- SHA-256 model и tokenizer совпадают с manifest.

При любой ошибке job MUST fail fast. Automatic fallback с ONNX на deterministic
provider запрещён.

### FR-08. Similarity key isolation

Каждый embedding MUST быть обработан в keyed state по ключу:

```text
tenantId + agentId + guardrailName + embeddingModelVersion
```

`sessionId` MUST NOT входить в key. Findings разных tenants, agents,
guardrails или model versions MUST NOT объединяться в один cluster.

### FR-09. Cluster assignment

Для каждого нового embedding operator MUST:

1. удалить clusters за пределами active event-time boundary;
2. вычислить cosine similarity до каждого active centroid;
3. выбрать cluster с максимальной similarity;
4. добавить finding в выбранный cluster, если similarity `>= threshold`;
5. создать новый cluster, если подходящего cluster нет;
6. пересчитать normalized centroid принятого cluster;
7. записать bounded state.

При равной similarity выбор cluster MUST быть deterministic.

### FR-10. Deduplication

Один `requestId` MUST учитываться в cluster не более одного раза. Повторная
доставка finding с тем же request ID не должна увеличивать:

- unique request count;
- `triggeredFindingsCount`;
- количество session IDs;
- количество evidence samples сверх configured bounds.

### FR-11. Campaign emission

Incident MUST быть создан только если cluster одновременно содержит:

- минимум `minUniqueRequests` уникальных request IDs;
- минимум `minDistinctSessions` разных session IDs;
- один similarity key;
- findings в active event-time window.

Default values:

```yaml
minUniqueRequests: 3
minDistinctSessions: 2
similarityThreshold: 0.5
severity: HIGH
window: 5m
```

При `emitUpdates=false` один cluster MUST emit-ить только один incident.
При `emitUpdates=true` последующие qualifying changes MUST emit-ить revision
events с монотонным `emissionRevision`.

### FR-12. Event-time behavior

Similarity window MUST использовать `eventTimeMillis`, а не processing time.
Finding на правой границе active window допускается согласно текущей event-time
семантике проекта. Cleanup timer MUST быть зарегистрирован так, чтобы finding
на левой inclusive boundary не удалялся преждевременно.

Late-event behavior MUST follow the existing project watermark and late
tolerance policy. Findings, routed to the existing late-event side output,
MUST NOT silently enter the on-time similarity branch.

### FR-13. Bounded state

State MUST use Flink managed keyed state and survive checkpoint/recovery.
Static Java collections and unbounded process-local caches запрещены.

Default limits:

```text
maxClustersPerKey = 20
maxTrackedRequestIdsPerCluster = 100
maxRequestIdsPerIncident = 50
maxSessionIdsPerCluster = 20
maxEvidenceSamplesPerCluster = 5
maxStoredEvidenceSampleLengthChars = 500
```

При cluster overflow MUST сначала применяться expiry, затем deterministic
eviction активного cluster с минимальным `lastEventTimeMillis`. При request ID
overflow новые IDs MUST не менять centroid/count, но MUST увеличивать
low-cardinality overflow counter.

### FR-14. Incident output

Результат MUST использовать существующий topic `basic-incidents` и содержать:

- `incidentId`;
- `tenantId`;
- `agentId`;
- triggering `sessionId`;
- `sessionIds`;
- `requestIds` в пределах output limit;
- `ruleName = SIMILAR_PROMPT_INJECTION_CAMPAIGN`;
- `severity`;
- `evidenceSnippets` в пределах sample limit;
- `embeddingModelVersion`;
- `firstEventTimeMillis`;
- `lastEventTimeMillis`;
- `triggeredFindingsCount`, равный unique request count;
- `emissionRevision`;
- bounded human-readable `summary`.

Embedding vector MUST NOT be serialized into incident payload.

## 5. Domain Invariants

Следующие утверждения MUST быть истинны после каждого успешного event
processing step:

- cluster принадлежит ровно одному similarity key;
- centroid имеет finite components и configured dimension;
- centroid имеет ненулевую L2-норму;
- `requestIds` не содержит duplicates;
- `sessionIds` не содержит duplicates;
- `requestIds.size <= maxTrackedRequestIdsPerCluster`;
- `sessionIds.size <= maxSessionIdsPerCluster`;
- `evidenceSnippets.size <= maxEvidenceSamplesPerCluster`;
- emitted incident не содержит больше `maxRequestIdsPerIncident` request IDs;
- `triggeredFindingsCount` не меньше числа request IDs в output list и отражает
  cluster unique count;
- model version incident совпадает с model version similarity key.

## 6. Configuration Contract

Конфигурация MUST поддерживаться в YAML и CLI mapping через
`JobConfigOptions`.

Обязательные similarity keys:

```text
incidentSimilarPromptInjectionEnabled
incidentSimilarPromptInjectionWindowMinutes
incidentSimilarPromptInjectionThreshold
incidentSimilarPromptInjectionMinUniqueRequests
incidentSimilarPromptInjectionMinDistinctSessions
incidentSimilarPromptInjectionSeverity
incidentSimilarPromptInjectionEmbeddingProvider
incidentSimilarPromptInjectionEmbeddingModelVersion
incidentSimilarPromptInjectionEmbeddingInputPrefix
incidentSimilarPromptInjectionEmbeddingModelPath
incidentSimilarPromptInjectionEmbeddingTokenizerPath
incidentSimilarPromptInjectionEmbeddingManifestPath
incidentSimilarPromptInjectionEmbeddingExpectedDimension
incidentSimilarPromptInjectionEmbeddingMaxTokens
incidentSimilarPromptInjectionEmbeddingParallelism
```

Deterministic local profile MUST remain runnable without model files. ONNX profile
MUST require explicit model, tokenizer and manifest paths.

## 7. Observability Contract

Similarity operator MUST expose low-cardinality counters at minimum:

```text
embeddings_generated_total
clusters_evicted_total
cluster_request_overflow_total
incidents_emitted_total
incident_updates_total
```

Counters MUST NOT use `requestId`, `sessionId`, `userId`, evidence text or
embedding as labels. Rule and severity labels are allowed only as bounded
configuration dimensions already used by the project.

The monitoring surface MUST make it possible to distinguish:

- no eligible attack findings;
- findings skipped because evidence is absent;
- embedding failure;
- clusters created/expired/evicted;
- campaign incident emitted;
- state/checkpoint or downstream Kafka failure.

## 8. Deployment Contract

ONNX runtime MUST execute in an immutable custom Flink image shared by
JobManager and TaskManager. The image MUST contain:

```text
/opt/airiskops/models/multilingual-e5-small/model.onnx
/opt/airiskops/models/multilingual-e5-small/tokenizer.json
/opt/airiskops/models/multilingual-e5-small/manifest.json
```

The Docker build MUST fail when artifacts are missing or checksums do not match.
Model and tokenizer files MUST NOT be committed to Git; manifest and documented
acquisition procedure MUST be versioned.

The ONNX profile MUST not silently use the default Flink image. Deployment must
make the selected image and selected config observable in the submitted job.

## 9. Behavioral Scenarios

### S-01. Valid cross-session campaign

Given three triggered prompt-injection findings for one tenant/agent, with
similarity at least `0.5`, distributed across two sessions and inside `5m`,
when the third unique request is processed, then exactly one HIGH
`SIMILAR_PROMPT_INJECTION_CAMPAIGN` incident is emitted.

### S-02. Missing evidence

Given a valid triggered prompt-injection finding without evidence, when it is
processed, then existing branches receive it, similarity branch skips it, and
no embedding counter increment occurs.

### S-03. Different agent isolation

Given similar findings for two agents, when both streams are processed, then
they produce separate clusters and cannot satisfy one another's thresholds.

### S-04. Same session only

Given three similar unique requests in one session, when they are processed,
then no similarity campaign incident is emitted until a second session is
represented.

### S-05. Duplicate delivery

Given the same request is delivered more than once, when it is processed, then
cluster unique counts and incident count do not increase because of duplicates.

### S-06. Non-similar findings

Given findings for one key with pairwise similarity below threshold, when they
are processed, then they form separate clusters and do not reach a common
campaign threshold.

### S-07. Model artifact failure

Given an ONNX profile with missing or checksum-mismatched artifacts, when the
operator initializes, then startup fails with an actionable error and no
deterministic fallback is used.

### S-08. Checkpoint recovery

Given a running similarity job with populated managed state, when the job is
restored from a checkpoint, then clusters, deduplication state and emission
revision continue according to the same invariants.

## 10. Acceptance Criteria

| ID | Acceptance criterion | Verification |
|---|---|---|
| AC-01 | Optional evidence parses without breaking old events | parser unit test |
| AC-02 | Only eligible triggered PI findings reach embedding | topology/filter test |
| AC-03 | Deterministic provider is reproducible | embedder unit test |
| AC-04 | ONNX vector is dimensionally valid, finite and normalized | ONNX adapter test with real artifact |
| AC-05 | ONNX manifest and checksums are fail-fast validated | artifact validation tests |
| AC-06 | Cross-session campaign emits at 3 requests / 2 sessions | process-function test |
| AC-07 | tenant and agent isolation holds | keyed-state test |
| AC-08 | same-session-only input does not emit | process-function test |
| AC-09 | duplicate request does not inflate state or incident | process-function test |
| AC-10 | below-threshold findings remain separate | similarity test |
| AC-11 | event-time boundary and late events are correct | timer/harness test |
| AC-12 | state limits and deterministic eviction hold | overflow test |
| AC-13 | output payload is bounded and excludes embedding vector | serde contract test |
| AC-14 | counters are present and low-cardinality | metric/integration test |
| AC-15 | deterministic replay is reproducible | generator test |
| AC-16 | ONNX custom image starts with both Flink services | Docker startup smoke |
| AC-17 | checkpoint/recovery preserves similarity state | Flink recovery test |
| AC-18 | local Kafka E2E emits expected campaign incident | replay + Kafka verification |
| AC-19 | Grafana exposes campaign and embedding health signals | dashboard/API check |
| AC-20 | complete regression and build pass | `run-regression.sh` + `build-job.sh` |

## 11. Current Implementation Matrix

This section records implementation status against the normative requirements;
it is not a substitute for acceptance.

| Area | Current state |
|---|---|
| Evidence field and parser | Implemented |
| Similarity filter and topology branch | Implemented |
| Deterministic embedder | Implemented |
| LangChain4j ONNX adapter | Implemented in code; real artifact test not fully accepted |
| Config and ONNX profile | Implemented |
| Custom Dockerfile | Implemented; model files supplied externally |
| Keyed clustering and deduplication | Implemented as concept path |
| Bounded cluster state | Implemented as concept path |
| Full per-finding rolling expiry | Not complete: old findings are not subtracted from an active centroid/count |
| Embedding failure/skipped/created/expired metrics | Incomplete |
| Replay scenario | Implemented |
| Full Kafka/Prometheus/Grafana E2E | Not accepted |
| Checkpoint/recovery acceptance | Not accepted |

## 12. Traceability

Основные реализации:

- event contract: `SafetyEvent`, `SafetyEventParser`;
- config: `JobConfigOptions`, `JobConfig`, `SimilarPromptInjectionRuleConfig`;
- embedding lifecycle: `EmbedPromptInjectionEvidenceFunction`;
- provider adapters: `EvidenceEmbedder`, `DeterministicEvidenceEmbedder`,
  `LangChain4jOnnxEvidenceEmbedder`;
- clustering: `SimilarAttackKey`, `SimilarAttackCluster`,
  `SimilarPromptInjectionCampaignFunction`;
- topology: `IncrementOneTopologyBuilder`;
- replay: `tools/generators/generate_events.py`, `tools/scripts/run-replay.sh`;
- deployment: `config/job/local-onnx.yaml`,
  `deployment/local/flink-onnx.Dockerfile`;
- tests: `flink-job/src/test/java/` and `tools/tests/`.

## 13. Open Decisions Before Production Acceptance

До перевода из `Draft` в `Accepted` необходимо принять решения и получить
доказательства по следующим вопросам:

1. Подтвердить точные model/tokenizer artifacts и выполнить реальный ONNX
   startup/inference test.
2. Определить корректную per-finding retention model, если centroid должен
   строго соответствовать только текущему rolling окну.
3. Зафиксировать embedding failure policy: fail job, quarantine finding или
   controlled skip с counter.
4. Добавить и проверить метрики skipped, failed, created, expired и duplicate.
5. Провести threshold calibration на representative corpus, а не только на
   synthetic replay.
6. Выполнить длительный live/load test с DEFAULT, RocksDB и ONNX profiles.
7. Подтвердить savepoint/checkpoint compatibility при изменении topology.
8. Завершить local Kafka, Prometheus и Grafana acceptance matrix.

## 14. Definition Of Done

Фича считается принятой только если:

- все `AC-01`--`AC-20` имеют ссылку на тест или execution evidence;
- ONNX artifacts воспроизводимо валидируются;
- failure semantics явно задокументированы и проверены;
- state invariants подтверждены после restart/recovery;
- нет расхождения между этой спецификацией, YAML defaults, runbook и кодом;
- `bash tools/scripts/run-regression.sh` и
  `bash tools/scripts/build-job.sh` завершаются успешно;
- зафиксированы результаты controlled replay и длительного load test.
