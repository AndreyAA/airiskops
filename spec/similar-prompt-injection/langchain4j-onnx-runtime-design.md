# LangChain4j ONNX runtime для Similar Prompt Injection Campaign

Дата актуальности: 2026-09-08

Связанные документы:

- [feature specification](similar-prompt-injection-feature.md);
- [implementation plan](similar-prompt-injection-implementation-plan.md).

## 1. Цель и границы

Добавить локальный синхронный adapter над LangChain4j `OnnxEmbeddingModel` и
`multilingual-e5-small` наряду с уже существующим
`DeterministicEvidenceEmbedder`. Обе реализации выполняются в JVM каждого
Flink TaskManager; во время обработки события нет HTTP, gRPC, model-registry
или другого сетевого вызова.

Design не меняет входной контракт `SafetyEvent`, Kafka topics, watermark,
rolling-window или similarity key. Версия модели остаётся частью key, поэтому
embeddings разных artifacts никогда не смешиваются в state.

## 2. Принятый путь

```text
GUARDRAIL_FINDING
  -> existing PI/evidence filter
  -> EmbedPromptInjectionEvidenceFunction (one instance per subtask)
       open(): validate local artifacts; construct OnnxEmbeddingModel
       map(): truncate -> "query: " + evidence -> embed() -> L2 normalize
       close(): release adapter-owned resources where public API permits
  -> SimilarAttackKey(tenant, agent, guardrail, modelVersion)
  -> existing keyed campaign operator
```

`EmbeddingModel` не сериализуется, не попадает в Flink managed state и не
передаётся через Kafka. Поле adapter-а в Flink function — `transient`; модель
создаётся один раз в `open()` и переиспользуется для всех input records subtask.

Для одного event вызывается один синхронный `embed()`. `AsyncFunction`,
локальный HTTP sidecar и remote embedding API исключены из этого этапа.

## 3. Артефакты и custom Flink image

TaskManager и JobManager используют один immutable custom image. В image
находятся только заранее скачанные и проверенные файлы:

```text
/opt/airiskops/models/multilingual-e5-small/
  model.onnx                 # утверждённый generic/CPU-compatible ONNX artifact
  tokenizer.json
  manifest.json
```

`manifest.json` обязан содержать минимум:

```json
{
  "modelVersion": "multilingual-e5-small-onnx-qint8-v1",
  "modelSha256": "...",
  "tokenizerSha256": "...",
  "embeddingDimension": 384,
  "pooling": "MEAN",
  "inputPrefix": "query: ",
  "maxTokens": 256
}
```

Docker build проверяет SHA-256 до создания image. Adapter в `open()` повторно
сверяет manifest, file presence и dimension. Отсутствие/несовпадение в ONNX
режиме — понятная startup failure subtask, а не неявный fallback на hash
embedding.

Не использовать host volume в качестве production source и не скачивать
модель при первом event. Init-container с внутренним immutable registry можно
принять позднее как альтернативный delivery mechanism, но его output также
должен быть локальным read-only path с тем же manifest.

## 4. Maven и dependency boundary

Добавить одну pinned release-version LangChain4j и использовать generic module:

```xml
<dependency>
  <groupId>dev.langchain4j</groupId>
  <artifactId>langchain4j-embeddings</artifactId>
  <version>${langchain4j.version}</version>
</dependency>
```

Версию фиксировать в одном Maven property; не использовать SNAPSHOT или beta
в production image без отдельного решения. До merge выполнить dependency tree
и shaded-JAR smoke test: в JAR должна попасть ровно одна совместимая версия
`onnxruntime`, native library обязана загружаться в базовом Flink image.

Нельзя использовать pre-packaged LangChain4j model artifact: он поставляет
другую модель. Нужен generic `OnnxEmbeddingModel(Path model, Path tokenizer,
PoolingMode.MEAN)` для утверждённого локального `multilingual-e5-small`.

## 5. Изменения Java

### 5.1 Adapter

Добавить `com.bank.airiskops.infra.embedding.LangChain4jOnnxEvidenceEmbedder`,
реализующий уже существующий `EvidenceEmbedder`:

```java
public final class LangChain4jOnnxEvidenceEmbedder implements EvidenceEmbedder {
    private final OnnxEmbeddingModel model;
    private final String modelVersion;
    private final String inputPrefix;

    public float[] embed(String evidence) {
        float[] vector = model.embed(inputPrefix + evidence).content().vector();
        return validateAndNormalize(vector);
    }
}
```

Adapter отвечает за manifest validation, prefix, dimension/finite/L2 checks и
за то, что evidence text не появится в logs. `EmbedPromptInjectionEvidenceFunction`
сохраняет truncation до `maxEvidenceLength` и lifecycle `open()/close()`.

`DeterministicEvidenceEmbedder` остаётся полноценной реализацией
`EvidenceEmbedder` для воспроизводимых unit, harness и replay tests. Он не
зависит от ONNX artifacts и сохраняет `deterministic-hash-v1` как отдельную
model version. Выбор implementation выполняется исключительно явным config
flag; ONNX failure никогда не переключает job на hash embedding автоматически.

### 5.2 Ошибки inference

Startup/model integrity failure должен fail-fast остановить subtask. Ошибка
одного inference не должна silently менять смысл кампании. Для per-record
ошибки заменить map-stage на rich process stage с техническим side output
`embedding-failures` либо определённой dead-letter route. Запись в similarity
state в таком случае не производится. Payload failure event не содержит
evidence; допустимы только error class/code, model version и event timestamp.

### 5.3 Конфигурация

Расширить `SimilarPromptInjectionRuleConfig`, `JobConfigOptions`, `JobConfig`
и оба local YAML профиля следующими значениями:

```yaml
incidentSimilarPromptInjectionEmbeddingModelPath: /opt/airiskops/models/multilingual-e5-small/model.onnx
incidentSimilarPromptInjectionEmbeddingTokenizerPath: /opt/airiskops/models/multilingual-e5-small/tokenizer.json
incidentSimilarPromptInjectionEmbeddingManifestPath: /opt/airiskops/models/multilingual-e5-small/manifest.json
incidentSimilarPromptInjectionEmbeddingProvider: langchain4j-onnx
incidentSimilarPromptInjectionEmbeddingModelVersion: multilingual-e5-small-onnx-qint8-v1
incidentSimilarPromptInjectionEmbeddingInputPrefix: "query: "
incidentSimilarPromptInjectionEmbeddingExpectedDimension: 384
incidentSimilarPromptInjectionEmbeddingMaxTokens: 256
incidentSimilarPromptInjectionEmbeddingParallelism: 1
```

Paths, version, prefix, dimension и token limit являются частью semantic model
identity. Их изменение требует новой `embeddingModelVersion` и rollout как
новой state key; менять их молча запрещено.

Допустимые значения `embeddingProvider`:

- `langchain4j-onnx` — production candidate; требует model/tokenizer/manifest;
- `deterministic` — детерминированные tests и локальные reproducible replay;
  использует `deterministic-hash-v1` и игнорирует ONNX paths.

Профиль `local-job.yaml` может оставлять `deterministic`; отдельный ONNX local
profile и staging/production profiles явно задают `langchain4j-onnx`. Такое
разделение не допускает случайной подмены semantic model при тестировании.

## 6. CPU, parallelism и backpressure

Embedding operator получает явный `setParallelism(config.embeddingParallelism())`.
Для local MVP стартовое значение — `1`; production value выбирается только
после benchmark на фактическом node type.

Вызов остаётся synchronous в mailbox thread. Это обеспечивает ordering и
простую checkpoint semantics, но latency inference формирует backpressure.
Нужны gauges/counters latency и нагрузочное измерение до повышения parallelism.

LangChain4j public `OnnxEmbeddingModel` API не предоставляет в своём
constructor явную настройку ONNX Runtime intra-op threads. До production
rollout spike обязан подтвердить, что выбранная LangChain4j/ORT версия и
container environment ограничивают CPU consumption предсказуемо. Если это
невозможно, данный вариант не проходит production gate: следует выбрать raw
ONNX Runtime adapter, а не обходить ограничение неконтролируемым executor-ом.

Не передавать executor в `OnnxEmbeddingModel` на первом этапе. Flink parallelism
является единственным уровнем меж-event parallelism; nested executor создаёт
риск oversubscription и неконтролируемой очереди в subtask.

## 7. Метрики

Добавить low-cardinality metrics в embedding operator:

- `airiskops_similarity_embedding_embeddings_generated_total`;
- `airiskops_similarity_embedding_embeddings_failed_total{reason}`;
- `airiskops_similarity_embedding_findings_skipped_total{reason}`;
- `airiskops_similarity_embedding_inference_latency_ms` (histogram, если
  доступен runtime reporter; иначе bounded timer/gauge policy);
- `airiskops_similarity_embedding_model_info{model_version}` = 1.

Допустимые `reason`: `blank_evidence`, `oversized_truncated`, `inference_error`,
`invalid_vector`, `manifest_mismatch`. Labels никогда не содержат tenant,
agent, session, request ID, evidence или embedding.

## 8. Тесты и rollout gates

1. Unit: fake adapter, filter, prefix, truncation, normalization, version
   propagation, close lifecycle и error side output.
   `DeterministicEvidenceEmbedder` является стандартной implementation для
   воспроизводимых cluster/harness tests.
2. Artifact smoke: реальный model/tokenizer из image; 384 finite components,
   L2 norm около 1, два одинаковых strings дают эквивалентный vector.
3. Compatibility: RU, EN и mixed evidence; positive paraphrases и hard
   negatives; зафиксировать distribution cosine scores.
4. Flink harness: checkpoint/restore функции с transient model; никаких model
   bytes в state.
5. Image smoke: TaskManager реально загружает native ONNX Runtime на целевой
   CPU architecture.
6. E2E: replay `mixed` создаёт campaign incident; Kafka, Prometheus и Grafana
   показывают expected rule и embedding health metrics.
7. Load: p50/p95/p99 inference latency, CPU, RSS, checkpoint duration и
   backpressure при target RPS.

Первоначальный threshold `0.5` — только concept default. После real-model
corpus калибровки требуется отдельное решение о production threshold.

## 9. Rollout и rollback

1. Сохранить deterministic provider для baseline regression и replay.
2. Собрать ONNX image, пройти artifact smoke и benchmark.
3. Развернуть job с ONNX model version и feature flag `false`.
4. Включить feature на локальном/staging replay, проверить метрики и incidents.
5. Включить production после acceptance gates.

Rollback — `incidentSimilarPromptInjectionEnabled=false`; существующие
aggregates и session incidents не меняются. При замене модели сначала вводится
новая version key, а предыдущий state естественно очищается по configured
retention; state разных версий не объединяется.

## 10. Решения, требующие подтверждения перед implementation

- точная released LangChain4j version после dependency/shade spike;
- generic CPU-compatible ONNX artifact для target architecture и его hashes;
- образ/registry delivery mechanism;
- окончательные names/default providers для local, staging и production config
  profiles;
- допустимый p95 latency и CPU budget на TaskManager;
- production threshold после calibration corpus;
- путь технического side output и его retention/access policy.
