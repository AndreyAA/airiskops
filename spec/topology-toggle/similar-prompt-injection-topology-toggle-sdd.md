# SDD: Topology-Level Toggle для Similar Prompt-Injection Detection

Статус: `Implemented`
Дата: `2026-09-13`
Область: AIRiskOps Flink MVP  
Связанная фича: `SIMILAR_PROMPT_INJECTION_CAMPAIGN`

## 1. Назначение

Параметр `incidentSimilarPromptInjectionEnabled` должен быть настоящим
topology-level feature toggle. При значении `false` similarity branch не должна
создаваться в Flink job graph вообще.

Требуемый результат:

- ONNX model не загружается;
- embedding operator не создаётся;
- similarity process function не создаётся;
- similarity state не регистрируется;
- similarity branch не потребляет Flink slots и runtime resources;
- similarity counters не появляются;
- существующие incident rules продолжают работать без изменения semantics;
- job graph и runtime поведение однозначно соответствуют конфигурации.

Реализация в `IncrementOneTopologyBuilder` проверяет флаг до создания similarity
операторов. Поэтому `EmbedPromptInjectionEvidenceFunction.open()` не вызывается
в выключенном режиме и ONNX embedder не создаётся даже при отсутствии событий.

## 2. Scope

### 2.1 In scope

- условное добавление similarity branch в `IncrementOneTopologyBuilder`;
- отсутствие similarity operators при выключенном флаге;
- отсутствие ONNX/deterministic embedder lifecycle при выключенном флаге;
- отсутствие similarity metrics и managed state при выключенном флаге;
- сохранение обычных session incident rules;
- конфигурационный контракт YAML/CLI;
- topology, configuration и deployment tests;
- документация и runbook.

### 2.2 Out of scope

- изменение алгоритма embedding или clustering;
- изменение similarity thresholds и state limits;
- изменение Kafka topic contracts;
- динамическое переключение флага без redeploy/restart job;
- отключение отдельных существующих incident rules;
- удаление similarity-кода из артефакта JAR.

## 3. Термины

| Термин | Определение |
|---|---|
| Similarity branch | `filter -> embedding map -> keyBy -> similarity process` path. |
| Topology-level toggle | Конфигурация, влияющая на состав Flink job graph до его запуска. |
| Incident layer | Все incident branches, включая session и similarity rules. |
| Disabled job graph | Graph, в котором нет similarity operator nodes, state descriptors и counters. |
| Static configuration | Значение, прочитанное при создании job graph; для изменения требуется redeploy. |

## 4. Configuration Contract

Основной toggle:

```yaml
incidentSimilarPromptInjectionEnabled: false
```

Java configuration field:

```java
config.incidentConfig().similarPromptInjection().enabled()
```

CLI и YAML должны использовать один ключ:

```text
incidentSimilarPromptInjectionEnabled
```

Default MUST быть безопасным и backward-compatible:

```java
DEFAULT_SIMILAR_PI_ENABLED = false
```

Профили, которые явно хотят включить feature, должны содержать:

```yaml
incidentEnabled: true
incidentSimilarPromptInjectionEnabled: true
```

ONNX provider и его paths не должны требоваться при выключенном toggle.
Следовательно, конфигурация с:

```yaml
incidentSimilarPromptInjectionEnabled: false
incidentSimilarPromptInjectionEmbeddingProvider: langchain4j-onnx
incidentSimilarPromptInjectionEmbeddingModelPath: /missing/model.onnx
```

должна успешно построить и запустить job, потому что ONNX provider не будет
создан. Ошибки в неиспользуемой similarity-конфигурации не должны блокировать
выключенный feature.

## 5. Требуемая семантика флагов

### 5.1 `incidentEnabled = false`

Отключает весь incident layer:

- session incident branch не создаётся;
- similarity branch не создаётся независимо от
  `incidentSimilarPromptInjectionEnabled`;
- policy update source для incidents не создаётся;
- incident sink `basic-incidents` не создаётся.

### 5.2 `incidentEnabled = true`, similarity `false`

Обычные incidents продолжают работать:

- session incident branch создаётся;
- policy update source создаётся;
- similarity branch не создаётся;
- ONNX/deterministic embedder не создаётся;
- similarity state и metrics не регистрируются;
- `basic-incidents` продолжает принимать incidents существующих правил.

### 5.3 `incidentEnabled = true`, similarity `true`

Создаётся полный similarity path:

```text
triggered guardrail findings
  -> similarity filter
  -> EmbedPromptInjectionEvidenceFunction
  -> SimilarAttackKeySelector
  -> SimilarPromptInjectionCampaignFunction
  -> union with session incidents
  -> basic-incidents sink
```

Для ONNX provider должны применяться обычные fail-fast проверки model,
tokenizer и manifest. Для deterministic provider model artifacts не требуются.

## 6. Topology Requirements

### TR-01. Conditional construction

`IncrementOneTopologyBuilder` MUST проверять toggle до создания similarity
operators:

```java
boolean similarPromptInjectionEnabled =
        config.incidentConfig().similarPromptInjection().enabled();

DataStream<BasicIncident> incidents = sessionIncidents;
if (similarPromptInjectionEnabled) {
    DataStream<BasicIncident> similarityIncidents = guardrailFindings
            .filter(...)
            .map(new EmbedPromptInjectionEvidenceFunction(...))
            .setParallelism(...)
            .keyBy(new SimilarAttackKeySelector())
            .process(new SimilarPromptInjectionCampaignFunction(...));
    incidents = sessionIncidents.union(similarityIncidents);
}

serializeToJson(incidents)
        .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().basicIncidentsTopic()));
```

The exact implementation may use a helper method, but the observable graph
semantics MUST be equivalent.

### TR-02. No eager construction

When similarity is disabled, the builder MUST NOT instantiate:

- `EmbedPromptInjectionEvidenceFunction`;
- `SimilarPromptInjectionCampaignFunction`;
- `SimilarAttackKeySelector`;
- configured `EvidenceEmbedder`;
- `LangChain4jOnnxEvidenceEmbedder`;
- `DeterministicEvidenceEmbedder`.

It is acceptable for the Java classes to remain present in the JAR. They MUST
not be instantiated by the disabled topology.

### TR-03. No operator lifecycle

When similarity is disabled, Flink MUST NOT call `open`, `close` or
`processElement` for similarity operators because those operators MUST not be
part of the graph.

### TR-04. Stable existing UIDs

Existing operator UIDs MUST remain unchanged:

- source and parse operators;
- aggregate operators;
- session incident evaluator;
- existing incident sink.

Similarity UIDs remain stable when the feature is enabled:

```text
embed-similar-prompt-injection
similar-prompt-injection-campaign
```

Disabling the branch removes these nodes from the new graph. Re-enabling the
branch requires normal Flink topology compatibility review and MUST not be
claimed as savepoint-compatible without verification.

### TR-05. Shared sink behavior

The `basic-incidents` sink MUST be created exactly once when
`incidentEnabled=true`, regardless of similarity toggle value.

When similarity is disabled, the sink input MUST contain only session incidents.

When both incident layers are disabled, the sink MUST not be created.

## 7. Resource Semantics

With `incidentSimilarPromptInjectionEnabled=false`:

- no embedding operator parallelism is reserved;
- no ONNX native session is allocated;
- no model/tokenizer files are opened;
- no embedding thread pool is allocated;
- no similarity keyed state backend state is registered;
- no similarity checkpoint state is persisted;
- no similarity counters are exported;
- no similarity-specific CPU or heap overhead is introduced by the graph.

The base job may still consume resources for parsing, aggregates, watermarks,
session incidents and other enabled branches. The toggle MUST NOT be interpreted
as disabling the complete Flink job.

## 8. Metrics And Observability

### 8.1 Disabled state

Similarity-specific metrics MUST be absent from the Flink/Prometheus exposition,
not merely present with value `0`:

```text
embeddings_generated_total
embeddings_failed_total
clusters_created_total
clusters_expired_total
clusters_evicted_total
cluster_request_overflow_total
incidents_emitted_total for similarity rule
incident_updates_total for similarity branch
```

The exact metric names must follow the project naming convention. The key
requirement is that disabled topology does not register them.

Existing generic incident metrics for session rules may remain present when
`incidentEnabled=true`.

### 8.2 Enabled state

When enabled, similarity metrics MUST be present after operator initialization
and MUST retain their current low-cardinality label policy. No request ID,
session ID, evidence text or embedding vector may be used as a label.

### 8.3 Configuration visibility

The submitted job or startup log SHOULD expose the effective value of
`incidentSimilarPromptInjectionEnabled` and selected embedding provider. This
is needed to distinguish “feature disabled” from “feature enabled but no
eligible findings”.

## 9. Failure Semantics

When similarity is disabled:

- missing ONNX files MUST NOT fail the job;
- invalid ONNX manifest MUST NOT fail the job;
- unsupported embedding provider value SHOULD still be rejected during config
  parsing if the config is syntactically invalid, or may be ignored if the
  project adopts lazy validation for disabled features; this choice must be
  fixed consistently;
- similarity runtime exceptions cannot occur because similarity operators are
  absent.

When similarity is enabled:

- ONNX artifact and manifest errors MUST fail fast;
- embedder initialization errors MUST fail the task/job according to existing
  Flink failure policy;
- similarity processing errors MUST not silently fall back to another provider.

Recommended policy: validate the toggle first, then validate only the
configuration of the selected enabled provider. This permits disabled profiles
to omit ONNX artifacts while retaining strict validation for enabled profiles.

## 10. Behavioral Scenarios

### S-01. Similarity disabled, session incidents enabled

Given:

```yaml
incidentEnabled: true
incidentSimilarPromptInjectionEnabled: false
```

When the job graph is built, then session incident operators and the existing
incident sink are present, while both similarity operators are absent.

### S-02. Similarity disabled, ONNX artifacts absent

Given similarity is disabled and ONNX paths point to missing files, when the job
is built and submitted, then job startup does not attempt to read model files
and is not rejected because of those files.

### S-03. Similarity disabled, eligible traffic arrives

Given similarity is disabled, when eligible triggered prompt-injection findings
with evidence arrive, then they are processed by existing branches but no
embedding is generated and no similarity incident is emitted.

### S-04. Similarity enabled

Given similarity is enabled and a valid provider is configured, when the job
graph is built, then the filter, embedding and clustering operators are present
with their stable UIDs and configured parallelism.

### S-05. All incidents disabled

Given `incidentEnabled=false`, when the graph is built, then neither session nor
similarity incident branch exists and `basic-incidents` sink is absent.

### S-06. Toggle requires redeploy

Given a running job, when the YAML value is changed on disk without job restart,
then the running job behavior MUST NOT change. A new job deployment is required
to apply the toggle.

### S-07. No similarity metrics when disabled

Given similarity is disabled, when Flink and Prometheus are queried, then no
similarity-specific metric series are returned.

## 11. Acceptance Criteria

| ID | Criterion | Verification |
|---|---|---|
| TOG-01 | Default toggle is `false` | `JobConfigTest` |
| TOG-02 | Disabled similarity branch is absent from job graph | topology test / planned graph assertion |
| TOG-03 | Disabled graph does not instantiate embedder | constructor/lifecycle test |
| TOG-04 | Missing ONNX files do not block disabled profile | config/topology test |
| TOG-05 | Enabled ONNX profile still validates artifacts | ONNX adapter test |
| TOG-06 | Session incidents continue when similarity is disabled | topology/integration test |
| TOG-07 | Similarity operators retain stable UIDs when enabled | topology plan assertion |
| TOG-08 | Similarity embedding parallelism applies only when enabled | topology test |
| TOG-09 | Similarity keyed state is absent when disabled | state/operator graph test |
| TOG-10 | Similarity metrics are absent when disabled | Flink/Prometheus integration test |
| TOG-11 | Similarity metrics appear when enabled | Flink/Prometheus integration test |
| TOG-12 | Eligible traffic does not produce similarity incidents when disabled | Kafka E2E |
| TOG-13 | Eligible traffic produces similarity incident when enabled | replay + Kafka E2E |
| TOG-14 | Toggle change requires new job deployment | runbook test/manual verification |
| TOG-15 | Regression and build pass | `run-regression.sh` + `build-job.sh` |

## 12. Test Design

### 12.1 Configuration tests

Tests MUST cover:

- default `false` when key is absent;
- explicit `false` from YAML;
- explicit `true` from YAML;
- CLI override precedence over YAML if supported;
- `incidentEnabled=false` dominating similarity setting;
- disabled ONNX config with missing paths;
- enabled ONNX config requiring paths.

### 12.2 Topology tests

The preferred test should inspect the generated `StreamGraph` and assert:

Disabled:

```text
contains no node named "Embed Similar Prompt Injection Evidence"
contains no node named "Similar Prompt Injection Campaign"
contains no UID "embed-similar-prompt-injection"
contains no UID "similar-prompt-injection-campaign"
```

Enabled:

```text
contains both similarity nodes
contains both similarity UIDs
embedding parallelism equals configured value
```

The test MUST also assert that the common incident sink remains present only
when `incidentEnabled=true`.

### 12.3 Lifecycle tests

Inject a test `EvidenceEmbedder` that records construction and `open/close`
events. Assert:

- disabled graph does not construct the embedder;
- enabled graph constructs it once per expected operator lifecycle;
- disabled graph does not increment embedding counters.

### 12.4 Integration tests

Run the same eligible replay twice:

- once with similarity disabled: no similarity incident;
- once with similarity enabled: expected campaign incident;
- in both cases existing outputs and session incident behavior remain valid.

## 13. Implementation Plan

1. Refactor similarity branch creation into a helper that is called only when
   `similarPromptInjection.enabled()` is `true`.
2. Build `incidents` as session-only stream by default.
3. Union similarity incidents only inside the enabled branch.
4. Keep existing UIDs unchanged for both existing and similarity operators.
5. Add StreamGraph tests for enabled and disabled combinations.
6. Add lifecycle test proving no provider construction when disabled.
7. Add config tests for missing ONNX paths under disabled and enabled modes.
8. Update runbook and SDD status after acceptance tests pass.
9. Run `bash tools/scripts/run-regression.sh` and
   `bash tools/scripts/build-job.sh`.

## 14. Compatibility And Rollout

The toggle is static job configuration. Changing it changes the StreamGraph,
therefore rollout requires submitting a new job. Existing savepoints MUST NOT
be assumed compatible when changing the presence of similarity operators.

Recommended rollout sequence:

1. Deploy with `incidentSimilarPromptInjectionEnabled=false`.
2. Verify that existing incident outputs and metrics remain unchanged.
3. Build and validate ONNX image/artifacts separately.
4. Deploy a new job with the flag enabled.
5. Run positive, negative and artifact-failure acceptance scenarios.
6. Keep rollback configuration with the flag disabled.

## 15. Definition Of Done

This change is complete when:

- the disabled StreamGraph contains no similarity operator nodes;
- no embedding provider is instantiated in disabled mode;
- absent ONNX files do not affect disabled mode;
- similarity state and metrics are absent in disabled mode;
- existing session incidents remain unchanged;
- enabled mode preserves current similarity behavior and stable UIDs;
- all `TOG-01`--`TOG-15` criteria have test or execution evidence;
- documentation and effective configuration examples are updated;
- regression and job build pass successfully.
