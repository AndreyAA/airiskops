# Increment 3 Implementation Spec

Дата актуальности: 2026-08-30

## Назначение

Этот документ превращает идеи `Increment 3` из [near-term-improvement-plan.md](near-term-improvement-plan.md) в прикладную спецификацию реализации.

Фокус именно на том, что нужно сделать в текущем репозитории для следующего инкремента AISafetyOps MVP:

- какие файлы менять;
- какие параметры добавить;
- какие Kafka topics, Prometheus metrics и Grafana dashboards нужны;
- какие тесты обязательны;
- какой конкретный business value появляется после каждого подэтапа.

Документ ориентирован на текущий локальный Docker-based контур и на последующий перенос в банковскую инфраструктуру без смены базовой модели данных.

## 1. Границы Increment 3

`Increment 3` в рамках текущего MVP состоит из трех подэтапов:

1. `3.1 Incident layer`
   - перейти от aggregate-only модели к минимально полезным incident signals.
2. `3.2 Runtime policy updates`
   - сделать policy частью реального runtime path, а не только стартовой конфигурации.
3. `3.3 Scenario replay and operational observability`
   - усилить replay до сценарного regression-инструмента и одновременно закрыть runtime contract visibility и performance monitoring.

Такое разбиение выбрано потому, что каждый шаг сам по себе дает отдельный measurable value для Operational Risk.

## 2. Текущая база, от которой отталкиваемся

Перед началом `Increment 3` в проекте уже есть:

- canonical event parsing и validation;
- side outputs для `invalid-events` и `late-events`;
- NRT-агрегаты по `1m` и `5m`;
- метрики и dashboards по findings и runtime;
- replay generator и live generator;
- Kafka-based локальный контур.

### 2.1 Текущий runtime contract

Этот контракт надо явно сохранить и дополнить, а не менять неявно:

- `window type`
  - текущее значение: `Tumbling Event-Time Window`;
  - где задается: [IncrementOneTopologyBuilder.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/usecase/IncrementOneTopologyBuilder.java).
- `window sizes`
  - текущее значение: `1m` и `5m`;
  - где задается: [IncrementOneTopologyBuilder.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/usecase/IncrementOneTopologyBuilder.java).
- `outOfOrdernessSeconds`
  - текущее значение: `30`;
  - где задается: [local-job.yaml](../../config/job/local-job.yaml).
- `idleTimeoutMinutes`
  - текущее значение: `1`;
  - где задается: [local-job.yaml](../../config/job/local-job.yaml).
- `lateToleranceMinutes`
  - текущее значение: `5`;
  - где задается: [local-job.yaml](../../config/job/local-job.yaml).
- `checkpointIntervalSeconds`
  - текущее значение: `30`;
  - где задается: [local-job.yaml](../../config/job/local-job.yaml).
- `autoWatermarkIntervalSeconds`
  - текущее значение: `5`;
  - где задается: [local-job.yaml](../../config/job/local-job.yaml).
- `delivery guarantee`
  - текущее значение: `AT_LEAST_ONCE`;
  - где задается: [KafkaSinkFactory.java](../../flink-job/src/main/java/com/bank/aisafetyops/infra/sink/KafkaSinkFactory.java).

### 2.2 Что важно не сломать

- `Increment 3` не должен ломать текущие `1m` и `5m` aggregate outputs.
- Новая incident-логика должна идти как дополнительный слой, а не как замена aggregate layer.
- Все новые параметры должны идти через YAML с возможностью override через CLI.
- Основной business key для корреляции: `agentId + sessionId`.
- `requestId` остается обязательным для drill-down.

## 3. Целевая архитектура Increment 3

После реализации `Increment 3` у job появятся три логических слоя:

1. `Foundation layer`
   - parsing, validation, timestamp assignment, watermarks, invalid/late routing.
2. `Aggregate layer`
   - существующие `1m` и `5m` window aggregates.
3. `Incident and control layer`
   - session-oriented correlation;
   - policy-aware severity classification;
   - incident output;
   - detector quality metrics;
   - scenario-driven replay validation.

Это по-прежнему одна Flink job, а не набор микросервисов.

## 4. Подэтап 3.1: Incident Layer

### 4.1 Цель

Добавить минимально полезную операционную сущность `BasicIncident`, чтобы risk-команда работала не только с графиками и findings, но и с готовыми сигналами по агенту и пользовательской сессии.

### 4.2 Что реализуем

- `keyBy(agentId + sessionId)` для finding events;
- keyed state с TTL по сессии;
- накопление finding-паттернов внутри сессии;
- базовые correlation rules без тяжелого CEP;
- расчет `severity`;
- выпуск incident-событий в Kafka и в structured metrics/logging.

### 4.3 Минимальные correlation rules

Для первого практического шага достаточно ввести 4 rule family:

1. `prompt_injection_burst`
   - несколько `PROMPT_INJECTION` findings в одной сессии за короткий интервал;
   - severity повышается по мере роста `confidence`.
2. `toxicity_campaign`
   - несколько `TOXICITY` findings подряд в одной сессии;
   - severity растет при устойчивом повторении.
3. `leakage_with_injection`
   - сочетание `PROMPT_INJECTION` и `SYSTEM_PROMPT_LEAKAGE`;
   - severity сразу не ниже `high`.
4. `looping_persistence`
   - повторный `LOOPING=true` в одной сессии;
   - помогает выявлять деградацию agent workflow.

### 4.4 Какие файлы менять

#### Новые model classes

- `flink-job/src/main/java/com/bank/aisafetyops/model/BasicIncident.java`
- `flink-job/src/main/java/com/bank/aisafetyops/model/BasicIncidentRule.java`
- `flink-job/src/main/java/com/bank/aisafetyops/model/IncidentSeverity.java`
- `flink-job/src/main/java/com/bank/aisafetyops/model/IncidentStatus.java`
- `flink-job/src/main/java/com/bank/aisafetyops/model/SessionIncidentKey.java`
- `flink-job/src/main/java/com/bank/aisafetyops/model/SessionRiskSnapshot.java`

#### Новые app classes

- `flink-job/src/main/java/com/bank/aisafetyops/app/functions/SessionIncidentEvaluatorFunction.java`
- `flink-job/src/main/java/com/bank/aisafetyops/app/functions/SerializeBasicIncidentFunction.java`
- `flink-job/src/main/java/com/bank/aisafetyops/app/functions/SessionIncidentMetricsFunction.java`
- `flink-job/src/main/java/com/bank/aisafetyops/app/functions/SessionIncidentKeySelector.java`

#### Изменяемые app/config classes

- [JobConfig.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/config/JobConfig.java)
- [JobConfigOptions.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/config/JobConfigOptions.java)
- [OutputTopics.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/config/OutputTopics.java)
- [IncrementOneTopologyBuilder.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/usecase/IncrementOneTopologyBuilder.java)
- [JobTopology.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/support/JobTopology.java)
- [AiSafetyOpsMvpJob.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/job/AiSafetyOpsMvpJob.java)

#### Возможные infra changes

- [KafkaSinkFactory.java](../../flink-job/src/main/java/com/bank/aisafetyops/infra/sink/KafkaSinkFactory.java)
  - добавить sink для `BasicIncident`.
- [JsonSerde.java](../../flink-job/src/main/java/com/bank/aisafetyops/infra/serde/JsonSerde.java)
  - убедиться, что новая incident model сериализуется без ad-hoc JSON logic.

### 4.5 Какие параметры добавить в YAML и CLI

#### Incident enablement

- `incident.enabled: true|false`
- `incident.outputTopic: basic-incidents`
- `incident.emitUpdates: true|false`
  - если `true`, позднее обновление по сессии может выпускать новый incident state.

#### Session state

- `incident.sessionStateTtlMinutes: 30`
- `incident.maxRequestIdsPerIncident: 50`
- `incident.maxFindingsPerSession: 100`

#### Threshold rules

- `incident.promptInjectionBurst.minFindings: 3`
- `incident.promptInjectionBurst.windowMinutes: 5`
- `incident.promptInjectionBurst.highSeverityConfidence: 0.90`
- `incident.toxicityCampaign.minFindings: 3`
- `incident.toxicityCampaign.windowMinutes: 5`
- `incident.toxicityCampaign.highSeverityConfidence: 0.85`
- `incident.loopingPersistence.minOccurrences: 2`
- `incident.leakageWithInjection.windowMinutes: 5`

#### Severity mapping

- `incident.severity.mediumScore: 40`
- `incident.severity.highScore: 70`
- `incident.severity.criticalScore: 90`

#### CLI overrides

Для локального запуска надо поддержать override через `-D` или Flink `--` args:

- `--incident-enabled`
- `--incident-output-topic`
- `--incident-session-state-ttl-minutes`
- `--incident-prompt-injection-burst-min-findings`
- `--incident-toxicity-campaign-min-findings`
- `--incident-emit-updates`

### 4.6 Какие новые topics, metrics и dashboards нужны

#### Kafka topics

- новый topic `basic-incidents`
  - основной incident stream для downstream consumption.
- опционально новый topic `incident-debug`
  - только для локального режима и расследований.

#### Prometheus metrics

- `aisafetyops_incidents_emitted_total`
  - число выпущенных incidents.
- `aisafetyops_incidents_by_severity_total{severity=...}`
  - распределение по severity.
- `aisafetyops_incident_rule_hits_total{rule=...}`
  - какие correlation rules реально срабатывают.
- `aisafetyops_incident_open_sessions_gauge`
  - сколько session states сейчас живет.
- `aisafetyops_incident_findings_per_session_gauge`
  - last observed size текущих сессионных накоплений.
- `aisafetyops_incident_updates_total`
  - сколько раз incident переэмитился после новых findings.

#### Grafana

Добавить новый dashboard:

- `observability/grafana/dashboards/aisafetyops-incidents.json`

Минимальные панели:

- `Incidents by Severity (1m)`
- `Incident Rule Hits (5m)`
- `Top Agents by Incident Count`
- `Top Sessions by Repeated Findings`
- `Incident Re-emissions`
- `Open Session States`

### 4.7 Какие тесты написать

#### Unit tests

- severity scoring для каждого rule family;
- escalation logic `medium -> high -> critical`;
- TTL-expiration поведения session state;
- dedup/request list truncation logic;
- incident serialization contract.

#### Flink operator tests

- `KeyedProcessFunction` test harness для `SessionIncidentEvaluatorFunction`;
- проверка, что findings одной сессии дают incident, а findings разных сессий не смешиваются;
- проверка late update поведения при `emitUpdates=true`.

#### Integration tests

- end-to-end test:
  - подать последовательность findings по `agentId + sessionId`;
  - проверить emission в `basic-incidents`;
  - проверить сохранение существующих aggregate topics.

### 4.8 Business value после 3.1

После `3.1` вы получаете:

- первую операционную сущность, которую можно показать risk-аналитику;
- возможность руками открыть конкретный incident и увидеть, из каких `requestId` и findings он собрался;
- основу для интеграции с case-management или ручным triage;
- более предметный разговор с бизнесом: не "есть всплеск на графике", а "есть конкретные подозрительные агентные сессии".

Как это пощупать:

- запустить replay `prompt_injection_burst`;
- открыть topic `basic-incidents`;
- в Grafana увидеть рост `Incidents by Severity`;
- выбрать `agentId` и руками сопоставить incident с входными findings.

## 5. Подэтап 3.2: Runtime Policy Updates

### 5.1 Цель

Убрать зависимость от hardcoded thresholds внутри incident-логики и сделать policy версионируемым runtime input.

### 5.2 Что реализуем

- policy model для incident thresholds и severity rules;
- загрузка active policy из YAML при старте;
- optional runtime updates через отдельный Kafka topic;
- broadcast state с применением policy ко всем subtasks;
- запись `policyVersion` во все derived outputs incident layer.

### 5.3 Какие файлы менять

#### Новые model classes

- `flink-job/src/main/java/com/bank/aisafetyops/model/PolicyUpdateEvent.java`
- `flink-job/src/main/java/com/bank/aisafetyops/model/IncidentPolicy.java`
- `flink-job/src/main/java/com/bank/aisafetyops/model/SeverityRuleSet.java`

#### Новые app classes

- `flink-job/src/main/java/com/bank/aisafetyops/app/functions/PolicyBroadcastProcessFunction.java`
- `flink-job/src/main/java/com/bank/aisafetyops/app/functions/PolicyAwareIncidentEvaluatorFunction.java`

#### Изменяемые classes

- [JobConfig.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/config/JobConfig.java)
- [JobConfigOptions.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/config/JobConfigOptions.java)
- [OutputTopics.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/config/OutputTopics.java)
- [IncrementOneTopologyBuilder.java](../../flink-job/src/main/java/com/bank/aisafetyops/app/usecase/IncrementOneTopologyBuilder.java)
- [KafkaSourceFactory.java](../../flink-job/src/main/java/com/bank/aisafetyops/infra/source/KafkaSourceFactory.java)
- [YamlJobConfigLoader.java](../../flink-job/src/main/java/com/bank/aisafetyops/infra/config/YamlJobConfigLoader.java)

#### Config and tooling

- `config/job/local-job.yaml`
- новый policy file, например `config/policies/local-incident-policy.yaml`
- [load-policies.sh](../../tools/scripts/load-policies.sh)
- [init-topics.sh](../../tools/scripts/init-topics.sh)
- [reset-topics.sh](../../tools/scripts/reset-topics.sh)

### 5.4 Какие параметры добавить в YAML и CLI

#### Policy source

- `policy.enabled: true|false`
- `policy.sourceType: yaml|kafka`
- `policy.bootstrapFile: config/policies/local-incident-policy.yaml`
- `policy.updatesTopic: policy-updates`
- `policy.requireBootstrapPolicy: true|false`

#### Broadcast state behavior

- `policy.broadcastStateName: incident-policy-state`
- `policy.rejectOlderVersions: true|false`
- `policy.missingPolicyFallback: last-known|bootstrap-fail`

#### CLI overrides

- `--policy-enabled`
- `--policy-source-type`
- `--policy-bootstrap-file`
- `--policy-updates-topic`
- `--policy-reject-older-versions`

### 5.5 Какие новые topics, metrics и dashboards нужны

#### Kafka topics

- `policy-updates`
  - поток runtime updates для incident thresholds.

#### Prometheus metrics

- `aisafetyops_policy_updates_total`
- `aisafetyops_policy_active_version_info{policy_version=...}`
- `aisafetyops_policy_rejected_updates_total`
- `aisafetyops_policy_last_update_epoch_ms`
- `aisafetyops_incident_policy_miss_total`

#### Grafana

Расширить:

- `observability/grafana/dashboards/aisafetyops-incidents.json`
- `observability/grafana/dashboards/aisafetyops-flink-overview.json`

Добавить панели:

- `Active Policy Version`
- `Policy Update Rate`
- `Rejected Policy Updates`
- `Incident Count Before/After Policy Change`

### 5.6 Какие тесты написать

#### Unit tests

- policy version ordering;
- policy merge/replace behavior;
- fallback behavior при отсутствии policy;
- severity recalculation с разными thresholds.

#### Flink operator tests

- broadcast state update test harness;
- проверка, что после policy update новые findings считают severity по новой версии;
- проверка, что старая policy не перезаписывает новую при `rejectOlderVersions=true`.

#### Integration tests

- старт job с YAML bootstrap policy;
- публикация policy update в topic;
- проверка, что subsequent incidents содержат новый `policyVersion`;
- проверка, что aggregate outputs не ломаются.

### 5.7 Business value после 3.2

После `3.2` вы получаете:

- управляемое изменение risk rules без перекомпиляции и redeploy логики;
- наблюдаемую связь между версией policy и incident outcome;
- безопасный способ калибровать собственные guardrail-правила;
- основу для controlled rollout новых порогов.

Как это пощупать:

- загрузить bootstrap policy;
- запустить replay на фиксированном dataset;
- отправить policy update со сниженными thresholds;
- увидеть, как меняются severity и count incidents без перезапуска job.

## 6. Подэтап 3.3: Scenario Replay And Operational Observability

### 6.1 Цель

Сделать систему одновременно:

- воспроизводимой для regression и демонстрации;
- наблюдаемой по runtime contract;
- пригодной для поиска bottleneck-ов и ресурсных ограничений.

### 6.2 Что реализуем

- named replay scenarios;
- delivery/quality modes;
- инъекцию late events, invalid events и detector degradation;
- явную фиксацию runtime contract в конфиге и дашбордах;
- performance-oriented dashboards и PromQL для bottleneck analysis.

### 6.3 Какие файлы менять

#### Tooling

- [generate_events.py](../../tools/generators/generate_events.py)
- [stream_live_events.py](../../tools/generators/stream_live_events.py)
- [run-replay.sh](../../tools/scripts/run-replay.sh)
- [run-live-generator.sh](../../tools/scripts/run-live-generator.sh)
- опционально новый файл:
  - `tools/generators/scenario_profiles.py`

#### Config

- `config/job/local-job.yaml`
- новый replay profile file, например:
  - `config/replay/default-replay.yaml`

#### Docs

- [monitoring-debugging-guide.md](../monitoring/monitoring-debugging-guide.md)
- [mvp-runbook.md](../runbooks/mvp-runbook.md)
- [local-walkthrough.md](../runbooks/local-walkthrough.md)
- [event-contracts.md](../architecture/event-contracts.md)

#### Observability

- `observability/grafana/dashboards/aisafetyops-business-metrics.json`
- `observability/grafana/dashboards/aisafetyops-flink-overview.json`
- новый dashboard:
  - `observability/grafana/dashboards/aisafetyops-capacity-and-performance.json`

### 6.4 Какие параметры добавить в YAML и CLI

#### Runtime contract visibility

Эти параметры уже есть или частично есть, но теперь должны быть явно документированы и выводимы в observability:

- `runtime.windowType: tumbling-event-time`
- `runtime.windowSizes: [1m, 5m]`
- `runtime.outOfOrdernessSeconds: 30`
- `runtime.idleTimeoutMinutes: 1`
- `runtime.lateToleranceMinutes: 5`
- `runtime.checkpointIntervalSeconds: 30`
- `runtime.autoWatermarkIntervalSeconds: 5`
- `runtime.deliveryGuarantee: at-least-once`

Если часть значений уже задается плоскими ключами, на этом этапе можно:

- либо сохранить backward compatibility и просто добавить export в docs/metrics;
- либо ввести новую секцию `runtime.*` и сделать controlled migration.

#### Replay configuration

- `replay.businessScenario: normal|attack|mixed|prompt_injection_burst|toxicity_campaign|looping_false_positive_check|policy_regression_case`
- `replay.deliveryMode: baseline|late-events|invalid-events|detector-errors|combined-chaos`
- `replay.durationSeconds: 300`
- `replay.minRps: 1`
- `replay.maxRps: 5`
- `replay.burstStartSecond: 60`
- `replay.burstDurationSeconds: 45`
- `replay.burstMultiplier: 5`
- `replay.lateShare: 0.10`
- `replay.tooLateShare: 0.03`
- `replay.invalidShare: 0.05`
- `replay.errorShare: 0.05`
- `replay.detectorLatencyMultiplier: 3.0`
- `replay.outOfOrdernessSeconds: 30`

#### CLI overrides

- `--business-scenario`
- `--delivery-mode`
- `--duration-seconds`
- `--min-rps`
- `--max-rps`
- `--burst-start-second`
- `--burst-duration-seconds`
- `--burst-multiplier`
- `--late-share`
- `--too-late-share`
- `--invalid-share`
- `--error-share`
- `--detector-latency-multiplier`
- `--out-of-orderness-seconds`

### 6.5 Какие новые topics, metrics и dashboards нужны

#### Kafka topics

Новых обязательных topics здесь нет, если использовать уже существующие:

- `raw-events`
- `invalid-events`
- `late-events`
- `guardrail-window-aggregates`
- `basic-incidents`
- `policy-updates`

Опционально можно добавить:

- `detector-quality-metrics`
  - если решите публиковать quality snapshots не только в Prometheus, но и в Kafka.

#### Prometheus metrics

##### Runtime contract visibility

- `aisafetyops_runtime_contract_info{window_type=...,window_sizes=...,delivery_guarantee=...}`
- `aisafetyops_runtime_out_of_orderness_seconds`
- `aisafetyops_runtime_late_tolerance_minutes`
- `aisafetyops_runtime_checkpoint_interval_seconds`

##### Performance and saturation

- `aisafetyops_source_records_in_total`
- `aisafetyops_aggregate_records_out_total`
- `aisafetyops_incident_records_out_total`
- `aisafetyops_operator_busy_ratio_gauge`
- `aisafetyops_operator_backpressure_proxy_gauge`
- `aisafetyops_checkpoint_duration_ms`
- `aisafetyops_checkpoint_failures_total`
- `aisafetyops_taskmanager_heap_used_ratio`
- `aisafetyops_taskmanager_cpu_used_ratio`
- `aisafetyops_kafka_consumer_lag`

#### Grafana

##### Existing dashboards to extend

- `AISafetyOps Business Metrics`
  - добавить annotation/variables для scenario mode;
  - показать эффект burst и late events.
- `AISafetyOps Flink Overview`
  - добавить checkpoint, lag, restart и runtime contract panels.

##### New dashboard

- `AISafetyOps Capacity And Performance`

Минимальные панели:

- `Kafka Consumer Lag By Topic`
- `Source Throughput vs Aggregate Emissions`
- `Incident Emission Throughput`
- `Operator Busy Ratio`
- `Operator Backpressure Proxy`
- `Checkpoint Duration Trend`
- `Checkpoint Failures Trend`
- `TaskManager CPU Used Ratio`
- `TaskManager Heap Used Ratio`
- `Restart Count`
- `Runtime Contract Summary`

### 6.6 Какие тесты написать

#### Tooling tests

- unit tests для сценарной генерации:
  - правильный mix guardrail types;
  - late/invalid/error shares соответствуют конфигу;
  - burst действительно увеличивает RPS и trigger rates.

#### Integration tests

- replay `prompt_injection_burst` должен приводить к росту:
  - `PROMPT_INJECTION` findings;
  - incident count;
  - `triggeredP95Confidence`.
- replay `late-events` должен давать:
  - часть событий в основном окне;
  - часть как late updates;
  - часть в `late-events`.
- replay `invalid-events` должен увеличивать `invalid-events` sink.
- replay `detector-errors` должен поднимать detector quality metrics.

#### Regression tests

- полный `run-regression.sh` после каждого подэтапа;
- отдельный scripted smoke:
  - `baseline`;
  - `prompt_injection_burst`;
  - `combined-chaos`.

### 6.7 Business value после 3.3

После `3.3` вы получаете:

- воспроизводимый сценарный regression suite для risk use-cases;
- прозрачное понимание, какие runtime guarantees реально дает текущая Flink job;
- возможность быстро видеть, где система тормозит и где упирается в ресурсы;
- лучшую готовность к переносу в банковский контур.

Как это пощупать:

- запустить `baseline` и `prompt_injection_burst`;
- сравнить дашборды по findings, incidents и lag;
- включить `late-events` или `combined-chaos`;
- увидеть, как меняются `invalid`, `late`, checkpoint и lag panels.

## 7. Рекомендуемая последовательность реализации

Практически безопаснее делать `Increment 3` в таком порядке:

1. `3.1a`
   - добавить `BasicIncident` model, sink и простейшую single-rule correlation.
2. `3.1b`
   - расширить до всех 4 rule family и severity mapping.
3. `3.2a`
   - bootstrap policy из YAML без Kafka updates.
4. `3.2b`
   - runtime `policy-updates` через broadcast state.
5. `3.3a`
   - named replay scenarios и chaos modes.
6. `3.3b`
   - capacity/performance dashboard и runtime contract panels.

Это снижает риск смешать новую бизнес-логику с новой динамической конфигурацией и сразу облегчает локальную проверку.

## 8. Business value по подэтапам

### После 3.1a

- уже есть первый incident topic;
- можно показать конкретные подозрительные сессии по `agentId + sessionId`;
- ценность видна даже без dynamic policy.

### После 3.1b

- incident classification становится пригодной для ручного triage;
- risk-команда видит не просто "много finding-ов", а типовые operational patterns.

### После 3.2a

- thresholds перестают быть зашитыми в код;
- можно калибровать систему через YAML и быстро перезапускать локальный стенд.

### После 3.2b

- thresholds меняются без redeploy;
- появляется controlled runtime tuning и traceability по `policyVersion`.

### После 3.3a

- появляется воспроизводимый regression language для бизнеса, QA и аудита;
- можно демонстрировать систему сценариями, а не только ручной генерацией событий.

### После 3.3b

- появляется понятная картина производительности и ресурсных ограничений;
- снижается риск "локально работало, а в инфраструктуре внезапно деградировало".

## 9. Definition of Done для всего Increment 3

`Increment 3` считается завершенным, если:

- job продолжает публиковать существующие aggregates без регрессии;
- incident stream публикуется в Kafka и виден в Grafana;
- policy thresholds читаются из YAML, а затем могут обновляться runtime-потоком;
- replay tooling поддерживает business scenarios и chaos modes;
- observability показывает runtime contract, lag, checkpoint behavior и saturation signals;
- для каждого подэтапа есть локальные unit/integration tests;
- после каждого подэтапа проходит полный regression.

## 10. Что намеренно не включено

В этот `Increment 3` специально не включаем:

- тяжелый CEP как обязательную базу;
- PyFlink в critical path;
- внешние REST lookups в runtime path;
- exactly-once downstream semantics;
- автоматическое создание кейсов во внешних системах.

Это можно планировать следующим шагом только после стабилизации incident layer и policy control plane.
