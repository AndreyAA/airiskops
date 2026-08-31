# AIRiskOps Flink MVP: Stage 2 Results

Дата фиксации: 2026-08-27

## Что реализовано

Stage 2 добавляет оконную агрегацию по событиям `GUARDRAIL_FINDING` и пишет результат в Kafka topic `guardrail-aggregates`.

Реализованы:

- агрегаты по окнам `1m` и `5m`;
- группировка по `agentId`, `guardrailName`, `guardrailVersion`, `policyVersion`, `modelName`;
- метрики по числу сработок, triggered-событиям, confidence, latency, ошибкам детектора, токенам;
- сериализация агрегатов в JSON и публикация в отдельный output topic;
- интеграционный тест на event-time window emission;
- интеграционный тест на late-arrival replay pattern.

## Найденная проблема

На чистом локальном replay оконные операторы получали `480` записей, но не эмитили ни одного aggregate.

Симптомы:

- `Guardrail Aggregates 1m`: `numRecordsIn=480`, `numRecordsOut=0`;
- `Guardrail Aggregates 5m`: `numRecordsIn=480`, `numRecordsOut=0`;
- Kafka topic `guardrail-aggregates` оставался пустым.

Корневая причина:

- replay публикует события по темам последовательно:
  - сначала `agent-requests`,
  - потом `agent-responses`,
  - потом `guardrail-findings`;
- watermark успевает продвинуться по request/response событиям;
- `RouteLateEventsFunction` пропускает такие guardrail-события, потому что они ещё попадают в нашу бизнес-договорённость `lateTolerance=5m`;
- но сами event-time окна имели `allowedLateness=0`, поэтому window operator дропал их как late records.

## Исправление

В `IncrementOneTopologyBuilder` для оконных агрегаций добавлен:

```java
.allowedLateness(Time.milliseconds(config.lateTolerance().toMillis()))
```

То есть оконная ветка теперь использует ту же tolerance-модель, что и основной NRTP pipeline.

## Проверка

### Регрессия

Полный локальный регресс пройден:

- Python tests: `3/3 OK`
- Java/Flink tests: `11/11 OK`

### Сборка

Shaded artifact собран успешно:

- `flink-job/target/flink-airiskops-1.0.0-SNAPSHOT-all.jar`

### Живая проверка

Чистый локальный прогон:

1. `docker compose -f deployment/local/docker-compose.yml down`
2. `bash tools/scripts/start-local.sh`
3. `./tools/scripts/init-topics.sh`
4. `bash tools/scripts/submit-job.sh`
5. `bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01`

Job:

- `JobID: 245780a7a41cb089d902b32af4d8b7dc`

Фактический результат:

- `Guardrail Aggregates 1m`: `read-records=480`, `write-records=360`
- `Serialize Guardrail Aggregates`: `read-records=360`
- Kafka offsets:
  - `guardrail-aggregates:0:100`
  - `guardrail-aggregates:1:96`
  - `guardrail-aggregates:2:164`

Итого в Kafka опубликовано `360` aggregate-сообщений.

Пример output:

```json
{
  "tenantId": "agent-risk-01",
  "agentId": "agent-risk-01",
  "guardrailName": "PROMPT_INJECTION",
  "guardrailVersion": "pi-v1",
  "policyVersion": "policy-v1",
  "modelName": "gpt-4.1-mini",
  "windowName": "1m",
  "windowStartMillis": 1787745600000,
  "windowEndMillis": 1787745660000,
  "totalEvents": 2,
  "guardrailFindingCount": 2,
  "triggeredCount": 1,
  "loopingTriggeredCount": 0,
  "systemPromptLeakageTriggeredCount": 0,
  "inputTokens": 301,
  "outputTokens": 501,
  "minConfidence": 0.6255,
  "avgConfidence": 0.6691499999999999,
  "maxConfidence": 0.7128,
  "minDetectorLatencyMs": 20,
  "avgDetectorLatencyMs": 20.0,
  "maxDetectorLatencyMs": 20,
  "detectorErrorCount": 0
}
```

## Важное замечание по окну 5m

В текущем replay-сценарии `120 requests` покрывают примерно `4` минуты event time.

Следствие:

- окно `1m` закрывается и даёт видимые aggregates;
- окно `5m` в таком сценарии ещё не закрывается, поэтому `write-records=0` для `Guardrail Aggregates 5m` является ожидаемым поведением, а не дефектом.

Чтобы увидеть `5m` aggregates локально, нужно одно из двух:

- увеличить replay хотя бы до `150+` запросов;
- или добавить события, которые продвинут watermark дальше конца пятиминутного окна.

## Business Value после Stage 2

После Stage 2 команда Operational Risk получает:

- потоковый near-real-time обзор по сработкам каждого guardrail;
- возможность быстро увидеть, какой агент даёт рост инъекций, токсичности, зацикливания или утечки системного промпта;
- базу для алертов, дашбордов и последующего enrichment/stateful анализа;
- проверяемый Kafka-output контракт, который можно подключать к downstream системам расследования и мониторинга.
