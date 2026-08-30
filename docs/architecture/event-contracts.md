# Kafka Event Contracts для AISafetyOps Flink MVP

Дата актуальности: 2026-08-29

## Назначение

Этот документ фиксирует текущий Kafka event contract локального AISafetyOps MVP:

- какие topics используются;
- какие JSON-сообщения по ним ходят;
- какие поля обязательны;
- как события связаны между собой;
- почему для Flink выбрана модель `одно сообщение = одно событие`.

Документ относится к текущему локальному MVP и должен обновляться при изменении:

- набора topics;
- схемы JSON;
- обязательных полей;
- правил валидации;
- стратегии корреляции событий.

## 1. Базовый принцип модели событий

Для текущего MVP действует правило:

- одно Kafka-сообщение = одно доменное событие;
- один пользовательский запрос = набор связанных событий, а не одно сообщение;
- связь между событиями строится по `agentId`, `sessionId`, `requestId`.

Это означает:

- `AGENT_REQUEST` публикуется отдельным событием;
- `AGENT_RESPONSE` публикуется отдельным событием;
- каждый результат конкретного гардрейла публикуется отдельным `GUARDRAIL_FINDING`.

Для одного `requestId` типичная картина такая:

- 1 событие `AGENT_REQUEST`;
- 1 событие `AGENT_RESPONSE`;
- до 4 событий `GUARDRAIL_FINDING`;
- `PROMPT_INJECTION`;
- `TOXICITY`;
- `LOOPING`;
- `SYSTEM_PROMPT_LEAKAGE`.

Почему это правильно для Flink:

- проще event-time обработка и watermarks;
- проще late-event routing;
- проще join и корреляция по ключам;
- проще расследование инцидентов;
- проще перейти с JSON на schema-based формат позже.

## 2. Входные topics

### 2.1 `agent-requests`

Назначение:

- фиксирует факт поступления пользовательского запроса в конкретный агент;
- даёт точку входа для NRTP-анализа активности по `agentId` и `sessionId`;
- даёт контекст по входным токенам и каналу взаимодействия.

Тип события:

- `AGENT_REQUEST`

Пример JSON:

```json
{
  "eventType": "AGENT_REQUEST",
  "agentId": "agent-risk-01",
  "tenantId": "agent-risk-01",
  "sessionId": "session-001",
  "requestId": "req-123",
  "turnId": "turn-00001",
  "eventTime": "2026-08-29T10:00:00Z",
  "modelName": "gpt-4.1-mini",
  "userId": "user-001",
  "channel": "web",
  "inputTokens": 180,
  "outputTokens": 0
}
```

Что означает:

- пользователь через канал `web` отправил запрос в агент `agent-risk-01`;
- запрос относится к сессии `session-001`;
- это конкретный бизнес-запрос `requestId=req-123`;
- `inputTokens` показывают размер входа в модель.

### 2.2 `agent-responses`

Назначение:

- фиксирует факт ответа агента или модели;
- даёт точку корреляции между запросом и downstream findings;
- даёт базу для анализа output-токенов и latency detector-а относительно ответа.

Тип события:

- `AGENT_RESPONSE`

Пример JSON:

```json
{
  "eventType": "AGENT_RESPONSE",
  "agentId": "agent-risk-01",
  "tenantId": "agent-risk-01",
  "sessionId": "session-001",
  "requestId": "req-123",
  "turnId": "turn-00001",
  "eventTime": "2026-08-29T10:00:01Z",
  "modelName": "gpt-4.1-mini",
  "userId": "user-001",
  "channel": "web",
  "inputTokens": 0,
  "outputTokens": 320
}
```

Что означает:

- агент завершил обработку запроса `req-123`;
- ответ вышел через 1 секунду после входного запроса;
- `outputTokens` показывают размер модельного ответа;
- именно к этому ответу могут относиться downstream findings.

### 2.3 `guardrail-findings`

Назначение:

- содержит сырые результаты срабатывания гардрейлов;
- является главным входом для risk analytics, оконной агрегации и метрик;
- даёт материал для расследований по `agentId`, `sessionId`, `requestId`.

Тип события:

- `GUARDRAIL_FINDING`

Пример JSON для confidence-based гардрейла:

```json
{
  "eventType": "GUARDRAIL_FINDING",
  "guardrailName": "PROMPT_INJECTION",
  "guardrailVersion": "pi-v1",
  "policyVersion": "policy-v1",
  "agentId": "agent-risk-01",
  "tenantId": "agent-risk-01",
  "sessionId": "session-001",
  "requestId": "req-123",
  "eventTime": "2026-08-29T10:00:01Z",
  "modelName": "gpt-4.1-mini",
  "inputTokens": 180,
  "outputTokens": 320,
  "confidence": 0.91,
  "triggered": true,
  "detectorLatencyMs": 18,
  "detectorStatus": "OK"
}
```

Пример JSON для boolean гардрейла:

```json
{
  "eventType": "GUARDRAIL_FINDING",
  "guardrailName": "LOOPING",
  "guardrailVersion": "loop-v1",
  "policyVersion": "policy-v1",
  "agentId": "agent-risk-01",
  "tenantId": "agent-risk-01",
  "sessionId": "session-001",
  "requestId": "req-123",
  "eventTime": "2026-08-29T10:00:01Z",
  "modelName": "gpt-4.1-mini",
  "inputTokens": 180,
  "outputTokens": 320,
  "triggered": false,
  "detectorLatencyMs": 7,
  "detectorStatus": "OK"
}
```

Что означает:

- одно сообщение в `guardrail-findings` соответствует одному результату одного детектора;
- для `PROMPT_INJECTION` и `TOXICITY` ожидается `confidence`;
- для `LOOPING` и `SYSTEM_PROMPT_LEAKAGE` ключевой сигнал сейчас boolean `triggered`;
- `detectorLatencyMs` показывает задержку вычисления конкретного гардрейла;
- `policyVersion` и `guardrailVersion` нужны для аудита и расследований.

## 3. Обязательные поля в MVP

Для всех входных событий обязательны:

- `eventType`
- `agentId`
- `sessionId`
- `requestId`
- `eventTime`

Дополнительно для `GUARDRAIL_FINDING` обязательны:

- `guardrailName`
- `triggered`

Дополнительно для confidence-based гардрейлов обязательны:

- `confidence` для `PROMPT_INJECTION`
- `confidence` для `TOXICITY`

Если этих полей нет:

- событие не должно участвовать в основной аналитике;
- оно уходит в `invalid-events`.

## 4. Выходные topics

### 4.1 `normalized-events`

Назначение:

- поток валидных нормализованных событий после парсинга;
- единый внутренний контракт, с которым дальше работает job.

Что туда попадает:

- валидные `AGENT_REQUEST`;
- валидные `AGENT_RESPONSE`;
- валидные `GUARDRAIL_FINDING`.

Что это значит:

- intake, schema validation и нормализация работают корректно;
- downstream этапы получают уже унифицированный поток.

### 4.2 `invalid-events`

Назначение:

- поток сообщений, которые не прошли обязательную валидацию.

Типичные причины:

- отсутствует `eventType`;
- отсутствует `agentId`, `sessionId`, `requestId` или `eventTime`;
- для `GUARDRAIL_FINDING` отсутствует `guardrailName`;
- для `PROMPT_INJECTION` или `TOXICITY` отсутствует `confidence`;
- для `GUARDRAIL_FINDING` отсутствует `triggered`.

Что это значит:

- upstream producer нарушил контракт;
- такие записи нужно разбирать отдельно как data quality issue.

### 4.3 `late-events`

Назначение:

- поток валидных, но слишком поздних событий для текущего окна и watermark policy.

Что это значит:

- событие формально корректное;
- но в текущую event-time аналитику оно уже не вписывается;
- рост этого topic обычно означает проблему с задержкой доставки или timestamp discipline upstream-систем.

### 4.4 `guardrail-aggregates`

Назначение:

- поток оконных агрегатов по сработкам гардрейлов;
- главный бизнес-результат текущего MVP.

Что туда попадает:

- агрегаты по `agentId + guardrailName + windowName`;
- сейчас строятся окна `1m` и `5m`.

Что это значит:

- именно этот topic показывает NRTP risk picture по агентам;
- его можно читать из downstream alerting, dashboards и incident tooling.

### 4.5 `basic-incidents`

Назначение:

- поток минимальных incident-сигналов по `agentId + sessionId`;
- первый operational output поверх сырых findings и оконных агрегатов.

Что туда попадает:

- incidents по правилам:
  - `PROMPT_INJECTION_BURST`;
  - `TOXICITY_CAMPAIGN`;
  - `LEAKAGE_WITH_INJECTION`;
  - `LOOPING_PERSISTENCE`.

Что содержит запись:

- `incidentId`;
- `tenantId`;
- `agentId`;
- `sessionId`;
- `ruleName`;
- `severity`;
- список связанных `requestIds`;
- список guardrail names, versions и policy versions;
- `appliedPolicyVersion`;
- `firstEventTimeMillis` и `lastEventTimeMillis`;
- `triggeredFindingsCount`;
- `emissionRevision`;
- `summary`.

Что это значит:

- пайплайн умеет переходить от telemetry и dashboards к конкретным подозрительным сессиям;
- risk-команда может разбирать не только всплеск на графике, но и конкретный incident payload для triage.
- видно, по какой bootstrap policy incident был классифицирован в текущем runtime.

## 5. Topics следующей очереди

Следующие topics уже заведены для развития, но пока не являются центральной частью runtime-потока MVP:

### 5.1 `guardrail-quality-metrics`

Потенциальное назначение:

- отдельный поток quality-сигналов по работе детекторов;
- метрики полноты, ошибок, нестабильности и дрейфа.

### 5.2 `policy-updates`

Потенциальное назначение:

- поток обновлений policy для будущего dynamic rules / broadcast state сценария.

### 5.3 `guardrail-quality-metrics`

Потенциальное назначение:

- отдельный поток quality-сигналов по работе детекторов;
- агрегаты по invalid/late/error degradation вне основного incident stream.

### 5.4 `debug-incidents`

Потенциальное назначение:

- диагностический поток для расследований и enrich/debug output.

## 6. Почему не одно большое сообщение на весь запрос

Для AISafetyOps tempting-подход выглядит так:

- взять один `requestId`;
- собрать в одном JSON и запрос, и ответ, и все findings.

Для MVP это хуже, чем event-by-event модель:

- теряется естественная временная последовательность;
- сложнее обрабатывать late events;
- сложнее отдельно считать latency детекторов;
- сложнее отлаживать частичные сбои;
- сложнее масштабировать pipeline по разным типам событий.

Правильная интерпретация текущего дизайна:

- одно Kafka-сообщение должно быть одним событием;
- один пользовательский запрос почти всегда представлен несколькими связанными событиями;
- агрегированная бизнес-картина собирается уже во Flink.
