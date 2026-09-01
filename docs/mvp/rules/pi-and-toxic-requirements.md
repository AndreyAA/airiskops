# PI_AND_TOXIC Rule Specification

Дата актуальности: 2026-08-31

## 1. Назначение

`PI_AND_TOXIC` — session-level incident rule для AIRiskOps MVP.

Правило сигнализирует, что в рамках одной пользовательской сессии одновременно наблюдаются признаки:

- `PROMPT_INJECTION`;
- `TOXICITY`.

Такая комбинация трактуется как отдельный incident signal с более высоким приоритетом, чем одиночные findings каждого типа по отдельности.

## 2. Scope

- `ruleName`: `PI_AND_TOXIC`
- `scope`: `session`
- `correlationKey`: текущий session incident key
- `outputType`: `BasicIncident`

## 3. Time Semantics

- time model: `eventTime`
- correlation window: configurable
- default correlation window: `5m`

Finding считается eligible для rule window, если его `eventTime` попадает в интервал:

- `eventTime >= referenceTime - window`
- `eventTime <= referenceTime`

Boundary semantics: inclusive.

## 4. Input Signals

Rule рассматривает только findings следующих типов:

- `PROMPT_INJECTION`
- `TOXICITY`

Базовая eligibility:

- `triggered == true`

Findings с `triggered == false` никогда не участвуют в этом правиле.

## 5. Config Fields

Обязательные поля:

- `incidentPiAndToxicEnabled`
- `incidentPiAndToxicWindowMinutes`
- `incidentPiAndToxicSeverity`
- `incidentPiAndToxicMinPromptInjectionTriggeredCount`
- `incidentPiAndToxicMinToxicityTriggeredCount`

Поддерживаемые optional поля:

- `incidentPiAndToxicMinPromptInjectionConfidence`
- `incidentPiAndToxicMinToxicityConfidence`

Рекомендуемые defaults:

- `enabled: true`
- `windowMinutes: 5`
- `severity: HIGH`
- `minPromptInjectionTriggeredCount: 1`
- `minToxicityTriggeredCount: 1`
- `minPromptInjectionConfidence: null`
- `minToxicityConfidence: null`

## 6. Qualification Logic

Для каждого finding qualification определяется в два этапа.

### Stage 1. Base Filter

Finding проходит базовый фильтр, если:

- его `guardrailName` соответствует ожидаемому типу;
- `triggered == true`.

### Stage 2. Confidence Filter

Для `PROMPT_INJECTION`:

- если `minPromptInjectionConfidence` не задан:
  - любой base-eligible `PROMPT_INJECTION` finding считается qualified;
  - отсутствие confidence не мешает qualification;
- если `minPromptInjectionConfidence` задан:
  - finding считается qualified только если confidence присутствует и `confidence >= threshold`;
  - если confidence отсутствует, finding не считается qualified;
  - если confidence ниже threshold, finding не считается qualified.

Для `TOXICITY`:

- если `minToxicityConfidence` не задан:
  - любой base-eligible `TOXICITY` finding считается qualified;
  - отсутствие confidence не мешает qualification;
- если `minToxicityConfidence` задан:
  - finding считается qualified только если confidence присутствует и `confidence >= threshold`;
  - если confidence отсутствует, finding не считается qualified;
  - если confidence ниже threshold, finding не считается qualified.

Правило сравнения threshold:

- использовать `>=`.

## 7. Trigger Condition

Rule emits incident, если в одном и том же rule window одновременно выполняются оба условия:

- `qualifiedPromptInjectionCount >= minPromptInjectionTriggeredCount`
- `qualifiedToxicityCount >= minToxicityTriggeredCount`

Если хотя бы одно условие не выполнено, incident не выпускается.

## 8. Severity

- severity задаётся конфигом;
- severity не выводится автоматически из confidence;
- default severity: `HIGH`.

## 9. Emission Semantics

- При первом выполнении условия выпускается один incident `PI_AND_TOXIC`.
- Если `incidentEmitUpdates == false`, новые qualifying findings не должны выпускать второй incident этого rule.
- Если `incidentEmitUpdates == true`, допускается повторная emission как update с новой revision.
- Rule не suppress'ит существующие incident rules.
- Одна и та же session может породить:
  - `PI_AND_TOXIC`;
  - `PROMPT_INJECTION_BURST`;
  - `TOXICITY_CAMPAIGN`;
  - другие rules, если их условия тоже выполнены.

## 10. Payload Requirements

Каждый emitted `BasicIncident` для этого rule должен содержать:

- `ruleName = PI_AND_TOXIC`
- `severity`
- `requestIds`
- `guardrailNames`
- `triggeredFindingsCount`
- `appliedPolicyVersion`
- `firstEventTimeMillis`
- `lastEventTimeMillis`
- `emittedAtEventTimeMillis`

Поле `summary` должно явно включать:

- qualified `PROMPT_INJECTION` count;
- qualified `TOXICITY` count;
- max qualified `PROMPT_INJECTION` confidence;
- max qualified `TOXICITY` confidence.

Рекомендуемые structured extensions на будущее:

- `promptInjectionCount`
- `toxicityCount`
- `maxPromptInjectionConfidence`
- `maxToxicityConfidence`

## 11. State Semantics

- Rule должен использовать event-time correlation window, а не всю историю session без ограничений.
- Длинная session не должна бесконечно накапливать findings для этого rule.
- В расчёт должны входить только findings, которые остаются внутри активного correlation window.
- После cleanup по inactivity timeout старая session state больше не должна влиять на новую активность.

## 12. Late Event Semantics

- Если finding ещё участвует в incident layer и попадает в rule window, он может участвовать в rule.
- Если finding уже исключён из incident processing как слишком late, он не должен влиять на rule.
- Поведение должно оставаться согласованным с текущей late-event semantics пайплайна.

## 13. Dedup Semantics

- В MVP, если явный dedup отсутствует, повторно пришедшие идентичные findings считаются отдельными findings.
- Это признанное ограничение MVP.
- Если dedup будет добавлен позже, его semantics должна описываться отдельно для каждого rule.

## 14. Config Update Semantics

- `PI_AND_TOXIC` в текущей реализации настраивается через job config, а не через runtime policy updates.
- Изменение параметров rule требует обновления конфигурации и нового submit job.
- Накопленная session state не пересчитывается задним числом после restart.

## 15. Edge Cases

Ниже перечислены обязательные edge cases, которые rule specification должна покрывать явно.

### 15.1 Один тип есть, второго нет

- `PROMPT_INJECTION` присутствует, `TOXICITY` отсутствует: incident не emitted.
- `TOXICITY` присутствует, `PROMPT_INJECTION` отсутствует: incident не emitted.

### 15.2 Не выполнен count threshold

- Один из guardrail types присутствует, но не достигает своего configured minimum.
- Incident не emitted.

### 15.3 Не выполнен confidence threshold

- Finding есть и `triggered=true`, но confidence ниже required threshold.
- Такой finding не qualified.

### 15.4 Confidence отсутствует

- Если threshold для данного guardrail не задан, finding может считаться qualified.
- Если threshold задан, finding без confidence не qualified.

### 15.5 Смешанные qualified и non-qualified findings

- В счёт идут только qualified findings.
- Non-qualified findings не должны влиять на count conditions.

### 15.6 Граница окна

- Event ровно на границе окна qualified.
- Event на `1 ms` за пределом окна не qualified.

### 15.7 Порядок поступления

- Rule зависит от `eventTime`, а не от порядка arrival.
- Если `TOXICITY` пришёл раньше `PROMPT_INJECTION`, rule всё равно может сработать, если оба попали в окно.

### 15.8 Дубликаты

- При отсутствии dedup semantics дубликаты считаются отдельными findings.
- Это может ускорить emission и считается текущим ограничением MVP.

### 15.9 Update behavior

- При `incidentEmitUpdates=false` repeated qualifying events не должны давать новый incident.
- При `incidentEmitUpdates=true` update допустим.

### 15.10 Config changes after restart

- Более строгий config может предотвратить future emission после нового запуска job.
- Более мягкий config может разрешить emission в следующих сессиях после нового запуска job.

### 15.11 Session cleanup

- После cleanup по inactivity timeout старая history не должна участвовать в новых emissions.

## 16. Required Test Matrix

### Positive Cases

- Один qualified `PROMPT_INJECTION` и один qualified `TOXICITY` внутри окна эмитят `PI_AND_TOXIC`.
- Оба count thresholds больше `1` и оба выполнены: incident emitted.
- События пришли в обратном порядке, но внутри одного окна: incident emitted.

### Negative Cases

- Только `PROMPT_INJECTION`: incident not emitted.
- Только `TOXICITY`: incident not emitted.
- Один из типов есть только с `triggered=false`: incident not emitted.
- Один из типов не достигает configured count threshold: incident not emitted.
- Один из типов не проходит confidence threshold: incident not emitted.
- Один из типов без confidence при включённом threshold: incident not emitted.
- События попали вне configured window: incident not emitted.

### Boundary Cases

- `confidence == threshold`: finding qualified.
- Event ровно на границе окна: finding qualified.
- Event на `1 ms` за границей окна: finding not qualified.

### Mixed Filter Cases

- Threshold задан только для `PROMPT_INJECTION`, для `TOXICITY` не задан: проверить mixed semantics.
- Threshold задан только для `TOXICITY`, для `PROMPT_INJECTION` не задан: проверить mixed semantics.
- Из нескольких findings одного типа только часть qualified: в счёт идут только qualified.

### Update Cases

- При `incidentEmitUpdates=false` repeated emission не происходит.
- При `incidentEmitUpdates=true` update может переэмититься с новой revision.

### Config Cases

- После restart с более строгим config emission не происходит, если новые thresholds не выполнены.
- После restart с более мягким config emission может происходить для новых qualifying sessions.

### State Lifecycle Cases

- После session cleanup прежние findings больше не должны влиять на новую activity.

### Known Limitation Cases

- Повторно пришедшие дубликаты при отсутствии dedup учитываются как отдельные findings.

## 17. Template For New Incident Rules

Каждое новое rule должно быть описано по одному и тому же шаблону.

- `ruleName`
- `businessPurpose`
- `scope`
- `correlationKey`
- `timeSemantics`
- `window`
- `inputSignals`
- `baseEligibility`
- `optionalFilters`
- `countThresholds`
- `dedupSemantics`
- `lateEventSemantics`
- `severitySource`
- `emissionSemantics`
- `stateRequirements`
- `payloadRequirements`
- `metricsRequirements`
- `testMatrix`
- `knownLimitations`

## 18. Recommendations For New Rules

- Не добавлять новое rule без явной window semantics.
- Не добавлять новое rule без явной semantics для missing confidence.
- Не добавлять новое rule без negative и boundary tests.
- Для combination rules thresholds должны задаваться отдельно по каждому входному сигналу.
- Если rule зависит от последовательности событий, эта последовательность должна быть описана явно.
- Если rule пересекается с существующими rules, нужно заранее зафиксировать:
  - допускается ли параллельная emission;
  - нужен ли suppression;
  - нужен ли precedence order.
- Если rule требует нового state, нужно заранее проверить рост state и cleanup semantics.
- Если rule может поднять cardinality метрик или размер incident payload, это должно быть отдельным review пунктом.
