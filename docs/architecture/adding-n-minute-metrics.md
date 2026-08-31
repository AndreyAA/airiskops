# Flink для AIRiskOps: как правильно добавить новую метрику с агрегированием за N минут

Дата актуальности: 2026-08-29

## Глоссарий

- **Метрика** — числовой сигнал, который публикуется из Flink runtime в систему мониторинга, например в Prometheus.
- **Бизнес-метрика** — метрика, отражающая смысл доменного процесса, например число triggered findings по guardrail.
- **Техническая метрика** — метрика здоровья runtime, например failed checkpoints или mailbox latency.
- **Окно** — временной интервал, внутри которого Flink группирует события для расчёта агрегата.
- **Event Time** — бизнес-время события из payload, а не системное время машины.
- **Watermark** — оценка прогресса event time, по которой Flink понимает, когда окно можно закрывать.
- **Emission** — публикация агрегированного результата окна downstream.
- **Finding** — сырое событие срабатывания guardrail по конкретному запросу или ответу агента.
- **Guardrail aggregate** — агрегат по окну, guardrail и агенту, который публикуется в `guardrail-aggregates`.
- **Metric labels** — измерения метрики, например `window="1m"` и `guardrail="TOXICITY"`.
- **Cardinality** — число уникальных комбинаций labels у метрики.

## 1. Назначение документа

Этот документ объясняет, как в вашем AIRiskOps-пайплайне правильно добавить новую метрику, которая отражает агрегированное значение за `N` минут.

Фокус на практическом кейсе:

- входной поток состоит из событий LLM-агентов и guardrail findings;
- окна анализа составляют `1-5` минут;
- интересуют агрегированные сигналы по `agentId`, `guardrail`, типу риска, токенам, ошибкам детекторов;
- метрики потом должны быть доступны в Prometheus и Grafana.

Документ ориентирован на Java-разработчика, который умеет писать код, но пока не хочет наступить на типичные ловушки Flink-мониторинга.

## 2. Главный принцип

Если нужен показатель "за N минут", почти всегда лучше делать это в два слоя:

1. Сначала считать **доменный оконный агрегат** внутри data pipeline.
2. Потом уже строить **наблюдаемую метрику** поверх этого агрегата.

То есть правильный вопрос не "как мне сразу сделать Prometheus-метрику за 5 минут", а:

- где в pipeline рождается корректный оконный бизнес-результат;
- как его безопасно и интерпретируемо отразить в метриках.

Для AIRiskOps это особенно важно, потому что:

- одно сырое событие может породить несколько findings;
- окна работают по event time, а не по wall clock;
- late events могут переизлучить окно;
- один и тот же бизнес-сигнал можно неверно посчитать, если считать его в source, а не на этапе готового aggregate.

## 3. Какие есть варианты добавления метрики

### Вариант A. Метрика по сырым событиям

Пример:

- число входных `GUARDRAIL_FINDING`;
- число invalid событий;
- число late событий.

Когда подходит:

- нужен operational signal intake-слоя;
- не нужна оконная бизнес-интерпретация;
- допустимо считать значения по факту прохождения события через оператор.

Когда не подходит:

- нужен показатель именно "за N минут";
- значение должно совпадать с оконным business aggregate;
- важно учитывать watermark и late events.

### Вариант B. Метрика по готовым оконным агрегатам

Пример:

- число triggered findings в окне `1m`;
- число detector errors в окне `5m`;
- суммарные `inputTokens` по guardrail за окно;
- доля toxicity findings выше порога за окно.

Когда подходит:

- нужен показатель "что реально получилось на окне";
- важно, чтобы метрика соответствовала business semantics;
- downstream уже использует эти же aggregates.

Для вашего проекта это обычно правильный путь.

### Вариант C. Метрика только в PromQL без изменений во Flink

Пример:

- взять существующий monotonic counter и посчитать `increase(...[5m])`.

Когда подходит:

- во Flink уже публикуется правильный counter;
- не нужно менять код job;
- нужна только новая визуализация или alert в Prometheus/Grafana.

Когда не подходит:

- в Flink вообще нет исходной базовой метрики;
- нужна новая доменная логика, а не просто новый PromQL;
- нужна точная связь с конкретным оконным агрегатом.

Инженерное правило:

- сначала проверьте, можно ли обойтись новым PromQL на уже существующих counters;
- если нет, добавляйте новую метрику в код.

## 4. Что считать "правильной" метрикой за N минут

Правильная метрика должна отвечать на четыре вопроса.

### 4.1 Что является бизнес-объектом измерения

Надо зафиксировать:

- raw finding;
- triggered finding;
- aggregate emission;
- уникальная сессия;
- уникальный агент;
- уникальный инцидент.

Пример корректной формулировки:

- "число triggered findings по `PROMPT_INJECTION` за 5 минут".

Пример некорректной формулировки:

- "количество проблем за 5 минут".

Слово "проблем" не определяет, это findings, incidents, sessions или окна.

### 4.2 На каком этапе pipeline это значение становится корректным

Примеры:

- `invalid_events_total` корректно считать после parse/validate;
- `late_events_total` корректно считать после watermark routing;
- `triggered findings per 5m` корректно считать на оконном aggregate operator;
- `session incidents per 5m` корректно считать только после session correlation stage.

### 4.3 Что делать с late events

Надо заранее решить:

- допускается ли переизлучение окна;
- должна ли метрика увеличиваться при late update;
- ожидаете ли вы exact business count или operational count of emissions.

Это критично.

Пример:

- если окно `5m` было уже эмитировано, а потом пришло late событие в allowed lateness, Flink может выпустить aggregate повторно;
- если вы инкрементируете `emissions_total`, значение вырастет ещё раз;
- это корректно для метрики "сколько aggregate emissions произошло";
- это некорректно для метрики "сколько уникальных окон было закрыто".

### 4.4 Какой уровень labels нужен

Хорошие labels:

- `window`
- `guardrail`
- иногда `environment`

Опасные labels:

- `requestId`
- `sessionId`
- `userId`
- `agentId`, если агентов очень много и метрика уходит в Prometheus

Причина:

- высококардинальные labels быстро убивают Prometheus и делают dashboard дорогим и шумным.

Правило:

- для Prometheus-метрик не выводить в labels сущности уровня отдельного запроса;
- drill-down по `requestId` делать через Kafka, lake, incident store или логи, а не через Prometheus.

## 5. Рекомендуемый шаблон для AIRiskOps

Для вашей системы правильная цепочка обычно такая:

1. `SafetyEvent` приходит в pipeline.
2. Событие валидируется и нормализуется.
3. Поток фильтруется до `GUARDRAIL_FINDING`.
4. События группируются по бизнес-ключу.
5. На `TumblingEventTimeWindows.of(...)` считается доменный aggregate.
6. В `ProcessWindowFunction`:
   - формируется `GuardrailWindowAggregate`;
   - обновляются counters/metrics;
   - aggregate отправляется downstream.
7. В Prometheus/Grafana выбирается нужный `window`.

Это уже близко к текущему проекту.

## 6. Что уже есть в проекте

Сейчас в проекте метрики агрегатов обновляются в:

- [GuardrailWindowProcessFunction.java](../../flink-job/src/main/java/com/bank/airiskops/app/functions/GuardrailWindowProcessFunction.java)

Там уже есть разрез:

- `airiskops/window=<window>/guardrail=<guardrail>`

И уже публикуются counters:

- `aggregates_emitted_total`
- `events_total`
- `triggered_total`
- `detector_errors_total`
- `input_tokens_total`
- `output_tokens_total`

Это хороший базовый шаблон для расширения.

## 7. Алгоритм добавления новой метрики

Ниже практический алгоритм, который стоит использовать почти всегда.

### Шаг 1. Чётко сформулировать бизнес-смысл

Надо письменно зафиксировать:

- что именно считаем;
- по каким guardrail-ам;
- за какое окно;
- нужно ли считать только `triggered=true`;
- как трактуются late events;
- нужен ли разрез по environment.

Пример:

- "Нужна метрика среднего confidence по `PROMPT_INJECTION` и `TOXICITY` за окна `1m` и `5m`".

### Шаг 2. Проверить, нет ли уже базовых данных в aggregate

Сначала надо посмотреть:

- есть ли нужное поле в `GuardrailAggregateAccumulator`;
- есть ли оно в `GuardrailWindowAggregate`;
- можно ли посчитать метрику через PromQL на уже существующих counters.

Если можно обойтись Grafana/PromQL, код Flink менять не нужно.

### Шаг 3. Определить тип метрики

Обычно выбор такой:

- `Counter`
  - для monotonic значений, например `triggered_total`;
- `Gauge`
  - для текущего последнего значения, например `last_avg_confidence`;
- `Meter`
  - если нужен rate внутри Flink runtime;
- `Histogram`
  - если реально нужна distribution, но с ней надо быть осторожнее.

Для вашего проекта:

- totals лучше делать как `Counter`;
- средние значения окна часто удобнее делать как `Gauge`, обновляемый последним emitted aggregate;
- percentile-подобные вещи лучше сначала делать вне Flink metrics, а не плодить сложные histograms без необходимости.

### Шаг 4. Выбрать точку обновления метрики

Правильное место чаще всего:

- `ProcessWindowFunction`, где aggregate уже собран и бизнес-смысл стабилен.

Неправильное место:

- сырые source-операторы, если вы хотите именно оконную метрику;
- `map()` до windowing, если там ещё неизвестно, попадёт ли событие в окно и в какой aggregate.

### Шаг 5. Ограничить cardinality labels

Нужно заранее решить:

- какие labels правда нужны;
- не попадут ли туда `agentId`, `sessionId`, `requestId`.

Обычно для MVP достаточно:

- `window`
- `guardrail`

Иногда можно добавить:

- `environment`

Но не надо без крайней необходимости добавлять:

- `policyVersion`
- `modelName`
- `agentId`

Потому что число time series начнёт расти слишком быстро.

### Шаг 6. Обновить код operator-а

Нужно:

1. добавить константу имени метрики;
2. расширить `GuardrailMetricSet`;
3. зарегистрировать метрику в `createMetricSet(...)`;
4. обновлять её внутри `record(...)`.

### Шаг 7. Проверить, как метрика будет читаться в Prometheus

Важно помнить:

- Flink экспортирует metric groups в Prometheus со своим преобразованием имён;
- удобнее проектировать метрики так, чтобы PromQL был читаемым.

То есть лучше иметь что-то вроде:

- `flink_taskmanager_job_task_operator_airiskops_window_guardrail_triggered_total`

чем слишком сложную и нестабильную схему имён.

### Шаг 8. Добавить dashboard и тестовую проверку

После изменения кода нужно:

- проверить метрику на локальном стенде;
- убедиться, что она видна в Prometheus;
- добавить panel в Grafana;
- прогнать regression.

## 8. Пример 1. Добавление новой метрики `confidence_sum_total`

Предположим, вы хотите в будущем строить средний confidence через PromQL и хранить базу для сверки.

Тогда можно добавить cumulative counter суммы confidence.

### Когда это полезно

- нужно проверить корректность `avgConfidence`;
- нужно сравнивать сумму confidence между окнами;
- хочется строить derived metrics вне Flink.

### Ограничение

Такая метрика имеет смысл только для confidence-based guardrails:

- `PROMPT_INJECTION`
- `TOXICITY`

Для boolean guardrails значение надо трактовать отдельно.

### Пример изменений в коде

```java
private static final String EMITTED_CONFIDENCE_SUM_METRIC = "confidence_sum_total";
```

Расширение metric set:

```java
private record GuardrailMetricSet(
        Counter emittedAggregatesCounter,
        Counter eventsCounter,
        Counter triggeredCounter,
        Counter detectorErrorsCounter,
        Counter inputTokensCounter,
        Counter outputTokensCounter,
        Counter confidenceSumMilliCounter
) {
    private static final long CONFIDENCE_SCALE = 1_000L;

    private void record(GuardrailAggregateAccumulator aggregate) {
        emittedAggregatesCounter.inc();
        eventsCounter.inc(aggregate.totalEvents());
        triggeredCounter.inc(aggregate.triggeredCount());
        detectorErrorsCounter.inc(aggregate.detectorErrorCount());
        inputTokensCounter.inc(aggregate.inputTokens());
        outputTokensCounter.inc(aggregate.outputTokens());
        if (aggregate.avgConfidence() != null) {
            long scaledConfidenceSum = Math.round(
                    aggregate.avgConfidence() * aggregate.guardrailFindingCount() * CONFIDENCE_SCALE
            );
            confidenceSumMilliCounter.inc(scaledConfidenceSum);
        }
    }
}
```

Регистрация:

```java
return new GuardrailMetricSet(
        metricGroup.counter(EMITTED_AGGREGATES_METRIC),
        metricGroup.counter(EMITTED_EVENTS_METRIC),
        metricGroup.counter(EMITTED_TRIGGERED_METRIC),
        metricGroup.counter(EMITTED_DETECTOR_ERRORS_METRIC),
        metricGroup.counter(EMITTED_INPUT_TOKENS_METRIC),
        metricGroup.counter(EMITTED_OUTPUT_TOKENS_METRIC),
        metricGroup.counter(EMITTED_CONFIDENCE_SUM_METRIC)
);
```

Почему тут scale:

- `Counter` инкрементируется `long`, а не `double`;
- для decimal-значений удобно хранить scaled integer.

### Пример PromQL

Средний confidence за последние 5 минут:

```promql
sum by (guardrail) (
  increase(flink_taskmanager_job_task_operator_airiskops_window_guardrail_confidence_sum_total{window="5m"}[5m])
)
/
sum by (guardrail) (
  increase(flink_taskmanager_job_task_operator_airiskops_window_guardrail_events_total{window="5m"}[5m])
)
/
1000
```

Здесь надо внимательно решить, делить на `events_total` или `guardrailFindingCount`.

Если семантика нужна именно по findings, лучше базовый denominator тоже экспортировать отдельно и явно назвать.

## 9. Пример 2. Добавление gauge для последнего emitted average

Если нужна панель "какой был последний `avgConfidence` у окна `5m`", удобнее сделать `Gauge`.

### Когда это полезно

- нужно видеть последний опубликованный оконный результат;
- не нужен cumulative total;
- важна интерпретация "что последним отдал pipeline".

### Пример кода

```java
private static final String LAST_AVG_CONFIDENCE_METRIC = "last_avg_confidence";
```

Новый mutable holder:

```java
private static final class DoubleGaugeValue {
    private volatile double value;

    private void set(double value) {
        this.value = value;
    }

    private double get() {
        return value;
    }
}
```

Регистрация:

```java
private transient Map<String, DoubleGaugeValue> avgConfidenceGaugeValues;

@Override
public void open(Configuration parameters) {
    emittedAggregateCounter = getRuntimeContext().getMetricGroup().counter(METRIC_PREFIX + windowName);
    airiskOpsMetricGroup = getRuntimeContext().getMetricGroup().addGroup(AIRISKOPS_GROUP);
    guardrailMetricSets = new ConcurrentHashMap<>();
    avgConfidenceGaugeValues = new ConcurrentHashMap<>();
}

private GuardrailMetricSet createMetricSet(String guardrailName) {
    MetricGroup metricGroup = airiskOpsMetricGroup
            .addGroup(WINDOW_GROUP, windowName)
            .addGroup(GUARDRAIL_GROUP, guardrailName);

    DoubleGaugeValue gaugeValue = new DoubleGaugeValue();
    metricGroup.gauge(LAST_AVG_CONFIDENCE_METRIC, gaugeValue::get);

    return new GuardrailMetricSet(
            metricGroup.counter(EMITTED_AGGREGATES_METRIC),
            metricGroup.counter(EMITTED_EVENTS_METRIC),
            metricGroup.counter(EMITTED_TRIGGERED_METRIC),
            metricGroup.counter(EMITTED_DETECTOR_ERRORS_METRIC),
            metricGroup.counter(EMITTED_INPUT_TOKENS_METRIC),
            metricGroup.counter(EMITTED_OUTPUT_TOKENS_METRIC),
            gaugeValue
    );
}
```

Использование:

```java
private record GuardrailMetricSet(
        Counter emittedAggregatesCounter,
        Counter eventsCounter,
        Counter triggeredCounter,
        Counter detectorErrorsCounter,
        Counter inputTokensCounter,
        Counter outputTokensCounter,
        DoubleGaugeValue lastAvgConfidence
) {
    private void record(GuardrailAggregateAccumulator aggregate) {
        emittedAggregatesCounter.inc();
        eventsCounter.inc(aggregate.totalEvents());
        triggeredCounter.inc(aggregate.triggeredCount());
        detectorErrorsCounter.inc(aggregate.detectorErrorCount());
        inputTokensCounter.inc(aggregate.inputTokens());
        outputTokensCounter.inc(aggregate.outputTokens());
        if (aggregate.avgConfidence() != null) {
            lastAvgConfidence.set(aggregate.avgConfidence());
        }
    }
}
```

### Когда не надо так делать

Не делайте gauge, если нужен:

- точный cumulative business total;
- historical exact count;
- расчёт distribution за много окон подряд.

Gauge хранит только последнее значение, а не историю.

## 10. Пример 3. Добавление новой оконной метрики "affected sessions per 5m"

Это уже более интересный AIRiskOps-кейс.

Предположим, вам нужна метрика:

- "сколько уникальных `sessionId` имели хотя бы один triggered finding за 5 минут".

Это уже нельзя корректно сделать простым counter на сырых событиях, потому что:

- в одной сессии может быть много findings;
- нужно дедуплицировать в пределах окна;
- business value именно в числе затронутых сессий, а не числе событий.

Правильный путь:

1. Расширить оконный accumulator так, чтобы он собирал уникальные session identifiers.
2. Публиковать `affectedSessionCount` в `GuardrailWindowAggregate`.
3. Из `ProcessWindowFunction` обновлять metric по готовому aggregate.

Пример идеи:

```java
public final class GuardrailAggregateAccumulator {
    private final Set<String> affectedSessions = new HashSet<>();

    public void addFinding(SafetyEvent event) {
        if (Boolean.TRUE.equals(event.triggered())) {
            affectedSessions.add(event.sessionId());
        }
    }

    public long affectedSessionCount() {
        return affectedSessions.size();
    }
}
```

Тогда метрика может быть:

- `affected_sessions_total`
  - cumulative sum emitted counts;
- или `last_affected_sessions`
  - последнее значение emitted окна.

Для MVP чаще полезнее второй вариант.

## 11. Типичные ошибки

### Ошибка 1. Считать "за 5 минут" в Prometheus по метрике, которая не отражает event-time окно

Проблема:

- вы берёте raw event counter и делаете `increase(...[5m])`;
- но бизнес-смысл окна во Flink и скользящего интервала Prometheus не совпадают.

Исправление:

- если нужен именно Flink window aggregate, считайте метрику на оконном операторе;
- если нужен просто operational rate за последние 5 минут wall-clock, PromQL достаточно.

### Ошибка 2. Путать findings и emissions

Проблема:

- команда думает, что `aggregates_emitted_total` равно числу risk cases.

Исправление:

- явно различать:
  - raw findings;
  - triggered findings;
  - emitted aggregates;
  - incidents.

### Ошибка 3. Добавлять высококардинальные labels

Проблема:

- в labels попали `requestId` или `sessionId`;
- Prometheus начинает разрастаться и тормозить.

Исправление:

- labels держать только на coarse-grained уровне;
- детализацию выносить в другие хранилища.

### Ошибка 4. Использовать `Gauge` там, где нужен `Counter`

Проблема:

- текущее значение gauge перезаписывается;
- потом его ошибочно интерпретируют как cumulative count.

Исправление:

- totals делать через `Counter`;
- "последнее observed значение окна" делать через `Gauge`.

### Ошибка 5. Добавлять метрику без проверки, как она переживает late events

Проблема:

- окно переизлучается, а метрика начинает казаться "завышенной".

Исправление:

- ещё на этапе дизайна зафиксировать, считается ли метрика:
  - по emissions;
  - по уникальным окнам;
  - по уникальным бизнес-объектам.

## 12. Практический checklist перед добавлением метрики

- Чётко ли определён бизнес-объект измерения.
- Понятно ли, raw это metric или aggregate metric.
- Понятно ли, на каком operator stage значение становится корректным.
- Зафиксировано ли поведение при late events.
- Проверена ли cardinality labels.
- Выбран ли правильный тип: `Counter` или `Gauge`.
- Проверено ли, нельзя ли обойтись PromQL поверх существующей метрики.
- Понятно ли, как это будет выглядеть в Grafana.
- Есть ли локальный сценарий проверки через replay или live generator.

## 13. Рекомендуемый порядок внедрения в этом проекте

Если вам нужно добавить новую метрику в текущий AIRiskOps MVP, я бы рекомендовал такой порядок:

1. Сначала проверить, хватает ли уже существующих counters:
   - `events_total`
   - `triggered_total`
   - `detector_errors_total`
   - `input_tokens_total`
   - `output_tokens_total`
2. Если не хватает, добавлять метрику в [GuardrailWindowProcessFunction.java](../../flink-job/src/main/java/com/bank/airiskops/app/functions/GuardrailWindowProcessFunction.java).
3. Не добавлять `agentId` в labels на уровне Prometheus.
4. Сначала валидировать на окне `1m`.
5. Потом смотреть, нужен ли тот же сигнал для `5m`.
6. После этого добавлять панель в Grafana и сценарий в runbook.

## 14. Что в вашем кейсе особенно полезно добавить дальше

Для AIRiskOps с фокусом на operational risk я бы в первую очередь рассматривал такие новые метрики:

- `last_avg_confidence`
  - отдельно по `PROMPT_INJECTION` и `TOXICITY`;
- `triggered_share`
  - доля triggered findings от всех findings;
- `affected_sessions`
  - число уникальных сессий с triggered findings;
- `system_prompt_leakage_triggered_total`
  - отдельный counter, если этот риск для вас приоритетный;
- `looping_triggered_total`
  - отдельный counter для операционного анализа agent failures;
- `policy_version_mismatch_total`
  - если позже появится поток изменений policy и нужно контролировать несовпадения.

## 15. Вывод

Правильное добавление новой метрики "за N минут" во Flink почти никогда не начинается с Prometheus.

Оно начинается с ответа на вопросы:

- какой именно бизнес-объект измеряем;
- на каком этапе pipeline это значение становится истинным;
- что происходит при late events;
- не создаём ли мы лишнюю cardinality и ложную интерпретацию.

Для вашего AIRiskOps-проекта базовое правило такое:

- если метрика должна отражать смысл оконного risk aggregate, добавляйте её на этапе `ProcessWindowFunction`;
- если это просто производная от уже существующих counters, сначала попробуйте решить задачу в PromQL;
- если нужен drill-down до `requestId` или `sessionId`, не пытайтесь решать это через labels Prometheus.
