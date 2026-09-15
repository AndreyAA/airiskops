# Документация AIRiskOps Flink

Дата актуальности: 2026-09-01

## Назначение

Этот каталог содержит всю проектную документацию по локальному AIRiskOps MVP на Apache Flink.

Документы разложены по четырём группам:

- `architecture` — устройство решения и инженерные принципы;
- `monitoring` — наблюдаемость, диагностика, Grafana, Prometheus;
- `runbooks` — пошаговые инструкции запуска и эксплуатации;
- `mvp` — спецификация и результаты инкрементов.

Связанные SDD-документы similarity-фичи находятся в корневом каталоге
`spec/`, потому что описывают отдельный feature-driven цикл:

- [similarity feature](../spec/similar-prompt-injection/similar-prompt-injection-feature.md);
- [LangChain4j/ONNX runtime design](../spec/similar-prompt-injection/langchain4j-onnx-runtime-design.md);
- [implementation plan](../spec/similar-prompt-injection/similar-prompt-injection-implementation-plan.md);
- [topology-level toggle](../spec/topology-toggle/similar-prompt-injection-topology-toggle-sdd.md).

## Быстрый маршрут чтения

Если нужно быстро понять проект с нуля:

1. [README.md](../README.md)
2. [architecture/airiskops-manual.md](architecture/airiskops-manual.md)
3. [runbooks/local-walkthrough.md](runbooks/local-walkthrough.md)
4. [monitoring/monitoring-debugging-guide.md](monitoring/monitoring-debugging-guide.md)

Если нужно запустить локальный стенд:

1. [runbooks/mvp-runbook.md](runbooks/mvp-runbook.md)
2. [runbooks/local-walkthrough.md](runbooks/local-walkthrough.md)

Если нужно проверить локальную регрессию и покрытие `flink-job`:

1. [README.md](../README.md)
2. [runbooks/mvp-runbook.md](runbooks/mvp-runbook.md)

Если нужно понять архитектуру и границы модулей:

1. [architecture/airiskops-manual.md](architecture/airiskops-manual.md)
2. [architecture/event-contracts.md](architecture/event-contracts.md)
3. [architecture/adding-n-minute-metrics.md](architecture/adding-n-minute-metrics.md)
4. [architecture/flink-rocksdb-best-practices.md](architecture/flink-rocksdb-best-practices.md)

Если нужно понять MVP и этапы внедрения:

Для управленческого решения: [отчёт о целесообразности внедрения](mvp/flink-adoption-management-report.md) — покрытие MVP, НТ, разрыв до прода и бюджет PoC.

1. [mvp/mvp-spec.md](mvp/mvp-spec.md)
2. [mvp/near-term-improvement-plan.md](mvp/near-term-improvement-plan.md)
3. [mvp/increment-3-implementation-spec.md](mvp/increment-3-implementation-spec.md)
4. [mvp/local-rocksdb-profile-implementation-spec.md](mvp/local-rocksdb-profile-implementation-spec.md)
5. [mvp/stage-1-results.md](mvp/stage-1-results.md)
6. [mvp/stage-2-results.md](mvp/stage-2-results.md)
7. [mvp/load-testing-plan.md](mvp/load-testing-plan.md)

## Состав каталога

### `architecture/`

- [airiskops-manual.md](architecture/airiskops-manual.md)
  - основной manual по Flink для кейса AIRiskOps;
  - архитектура пайплайна, runtime, deployment, data model, increments.
- [event-contracts.md](architecture/event-contracts.md)
  - Kafka topics и JSON-контракты локального MVP;
  - обязательные поля, связи между событиями и смысл каждого потока.
- [adding-n-minute-metrics.md](architecture/adding-n-minute-metrics.md)
  - как правильно добавлять новые агрегированные метрики за `N` минут;
  - принципы, типовые ошибки, примеры кода.
- [highload-50k-rps-analysis.md](architecture/highload-50k-rps-analysis.md)
  - анализ high-load режима `50K RPS`;
  - влияние RocksDB latency, backpressure, state и checkpointing;
  - рекомендации по окнам, parallelism и capacity testing.
- [guardrail-review-deep-research.md](architecture/guardrail-review-deep-research.md)
  - исследование практических реализаций streaming и перепроверки гардрейлов при `50K+ RPS`;
  - пятиминутный NRT, отдельный Python review-контур, sampling и подготовка заданий на дообучение;
  - первичные источники, ограничения текущего AIRiskOps и [расчётные capacity-сценарии](architecture/guardrail-review-capacity.csv).
- [guardrail-latency-post-response-research.md](architecture/guardrail-latency-post-response-research.md)
  - коммерческий опыт и исследования гардрейлов с разной latency на большом потоке;
  - streaming, holdback, каскады моделей и реакция на нарушения после доставки ответа;
  - банковские playbooks, границы компенсации, enforcement ACK и предложения для AIRiskOps.
- [flink-rocksdb-best-practices.md](architecture/flink-rocksdb-best-practices.md)
  - когда для Flink нужен `RocksDB state backend`, а когда нет;
  - какие проблемы он решает;
  - какие trade-offs и production practices появляются при large-state workload.

### `monitoring/`

- [monitoring-debugging-guide.md](monitoring/monitoring-debugging-guide.md)
  - мониторинг Flink job;
  - как читать Grafana dashboards;
  - какие Prometheus-запросы использовать;
  - как локализовать ошибки по этапам пайплайна;
  - где смотреть runtime contract и saturation signals.
  - как интерпретировать state/checkpoint pressure перед переходом на `RocksDB`.
- [local-job-prometheus-reliability-review.md](monitoring/local-job-prometheus-reliability-review.md)
  - надёжность текущего `local-job` профиля;
  - ограничения checkpoint/recovery и Kafka single-node режима;
  - состояние Prometheus/Grafana и backlog доработок.

### `runbooks/`

- [local-walkthrough.md](runbooks/local-walkthrough.md)
  - ручная проверка локального контура;
  - что запускать, куда смотреть, как интерпретировать.
- [mvp-runbook.md](runbooks/mvp-runbook.md)
  - эксплуатационный runbook для локального MVP;
  - запуск, replay, live generator, destructive e2e smoke, reset, regression.

### `mvp/`

- [flink-adoption-management-report.md](mvp/flink-adoption-management-report.md)
  - управленческая оценка Flink для AISafety Ops на 50K сообщений/с;
  - доказательства из кода и прогонов, риски, критерии успеха и оценка расширенного PoC.
- [mvp-spec.md](mvp/mvp-spec.md)
  - детальная спецификация MVP и инкрементов.
- [near-term-improvement-plan.md](mvp/near-term-improvement-plan.md)
  - ближайшие возможные улучшения проекта;
  - приоритеты по business value, observability и operational readiness.
- [increment-3-implementation-spec.md](mvp/increment-3-implementation-spec.md)
  - прикладная спецификация реализации `Increment 3`;
  - список файлов, параметров, topics, metrics, dashboards, тестов и ожидаемого business value.
- [local-rocksdb-profile-implementation-spec.md](mvp/local-rocksdb-profile-implementation-spec.md)
  - детальная спецификация отдельного local runtime profile с `RocksDB`;
  - границы изменений, config model, profile switching, проверки и риски реализации.
- [load-testing-plan.md](mvp/load-testing-plan.md)
  - текущая стратегия нагрузочного тестирования локального MVP;
  - сценарии, критерии деградации, результаты short-run DEFAULT/RocksDB и
    ключевые latency/runtime метрики.
- [prompt-injection-campaign-requirements.md](mvp/prompt-injection-campaign-requirements.md)
  - бизнес-требования для campaign-level детекции однотипных `PROMPT_INJECTION` атак;
  - режимы `GLOBAL` и `PER_AGENT`, требования к `REST`, Grafana и access control.
- [stage-1-results.md](mvp/stage-1-results.md)
  - результаты и бизнес-value первого этапа.
- [stage-2-results.md](mvp/stage-2-results.md)
  - результаты и бизнес-value второго этапа.
- `rules/`
  - точечные спецификации отдельных incident rules и шаблоны для следующих правил.
- [rules/pi-and-toxic-requirements.md](mvp/rules/pi-and-toxic-requirements.md)
  - нормативная спецификация rule `PI_AND_TOXIC`;
  - edge cases, config semantics, payload requirements и test matrix.

## Принцип именования

Внутри `docs/` используется короткое имя файла без лишнего повторения слова `flink` в каждом документе.

Причина:

- контекст уже задаётся каталогом и корневым `README`;
- ссылки короче и читаются легче;
- проще поддерживать документацию при дальнейшем расширении.
