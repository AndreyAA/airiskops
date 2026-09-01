# Документация AIRiskOps Flink

Дата актуальности: 2026-08-30

## Назначение

Этот каталог содержит всю проектную документацию по локальному AIRiskOps MVP на Apache Flink.

Документы разложены по четырём группам:

- `architecture` — устройство решения и инженерные принципы;
- `monitoring` — наблюдаемость, диагностика, Grafana, Prometheus;
- `runbooks` — пошаговые инструкции запуска и эксплуатации;
- `mvp` — спецификация и результаты инкрементов.

## Быстрый маршрут чтения

Если нужно быстро понять проект с нуля:

1. [README.md](../README.md)
2. [architecture/airiskops-manual.md](architecture/airiskops-manual.md)
3. [runbooks/local-walkthrough.md](runbooks/local-walkthrough.md)
4. [monitoring/monitoring-debugging-guide.md](monitoring/monitoring-debugging-guide.md)

Если нужно запустить локальный стенд:

1. [runbooks/mvp-runbook.md](runbooks/mvp-runbook.md)
2. [runbooks/local-walkthrough.md](runbooks/local-walkthrough.md)

Если нужно понять архитектуру и границы модулей:

1. [architecture/airiskops-manual.md](architecture/airiskops-manual.md)
2. [architecture/event-contracts.md](architecture/event-contracts.md)
3. [architecture/adding-n-minute-metrics.md](architecture/adding-n-minute-metrics.md)
4. [architecture/flink-rocksdb-best-practices.md](architecture/flink-rocksdb-best-practices.md)

Если нужно понять MVP и этапы внедрения:

1. [mvp/mvp-spec.md](mvp/mvp-spec.md)
2. [mvp/near-term-improvement-plan.md](mvp/near-term-improvement-plan.md)
3. [mvp/increment-3-implementation-spec.md](mvp/increment-3-implementation-spec.md)
4. [mvp/stage-1-results.md](mvp/stage-1-results.md)
5. [mvp/stage-2-results.md](mvp/stage-2-results.md)

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

### `runbooks/`

- [local-walkthrough.md](runbooks/local-walkthrough.md)
  - ручная проверка локального контура;
  - что запускать, куда смотреть, как интерпретировать.
- [mvp-runbook.md](runbooks/mvp-runbook.md)
  - эксплуатационный runbook для локального MVP;
  - запуск, replay, live generator, destructive e2e smoke, reset, regression.

### `mvp/`

- [mvp-spec.md](mvp/mvp-spec.md)
  - детальная спецификация MVP и инкрементов.
- [near-term-improvement-plan.md](mvp/near-term-improvement-plan.md)
  - ближайшие возможные улучшения проекта;
  - приоритеты по business value, observability и operational readiness.
- [increment-3-implementation-spec.md](mvp/increment-3-implementation-spec.md)
  - прикладная спецификация реализации `Increment 3`;
  - список файлов, параметров, topics, metrics, dashboards, тестов и ожидаемого business value.
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
