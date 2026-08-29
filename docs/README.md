# Документация AISafetyOps Flink

Дата актуальности: 2026-08-29

## Назначение

Этот каталог содержит всю проектную документацию по локальному AISafetyOps MVP на Apache Flink.

Документы разложены по четырём группам:

- `architecture` — устройство решения и инженерные принципы;
- `monitoring` — наблюдаемость, диагностика, Grafana, Prometheus;
- `runbooks` — пошаговые инструкции запуска и эксплуатации;
- `mvp` — спецификация и результаты инкрементов.

## Быстрый маршрут чтения

Если нужно быстро понять проект с нуля:

1. [README.md](/home/bob/old_bob/IdeaProjects/flink/README.md)
2. [architecture/aisafetyops-manual.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/aisafetyops-manual.md)
3. [runbooks/local-walkthrough.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/local-walkthrough.md)
4. [monitoring/monitoring-debugging-guide.md](/home/bob/old_bob/IdeaProjects/flink/docs/monitoring/monitoring-debugging-guide.md)

Если нужно запустить локальный стенд:

1. [runbooks/mvp-runbook.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/mvp-runbook.md)
2. [runbooks/local-walkthrough.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/local-walkthrough.md)

Если нужно понять архитектуру и границы модулей:

1. [architecture/aisafetyops-manual.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/aisafetyops-manual.md)
2. [architecture/adding-n-minute-metrics.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/adding-n-minute-metrics.md)

Если нужно понять MVP и этапы внедрения:

1. [mvp/mvp-spec.md](/home/bob/old_bob/IdeaProjects/flink/docs/mvp/mvp-spec.md)
2. [mvp/stage-1-results.md](/home/bob/old_bob/IdeaProjects/flink/docs/mvp/stage-1-results.md)
3. [mvp/stage-2-results.md](/home/bob/old_bob/IdeaProjects/flink/docs/mvp/stage-2-results.md)

## Состав каталога

### `architecture/`

- [aisafetyops-manual.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/aisafetyops-manual.md)
  - основной manual по Flink для кейса AISafetyOps;
  - архитектура пайплайна, runtime, deployment, data model, increments.
- [adding-n-minute-metrics.md](/home/bob/old_bob/IdeaProjects/flink/docs/architecture/adding-n-minute-metrics.md)
  - как правильно добавлять новые агрегированные метрики за `N` минут;
  - принципы, типовые ошибки, примеры кода.

### `monitoring/`

- [monitoring-debugging-guide.md](/home/bob/old_bob/IdeaProjects/flink/docs/monitoring/monitoring-debugging-guide.md)
  - мониторинг Flink job;
  - как читать Grafana dashboards;
  - какие Prometheus-запросы использовать;
  - как локализовать ошибки по этапам пайплайна.

### `runbooks/`

- [local-walkthrough.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/local-walkthrough.md)
  - ручная проверка локального контура;
  - что запускать, куда смотреть, как интерпретировать.
- [mvp-runbook.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/mvp-runbook.md)
  - эксплуатационный runbook для локального MVP;
  - запуск, replay, live generator, reset, regression.

### `mvp/`

- [mvp-spec.md](/home/bob/old_bob/IdeaProjects/flink/docs/mvp/mvp-spec.md)
  - детальная спецификация MVP и инкрементов.
- [stage-1-results.md](/home/bob/old_bob/IdeaProjects/flink/docs/mvp/stage-1-results.md)
  - результаты и бизнес-value первого этапа.
- [stage-2-results.md](/home/bob/old_bob/IdeaProjects/flink/docs/mvp/stage-2-results.md)
  - результаты и бизнес-value второго этапа.

## Принцип именования

Внутри `docs/` используется короткое имя файла без лишнего повторения слова `flink` в каждом документе.

Причина:

- контекст уже задаётся каталогом и корневым `README`;
- ссылки короче и читаются легче;
- проще поддерживать документацию при дальнейшем расширении.
