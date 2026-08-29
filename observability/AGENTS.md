# AGENTS.md

Дата актуальности: 2026-08-29

## Scope

Этот файл относится к каталогу `observability/`.

## Что входит в scope

- `observability/prometheus/`
- `observability/grafana/`

## Правила для observability

- Изменения должны быть согласованы между Flink metrics, Prometheus и Grafana.
- Если добавляется новая пользовательская метрика, нужно проверить, что она реально экспортируется из Flink.
- Названия панелей и описание в документации должны совпадать с фактическими dashboard-файлами.
- Не вводить high-cardinality labels в Prometheus без отдельного обоснования.

## При изменении метрик

Нужно синхронно проверить:

- имя метрики в Java-коде;
- итоговое имя метрики в Prometheus exporter;
- PromQL в dashboard;
- интерпретацию метрики в monitoring docs.

Особенно внимательно относиться к различию между:

- raw events;
- findings;
- triggered findings;
- aggregate emissions;
- incidents.

## При изменении dashboard

- Описывать не только панель, но и её смысл.
- Не добавлять panel, если по ней нельзя объяснить:
  - что она измеряет;
  - какая динамика нормальна;
  - какое отклонение подозрительно.

## Проверки

После изменений в `observability/`:

```bash
bash tools/scripts/run-regression.sh
```

И желательно локально проверить:

- что Grafana поднимается;
- что dashboards грузятся;
- что Prometheus видит нужные targets и метрики.
