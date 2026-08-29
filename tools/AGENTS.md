# AGENTS.md

Дата актуальности: 2026-08-29

## Scope

Этот файл относится к каталогу `tools/`.

## Структура

- `tools/scripts/`
  - shell-скрипты для локального запуска, replay, cleanup и regression.
- `tools/generators/`
  - Python-генераторы replay и live traffic.
- `tools/tests/`
  - Python tests для генераторов.

## Правила для shell scripts

- Использовать `set -euo pipefail`.
- В начале каждого shell-скрипта должен быть короткий header-комментарий:
  - что делает скрипт;
  - когда его использовать.
- Скрипты должны корректно работать из корня репозитория.
- Для Docker использовать `deployment/local/docker-compose.yml`.
- Ошибки должны быть понятны без чтения кода скрипта.
- Если меняется путь к артефакту, compose-файлу или config-файлу, обновлять все связанные скрипты сразу.

## Правила для генераторов

- В начале каждого generator script должен быть короткий header-комментарий:
  - что делает скрипт;
  - когда его использовать.
- Replay-генераторы должны поддерживать deterministic режим через `seed`.
- Live-генераторы должны иметь явные CLI-параметры.
- При изменении event schema нужно обновлять:
  - генератор;
  - Python tests;
  - при необходимости runbook.
- Если добавляется новый guardrail, он должен появиться и в replay, и в live generator, если это уместно.

## Проверки

После изменений в `tools/` выполнить:

```bash
bash tools/scripts/run-regression.sh
```

Если менялся Python-код генераторов, дополнительно:

```bash
python3 -m py_compile tools/generators/generate_events.py tools/generators/stream_live_events.py
```
