# Test Data Policy

Дата актуальности: 2026-08-29

В репозитории следует хранить только те `jsonl`-файлы, которые нужны как маленькие test fixtures.

Хранить можно:

- короткие deterministic datasets для unit/integration tests;
- edge-case fixtures, которые сложно надёжно генерировать;
- golden samples для проверки parser/serde.

Не хранить в git:

- replay dumps из локальных прогонов;
- файлы из `runtime/replay/latest/`;
- большие demo datasets;
- любые данные, которые воспроизводятся генераторами из `tools/generators/`.

Для сгенерированных test data при необходимости использовать:

- `tools/testdata/generated/`

Этот каталог добавлен в `.gitignore`.
