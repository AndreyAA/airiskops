# AGENTS.md

Дата актуальности: 2026-08-29

## Scope

Этот файл относится к каталогу `flink-job/` и Java/Flink коду.

## Архитектурные правила

Использовать только канонические пакеты:

- `com.bank.aisafetyops.model`
- `com.bank.aisafetyops.app`
- `com.bank.aisafetyops.infra`

Назначение слоёв:

- `model`
  - доменные records, enum-ы, event contracts, aggregate models, domain constants.
- `app`
  - topology builders, Flink functions, orchestration, config models, topology/support constants.
- `infra`
  - source/sink factories, parser, serde, YAML loader и другие transport adapters.

Не переносить:

- Flink connector code в `model`;
- доменную агрегацию в `infra`;
- YAML/file I/O в `app`, если это infrastructure concern.

## Правила для Flink topology

При изменении topology:

- сохранять стабильные `uid()` у существующих операторов;
- не менять names/uid без причины;
- проверять output topics и side outputs;
- не менять window semantics случайно;
- отдельно думать про late events и watermarks.

Если меняется:

- `keyBy`
- `window`
- `allowedLateness`
- timestamp assignment

нужно явно проверить, как это влияет на aggregates и metrics.

## Правила для метрик

Если метрика относится к оконному бизнес-результату, по умолчанию добавлять её на этапе `ProcessWindowFunction`.

Перед добавлением новой метрики определить:

- это raw metric или aggregate metric;
- на каком operator stage она истинна;
- нужен `Counter` или `Gauge`;
- что происходит при late events;
- не создаёт ли она high-cardinality labels.

Не добавлять в Prometheus labels без явной причины:

- `requestId`
- `sessionId`
- `userId`

Осторожно относиться к:

- `agentId`
- `policyVersion`
- `modelName`

## Правила кодирования

- Для production Java-классов добавлять class-level Javadoc по best practices.
- Javadoc должен объяснять:
  - назначение класса;
  - его роль в pipeline или архитектуре;
  - важные ограничения или semantics, если они неочевидны.
- Особенно это обязательно для:
  - Flink functions;
  - topology builders;
  - job entrypoints;
  - config classes;
  - source/sink factories;
  - parser/serde adapters.
- Не писать шаблонный Javadoc, который просто повторяет имя класса.
- Для простых `record`, `enum` и очевидных value objects краткий Javadoc желателен, но не должен превращаться в шум.
- Выносить строковые и числовые константы в общие классы или config models.
- Все часто меняющиеся runtime-параметры по умолчанию выносить в YAML/CLI config.
- Не хардкодить в Java значения, которые регулярно меняются между локальной средой, стендом и production:
  - checkpoint intervals;
  - watermark emission intervals;
  - timeouts;
  - topic names;
  - feature flags;
  - пороги и окна анализа.
- Кодовые default values допустимы только как fallback, если внешний config не передан.
- Не копировать имена topics, metric names, window names и guardrail names по нескольким классам.
- Добавлять комментарии там, где важны:
  - watermark decisions;
  - state semantics;
  - allowed lateness;
  - business intent оператора.

## Когда меняется guardrail или event model

Нужно синхронно проверить:

- domain constants;
- parser;
- генераторы тестовых данных;
- aggregation logic;
- metrics;
- dashboards;
- документацию.

## Проверки

После изменений в `flink-job/` выполнить:

```bash
bash tools/scripts/run-regression.sh
```

И отдельно:

```bash
bash tools/scripts/build-job.sh
```
