# AGENTS.md

Дата актуальности: 2026-08-29

## Назначение

Этот файл задаёт общие правила для изменений в репозитории `AISafetyOps Flink MVP`.

Детальные правила вынесены в локальные файлы:

- `flink-job/AGENTS.md`
- `tools/AGENTS.md`
- `docs/AGENTS.md`
- `observability/AGENTS.md`

Перед изменениями в конкретной зоне сначала читать соответствующий локальный `AGENTS.md`.

## Что важно всегда

- Не смешивать production-код Flink job, tooling, observability и документацию в одном изменении без необходимости.
- Сохранять каноническую Java-структуру:
  - `com.bank.aisafetyops.model`
  - `com.bank.aisafetyops.app`
  - `com.bank.aisafetyops.infra`
- Не менять event semantics, window semantics, Kafka topics или contracts молча.
- После изменения команд, путей, метрик, dashboards или pipeline semantics обновлять документацию.
- После каждого этапа должно быть ясно:
  - что изменено;
  - как это проверить;
  - какой появился business value.

## Когда нужно остановиться и спросить

Нужно запросить подтверждение перед:

- массовым рефакторингом Java packages;
- изменением схемы входных или выходных событий;
- изменением window strategy, watermark semantics или allowed lateness;
- изменением Kafka topics/contracts;
- объединением большого рефакторинга с новой бизнес-фичей в одном изменении.

## Источники истины

- обзор репозитория: [README.md](/home/bob/old_bob/IdeaProjects/flink/README.md)
- карта документации: [docs/README.md](/home/bob/old_bob/IdeaProjects/flink/docs/README.md)
- локальный regression path: [tools/scripts/run-regression.sh](/home/bob/old_bob/IdeaProjects/flink/tools/scripts/run-regression.sh)
- Maven build job-модуля: [flink-job/pom.xml](/home/bob/old_bob/IdeaProjects/flink/flink-job/pom.xml)
- локальный deployment: [deployment/local/docker-compose.yml](/home/bob/old_bob/IdeaProjects/flink/deployment/local/docker-compose.yml)

## Минимальные проверки

После большинства изменений:

```bash
bash tools/scripts/run-regression.sh
```

Если менялся Java/Flink runtime-код:

```bash
bash tools/scripts/build-job.sh
```

Если менялись deployment, observability, generators или runbooks, желательно дополнительно проверить локальный сценарий из:

- [docs/runbooks/local-walkthrough.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/local-walkthrough.md)
- [docs/runbooks/mvp-runbook.md](/home/bob/old_bob/IdeaProjects/flink/docs/runbooks/mvp-runbook.md)
