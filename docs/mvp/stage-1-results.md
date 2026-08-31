# AIRiskOps на Flink: результаты этапа 1

## 1. Назначение этапа

Этап 1 зафиксирован как базовый MVP foundation для локального NRTP-контура AIRiskOps:

- нормализация и валидация входных событий;
- потоковая обработка событий от LLM-агентов и гардрейлов;
- маршрутизация валидных, невалидных и поздних событий;
- публикация результатов в Kafka;
- базовая наблюдаемость через Prometheus;
- воспроизводимая локальная проверка через replay dataset.

Этап реализован и проверен на локальном Docker-стенде по состоянию на `2026-08-27`.

## 2. Зафиксированные договоренности

- режим обработки: `NRTP`, а не строгий real-time;
- окно анализа: `1-5 минут`;
- ожидаемое время реакции на триггер: `до 2-3 минут`;
- локальная среда разработки: ноутбук + `Docker`;
- входной транспорт MVP: `Kafka`;
- формат событий: `JSON`, с возможностью дальнейшей замены;
- конфигурация: через `YAML`;
- основной ключ для раннего анализа: `agentId`;
- `tenantId` на текущем этапе соответствует `agentId`;
- `requestId` остаётся отдельным идентификатором запроса;
- `sessionId` добавлен как идентификатор набора запросов пользователя;
- локальная нагрузка MVP: `10-20 RPS`;
- целевая пром-нагрузка в будущем: до `3000 RPS`.

## 3. Реализованный функционал

### 3.1 Входные потоки

Flink job читает данные из Kafka topics:

- `agent-requests`
- `agent-responses`
- `guardrail-findings`

Поддержанные события в модели:

- `AGENT_REQUEST`
- `AGENT_RESPONSE`
- `GUARDRAIL_FINDING`

Поддержанные гардрейлы:

- `PROMPT_INJECTION`
- `TOXICITY`
- `LOOPING`
- `SYSTEM_PROMPT_LEAKAGE`

### 3.2 Обработка

В рамках этапа 1 реализованы:

- парсинг входного JSON в доменную модель;
- валидация обязательных полей;
- разделение валидных и невалидных событий;
- назначение event time;
- watermarking для bounded out-of-orderness;
- отделение late events через side output;
- сериализация результатов обратно в JSON;
- публикация результатов в Kafka sink.

### 3.3 Выходные потоки

Flink job пишет результаты в Kafka topics:

- `normalized-events`
- `invalid-events`
- `late-events`

### 3.4 Наблюдаемость

Добавлены:

- Prometheus scrape для `jobmanager`;
- Prometheus scrape для `taskmanager`;
- пользовательские метрики Flink operators:
  - `valid_events_total`
  - `invalid_events_total`
  - `on_time_events_total`
  - `late_events_total`

### 3.5 Tooling

Добавлены operational-скрипты:

- `tools/scripts/start-local.sh`
- `tools/scripts/stop-local.sh`
- `tools/scripts/init-topics.sh`
- `tools/scripts/build-job.sh`
- `tools/scripts/submit-job.sh`
- `tools/scripts/run-replay.sh`
- `tools/scripts/check-output-topics.sh`
- `tools/scripts/run-regression.sh`

Добавлены supporting assets:

- `config/job/local-job.yaml`
- `observability/prometheus/prometheus.yml`
- `config/policies/default-policy.yaml`
- replay generator на Python

## 4. Архитектурный результат этапа

Сформирован работающий локальный контур:

1. Kafka принимает входные события.
2. Flink job читает их как единый raw stream.
3. События валидируются и нормализуются.
4. Late events и invalid events выводятся в отдельные потоки.
5. Валидные on-time события публикуются в `normalized-events`.
6. Активность операторов экспортируется в Prometheus.

На кодовом уровне проект уже разложен на слои:

- `model`
- `app`
- `infra`

Это создаёт основу для следующего инкремента без переписывания каркаса.

## 5. Проверка на живой системе

Этап проверен на локальном Docker-стенде.

Подтверждено:

- Kafka, Flink JobManager, Flink TaskManager и Prometheus успешно подняты;
- Flink job успешно отправляется в кластер;
- Flink job находится в состоянии `RUNNING`;
- replay dataset публикуется в Kafka;
- Flink реально обрабатывает опубликованные события;
- Prometheus targets имеют статус `up`;
- output topic `normalized-events` содержит данные;
- выборка сообщения из `normalized-events` успешно читается.

## 6. Фактические результаты проверки

После replay-прогона зафиксированы значения:

- `valid_events_total = 720`
- `invalid_events_total = 0`
- `on_time_events_total = 720`
- `late_events_total = 0`

Фактические offsets в `normalized-events`:

- partition `0`: `162`
- partition `1`: `226`
- partition `2`: `332`

Итого в `normalized-events`: `720` сообщений.

Фактические offsets:

- `invalid-events = 0`
- `late-events = 0`

Это согласуется с replay-сценарием и пользовательскими метриками.

## 7. Найденные ошибки и как они были исправлены

### Ошибка 1. Flink job не отправлялась из-за сериализации функции

Симптом:

- job submission падал с ошибкой сериализации `SafetyEventParser`.

Причина:

- `ParseAndValidateFunction` держала несериализуемый parser как обычное поле.

Фикс:

- поле parser сделано `transient`;
- инициализация перенесена в `open()`.

Проверка:

- `build-job` и `submit-job` после правки выполнились успешно.

### Ошибка 2. Kafka source периодически валил job в RESTARTING

Симптом:

- `SourceCoordinator` падал с `Failed to get metadata for topics`;
- job уходила в `RESTARTING`.

Причина:

- некорректная локальная конфигурация Kafka listeners/advertised listeners для Docker-сети.

Фикс:

- Kafka переведена на упрощённую схему с единым внутренним endpoint `kafka:9092`;
- `deployment/local/docker-compose.yml` и `config/job/local-job.yaml` синхронизированы.

Проверка:

- job перешла в стабильное состояние `RUNNING`;
- новые metadata timeout перестали появляться;
- replay успешно обработан.

### Ошибка 3. Output topics казались пустыми при ручной проверке

Симптом:

- `console-consumer` иногда возвращал `TimeoutException`;
- при этом counters Flink уже росли.

Причина:

- ложный отрицательный результат при локальной проверке single-node Kafka consumer tooling.

Фикс:

- добавлен `tools/scripts/check-output-topics.sh`;
- рекомендован способ проверки через offsets и чтение по `partition` и `offset`;
- runbook обновлён.

Проверка:

- offsets `normalized-events` показали `720` сообщений;
- sample message был успешно прочитан.

## 8. Business value после этапа 1

После завершения этапа 1 команда получает не абстрактную архитектуру, а работающий проверяемый фундамент.

Что именно получено:

- единый поток событий агентов и гардрейлов в Flink;
- нормализованный выходной поток для downstream-анализа;
- отделение технического шума:
  - невалидные события отдельно;
  - поздние события отдельно;
- воспроизводимый локальный контур для demo и regression;
- базовую наблюдаемость, чтобы видеть факт обработки, а не гадать по логам;
- стартовую платформу для следующих инкрементов:
  - оконные агрегации;
  - корреляция по `agentId` и `sessionId`;
  - policy updates;
  - incident routing;
  - stateful analysis.

Как это можно посмотреть и "пощупать":

- открыть Flink UI: `http://localhost:8081`
- открыть Prometheus: `http://localhost:9090`
- прогнать replay:
  - `bash tools/scripts/run-replay.sh --scenario mixed --requests 120 --sessions 12 --agent-id agent-risk-01`
- проверить output:
  - `bash tools/scripts/check-output-topics.sh`

## 9. Что не входит в завершённый этап 1

Пока ещё не реализованы:

- бизнес-окна `1-5 минут` с итоговыми агрегатами по `agentId`;
- stateful correlation по `sessionId`;
- broadcast policy updates в сам pipeline;
- incident scoring и emission в отдельный operational stream;
- async enrichment;
- CEP/anomaly detection;
- Python-часть в job graph.

Это остаётся предметом следующих инкрементов.

## 10. Артефакты этапа

Основные файлы результата:

- [airiskops-manual.md](../architecture/airiskops-manual.md)
- [mvp-spec.md](mvp-spec.md)
- [mvp-runbook.md](../runbooks/mvp-runbook.md)
- [monitoring-debugging-guide.md](../monitoring/monitoring-debugging-guide.md)
- [docker-compose.yml](../../deployment/local/docker-compose.yml)
- [local-job.yaml](../../config/job/local-job.yaml)
- [prometheus.yml](../../observability/prometheus/prometheus.yml)
- [check-output-topics.sh](../../tools/scripts/check-output-topics.sh)

## 11. Итог

Этап 1 завершён и сохранён как рабочий baseline.

На `2026-08-27` у проекта есть:

- работающий локальный Flink MVP;
- проверенная интеграция `Kafka -> Flink -> Kafka`;
- Prometheus-наблюдаемость;
- replay/regression контур;
- документированный operational процесс для локальной разработки.
