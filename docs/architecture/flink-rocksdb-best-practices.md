# Flink + RocksDB: best practices, trade-offs и условия применения

Дата актуальности: 2026-09-01

## Назначение

Этот документ объясняет, зачем в Apache Flink используют связку со `state backend` на базе `RocksDB`, какую проблему она решает, когда без неё можно обойтись и какие production trade-offs появляются взамен.

Документ ориентирован на инженерное принятие решения, а не на формулу "в production всегда нужен RocksDB".

## Краткий вывод

`RocksDB` в Flink нужен не для ускорения, а для того, чтобы job с большим `state` оставалась работоспособной и восстанавливаемой при объёмах, где `heap-based` state backend начинает упираться в:

- размер JVM heap;
- давление на GC;
- размер и длительность checkpoint;
- время recovery после failover;
- пределы масштабирования по числу ключей, окон, timers и correlation state.

Если state маленький и latency доступа к state важнее объёма, `RocksDB` может быть лишним и даже вредным.

## 1. Что именно решает RocksDB в Flink

Во Flink `state backend` определяет:

- где лежит рабочий state операторов;
- как этот state читается и обновляется на hot path;
- как state участвует в checkpoint;
- как job восстанавливается после сбоя;
- насколько предсказуемо job переживает рост cardinality и rescale.

### Проблема без RocksDB

Если используется `HashMapStateBackend`, то рабочий `keyed state` и `window state` в основном живут как Java-объекты в памяти TaskManager.

Это хорошо, пока state:

- относительно мал;
- имеет ограниченную cardinality;
- не держит большое число активных окон;
- не содержит длинноживущие session snapshots;
- не создаёт тяжёлую нагрузку на checkpoint и GC.

Когда state растёт, появляются типовые проблемы:

- `OutOfMemoryError`;
- длинные GC pauses;
- рост p99/p999 latency;
- слишком тяжёлые full checkpoints;
- долгое восстановление после падения job;
- сложность безопасного увеличения parallelism.

### Что делает RocksDB

`EmbeddedRocksDBStateBackend` хранит working state не как Java object graph на heap, а как сериализованные key/value записи в embedded `RocksDB`, используя local disk и native memory.

Практический эффект:

- state перестаёт быть жёстко ограничен размером JVM heap;
- снижается зависимость поведения job от GC;
- появляются `incremental checkpoints`;
- large-state job становится более реалистичной в эксплуатации.

Важно: это не бесплатная оптимизация. Вы обмениваете скорость доступа к state на вместимость и operational stability.

## 2. Какую физическую проблему это решает

На уровне системной механики проблема выглядит так:

1. Stateful operator должен держать большое число ключей и значений.
2. Эти данные нужно быстро читать и обновлять.
3. Их нужно периодически snapshot'ить.
4. После сбоя их нужно восстановить.

Если всё это находится в Java heap:

- объём state конкурирует с heap под user code, buffers и runtime;
- большие object graphs плохо дружат с GC;
- snapshot полного state становится дорогим;
- восстановление требует загрузить большой объём данных обратно в heap.

`RocksDB` меняет физику исполнения:

- working set хранится в LSM-based embedded storage;
- heap освобождается от основной массы state objects;
- часть стоимости переносится в native memory, serialization, JNI и disk I/O;
- checkpoint может опираться на SST-file oriented model и incremental snapshots.

Именно поэтому RocksDB особенно полезен не в "маленьких быстрых" job, а в job с большим и долгоживущим state.

## 3. Что будет, если не использовать RocksDB

Ответ зависит от профиля нагрузки.

### Сценарий A: state маленький

Если у job:

- тысячи, а не миллионы активных ключей;
- короткие окна;
- мало timers;
- компактный per-key state;
- умеренный объём checkpoint;

то `HashMapStateBackend` часто лучше:

- ниже latency;
- меньше serialization overhead;
- проще operational tuning;
- проще локальная отладка.

В таком режиме отсутствие RocksDB обычно не проблема.

### Сценарий B: state большой

Если у job:

- очень много активных сессий;
- длинные окна или session correlation;
- большие `MapState` / `ListState`;
- много event-time timers;
- высокая cardinality по `tenantId`, `agentId`, `sessionId`, `requestId`;

то без RocksDB обычно начинает происходить одно или несколько из следующего:

- heap приходится завышать до неудобных размеров;
- GC становится источником непредсказуемости;
- checkpoint duration растёт непропорционально;
- restart/recovery становится слишком долгим;
- job нестабильна при росте нагрузки, хотя CPU ещё не исчерпан.

То есть job ломается не по compute-path, а по state-path.

## 4. Что именно выигрывается с RocksDB

### 4.1. Больший state

Основной выигрыш: job может хранить state объёмом, который уже неудобно или опасно держать в чистом JVM heap.

Это критично для:

- session correlation;
- дедупликации;
- long-lived keyed snapshots;
- широких оконных агрегатов;
- suppression logic;
- incident history.

### 4.2. Более реалистичные checkpoint для large-state workload

Для job с большим state одной из главных практических выгод становятся `incremental checkpoints`.

Это означает, что при очередном checkpoint сохраняется в основном изменившаяся часть state, а не заново весь её объём.

Эффект:

- снижается checkpoint I/O pressure;
- уменьшается время checkpoint на живой job;
- large-state pipeline лучше переносит частые checkpoints.

### 4.3. Меньше зависимости от GC

Большие Java object graphs плохо предсказуемы по GC cost. RocksDB уменьшает объём "живого" heap-state и обычно делает поведение более стабильным под ростом state.

Это не устраняет memory problems полностью, но меняет их тип:

- меньше heap pressure;
- больше внимания к native memory и локальному диску.

### 4.4. Более честное масштабирование stateful job

Когда job эволюционирует от MVP к production-like нагрузке, bottleneck часто появляется не по CPU, а по сочетанию:

- state size;
- checkpoint duration;
- restore time;
- timer volume;
- memory fragmentation и GC.

RocksDB позволяет отложить этот предел существенно дальше.

## 5. Что вы теряете, когда включаете RocksDB

Это ключевая часть решения. RocksDB не "лучше вообще", он лучше только для определённого профиля задач.

### 5.1. Доступ к state становится медленнее

Причины:

- serialization/deserialization;
- JNI boundary;
- формат key/value store вместо прямого доступа к Java object;
- возможный disk activity и compaction side effects.

Для latency-sensitive workload это важно. Официальная документация Flink отдельно предупреждает, что `RocksDB` backend может быть заметно медленнее heap backend на hot path доступа к state.

### 5.2. Появляется новый слой operational complexity

Нужно думать про:

- local disk;
- native memory;
- compaction;
- число column families;
- incremental checkpoints;
- local recovery;
- RocksDB metrics;
- заполнение диска.

То есть проблема не исчезает, а переезжает из `heap + GC` в `disk + native memory + LSM maintenance`.

### 5.3. Ошибки структуры state становятся дороже

Если вы проектируете state без ограничений и позволяете значениям бесконтрольно разрастаться, RocksDB не спасёт архитектуру.

Плохие паттерны остаются плохими:

- бесконечно растущий `ListState`;
- слишком широкие значения на ключ;
- отсутствие TTL или cleanup semantics;
- state, который по смыслу надо агрегировать, а не копить сырьём.

## 6. Когда RocksDB почти наверняка нужен

Практически он оправдан, если выполняется значимая часть этих условий:

- миллионы активных keys;
- state заметно больше комфортного JVM heap;
- длинноживущие session snapshots;
- высокий объём timers;
- длительные event-time окна;
- большой checkpoint footprint;
- важен recovery SLA;
- нужен rescale без экстремального роста heap.

### Пример

Представим incident-correlation job, где на каждый `agentId + sessionId` хранится:

- список последних request ids;
- карта активных guardrail findings;
- suppression markers;
- summary counters;
- cleanup timer.

Если активных сессий десятки или сотни тысяч, такой state ещё может жить в heap. Если их миллионы, heap backend начинает становиться operationally fragile.

## 7. Когда RocksDB не нужен

Не стоит включать RocksDB только потому, что job "настоящая".

Он часто не нужен, если:

- pipeline почти stateless;
- state небольшой и стабилен;
- окна короткие и число ключей ограничено;
- у вас локальный demo или bounded replay;
- latency важнее вместимости state;
- full checkpoints остаются дешёвыми.

### Пример

Есть минутная агрегация по нескольким тысячам ключей, без длинной сессионной корреляции и с компактными accumulator'ами.

В таком случае heap backend обычно:

- проще;
- быстрее;
- дешевле по накладным расходам.

## 8. Best practices

### 8.1. Выбирать backend по профилю state, а не по статусу проекта

Правильный вопрос:

- сколько state реально живёт в job;
- как быстро он растёт;
- сколько стоит checkpoint;
- сколько стоит restore;
- насколько критичен latency per state access.

Неправильный вопрос:

- "это production, значит нужен RocksDB".

### 8.2. Для large-state job включать incremental checkpoints

Это одна из главных причин использовать RocksDB в production stateful Flink jobs.

Если state большой, а checkpoints остаются полными, вы теряете существенную часть практической выгоды.

### 8.3. Не путать state backend и checkpoint storage

`RocksDB` отвечает за working state backend, но не заменяет надёжное checkpoint storage.

То есть:

- локальный state может лежать рядом с TaskManager;
- но durable checkpoint должен храниться в подходящем внешнем хранилище.

Оставлять recovery-критичный snapshot только на локальном ephemeral storage опасно.

### 8.4. Держать RocksDB на быстром локальном диске

Для `RocksDB` local disk влияет на latency, compaction и restore.

Плохая идея:

- сетевой FS как primary local RocksDB storage.

Нормальная идея:

- локальный SSD/NVMe;
- достаточный запас по IOPS и свободному месту;
- контроль disk-full сценариев.

### 8.5. Не начинать с агрессивного low-level tuning

Сначала:

- измерить state size;
- измерить checkpoint duration;
- измерить restore time;
- измерить GC и backpressure;
- посмотреть bottleneck.

И только потом идти в тонкую настройку RocksDB options.

Иначе легко получить "тюнинг без модели".

### 8.6. Проектировать state компактно

Даже с RocksDB нужно:

- ограничивать размер value;
- использовать TTL/cleanup;
- агрегировать вместо бессрочного накопления сырья;
- избегать хранения повторяемых больших payload;
- держать state aligned с бизнес-семантикой ключа.

### 8.7. Осторожно обращаться с timers

Большое число event-time timers само по себе создаёт pressure на state subsystem.

Если timers очень много:

- важно оценить их cardinality;
- важно понимать cleanup semantics;
- важно не переносить в timers задачи, которые проще решаются windowing или compact state transitions.

### 8.8. Следить за native memory и локальным диском

С RocksDB у вас добавляется отдельный operational failure plane:

- disk full;
- compaction stalls;
- native memory pressure;
- performance collapse на плохом storage.

Если эти сигналы не мониторятся, вы просто меняете один класс отказов на другой.

### 8.9. Включать RocksDB metrics осознанно

Native RocksDB metrics полезны для диагностики:

- cache hit/miss;
- compaction;
- flush;
- stalls;

но не стоит включать всё подряд без необходимости, потому что часть метрик тоже стоит производительности.

### 8.10. Проверять restore и rescale не только steady-state throughput

Частая ошибка: job выглядит нормально в steady state, но:

- слишком долго восстанавливается;
- тяжело переживает restart;
- болезненно меняет parallelism;
- деградирует под длинной серией incremental checkpoints.

Для stateful Flink это не вторичный вопрос, а часть основной архитектуры.

## 9. Что это значит для AIRiskOps

Для текущего локального `AIRiskOps MVP` RocksDB не выглядит обязательным по умолчанию, если остаётся текущий профиль:

- локальный single-node стенд;
- demo и replay на ноутбуке;
- умеренное число активных session keys;
- короткие агрегатные окна `1m` и `5m`;
- относительно компактный incident state.

Но при развитии системы к production-like нагрузке RocksDB становится всё более вероятным кандидатом, если вы начнёте добавлять:

- больше сессионной корреляции;
- больше suppression state;
- длиннее retention по incident logic;
- выше cardinality по агентам и сессиям;
- более жёсткие требования к recovery.

### Конкретно для этого репозитория

Сейчас в job уже есть признаки stateful evolution:

- event-time окна и `allowed lateness`;
- session incident evaluation;
- cleanup timers;
- policy-aware correlation;
- несколько output contracts и observability layer.

Это означает, что вопрос про RocksDB для AIRiskOps не академический, а вполне практический на следующем этапе масштабирования.

## 10. Примеры инженерного выбора

### Выбор 1: оставить heap backend

Условия:

- сотни или тысячи активных session keys;
- replay и локальный demo;
- небольшой state на ключ;
- быстрые checkpoints;
- GC стабилен.

Почему это разумно:

- проще эксплуатация;
- меньше latency overhead;
- быстрее цикл локальной отладки.

### Выбор 2: перейти на RocksDB

Условия:

- сотни тысяч или миллионы активных keys;
- state growing faster than heap comfort zone;
- checkpoint duration начинает мешать;
- restore time становится operational concern;
- incident/session state усложняется.

Почему это разумно:

- state capacity становится более управляемой;
- checkpoint economics улучшается за счёт incremental snapshots;
- job меньше зависит от большого Java heap.

## 11. Формула истины

`Flink + RocksDB` выбирают тогда, когда основной риск stateful job смещается из области CPU и простого heap runtime в область большого state, checkpoint economics и recovery discipline.

## 12. Короткий production checklist

- Оценить реальный объём state на operator и key family.
- Замерить checkpoint duration и checkpoint size.
- Замерить restore time, а не только steady-state throughput.
- Проверить GC pressure на heap backend до перехода.
- Оценить cardinality keys и timers.
- Подготовить быстрый локальный диск для RocksDB local state.
- Включить incremental checkpoints для large-state сценария.
- Мониторить disk usage, native memory и checkpoint health.
- Не хранить безграничные структуры в `ListState` / `MapState`.
- Проверить поведение job при restart и rescale.

## Источники

- Apache Flink documentation: State Backends  
  https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/
- Apache Flink documentation: Tuning Checkpoints and Large State  
  https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/large_state_tuning/
- Apache Flink documentation: Fault Tolerance  
  https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/
- Apache Flink documentation 1.20: Fault Tolerance  
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/fault_tolerance/
- Apache Flink documentation: Deployment Configuration  
  https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/
- Apache Flink blog: Using RocksDB State Backend in Apache Flink: When and How  
  https://flink.apache.org/2021/01/18/using-rocksdb-state-backend-in-apache-flink-when-and-how/
