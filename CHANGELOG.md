# Changelog

Все значимые изменения go-hbase задокументированы здесь. Формат следует
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); версии следуют
[SemVer](https://semver.org/), причём wire-протокол версионируется независимо
(`virogg.hbasecop.v1`).

## [0.1.0] - unreleased

Первый релиз. HBase Observer-копроцессоры, написанные на Go, исполняемые в
долгоживущем Go-процессе на каждый RegionServer, связанные через
shared-memory ring buffer: без fork-per-call, без RPC-хопа.

### Ключевое

- **Все пять Observer-поверхностей**: Region, RegionServer, Master, WAL и
  BulkLoad observers; 103 Observer-хука (143 request/response wire-сообщения)
  диспетчеризуются через protobuf wire-протокол
  (`virogg.hbasecop.v1`). Master-поверхность поставляется в виде
  курируемого подмножества (20 из master-хуков HBase 2.5); остальные четыре
  поверхности полные. Wire-фрейминг имеет кросс-языковой golden corpus и
  Go↔proto-страж паритета hook-id; payload-сообщения хуков проходят
  round-trip в каждом языке (полный кросс-языковой byte-parity для payload
  отслеживается как follow-up).
- **Go SDK** (`pkg/hbasecop`): реализуйте `RegionObserver` (или любой другой
  observer-интерфейс), вызовите `hbasecop.Run(...)`; встраивания
  `Unimplemented*` сохраняют observers совместимыми вперёд. Паники в
  пользовательских хуках перехватываются и выдаются как ошибки хука, а не как
  падения процесса.
- **Один Go-процесс на RegionServer**, разделяемый между регионами и
  экземплярами копроцессоров (refcounted `SharedRuntime`),
  мультиплексируемый по region/hook/req_id.
- **Супервизия**: heartbeat-watchdog, обнаружение падений и авто-рестарт с
  экспоненциальным backoff; хуки, выполняющиеся в окне падения, завершаются с
  ошибкой согласно политике. Per-hook политики отказа `strict` /
  `best-effort` через конфигурацию HBase.
- **Упаковка**: CLI `hbasecop-build` собирает развёртываемый coproc-jar из
  пользовательского observer-класса + Go ELF; встроенный бинарь проверяется
  на целостность (SHA-256 manifest digest) при spawn.
- **Производительность** (бенчи T81/T82, под gate в CI): накладные расходы
  диспетчеризации prePut p50 ~70-80µs против no-op Java-observer (<100µs цель
  по SPEC) после оптимизации диспетчеризации spin-before-park; регрессия
  пропускной способности WAL-write с зарегистрированным WALObserver
  укладывается в gate <50%.
- **Hardening**: wire-декодеры с обеих сторон ограничивают число chunk и
  ожидающих reassembly (устойчивость к OOM-DoS), непрерывно фаззятся (нативный
  фаззинг Go + Java jazzer) и еженощно в CI; часовой soak с `kill -9` с gate
  по потере данных, росту RSS и zombie-supervisor.

### Безопасность

- Границы аллокаций wire-декодера (`MAX_CHUNKS`, `MAX_PENDING_REASSEMBLIES`
  и кумулятивный лимит удерживаемых байтов `MAX_PENDING_BYTES`) применяются в
  декодерах Go и Java перед любой аллокацией, управляемой peer.
- Встроенный Go ELF извлекается во временный файл 0700 и сверяется с
  manifest SHA-256 из coproc-jar перед exec.

### Известные ограничения

- Только Linux x86-64; HBase 2.5.x; Java 11+.
- Post-хуки диспетчеризуются синхронно (SPEC §3 fire-and-forget — задел на
  будущее).
- `MutationConverter` отбрасывает атрибуты уровня мутации (cellVisibility, ACL,
  TTL); конверсии Get/Scan отбрасывают per-CF временные диапазоны.
- Endpoint-копроцессоры вне области охвата v0.1.0 (реализованы в отдельном
  пост-v0.1.0 Tier 2 workstream — см. [`tasks/tier2-endpoints.md`](tasks/tier2-endpoints.md)).
- Топология soak с одним RS; multi-RS chaos не протестирован.
