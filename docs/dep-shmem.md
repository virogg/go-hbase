# Зависимость: `java-go-shmem`

Описывает интеграцию [virogg/java-go-shmem][upstream] — lock-free
shared-memory ring для Go↔Java IPC — а также порядок pin и обновления.

[upstream]: https://github.com/virogg/java-go-shmem

## Решение: git submodule + локальная установка

Upstream-библиотека не опубликована в Maven Central (её Java-артефакт —
`com.jgshmem:java-go-shmem:1.0.0-SNAPSHOT`), а Go-модуль объявляет
`github.com/viroge/go-shmem` из подкаталога
(`/go/`), поэтому ни одна из сторон не разрешается «из коробки» через
обычный `go get` / Maven Central.

Мы держим upstream как **git submodule** в `third_party/java-go-shmem/`
и подключаем каждую языковую сторону к локальной копии:

- **Go**: `go.mod` объявляет `require github.com/viroge/go-shmem` и
  `replace github.com/viroge/go-shmem => ./third_party/java-go-shmem/go`.
  Импорты используют upstream-путь модуля (`github.com/viroge/go-shmem/pkg/ring`).
  Никакого шага publish или vendoring не требуется.
- **Java**: `make deps-shmem` запускает `mvn install` на pom сабмодуля и
  кладёт `com.jgshmem:java-go-shmem:1.0.0-SNAPSHOT` в `~/.m2`. После этого
  pom моста зависит от него как от любого другого Maven-артефакта.

### Почему не другие варианты

- **Vendoring** (копирование исходников в `third_party/` без git-истории):
  теряется upstream SHA pin и простой путь обновления. Submodule — это
  минимальный инструмент, который точно фиксирует, против какого коммита мы
  собирались.
- **Maven Central**: недоступен; upstream — это `*-SNAPSHOT`, GA-релиза нет.
- **GitHub Packages Maven repo**: потребовал бы учётные данные при каждом
  клонировании и каждом прогоне CI ради единственного SNAPSHOT-артефакта.
  Несоразмерно.

## Pin

Текущий коммит сабмодуля: см.
[`.gitmodules`](../.gitmodules) и
[`git submodule status third_party/java-go-shmem`](#). Источником истины
является закреплённый SHA, а не то, что написано здесь.

Расхождение upstream `viroge` против `virogg` в пути Go-модуля (и тот факт,
что Go-модуль живёт в `/go/`, а не в корне репозитория) означает, что прямой
`go get github.com/virogg/java-go-shmem/...` работать не будет; replace
directive здесь несущая.

## Как обновить (bump)

```sh
git submodule update --remote third_party/java-go-shmem
# inspect the diff in third_party/java-go-shmem to confirm the change set
git -C third_party/java-go-shmem log --oneline -5

make deps-shmem          # reinstall the new SNAPSHOT into ~/.m2
make all                 # confirm both languages still build and test

git add third_party/java-go-shmem
git commit -m "deps: bump java-go-shmem to <sha>"
```

Если upstream-путь Go-модуля изменится с `github.com/viroge/go-shmem`,
обновите строки `require`/`replace` в `go.mod` соответствующим образом.

## Как работает свежий checkout

```sh
git clone --recurse-submodules https://github.com/virogg/go-hbase.git
# or, if you already cloned without submodules:
git submodule update --init --recursive

make deps-shmem    # one-time per ~/.m2; CI runs this on every job
make all
```

## Интеграция с CI

Оба job — `go` и `java` — в `.github/workflows/ci.yml` используют
`actions/checkout@v4` с `submodules: recursive`. Job `java` запускает
`make deps-shmem` перед `make java-test`, чтобы локальный SNAPSHOT был в
кэше Maven к моменту сборки моста.

## Целостность supply-chain (`make verify-deps`)

Этот сабмодуль — самая несущая зависимость, и при этом он находится вне
обычных контуров проверки целостности: Go-сторона потребляет его через
`replace` directive, поэтому **`go.sum` не содержит контрольной суммы для
него**, а Java-артефакт — это локальный `SNAPSHOT` (изменяемый). Чтобы это
компенсировать, pin явно записан как `SHMEM_EXPECTED_SHA` в `Makefile`, и
оба CI job запускают `make verify-deps` перед сборкой. Эта цель проваливает
сборку, если только выгруженный сабмодуль не совпадает **в точности** с
закреплённым коммитом и **без модификаций отслеживаемых исходников**
(неотслеживаемый вывод сборки, такой как `java/target/`, игнорируется).
Поэтому bump требует обновления `SHMEM_EXPECTED_SHA` и этого документа в
рамках одного изменения, так что bump не сможет проскользнуть мимо ревью, а
подделанное рабочее дерево будет обнаружено.

Остаточный риск (follow-up, требует действий со стороны upstream): pin на
голый коммит, а не на подписанный tag/release; а отказ от `replace`
directive / публикация не-`SNAPSHOT` Maven-координаты зависит от того,
выпустит ли upstream модуль по своему объявленному пути. `verify-deps`
ограничивает экспозицию внутри репозитория; он не заменяет опубликованный
релиз с контрольными суммами.
