# Contributing to rio

## Prerequisites

- Go 1.27 (`go.mod` declares `go 1.27.0`; the core has no third-party
  dependencies).
- Docker, only for the PostgreSQL, MySQL, and ClickHouse suites. SQLite tests
  run in-process on the pure-Go `modernc.org/sqlite` driver.

## Quick start

```bash
git clone https://github.com/go-rio/rio.git
cd rio

go build ./...
go vet ./...
go test ./...
go test -race ./...
```

None of this needs a database: the core suite runs against a fake
`database/sql` driver (`fakedb_test.go`) and a fake native driver
(`fakenative_test.go`), with golden SQL per dialect and allocation budgets
(`perf_test.go`). The sequence finishes inside ten minutes on a laptop.

Linting matches CI: `.golangci.yml` is a golangci-lint v2 configuration with
correctness linters only (`errcheck`, `govet`, `ineffassign`, `misspell`,
`staticcheck`, `unused`); CI also runs `govulncheck`.

```bash
golangci-lint run ./...
```

The placeholder rebinder and the snake_case inflector have fuzz targets; the
seed corpus runs in every `go test`, and the nightly workflow fuzzes longer:

```bash
go test -run='^$' -fuzz='^FuzzRebind$' -fuzztime=60s .
go test -run='^$' -fuzz='^FuzzSnakeCase$' -fuzztime=60s .
```

## Integration suites

`integration/` is its own module because it imports the real driver modules.
The SQLite suite always runs; the others skip unless their DSN is set:

| Variable | Suite |
|---|---|
| `RIO_POSTGRES_DSN` | PostgreSQL through pgx's `database/sql` adapter and through `postgres.OpenNative` |
| `RIO_MYSQL_DSN` | MySQL; the DSN must carry `parseTime=true` |
| `RIO_CLICKHOUSE_DSN` | ClickHouse, server 26 or later (older servers skip) |

`.github/workflows/test.yml` runs them against these containers; the same
setup works locally:

```bash
docker run -d --name rio-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=rio_test \
  -p 5432:5432 postgres:18.6-alpine
docker run -d --name rio-mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=rio_test \
  -p 3306:3306 mysql:9.7
docker run -d --name rio-ch -e CLICKHOUSE_DB=rio_test -e CLICKHOUSE_USER=rio \
  -e CLICKHOUSE_PASSWORD=rio -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server:26.7-alpine

cd integration
RIO_POSTGRES_DSN='postgres://postgres:postgres@localhost:5432/rio_test?sslmode=disable' \
RIO_MYSQL_DSN='root:root@tcp(localhost:3306)/rio_test?parseTime=true' \
RIO_CLICKHOUSE_DSN='clickhouse://rio:rio@localhost:9000/rio_test' \
go test -race -timeout 10m ./...
```

`integration/go.mod` replaces `github.com/go-rio/rio` with the parent
directory, so the suite always exercises your working tree.

## Benchmarks

`bench/` is a separate module comparing rio with hand-written `database/sql`
and GORM on the same schema; [bench/README.md](bench/README.md) is the
methodology (fixed machine, `-count=10`, `benchstat old.txt new.txt`). The
SQLite legs run in memory. The PostgreSQL and MySQL legs are gated on
`RIO_BENCH_PG_DSN` and `RIO_BENCH_MYSQL_DSN`, with the container commands
documented atop `bench/bench_pg_test.go` and `bench/bench_mysql_test.go`:

```bash
docker run -d --name rio-bench-pg -e POSTGRES_PASSWORD=bench -p 15432:5432 \
  postgres:17 -c fsync=off -c synchronous_commit=off -c full_page_writes=off
RIO_BENCH_PG_DSN='postgres://postgres:bench@127.0.0.1:15432/postgres?sslmode=disable' \
  go test -bench 'PG' -benchmem -run NONE ./...

docker run -d --name rio-bench-mysql -e MYSQL_ROOT_PASSWORD=bench \
  -e MYSQL_DATABASE=bench -p 13306:3306 mysql:8.4 \
  --skip-log-bin --innodb-flush-log-at-trx-commit=0
RIO_BENCH_MYSQL_DSN='root:bench@tcp(127.0.0.1:13306)/bench?parseTime=true' \
  go test -bench MySQL -benchmem -run NONE ./...
```

A performance change carries its interleaved benchstat comparison in the
commit body; numbers live in commit messages and benchmark output, not in
the docs.

## Commits and pull requests

- Subjects use a conventional prefix — `feat:`, `fix:`, `perf:`, `refactor:`,
  `docs:`, `test:`, `chore:` — with an optional scope (`fix(clickhouse):`,
  `perf(preload):`). The body states the contract that changed, not the
  diff.
- Every behavior change lands with the test that pins it: a fake-driver test
  asserting the rendered SQL and arguments, a golden per dialect when
  rendering differs, a fake-native test for SPI changes, and an integration
  case when only a real database can witness the behavior.
- Comment house style: doc comments state the contract — what the caller may
  rely on, what fails and how, the dialect differences — and nothing else.
  Internal comments are one or two lines explaining a decision the code
  cannot (a lifecycle rule, an unsafe boundary, a cross-dialect quirk); they
  never narrate the code and never reference reviews or commits.
- The core stays dependency-free. Driver-specific behavior belongs in the
  driver modules; SQL grammar and capability flags stay here.
- A public API change updates `README.md`, the `[Unreleased]` section of
  `CHANGELOG.md`, and `example_test.go` when an example clarifies the use.
- Before opening a PR, run what CI runs: `go vet ./...`,
  `go test -race ./...`, and the SQLite integration suite
  (`cd integration && go test -race ./...`).

## Releases

Maintainers cut releases; contributors do not tag.

- Versions follow SemVer with 0.x semantics: a minor version may break the
  API, and the changelog marks each break.
- Tags are signed (`git tag -s vX.Y.Z`). The tag message is the release note;
  `git tag -n50` reads it back. The `CHANGELOG.md` heading for the version
  carries the same date.
- Pushing a `v*` tag runs the GoReleaser workflow, which publishes the GitHub
  release (`.goreleaser.yaml` skips binary builds — rio is a library).
- After a core tag, the driver modules (`go-rio/postgres`, `go-rio/mysql`,
  `go-rio/sqlite`, `go-rio/clickhouse`) bump their `github.com/go-rio/rio`
  requirement and tag their own releases; `integration/go.mod` and
  `bench/go.mod` then pin the released drivers.

## Reporting issues

Open a [GitHub issue](https://github.com/go-rio/rio/issues) with the Go
version, the dialect and driver module version, the rendered statement (from
`Query.SQL` or a `QueryHook`), and the expected versus actual behavior.
