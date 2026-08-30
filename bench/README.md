# Benchmarks

rio vs idiomatic hand-written `database/sql` vs GORM on the same schema and
driver; PostgreSQL adds rio's pgx-native channel.

Setup, seeding, and periodic cleanup sit outside the timed region; insert
benchmarks reset the table roughly every 1,000 rows so growth does not skew
later iterations (PostgreSQL keeps sequence values across truncation). The
`database/sql` legs are ordinary application code — each `Rows.Scan` boxes
its variadic destinations, so results are end-to-end API cost, not a
decoder-only comparison. Batch anchors use one 100-row multi-VALUES
statement; SQLite and PostgreSQL consume the RETURNING ids (rio's backfill
contract), MySQL follows rio's no-backfill batch contract.

Compare on one machine with fixed toolchain, drivers, and database config:

```sh
go test -run '^$' -bench . -benchmem -count=10 > old.txt
# Apply one change, then:
go test -run '^$' -bench . -benchmem -count=10 > new.txt
benchstat old.txt new.txt
```

Narrow `-bench` when investigating one shape. The PostgreSQL and MySQL legs
need the DSNs documented atop `bench_pg_test.go` and `bench_mysql_test.go`;
without them they skip.
