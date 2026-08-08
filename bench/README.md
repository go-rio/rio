# Benchmarks

The suite compares rio, idiomatic hand-written `database/sql`, and GORM on
the same schema and driver. PostgreSQL also includes rio's pgx-native channel.

Setup, schema creation, seeding, and periodic table cleanup are outside the
timed region. Insert benchmarks remove accumulated rows after roughly 1,000
inserts so index and table growth do not make later iterations progressively
slower. PostgreSQL keeps sequence values across truncation because their
magnitude is irrelevant.

The `database/sql` legs are intentionally ordinary application code. In the
100-row read benchmark, each `Rows.Scan` call boxes its variadic destinations;
those allocations are part of the public API's end-to-end cost. Do not treat
the result as a decoder-only comparison. Batch anchors use one 100-row
multi-VALUES statement; SQLite and PostgreSQL consume every returned ID to
match rio's key-backfill contract, while MySQL follows rio's no-backfill batch
contract.

Run stable comparisons from this directory with the same machine, toolchain,
driver versions, and database configuration:

```sh
go test -run '^$' -bench . -benchmem -count=10 > old.txt
# Apply one change, then:
go test -run '^$' -bench . -benchmem -count=10 > new.txt
benchstat old.txt new.txt
```

Use a narrower `-bench` expression when investigating one shape. The
PostgreSQL and MySQL benchmarks require the DSNs documented at the top of
`bench_pg_test.go` and `bench_mysql_test.go`; without them those legs skip.
