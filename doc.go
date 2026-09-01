// Package rio is a type-safe ORM built around immutable, connection-free
// query values. A query touches the database only when a terminal method
// receives a context and Queryer:
//
//	users, err := rio.From[User]().
//		Where("age > ?", 18).
//		OrderBy("created_at DESC").
//		Limit(10).
//		With("Posts").
//		All(ctx, db)
//
// Queryer is implemented by DB and Tx, so the same query can run inside or
// outside a transaction. Use ? placeholders for every dialect; slice arguments
// in IN (?) expressions are expanded at execution time, and a Query.Sub
// argument splices a subquery in place.
//
// Models are ordinary structs. The rio tag configures column names, primary
// keys, optimistic locking, soft deletion, JSON, timestamp maintenance,
// omitted zero values, count targets, and relations. By convention ID is the
// primary key, CreatedAt and UpdatedAt are maintained timestamps, and a
// TableName method overrides the pluralized table name.
//
// Relations load only when requested. Raw and Exec provide SQL escape hatches;
// Query.Validate and Query.Must validate reusable query templates; and
// WithStmtCache enables prepared-statement reuse. Sentinel errors support
// errors.Is while preserving translated driver errors in the chain.
package rio
