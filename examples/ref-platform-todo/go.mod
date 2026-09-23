module github.com/oarkflow/fh/examples/ref-platform-todo

go 1.26.5

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/oarkflow/fh v0.0.25
	github.com/oarkflow/fh/ref v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/oarkflow/bcl v0.0.31 // indirect
	github.com/oarkflow/convert v0.0.6 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

replace github.com/oarkflow/fh => ../../

replace github.com/oarkflow/fh/ref => ../../ref
