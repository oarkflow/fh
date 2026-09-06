module github.com/oarkflow/fh/examples/template

go 1.26.5

require github.com/oarkflow/fh v0.0.21

require github.com/oarkflow/bcl v0.0.31

require (
	github.com/oarkflow/authz v0.0.5
	github.com/oarkflow/fh-contrib v0.0.4
	github.com/oarkflow/squealx v0.0.78
	github.com/oarkflow/template v0.0.3
	github.com/oarkflow/zlog v0.0.3
)

require golang.org/x/crypto v0.55.0 // indirect

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/goccy/go-reflect v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/oarkflow/convert v0.0.6 // indirect
	github.com/oarkflow/date v0.0.4 // indirect
	github.com/oarkflow/expr v0.0.11 // indirect
	github.com/oarkflow/interpreter v0.0.12 // indirect
	github.com/oarkflow/jet v0.0.4 // indirect
	github.com/oarkflow/json v0.0.28 // indirect
	github.com/oarkflow/spl v0.0.8 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	modernc.org/libc v1.73.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.53.0 // indirect
)

replace github.com/oarkflow/fh => ../..
