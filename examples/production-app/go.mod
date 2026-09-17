module github.com/oarkflow/fh/examples/production-app

go 1.26.5

require (
	github.com/oarkflow/authz v0.0.6
	github.com/oarkflow/config v0.0.1
	github.com/oarkflow/fh v0.0.24
	github.com/oarkflow/fh-contrib v0.0.4
	github.com/oarkflow/squealx v0.0.78
	github.com/oarkflow/tcpguard v0.0.16
	github.com/oarkflow/template v0.0.3
	github.com/oarkflow/zlog v0.0.3
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/goccy/go-reflect v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/oarkflow/bcl v0.0.30 // indirect
	github.com/oarkflow/date v0.0.4 // indirect
	github.com/oarkflow/expr v0.0.11 // indirect
	github.com/oarkflow/interpreter v0.0.12 // indirect
	github.com/oarkflow/ip v0.0.11 // indirect
	github.com/oarkflow/jet v0.0.4 // indirect
	github.com/oarkflow/json v0.0.28 // indirect
	github.com/oarkflow/rules v0.0.2 // indirect
	github.com/oarkflow/spl v0.0.8 // indirect
	github.com/oarkflow/wuid v0.0.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	modernc.org/libc v1.73.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.53.0 // indirect
)

// This example is a checked-in copy of fh-init's template output, kept
// buildable against the fh source in this repository (not the last
// published release) so it stays honest about what the framework's own
// working tree currently produces - see the root Makefile's
// production-app-check target.
replace github.com/oarkflow/fh => ../..
