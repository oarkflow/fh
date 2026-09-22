package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/intent"
)

// Database actions.
//
// Statements are written by the application author in their own SQL dialect and
// passed to the driver verbatim with bound parameters. This package never builds
// a WHERE clause by string concatenation from request data, and the one action
// that does generate SQL — database.crud — builds it entirely from validated
// identifiers fixed at load time, with every value bound.
//
// The reason to keep hand-written SQL as the primary interface rather than
// inventing a query DSL: an author who knows SQL can express exactly what they
// mean, including the CTEs and ON CONFLICT clauses that make an operation atomic.
// A DSL would have to grow to cover those, badly, and would still be a dialect
// nobody else can read.

func registerDatabaseActions(r *Registry) {
	mustAction(r, "database.query", databaseQueryAction, ActionInfo{
		Family:       "database",
		Summary:      "Run a query and publish the rows",
		ResourceKind: "database",
		Provides:     "A list of row objects",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "statement", Type: "sql", Required: true},
			{Name: "args", Type: "[]fact", Summary: "Fact paths bound to $1, $2, …"},
			{Name: "max_rows", Type: "int", Default: "1000", Summary: "Refuse a result larger than this"},
			{Name: "require_rows", Type: "bool", Summary: "Fail with not-found when the query returns nothing"},
			{Name: "not_found_message", Type: "string"},
		},
	})

	mustAction(r, "database.exec", databaseExecAction, ActionInfo{
		Family:       "database",
		Summary:      "Run a statement and publish the affected row count",
		ResourceKind: "database",
		Provides:     "The number of rows affected",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "statement", Type: "sql", Required: true},
			{Name: "args", Type: "[]fact"},
			{Name: "require_affected", Type: "bool", Summary: "Fail with not-found when nothing was changed"},
			{Name: "not_found_message", Type: "string"},
		},
	})

	mustAction(r, "database.cached_query", databaseCachedQueryAction, ActionInfo{
		Family:       "database",
		Summary:      "Cache-aside read: serve from the cache, else query and populate it",
		ResourceKind: "database",
		Provides:     "A list of row objects",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "statement", Type: "sql", Required: true},
			{Name: "args", Type: "[]fact"},
			{Name: "cache", Type: "resource", Required: true},
			{Name: "key_fact", Type: "fact", Required: true, Summary: "Fact whose value identifies the cache entry"},
			{Name: "prefix", Type: "string", Summary: "Cache key prefix, so one cache serves several queries"},
			{Name: "ttl", Type: "duration", Default: "30s"},
			{Name: "max_rows", Type: "int", Default: "1000"},
		},
	})

	mustAction(r, "database.transaction", databaseTransactionAction, ActionInfo{
		Family:       "database",
		Summary:      "Run several statements in one transaction, committing only if all succeed",
		ResourceKind: "database",
		Provides:     "Per-statement results",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "statements", Type: "[]object", Required: true, Summary: `statements [ { name … sql … args […] } … ]`},
			{Name: "isolation", Type: "string", Summary: "read_committed, repeatable_read or serializable"},
		},
	})

	mustAction(r, "database.crud", databaseCRUDAction, ActionInfo{
		Family:       "database",
		Summary:      "Generated list/get/create/update/delete against one table, with tenant and owner scoping",
		ResourceKind: "database",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "operation", Type: "string", Required: true, Summary: "list, get, create, update or delete"},
			{Name: "table", Type: "string", Required: true},
			{Name: "columns", Type: "[]string", Required: true, Summary: "Columns that may be read"},
			{Name: "writable", Type: "[]string", Summary: "Columns create and update may set. Defaults to columns minus the keys."},
			{Name: "id_column", Type: "string", Default: "id"},
			{Name: "tenant_column", Type: "string", Summary: "When set, every operation is scoped to the request's tenant"},
			{Name: "owner_column", Type: "string", Summary: "When set, every operation is scoped to the principal"},
			{Name: "soft_delete_column", Type: "string", Summary: "When set, delete stamps this column instead of removing the row"},
			{Name: "order_by", Type: "string", Summary: "Column to order a list by"},
			{Name: "descending", Type: "bool"},
			{Name: "limit", Type: "int", Default: "50"},
			{Name: "max_limit", Type: "int", Default: "200"},
			{Name: "id_fact", Type: "fact", Summary: "Where get/update/delete read the id from"},
			{Name: "input_fact", Type: "fact", Default: "input", Summary: "Where create/update read the record from"},
			{Name: "returning", Type: "bool", Default: "true", Summary: "Publish the written row. Disable on MySQL, which has no RETURNING."},
		},
	})
}

// ---------------------------------------------------------------------------
// Query and exec
// ---------------------------------------------------------------------------

// sqlCall is the compiled form every statement-running action shares.
type sqlCall struct {
	db        *Database
	statement string
	args      []string
	maxRows   int
}

func compileSQLCall(build BuildContext, spec NodeSpec) (*sqlCall, error) {
	db, err := requireResource[*Database](build, spec, "a database.sql resource")
	if err != nil {
		return nil, err
	}
	statement, err := requiredString(spec.Config, "statement")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if err := db.CheckStatement(statement); err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	maxRows, err := configInt(spec.Config, "max_rows", 1000)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return &sqlCall{db: db, statement: statement, args: configStrings(spec.Config, "args"), maxRows: maxRows}, nil
}

var databaseQueryAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileSQLCall(build, spec)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	requireRows := configBool(spec.Config, "require_rows", false)
	missingMessage := configString(spec.Config, "not_found_message", "")

	// A read-kind node reads from the replica when one is configured; anything
	// that might write must go to the primary, or it will fail or read stale.
	readOnly := strings.EqualFold(spec.Kind, "read")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		pool := call.db.DB
		if readOnly {
			pool = call.db.Reader()
		}
		rows, err := queryRows(ctx.Context, pool, call.statement, factArgs(ctx.Inputs, call.args))
		if err != nil {
			return ActionResult{}, databaseFailure(err)
		}
		if len(rows) > call.maxRows {
			return ActionResult{}, invalidInput("the query returned %d rows, over this node's max_rows of %d", len(rows), call.maxRows)
		}
		if requireRows && len(rows) == 0 {
			return ActionResult{}, notFoundOrMessage(missingMessage)
		}
		return singleOutput(spec, rows), nil
	}), nil
})

var databaseExecAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileSQLCall(build, spec)
	if err != nil {
		return nil, err
	}
	requireAffected := configBool(spec.Config, "require_affected", false)
	missingMessage := configString(spec.Config, "not_found_message", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		result, err := call.db.ExecContext(ctx.Context, call.statement, factArgs(ctx.Inputs, call.args)...)
		if err != nil {
			return ActionResult{}, databaseFailure(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			// A driver that cannot report affected rows cannot support
			// require_affected. Say so rather than treating "unknown" as "zero"
			// and failing a statement that actually worked.
			if requireAffected {
				return ActionResult{}, unavailable("this driver does not report affected rows, so require_affected cannot be enforced")
			}
			affected = 0
		}
		if requireAffected && affected == 0 {
			return ActionResult{}, notFoundOrMessage(missingMessage)
		}
		return acknowledgement(spec, affected), nil
	}), nil
})

var databaseCachedQueryAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileSQLCall(build, spec)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	cache, err := requireCache(build, spec, "cache")
	if err != nil {
		return nil, err
	}
	keyFact, err := requiredString(spec.Config, "key_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	ttl, err := configDuration(spec.Config, "ttl", 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	prefix := configString(spec.Config, "prefix", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		key, err := factString(ctx.Inputs, keyFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		key = prefix + key

		if raw, found, err := cache.get(ctx.Context, key); err == nil && found && len(raw) > 0 {
			var cached []map[string]any
			if json.Unmarshal(raw, &cached) == nil {
				return singleOutput(spec, cached), nil
			}
			// A cache entry we cannot decode is treated as a miss and replaced.
			// Failing the request over a corrupt cache value would make the cache
			// a source of outages rather than of speed.
		} else if err != nil {
			// A cache that is down must not take the read path down with it: fall
			// through to the database, which is the whole point of cache-aside.
			_ = err
		}

		rows, err := queryRows(ctx.Context, call.db.Reader(), call.statement, factArgs(ctx.Inputs, call.args))
		if err != nil {
			return ActionResult{}, databaseFailure(err)
		}
		if len(rows) > call.maxRows {
			return ActionResult{}, invalidInput("the query returned %d rows, over this node's max_rows of %d", len(rows), call.maxRows)
		}
		if encoded, err := json.Marshal(rows); err == nil {
			_ = cache.set(ctx.Context, key, encoded, ttl)
		}
		return singleOutput(spec, rows), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

type txStatement struct {
	name      string
	statement string
	args      []string
	query     bool
}

var databaseTransactionAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	db, err := requireResource[*Database](build, spec, "a database.sql resource")
	if err != nil {
		return nil, err
	}
	blocks := configBlocks(spec.Config, "statements", "statement")
	if len(blocks) == 0 {
		return nil, fmt.Errorf("node %q: database.transaction needs a config.statements list, e.g. statements [ { name … sql … args […] } ]", spec.Name)
	}
	statements := make([]txStatement, 0, len(blocks))
	for i, block := range blocks {
		sqlText := configString(block, "sql", configString(block, "statement", ""))
		if sqlText == "" {
			return nil, fmt.Errorf("node %q: statement[%d] needs sql", spec.Name, i)
		}
		if err := db.CheckStatement(sqlText); err != nil {
			return nil, fmt.Errorf("node %q: statement[%d]: %w", spec.Name, i, err)
		}
		statements = append(statements, txStatement{
			name:      configString(block, "name", fmt.Sprintf("statement_%d", i)),
			statement: sqlText,
			args:      configStrings(block, "args"),
			query:     configBool(block, "query", strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "SELECT")),
		})
	}
	isolation, err := parseIsolation(configString(spec.Config, "isolation", ""))
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		tx, err := db.BeginTx(ctx.Context, &sql.TxOptions{Isolation: isolation})
		if err != nil {
			return ActionResult{}, databaseFailure(err)
		}
		// Rollback on every path that does not reach Commit. A committed
		// transaction's Rollback is a no-op, so the deferred call is safe.
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		results := make(map[string]any, len(statements))
		// Later statements can reference earlier results, which is what makes a
		// transaction block useful rather than just a batch: insert a parent, then
		// insert children against the id it returned.
		scope := make(map[string]any, len(ctx.Inputs)+len(statements))
		for key, value := range ctx.Inputs {
			scope[key] = value
		}

		for _, statement := range statements {
			args := factArgs(scope, statement.args)
			if statement.query {
				rows, err := txQueryRows(ctx.Context, tx, statement.statement, args)
				if err != nil {
					return ActionResult{}, databaseFailure(err)
				}
				results[statement.name] = rows
				scope[statement.name] = rows
				continue
			}
			outcome, err := tx.ExecContext(ctx.Context, statement.statement, args...)
			if err != nil {
				return ActionResult{}, databaseFailure(err)
			}
			affected, _ := outcome.RowsAffected()
			results[statement.name] = affected
			scope[statement.name] = affected
		}
		if err := tx.Commit(); err != nil {
			return ActionResult{}, databaseFailure(err)
		}
		committed = true
		return acknowledgement(spec, results), nil
	}), nil
})

func parseIsolation(name string) (sql.IsolationLevel, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "default":
		return sql.LevelDefault, nil
	case "read_committed":
		return sql.LevelReadCommitted, nil
	case "repeatable_read":
		return sql.LevelRepeatableRead, nil
	case "serializable":
		return sql.LevelSerializable, nil
	default:
		return 0, fmt.Errorf("unknown isolation %q (use read_committed, repeatable_read or serializable)", name)
	}
}

func txQueryRows(ctx context.Context, tx *sql.Tx, statement string, args []any) ([]map[string]any, error) {
	rows, err := tx.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, 8)
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			if raw, ok := values[i].([]byte); ok {
				row[column] = string(raw)
				continue
			}
			row[column] = values[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

// crudPlan is a fully compiled CRUD operation.
//
// Every identifier — table, columns, id, tenant and owner columns, order-by — is
// validated at load time and then only ever interpolated from that validated
// set. Every value is bound. There is no path by which request data reaches the
// SQL text, which is what makes generating SQL from configuration safe.
type crudPlan struct {
	db          *Database
	operation   string
	table       string
	columns     []string
	writable    []string
	idColumn    string
	tenant      string
	owner       string
	softDelete  string
	orderBy     string
	descending  bool
	limit       int
	maxLimit    int
	idFact      string
	inputFact   string
	returning   bool
	columnList  string
	writableSet map[string]struct{}
}

var databaseCRUDAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	db, err := requireResource[*Database](build, spec, "a database.sql resource")
	if err != nil {
		return nil, err
	}
	operation := strings.ToLower(configString(spec.Config, "operation", ""))
	switch operation {
	case "list", "get", "create", "update", "delete":
	default:
		return nil, fmt.Errorf("node %q: operation must be list, get, create, update or delete", spec.Name)
	}
	table, err := safeIdentifier(configString(spec.Config, "table", ""))
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	columns, err := safeIdentifiers(configStrings(spec.Config, "columns"))
	if err != nil {
		return nil, fmt.Errorf("node %q: columns: %w", spec.Name, err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("node %q: database.crud needs an explicit columns list — an implicit SELECT * would leak whatever column somebody adds later", spec.Name)
	}
	idColumn, err := safeIdentifier(configString(spec.Config, "id_column", "id"))
	if err != nil {
		return nil, fmt.Errorf("node %q: id_column: %w", spec.Name, err)
	}

	plan := &crudPlan{
		db:        db,
		operation: operation,
		table:     table,
		columns:   columns,
		idColumn:  idColumn,
		idFact:    configString(spec.Config, "id_fact", ""),
		inputFact: configString(spec.Config, "input_fact", "input"),
		returning: configBool(spec.Config, "returning", db.Dialect != "mysql"),
	}
	for _, pair := range []struct {
		key    string
		target *string
	}{
		{"tenant_column", &plan.tenant},
		{"owner_column", &plan.owner},
		{"soft_delete_column", &plan.softDelete},
		{"order_by", &plan.orderBy},
	} {
		if raw := configString(spec.Config, pair.key, ""); raw != "" {
			safe, err := safeIdentifier(raw)
			if err != nil {
				return nil, fmt.Errorf("node %q: %s: %w", spec.Name, pair.key, err)
			}
			*pair.target = safe
		}
	}
	writable := configStrings(spec.Config, "writable")
	if len(writable) == 0 {
		// Default to every column except the ones the platform owns. An author
		// who does not list writable columns still cannot have a client set the
		// id, the tenant or the owner.
		for _, column := range columns {
			if column == plan.idColumn || column == plan.tenant || column == plan.owner || column == plan.softDelete {
				continue
			}
			writable = append(writable, column)
		}
	}
	plan.writable, err = safeIdentifiers(writable)
	if err != nil {
		return nil, fmt.Errorf("node %q: writable: %w", spec.Name, err)
	}
	plan.writableSet = make(map[string]struct{}, len(plan.writable))
	for _, column := range plan.writable {
		plan.writableSet[column] = struct{}{}
	}
	if plan.limit, err = configInt(spec.Config, "limit", 50); err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if plan.maxLimit, err = configInt(spec.Config, "max_limit", 200); err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	plan.descending = configBool(spec.Config, "descending", false)
	plan.columnList = strings.Join(plan.columns, ", ")

	if (operation == "get" || operation == "update" || operation == "delete") && plan.idFact == "" {
		return nil, fmt.Errorf("node %q: %s needs config.id_fact naming the fact holding the record id", spec.Name, operation)
	}
	if err := exactlyOneOutput(spec); err != nil && operation != "delete" {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) { return plan.run(ctx, spec) }), nil
})

func (p *crudPlan) run(ctx *ActionContext, spec NodeSpec) (ActionResult, error) {
	switch p.operation {
	case "list":
		return p.list(ctx, spec)
	case "get":
		return p.get(ctx, spec)
	case "create":
		return p.create(ctx, spec)
	case "update":
		return p.update(ctx, spec)
	default:
		return p.delete(ctx, spec)
	}
}

// scope builds the tenant and owner predicates every operation shares. A
// configured tenant or owner column with no value available is an error, not an
// unscoped query: quietly dropping the predicate would return every tenant's
// rows.
func (p *crudPlan) scope(ctx *ActionContext, args []any) ([]string, []any, error) {
	var conditions []string
	if p.tenant != "" {
		if ctx.TenantID == "" {
			return nil, nil, permissionDenied("this operation is tenant-scoped but no tenant could be determined for the request")
		}
		args = append(args, ctx.TenantID)
		conditions = append(conditions, fmt.Sprintf("%s = $%d", p.tenant, len(args)))
	}
	if p.owner != "" {
		if ctx.Principal.ID == "" {
			return nil, nil, permissionDenied("this operation is owner-scoped but the request is not authenticated")
		}
		args = append(args, ctx.Principal.ID)
		conditions = append(conditions, fmt.Sprintf("%s = $%d", p.owner, len(args)))
	}
	if p.softDelete != "" {
		conditions = append(conditions, p.softDelete+" IS NULL")
	}
	return conditions, args, nil
}

func (p *crudPlan) list(ctx *ActionContext, spec NodeSpec) (ActionResult, error) {
	conditions, args, err := p.scope(ctx, nil)
	if err != nil {
		return ActionResult{}, err
	}
	limit := p.limit
	if requested, found := resolvePath(ctx.Inputs, "input.limit"); found {
		if value, ok := ToFloat(requested); ok && value > 0 {
			limit = min(int(value), p.maxLimit)
		}
	}
	offset := 0
	if requested, found := resolvePath(ctx.Inputs, "input.offset"); found {
		if value, ok := ToFloat(requested); ok && value > 0 {
			offset = int(value)
		}
	}

	statement := "SELECT " + p.columnList + " FROM " + p.table
	if len(conditions) > 0 {
		statement += " WHERE " + strings.Join(conditions, " AND ")
	}
	if p.orderBy != "" {
		statement += " ORDER BY " + p.orderBy
		if p.descending {
			statement += " DESC"
		}
	}
	args = append(args, limit, offset)
	statement += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	rows, err := queryRows(ctx.Context, p.db.Reader(), rebind(p.db.Dialect, statement), args)
	if err != nil {
		return ActionResult{}, databaseFailure(err)
	}
	return singleOutput(spec, rows), nil
}

func (p *crudPlan) get(ctx *ActionContext, spec NodeSpec) (ActionResult, error) {
	id, found := resolvePath(ctx.Inputs, p.idFact)
	if !found || Stringify(id) == "" {
		return ActionResult{}, invalidInput("a record id is required")
	}
	args := []any{normalizeSQLArg(id)}
	conditions := []string{fmt.Sprintf("%s = $1", p.idColumn)}
	scoped, args, err := p.scope(ctx, args)
	if err != nil {
		return ActionResult{}, err
	}
	conditions = append(conditions, scoped...)

	statement := "SELECT " + p.columnList + " FROM " + p.table + " WHERE " + strings.Join(conditions, " AND ") + " LIMIT 1"
	rows, err := queryRows(ctx.Context, p.db.Reader(), rebind(p.db.Dialect, statement), args)
	if err != nil {
		return ActionResult{}, databaseFailure(err)
	}
	if len(rows) == 0 {
		return ActionResult{}, notFound("record", Stringify(id))
	}
	return singleOutput(spec, rows[0]), nil
}

func (p *crudPlan) create(ctx *ActionContext, spec NodeSpec) (ActionResult, error) {
	record, err := p.record(ctx)
	if err != nil {
		return ActionResult{}, err
	}
	columns := make([]string, 0, len(p.writable)+2)
	placeholders := make([]string, 0, len(p.writable)+2)
	args := make([]any, 0, len(p.writable)+2)
	for _, column := range p.writable {
		value, present := record[column]
		if !present {
			continue
		}
		args = append(args, normalizeSQLArg(value))
		columns = append(columns, column)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}
	if p.tenant != "" {
		if ctx.TenantID == "" {
			return ActionResult{}, permissionDenied("this table is tenant-scoped but no tenant could be determined for the request")
		}
		args = append(args, ctx.TenantID)
		columns = append(columns, p.tenant)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}
	if p.owner != "" {
		if ctx.Principal.ID == "" {
			return ActionResult{}, permissionDenied("this table is owner-scoped but the request is not authenticated")
		}
		args = append(args, ctx.Principal.ID)
		columns = append(columns, p.owner)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}
	if len(columns) == 0 {
		return ActionResult{}, invalidInput("nothing to create: the request set none of this table's writable columns")
	}

	statement := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", p.table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	return p.write(ctx, spec, statement, args)
}

func (p *crudPlan) update(ctx *ActionContext, spec NodeSpec) (ActionResult, error) {
	record, err := p.record(ctx)
	if err != nil {
		return ActionResult{}, err
	}
	id, found := resolvePath(ctx.Inputs, p.idFact)
	if !found || Stringify(id) == "" {
		return ActionResult{}, invalidInput("a record id is required")
	}

	assignments := make([]string, 0, len(p.writable))
	args := make([]any, 0, len(p.writable)+3)
	for _, column := range p.writable {
		value, present := record[column]
		if !present {
			continue
		}
		args = append(args, normalizeSQLArg(value))
		assignments = append(assignments, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	if len(assignments) == 0 {
		return ActionResult{}, invalidInput("nothing to update: the request set none of this table's writable columns")
	}
	args = append(args, normalizeSQLArg(id))
	conditions := []string{fmt.Sprintf("%s = $%d", p.idColumn, len(args))}
	scoped, args, err := p.scope(ctx, args)
	if err != nil {
		return ActionResult{}, err
	}
	conditions = append(conditions, scoped...)

	statement := fmt.Sprintf("UPDATE %s SET %s WHERE %s", p.table, strings.Join(assignments, ", "), strings.Join(conditions, " AND "))
	return p.write(ctx, spec, statement, args)
}

func (p *crudPlan) delete(ctx *ActionContext, spec NodeSpec) (ActionResult, error) {
	id, found := resolvePath(ctx.Inputs, p.idFact)
	if !found || Stringify(id) == "" {
		return ActionResult{}, invalidInput("a record id is required")
	}
	args := []any{normalizeSQLArg(id)}
	conditions := []string{fmt.Sprintf("%s = $1", p.idColumn)}
	scoped, args, err := p.scope(ctx, args)
	if err != nil {
		return ActionResult{}, err
	}
	conditions = append(conditions, scoped...)

	var statement string
	if p.softDelete != "" {
		args = append(args, nowUTC())
		statement = fmt.Sprintf("UPDATE %s SET %s = $%d WHERE %s", p.table, p.softDelete, len(args), strings.Join(conditions, " AND "))
	} else {
		statement = fmt.Sprintf("DELETE FROM %s WHERE %s", p.table, strings.Join(conditions, " AND "))
	}
	result, err := p.db.ExecContext(ctx.Context, rebind(p.db.Dialect, statement), args...)
	if err != nil {
		return ActionResult{}, databaseFailure(err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		// Indistinguishable from "not yours": telling an unauthorised caller that
		// the record exists but belongs to somebody else is a disclosure.
		return ActionResult{}, notFound("record", Stringify(id))
	}
	return acknowledgement(spec, map[string]any{"deleted": affected}), nil
}

// write runs an insert or update, using RETURNING when the dialect supports it
// so the caller sees the stored row (with defaults and triggers applied) rather
// than an echo of what it sent.
func (p *crudPlan) write(ctx *ActionContext, spec NodeSpec, statement string, args []any) (ActionResult, error) {
	if p.returning {
		statement += " RETURNING " + p.columnList
		rows, err := queryRows(ctx.Context, p.db.DB, rebind(p.db.Dialect, statement), args)
		if err != nil {
			return ActionResult{}, databaseFailure(err)
		}
		if len(rows) == 0 {
			return ActionResult{}, notFound("record", "")
		}
		return singleOutput(spec, rows[0]), nil
	}
	result, err := p.db.ExecContext(ctx.Context, rebind(p.db.Dialect, statement), args...)
	if err != nil {
		return ActionResult{}, databaseFailure(err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ActionResult{}, notFound("record", "")
	}
	return singleOutput(spec, map[string]any{"affected": affected}), nil
}

// record reads the incoming record and drops anything not writable.
//
// Dropping rather than rejecting is deliberate: a client that echoes back a
// whole object including its id and timestamps is the normal case, and failing
// that request would make every round-trip edit awkward. What matters is that
// the extra fields cannot reach the database.
func (p *crudPlan) record(ctx *ActionContext) (map[string]any, error) {
	raw, found := resolvePath(ctx.Inputs, p.inputFact)
	if !found {
		return nil, invalidInput("no record supplied")
	}
	object, ok := raw.(map[string]any)
	if !ok {
		return nil, invalidInput("the record must be an object")
	}
	record := make(map[string]any, len(object))
	for key, value := range object {
		if _, writable := p.writableSet[key]; writable {
			record[key] = value
		}
	}
	return record, nil
}

// notFoundOrMessage lets a node supply a caller-facing not-found message, which
// matters because "todo not found" is useful and "record not found" is not.
func notFoundOrMessage(message string) error {
	if message == "" {
		return notFound("record", "")
	}
	return intent.Failure{Code: "NOT_FOUND", Category: intent.CategoryNotFound, Message: message}
}
