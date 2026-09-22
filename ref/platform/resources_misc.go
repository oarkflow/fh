package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/platform/spi"
)

// The remaining providers: secrets, search, and the transactional outbox.

func registerMiscResources(r *Registry) {
	mustResource(r, "secret.env", ResourceFactoryFunc(openEnvSecrets), ResourceKindInfo{
		Family:   "secret",
		Summary:  "Resolves secrets from the process environment.",
		Provides: []string{"SecretStore"},
		Config: []ConfigField{
			{Name: "prefix", Type: "string", Summary: "Prepended to every lookup, e.g. \"APP_\""},
		},
	})

	mustResource(r, "secret.file", ResourceFactoryFunc(openFileSecrets), ResourceKindInfo{
		Family:   "secret",
		Summary:  "Resolves secrets from a directory of files, the shape a Kubernetes secret mount and Docker secrets both take.",
		Provides: []string{"SecretStore"},
		Config: []ConfigField{
			{Name: "dir", Type: "string", Required: true},
		},
	})

	mustResource(r, "search.sql", ResourceFactoryFunc(openSQLSearch), ResourceKindInfo{
		Family:   "search",
		Summary:  "Keyword search over a SQL table. Honest about being keyword search, not relevance ranking.",
		Provides: []string{"SearchIndex"},
		Config: []ConfigField{
			{Name: "database", Type: "resource", Required: true},
			{Name: "table", Type: "string", Default: "platform_search"},
			{Name: "migrate", Type: "bool", Default: "true"},
		},
	})

	mustResource(r, "outbox.memory", ResourceFactoryFunc(openMemoryOutbox), ResourceKindInfo{
		Family:   "outbox",
		Summary:  "In-process transactional outbox, for development and tests.",
		Provides: []string{"Outbox"},
		Config: []ConfigField{
			{Name: "max_messages", Type: "int", Default: "10000"},
		},
	})
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

type envSecrets struct{ prefix string }

func openEnvSecrets(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("secret.env", spec.Config, "prefix"); err != nil {
		return nil, nil, err
	}
	return &envSecrets{prefix: configString(spec.Config, "prefix", "")}, nil, nil
}

// Secret implements spi.SecretStore.
func (s *envSecrets) Secret(name string) (string, bool, error) {
	value, found := os.LookupEnv(s.prefix + name)
	return value, found && value != "", nil
}

type fileSecrets struct{ dir string }

func openFileSecrets(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("secret.file", spec.Config, "dir"); err != nil {
		return nil, nil, err
	}
	dir, err := requiredString(spec.Config, "dir")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	if _, err := os.Stat(dir); err != nil {
		return nil, nil, fmt.Errorf("resource %q: secret directory: %w", spec.Name, err)
	}
	return &fileSecrets{dir: dir}, nil, nil
}

// Secret implements spi.SecretStore. The name is restricted to a plain file
// name so a lookup cannot be steered out of the directory.
func (s *fileSecrets) Secret(name string) (string, bool, error) {
	if name == "" || strings.ContainsAny(name, `/\.`) {
		return "", false, fmt.Errorf("secret name %q must be a plain name", name)
	}
	data, err := os.ReadFile(s.dir + string(os.PathSeparator) + name)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	value := strings.TrimRight(string(data), "\r\n")
	return value, value != "", nil
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// sqlSearch is keyword search over a document table.
//
// It is deliberately modest, and says so in its catalog summary. Each document's
// searchable fields are flattened into one text column and matched with LIKE
// over lowercased terms; results are ordered by how many terms matched. That is
// genuinely useful for "find the order with this reference" and genuinely not a
// relevance-ranked search engine. A deployment that needs BM25, typo tolerance
// or facets should register OpenSearch, Typesense or Meilisearch through the
// driver SPI — which is exactly why spi.SearchIndex exists.
type sqlSearch struct {
	db    *Database
	table string
}

func openSQLSearch(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("search.sql", spec.Config, "database", "table", "migrate"); err != nil {
		return nil, nil, err
	}
	db, err := requireSQLHandle(spec, "database")
	if err != nil {
		return nil, nil, err
	}
	table, err := safeIdentifier(configString(spec.Config, "table", "platform_search"))
	if err != nil {
		return nil, nil, fmt.Errorf("search.sql %q: %w", spec.Name, err)
	}
	index := &sqlSearch{db: db, table: table}
	if configBool(spec.Config, "migrate", true) {
		if err := index.migrate(ctx); err != nil {
			return nil, nil, fmt.Errorf("search.sql %q: %w", spec.Name, err)
		}
	}
	return index, nil, nil
}

func (s *sqlSearch) query(statement string) string { return rebind(s.db.Dialect, statement) }

func (s *sqlSearch) migrate(ctx context.Context) error {
	dialect := s.db.Dialect
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			collection %s NOT NULL,
			doc_id     %s NOT NULL,
			tenant     %s,
			body       TEXT NOT NULL,
			payload    TEXT,
			PRIMARY KEY (collection, doc_id)
		)`, s.table, textType(dialect), textType(dialect), textType(dialect)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_tenant_idx ON %s (collection, tenant)`, s.table, s.table),
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			if strings.Contains(statement, "CREATE INDEX") {
				continue
			}
			return err
		}
	}
	return nil
}

// Index implements spi.SearchIndex.
func (s *sqlSearch) Index(ctx context.Context, collection string, docs []spi.SearchDoc) error {
	if len(docs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	statement := fmt.Sprintf(`INSERT INTO %s (collection, doc_id, tenant, body, payload) VALUES ($1,$2,$3,$4,$5)`, s.table)
	if s.db.Dialect == "mysql" {
		statement += upsertClause(s.db.Dialect, "collection, doc_id",
			"tenant = VALUES(tenant)", "body = VALUES(body)", "payload = VALUES(payload)")
	} else {
		statement += upsertClause(s.db.Dialect, "collection, doc_id",
			"tenant = EXCLUDED.tenant", "body = EXCLUDED.body", "payload = EXCLUDED.payload")
	}
	for _, doc := range docs {
		payload, err := json.Marshal(doc.Payload)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, s.query(statement),
			collection, doc.ID, nullString(doc.Tenant), flattenSearchable(doc.Fields), string(payload)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Delete implements spi.SearchIndex.
func (s *sqlSearch) Delete(ctx context.Context, collection string, ids []string) error {
	statement := fmt.Sprintf("DELETE FROM %s WHERE collection = $1 AND doc_id = $2", s.table)
	for _, id := range ids {
		if _, err := s.db.ExecContext(ctx, s.query(statement), collection, id); err != nil {
			return err
		}
	}
	return nil
}

// Search implements spi.SearchIndex.
func (s *sqlSearch) Search(ctx context.Context, collection string, q spi.SearchQuery) (spi.SearchResult, error) {
	limit := q.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	conditions := []string{"collection = $1"}
	args := []any{collection}
	if q.Tenant != "" {
		args = append(args, q.Tenant)
		conditions = append(conditions, fmt.Sprintf("tenant = $%d", len(args)))
	}
	terms := strings.Fields(strings.ToLower(q.Text))
	for _, term := range terms {
		args = append(args, "%"+escapeLike(term)+"%")
		conditions = append(conditions, fmt.Sprintf(`body LIKE $%d ESCAPE '\'`, len(args)))
	}
	args = append(args, limit, q.Offset)
	statement := fmt.Sprintf(`SELECT doc_id, body, payload FROM %s WHERE %s ORDER BY doc_id LIMIT $%d OFFSET $%d`,
		s.table, strings.Join(conditions, " AND "), len(args)-1, len(args))

	rows, err := s.db.QueryContext(ctx, s.query(statement), args...)
	if err != nil {
		return spi.SearchResult{}, err
	}
	defer rows.Close()

	result := spi.SearchResult{Total: -1}
	for rows.Next() {
		var (
			id      string
			body    string
			payload sql.NullString
		)
		if err := rows.Scan(&id, &body, &payload); err != nil {
			return result, err
		}
		hit := spi.SearchHit{ID: id, Score: termScore(body, terms)}
		if payload.Valid && payload.String != "" {
			_ = json.Unmarshal([]byte(payload.String), &hit.Payload)
		}
		if !matchesFilters(hit.Payload, q.Filters) {
			continue
		}
		result.Hits = append(result.Hits, hit)
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	sort.SliceStable(result.Hits, func(i, j int) bool { return result.Hits[i].Score > result.Hits[j].Score })
	return result, nil
}

// flattenSearchable renders a document's fields into one lowercased text blob.
// Keys are included alongside values so a search for a field name finds
// documents that have it.
func flattenSearchable(fields map[string]any) string {
	var out strings.Builder
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out.WriteString(strings.ToLower(key))
		out.WriteByte(' ')
		out.WriteString(strings.ToLower(Stringify(fields[key])))
		out.WriteByte(' ')
	}
	return out.String()
}

// termScore is the fraction of query terms present, so a document matching every
// term outranks one matching a single term.
func termScore(body string, terms []string) float64 {
	if len(terms) == 0 {
		return 1
	}
	matched := 0
	for _, term := range terms {
		if strings.Contains(body, term) {
			matched++
		}
	}
	return float64(matched) / float64(len(terms))
}

func matchesFilters(payload map[string]any, filters map[string]any) bool {
	for path, expected := range filters {
		value, found := lookupPath(payload, strings.Split(path, "."))
		if !found || Stringify(value) != Stringify(expected) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

func openMemoryOutbox(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("outbox.memory", spec.Config, "max_messages"); err != nil {
		return nil, nil, err
	}
	maxMessages, err := configInt(spec.Config, "max_messages", 10000)
	if err != nil {
		return nil, nil, err
	}
	store := fh.NewMemoryOutboxInboxStoreWithLimit(maxMessages, maxMessages)
	return store, store, nil
}

// notFound builds the platform's standard not-found failure. Keeping one
// constructor means every provider's missing-resource error reaches the caller
// as a 404 with the same shape.
func notFound(kind, id string) error {
	return intent.Failure{
		Code:     "NOT_FOUND",
		Category: intent.CategoryNotFound,
		Message:  fmt.Sprintf("%s %q was not found", kind, id),
	}
}

var (
	_ spi.SecretStore = (*envSecrets)(nil)
	_ spi.SecretStore = (*fileSecrets)(nil)
	_ spi.SearchIndex = (*sqlSearch)(nil)
)
