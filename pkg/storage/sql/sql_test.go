package sqlstore

import "testing"

func TestSchemaGenerated(t *testing.T) {
	c := &core{dialect: DialectPostgres, prefix: "fh_"}
	if len(c.schema()) < 3 {
		t.Fatal("expected schema statements")
	}
	c.dialect = DialectSQLite
	if len(c.schema()) < 3 {
		t.Fatal("expected sqlite schema statements")
	}
}
