package acceptquery

import "testing"

func TestBuild(t *testing.T) {
	field, accepted, err := Build([]string{"application/jsonpath", `application/sql; charset="UTF-8"`})
	if err != nil {
		t.Fatal(err)
	}
	if field != `application/jsonpath, application/sql;charset="UTF-8"` {
		t.Fatalf("field=%q", field)
	}
	if len(accepted) != 2 || accepted[0] != "application/jsonpath" || accepted[1] != "application/sql" {
		t.Fatalf("accepted=%v", accepted)
	}
}

func TestBuildUsesStringForNonToken(t *testing.T) {
	field, _, err := Build([]string{"1application/example"})
	if err != nil {
		t.Fatal(err)
	}
	if field != `"1application/example"` {
		t.Fatalf("field=%q", field)
	}
}
