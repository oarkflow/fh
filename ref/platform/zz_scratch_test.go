package platform

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/oarkflow/bcl"
)

func TestScratchTodoBinding(t *testing.T) {
	path := filepath.Join(exampleRoot(t), "ref-platform-todo", "app.bcl")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc Document
	if err := bcl.UnmarshalWithOptions(source, &doc, &bcl.Options{
		BaseDir: filepath.Dir(path), AllowEnv: true, Env: todoEnv, ResolveImports: true, Strict: true,
	}); err != nil {
		t.Fatal(err)
	}
	for _, r := range doc.Routes {
		t.Logf("route %-22s method=%q path=%q intent=%q session=%q status=%d cache=%q",
			r.Name, r.Method, r.Path, r.Intent, r.Session, r.Status, r.CacheControl)
	}
	for _, res := range doc.Resources {
		t.Logf("resource %-14s kind=%q keys=%d %v", res.Name, res.Kind, len(res.Config), res.Config)
	}
	for _, in := range doc.Intents {
		if in.Name != "todos.list" && in.Name != "auth.login" {
			continue
		}
		for _, n := range in.Nodes {
			t.Logf("intent %s node %-14s family=%q uses=%q kind=%q spec=%q req=%v prov=%v cfg=%v",
				in.Name, n.Name, n.Family, n.Uses, n.Kind, n.Speculation, n.Requires, n.Provides, n.Config)
		}
	}
	for _, w := range doc.Workers {
		t.Logf("worker %s queue=%q job_type=%q intent=%q", w.Name, w.Queue, w.JobType, w.Intent)
	}
}

func todoEnv(name string) (string, bool) {
	values := map[string]string{
		"DATABASE_URL":           "postgres://localhost/todo",
		"SESSION_SECRET":         "0123456789abcdef0123456789abcdef0123",
		"TODO_NOTIFICATION_HOST": "127.0.0.1",
	}
	v, ok := values[name]
	return v, ok
}
