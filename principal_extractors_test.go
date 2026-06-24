package fh

import "testing"

func TestPrincipalExtractorFromCtxSources(t *testing.T) {
	c := &Ctx{}
	c.reset()
	c.Header.Set("X-Subject-ID", "u1")
	c.Header.Set("X-Tenant-ID", "t1")
	c.Header.Set("X-Roles", "admin, editor")
	c.Header.Set("X-Scopes", "orders:create")
	c.body = []byte(`{"auth":{"permissions":["orders:read","orders:write"],"claims":{"tier":"gold"}}}`)

	ex := PrincipalExtractor(PrincipalExtractors{
		ID:          HeaderString("X-Subject-ID"),
		TenantID:    HeaderString("X-Tenant-ID"),
		Roles:       HeaderCSV("X-Roles"),
		Scopes:      HeaderCSV("X-Scopes"),
		Permissions: BodyCSV("auth.permissions"),
		Claims: func(c *Ctx) (map[string]any, bool, error) {
			v, ok, err := BodyField("auth.claims")(c)
			if err != nil || !ok {
				return nil, ok, err
			}
			claims, ok := v.(map[string]any)
			return claims, ok, nil
		},
	})

	p, ok, err := ex(c)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected principal")
	}
	if p.ID != "u1" || p.TenantID != "t1" || !hasString(p.Roles, "admin") || !hasString(p.Scopes, "orders:create") || !hasString(p.Permissions, "orders:write") {
		t.Fatalf("unexpected principal: %#v", p)
	}
	if p.Claims["tier"] != "gold" {
		t.Fatalf("claims not extracted: %#v", p.Claims)
	}
}
