package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/oarkflow/fh/mw/apikey"
	"github.com/oarkflow/fh/mw/jwt"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "jwt:sign":
		jwtSign(os.Args[2:])
	case "apikey:generate":
		apiKeyGenerate(os.Args[2:])
	case "doctor":
		fmt.Println("fh doctor: run go test ./... and protect admin/pprof endpoints before production")
	default:
		usage()
		os.Exit(2)
	}
}
func jwtSign(args []string) {
	fs := flag.NewFlagSet("jwt:sign", flag.ExitOnError)
	secret := fs.String("secret", "", "HMAC secret")
	alg := fs.String("alg", "HS256", "algorithm")
	sub := fs.String("sub", "", "subject")
	tenant := fs.String("tenant", "", "tenant id")
	roles := fs.String("roles", "", "comma roles")
	scopes := fs.String("scopes", "", "space/comma scopes")
	perms := fs.String("permissions", "", "comma permissions")
	ttl := fs.Duration("ttl", time.Hour, "token lifetime")
	claimsJSON := fs.String("claims", "", "JSON claims")
	_ = fs.Parse(args)
	if *secret == "" {
		fmt.Fprintln(os.Stderr, "--secret is required")
		os.Exit(2)
	}
	now := time.Now()
	claims := map[string]any{"exp": now.Add(*ttl).Unix(), "iat": now.Unix()}
	if *sub != "" {
		claims["sub"] = *sub
	}
	if *tenant != "" {
		claims["tenant_id"] = *tenant
	}
	if *roles != "" {
		claims["roles"] = splitList(*roles)
	}
	if *scopes != "" {
		claims["scope"] = strings.Join(splitList(*scopes), " ")
	}
	if *perms != "" {
		claims["permissions"] = splitList(*perms)
	}
	if *claimsJSON != "" {
		var extra map[string]any
		if err := json.Unmarshal([]byte(*claimsJSON), &extra); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		for k, v := range extra {
			claims[k] = v
		}
	}
	tok, err := jwt.Sign(claims, []byte(*secret), *alg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(tok)
}
func apiKeyGenerate(args []string) {
	fs := flag.NewFlagSet("apikey:generate", flag.ExitOnError)
	prefix := fs.String("prefix", "fh_live", "API key prefix")
	bytes := fs.Int("bytes", 32, "random bytes")
	_ = fs.Parse(args)
	key, hash, err := apikey.Generate(*prefix, *bytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("key=" + key)
	fmt.Println("sha256=" + hash)
}
func splitList(s string) []string {
	s = strings.NewReplacer(",", " ").Replace(s)
	return strings.Fields(s)
}
func usage() {
	fmt.Println("fh commands:\n  fh jwt:sign --secret dev --sub u1 --roles admin --ttl 1h\n  fh apikey:generate --prefix fh_live\n  fh doctor")
}
