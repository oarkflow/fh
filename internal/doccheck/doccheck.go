// Package doccheck extracts and validates the ```go fenced code blocks that
// appear in the project's Markdown documentation (docs/*.md and README.md).
//
// Every block is classified as either:
//
//   - a full program: the block itself starts with "package main" (or
//     "package <name>") and is a complete, compilable Go file. These are
//     compiled for real, as an isolated module that requires this repo via a
//     replace directive, using `go build`.
//   - a fragment: an illustrative snippet (statements, a lone declaration, a
//     bare function signature, ...) that is not meant to compile standalone.
//     Fragments are checked in two independent ways:
//     1. Syntax validity: the fragment is wrapped in a synthetic file and
//     parsed with go/parser. This catches malformed Go (typos, unbalanced
//     braces, bad syntax) without tripping over placeholder identifiers
//     such as `listUsers` or `myStruct` that are illustrative, not real.
//     2. Symbol existence: the raw fragment text is scanned for
//     `<pkg>.<Identifier>` references where <pkg> resolves to a known
//     package (the root `fh` package or one of the `mw/*`/`pkg/*`
//     packages in this module). Each such reference is checked against
//     that package's real top-level exported identifiers (obtained via
//     `go doc`). This catches renamed/removed exported functions, types,
//     vars and consts. It deliberately does NOT check methods on values
//     (e.g. `c.JSON(...)`) or locally-scoped placeholder identifiers,
//     since that would require full type-checking and would produce
//     spurious failures on intentionally illustrative code.
package doccheck

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Block is one ```go fenced code block extracted from a Markdown file.
type Block struct {
	File      string // path to the markdown file, relative to repo root
	StartLine int    // 1-based line number of the ```go fence itself
	Source    string // raw code inside the fence, exactly as written
}

// Pos returns a "file:line" string for error messages.
func (b Block) Pos() string {
	return fmt.Sprintf("%s:%d", b.File, b.StartLine)
}

var goFenceOpen = regexp.MustCompile(`^` + "```" + `go\s*$`)

// ExtractGoBlocks scans a single Markdown file and returns every ```go
// fenced code block it contains, in document order.
func ExtractGoBlocks(path string) ([]Block, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var blocks []Block
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 10*1024*1024)
	lineNo := 0
	inBlock := false
	blockStart := 0
	var buf []string
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if !inBlock {
			if goFenceOpen.MatchString(strings.TrimRight(line, " \t")) {
				inBlock = true
				blockStart = lineNo
				buf = buf[:0]
			}
			continue
		}
		// inside a ```go block: look for the closing fence.
		if strings.TrimSpace(line) == "```" {
			blocks = append(blocks, Block{
				File:      path,
				StartLine: blockStart,
				Source:    strings.Join(buf, "\n"),
			})
			inBlock = false
			continue
		}
		buf = append(buf, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if inBlock {
		return nil, fmt.Errorf("%s: unterminated ```go fence starting at line %d", path, blockStart)
	}
	return blocks, nil
}

// ExtractGoBlocksFromFiles extracts blocks from multiple files, in the order
// the files are given.
func ExtractGoBlocksFromFiles(paths []string) ([]Block, error) {
	var all []Block
	for _, p := range paths {
		bs, err := ExtractGoBlocks(p)
		if err != nil {
			return nil, err
		}
		all = append(all, bs...)
	}
	return all, nil
}

// Kind classifies a Block.
type Kind int

const (
	// KindFullProgram: the block itself is a complete "package main" (or
	// other package) source file.
	KindFullProgram Kind = iota
	// KindFragment: an illustrative snippet, not standalone.
	KindFragment
)

var packageLineRe = regexp.MustCompile(`^package\s+[A-Za-z_]\w*\s*$`)

// Classify determines whether a block is a full standalone program or an
// illustrative fragment. A block is a full program only if its very first
// non-blank line is a bare `package <name>` clause - i.e. it is written as a
// complete Go source file.
func Classify(b Block) Kind {
	for _, line := range strings.Split(b.Source, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if packageLineRe.MatchString(trimmed) {
			return KindFullProgram
		}
		return KindFragment
	}
	return KindFragment
}

// ---------------------------------------------------------------------------
// Full program compilation
// ---------------------------------------------------------------------------

// CompileFullProgram writes b.Source out as its own throwaway module (with a
// replace directive pointing at repoRoot) under dir and runs `go build ./...`
// in it. dir must already exist (e.g. a t.TempDir()).
func CompileFullProgram(b Block, repoRoot, dir string) error {
	goMod := fmt.Sprintf(`module doccheck_example

go 1.24

require github.com/oarkflow/fh v0.0.0-00010101000000-000000000000

replace github.com/oarkflow/fh => %s
`, repoRoot)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(b.Source), 0o644); err != nil {
		return err
	}

	cmd := exec.Command("go", "build", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go build failed:\n%s", string(out))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Fragment syntax checking
// ---------------------------------------------------------------------------

var (
	importSingleRe = regexp.MustCompile(`^\s*import\s+"[^"]+"\s*$`)
	importBlockRe  = regexp.MustCompile(`^\s*import\s*\(\s*$`)
	namedFuncRe    = regexp.MustCompile(`^func\s+(\([^)]*\)\s*)?[A-Za-z_]\w*\s*\(`)
)

// stripImports removes top-level `import "..."` lines and `import (...)`
// blocks from code, returning the remaining code and the collected import
// paths (informational only - go/parser does not resolve imports, so these
// are not required for the syntax check to succeed).
func stripImports(code string) (rest string, imports []string) {
	lines := strings.Split(code, "\n")
	var out []string
	i := 0
	for i < len(lines) {
		line := lines[i]
		switch {
		case importSingleRe.MatchString(line):
			if m := regexp.MustCompile(`"([^"]+)"`).FindStringSubmatch(line); m != nil {
				imports = append(imports, m[1])
			}
			i++
		case importBlockRe.MatchString(line):
			i++
			for i < len(lines) && strings.TrimSpace(lines[i]) != ")" {
				if m := regexp.MustCompile(`"([^"]+)"`).FindStringSubmatch(lines[i]); m != nil {
					imports = append(imports, m[1])
				}
				i++
			}
			i++ // skip closing ")"
		default:
			out = append(out, line)
			i++
		}
	}
	return strings.Join(out, "\n"), imports
}

// splitSegments splits code into blank-line-separated paragraphs, preserving
// each paragraph's internal formatting.
func splitSegments(code string) []string {
	lines := strings.Split(code, "\n")
	var segments []string
	var cur []string
	flush := func() {
		if len(cur) == 0 {
			return
		}
		// Drop segments that are entirely blank/whitespace.
		joined := strings.Join(cur, "\n")
		if strings.TrimSpace(joined) != "" {
			segments = append(segments, joined)
		}
		cur = nil
	}
	depth := 0 // brace/paren/bracket nesting depth, so we don't split inside a multi-line construct
	for _, line := range lines {
		if strings.TrimSpace(line) == "" && depth == 0 {
			flush()
			continue
		}
		cur = append(cur, line)
		depth += strings.Count(line, "{") + strings.Count(line, "(") + strings.Count(line, "[")
		depth -= strings.Count(line, "}") + strings.Count(line, ")") + strings.Count(line, "]")
		if depth < 0 {
			depth = 0
		}
	}
	flush()
	return segments
}

// isSignatureNotationLine reports whether line (with any trailing "//"
// comment stripped) has the shape "<call-like-prefix>(...) <trailing>" where
// <trailing> looks like a bare type name. That shape is not valid Go as a
// statement or expression (a call result can't be juxtaposed with a type),
// so it identifies documentation-only "method signature" notation such as
// `file.Save(dst string) error` (see docs/codecs.md) - i.e. describing a
// method's signature, not showing code that runs. An empty line (or a line
// that is only a comment) trivially satisfies this, so it doesn't break a
// multi-line signature-notation segment.
func isSignatureNotationLine(line string) bool {
	if idx := strings.Index(line, "//"); idx >= 0 {
		line = line[:idx]
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return true
	}
	idx := strings.LastIndexByte(line, ')')
	if idx < 0 {
		return false
	}
	trailing := strings.TrimSpace(line[idx+1:])
	if trailing == "" {
		return false
	}
	return signatureTrailingTypeRe.MatchString(trailing)
}

var signatureTrailingTypeRe = regexp.MustCompile(`^[A-Za-z_][\w.\[\]*]*$`)

// isSignatureNotationSegment reports whether every line of seg looks like
// signature notation per isSignatureNotationLine, with at least one line
// that actually has trailing content (otherwise blank-only input would
// trivially match).
func isSignatureNotationSegment(seg string) bool {
	any := false
	for _, line := range strings.Split(seg, "\n") {
		if !isSignatureNotationLine(line) {
			return false
		}
		if strings.TrimSpace(line) != "" {
			any = true
		}
	}
	return any
}

// CheckFragmentSyntax verifies that a fragment is syntactically valid Go. It
// wraps the fragment in a synthetic file: named top-level func/method
// declarations are kept at package scope (Go disallows nesting them);
// everything else (statements, local type/const/var decls, expressions) is
// gathered into one synthetic function body. A segment that is a bare
// function *type* signature with no body (e.g. `func(c fh.Ctx) error`, used
// to document a handler signature) is special-cased into a type alias. A
// segment that is pure method-signature notation (e.g.
// `file.Save(dst string) error`, describing a method rather than showing
// runnable code) is recognized and skipped rather than forced through the
// parser - see isSignatureNotationSegment.
func CheckFragmentSyntax(b Block) error {
	code, _ := stripImports(b.Source)
	if strings.TrimSpace(code) == "" {
		// Nothing left to check (block was e.g. only an import line).
		return nil
	}

	segments := splitSegments(code)
	if len(segments) == 0 {
		return nil
	}

	var pkgLevel []string
	var bodyLevel []string
	for idx, seg := range segments {
		trimmedSeg := strings.TrimSpace(seg)
		firstLine := trimmedSeg
		if nl := strings.IndexByte(trimmedSeg, '\n'); nl >= 0 {
			firstLine = trimmedSeg[:nl]
		}
		switch {
		case strings.HasPrefix(firstLine, "func") && !strings.Contains(seg, "{"):
			// Bare function-type signature, no body: illustrative type only.
			pkgLevel = append(pkgLevel, fmt.Sprintf("type _doccheckSig%d = %s", idx, trimmedSeg))
		case isSignatureNotationSegment(seg):
			// Documentation-only method-signature notation; not real code.
			continue
		case namedFuncRe.MatchString(trimmedSeg):
			// Named top-level func/method declaration: can't nest, keep as-is.
			pkgLevel = append(pkgLevel, seg)
		default:
			bodyLevel = append(bodyLevel, seg)
		}
	}

	var sb strings.Builder
	sb.WriteString("package doccheck_frag\n\n")
	for _, p := range pkgLevel {
		sb.WriteString(p)
		sb.WriteString("\n\n")
	}
	if len(bodyLevel) > 0 {
		sb.WriteString("func _doccheckFrag() {\n")
		sb.WriteString(strings.Join(bodyLevel, "\n\n"))
		sb.WriteString("\n}\n")
	}

	fset := token.NewFileSet()
	_, err := parser.ParseFile(fset, b.Pos(), sb.String(), parser.AllErrors)
	if err != nil {
		return fmt.Errorf("syntax error: %w\n--- synthesized source ---\n%s", err, sb.String())
	}
	return nil
}

// ---------------------------------------------------------------------------
// Fragment symbol-existence checking
// ---------------------------------------------------------------------------

// PackageInfo records where a package short name (as used in qualified
// references like `fh.New` or `compress.New`) actually lives.
type PackageInfo struct {
	ImportPath string
	Dir        string // absolute directory containing the package's source
}

// PackageRegistry maps a Go package identifier (as it would be written in
// code, e.g. "compress") to every real package in this module that declares
// that identifier as its package name. It is a slice, not a single value,
// because short names can collide across independent packages - e.g. this
// module has both mw/httpsignature and pkg/httpsignature, both declaring
// `package httpsignature`. A qualified reference `httpsignature.Foo` is
// accepted if Foo exists in ANY candidate; see CheckFragmentSymbols.
type PackageRegistry map[string][]PackageInfo

// DiscoverPackages walks the module rooted at repoRoot and returns every
// package that documentation might plausibly reference by short name: the
// root module package (aliased "fh") plus every package under mw/ and pkg/.
func DiscoverPackages(repoRoot, modulePath string) (PackageRegistry, error) {
	reg := PackageRegistry{}
	reg["fh"] = []PackageInfo{{ImportPath: modulePath, Dir: repoRoot}}

	for _, sub := range []string{"mw", "pkg"} {
		root := filepath.Join(repoRoot, sub)
		entries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if err := discoverDir(filepath.Join(root, e.Name()), repoRoot, modulePath, reg); err != nil {
				return nil, err
			}
		}
	}
	return reg, nil
}

// discoverDir inspects dir for a non-test .go file, records its package name
// -> (import path, dir) in reg, and recurses into subdirectories (for nested
// packages such as pkg/storage/kv).
func discoverDir(dir, repoRoot, modulePath string, reg PackageRegistry) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	pkgName := ""
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		if m := regexp.MustCompile(`(?m)^package\s+([A-Za-z_]\w*)`).FindStringSubmatch(string(data)); m != nil {
			pkgName = m[1]
			break
		}
	}
	if pkgName != "" {
		rel, err := filepath.Rel(repoRoot, dir)
		if err == nil {
			importPath := modulePath + "/" + filepath.ToSlash(rel)
			reg[pkgName] = append(reg[pkgName], PackageInfo{ImportPath: importPath, Dir: dir})
		}
	}
	for _, e := range entries {
		if e.IsDir() {
			if err := discoverDir(filepath.Join(dir, e.Name()), repoRoot, modulePath, reg); err != nil {
				return err
			}
		}
	}
	return nil
}

// SymbolTable caches the exported top-level identifiers of packages, keyed by
// directory.
type SymbolTable struct {
	cache map[string]map[string]bool
}

func NewSymbolTable() *SymbolTable {
	return &SymbolTable{cache: map[string]map[string]bool{}}
}

// Load parses every non-test .go file directly in dir and returns the set of
// exported top-level identifiers (funcs, types, vars, consts - including
// consts/vars declared inside a type's own doc grouping) using go/doc, which
// - unlike the `go doc` CLI's human-oriented summary - never truncates
// grouped declarations, so it reliably reflects every real exported symbol.
func (st *SymbolTable) Load(dir string) (map[string]bool, error) {
	if syms, ok := st.cache[dir]; ok {
		return syms, nil
	}

	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", filepath.Join(dir, name), err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no non-test .go files found in %s", dir)
	}

	importPath := dir // only used as a label by go/doc; doesn't need to resolve
	pkg, err := doc.NewFromFiles(fset, files, importPath)
	if err != nil {
		return nil, fmt.Errorf("go/doc.NewFromFiles(%s): %w", dir, err)
	}

	syms := map[string]bool{}
	for _, f := range pkg.Funcs {
		syms[f.Name] = true
	}
	for _, v := range pkg.Vars {
		syms[v.Names[0]] = true
		for _, n := range v.Names {
			syms[n] = true
		}
	}
	for _, c := range pkg.Consts {
		for _, n := range c.Names {
			syms[n] = true
		}
	}
	for _, t := range pkg.Types {
		syms[t.Name] = true
		for _, f := range t.Funcs { // constructors grouped under the type
			syms[f.Name] = true
		}
		for _, v := range t.Vars {
			for _, n := range v.Names {
				syms[n] = true
			}
		}
		for _, c := range t.Consts {
			for _, n := range c.Names {
				syms[n] = true
			}
		}
		// Note: t.Methods (methods with a receiver) are deliberately NOT
		// added - they are called on values (e.g. `app.Get(...)`), not as
		// package-qualified references (`fh.Get`), so they're out of scope
		// for this check.
	}
	st.cache[dir] = syms
	return syms, nil
}

var qualifiedRefRe = regexp.MustCompile(`\b([a-z][a-zA-Z0-9]*)\.([A-Z]\w*)`)

// CheckFragmentSymbols scans the raw fragment text for <pkg>.<Identifier>
// references where <pkg> is a known package short name (per reg), and
// verifies each Identifier exists as a top-level exported declaration in
// that package's real source (per st). If the same short name resolves to
// more than one real package (e.g. mw/httpsignature and pkg/httpsignature
// both declare `package httpsignature`), the reference is accepted if the
// identifier exists in ANY candidate - disambiguating which one a given
// fragment means would need full type-checking, which is out of scope (see
// package doc comment), so this deliberately trades a little precision for
// not flagging genuinely correct references as broken.
//
// A short name that the fragment itself declares as a local variable or
// parameter (e.g. `admin := app.Group(...)`, later used as `admin.Get(...)`)
// is skipped entirely: that is a method call on a local value, not a
// package-qualified reference, even though it lexically collides with a
// real package name (e.g. mw/admin).
//
// It returns one problem string per unresolved reference; a nil/empty slice
// means every checkable reference resolved (or there were none to check).
func CheckFragmentSymbols(b Block, reg PackageRegistry, st *SymbolTable) []string {
	var problems []string
	seen := map[string]bool{}
	shadowed := locallyDeclaredNames(b.Source)
	for _, m := range qualifiedRefRe.FindAllStringSubmatch(b.Source, -1) {
		alias, ident := m[1], m[2]
		if shadowed[alias] {
			continue // locally declared - a method call on a value, not this package
		}
		candidates, ok := reg[alias]
		if !ok {
			continue // not a package we know how to check (stdlib, local var, etc.)
		}
		key := alias + "." + ident
		if seen[key] {
			continue
		}
		seen[key] = true

		found := false
		var loadErrs []string
		var triedPaths []string
		for _, info := range candidates {
			syms, err := st.Load(info.Dir)
			if err != nil {
				loadErrs = append(loadErrs, fmt.Sprintf("%s: %v", info.ImportPath, err))
				continue
			}
			triedPaths = append(triedPaths, info.ImportPath)
			if syms[ident] {
				found = true
				break
			}
		}
		if found {
			continue
		}
		if len(loadErrs) > 0 && len(triedPaths) == 0 {
			problems = append(problems, fmt.Sprintf("could not load symbols for %s.%s: %s", alias, ident, strings.Join(loadErrs, "; ")))
			continue
		}
		problems = append(problems, fmt.Sprintf("%s.%s: no such exported identifier in %s", alias, ident, strings.Join(triedPaths, " or ")))
	}
	sort.Strings(problems)
	return problems
}

var (
	localAssignRe = regexp.MustCompile(`\b([A-Za-z_]\w*)\s*:=`)
	localVarRe    = regexp.MustCompile(`\bvar\s+([A-Za-z_]\w*)\b`)
)

// locallyDeclaredNames returns every identifier that source declares via a
// short variable declaration (`name := ...`) or a `var name ...` statement,
// so callers can avoid mistaking `name.Field` for a package-qualified
// reference when `name` is actually shadowing a real package's short name.
func locallyDeclaredNames(source string) map[string]bool {
	names := map[string]bool{}
	for _, m := range localAssignRe.FindAllStringSubmatch(source, -1) {
		names[m[1]] = true
	}
	for _, m := range localVarRe.FindAllStringSubmatch(source, -1) {
		names[m[1]] = true
	}
	return names
}
