// Command apicheck reports removed or changed exported Go declarations relative
// to a Git revision. It is a conservative source API gate for stable releases.
package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	baseline := flag.String("baseline", "", "Git revision to compare against")
	flag.Parse()
	if *baseline == "" {
		fmt.Fprintln(os.Stderr, "usage: go run ./cmd/apicheck -baseline <git-ref>")
		os.Exit(2)
	}
	oldFiles, err := filesAtRef(*baseline)
	if err != nil {
		fatal(err)
	}
	newFiles, err := filesOnDisk(".")
	if err != nil {
		fatal(err)
	}
	oldAPI, err := declarations(oldFiles)
	if err != nil {
		fatal(err)
	}
	newAPI, err := declarations(newFiles)
	if err != nil {
		fatal(err)
	}
	var changes []string
	for name, old := range oldAPI {
		current, exists := newAPI[name]
		if !exists {
			changes = append(changes, "removed: "+name)
		} else if current != old {
			changes = append(changes, "changed: "+name+"\n  before: "+old+"\n  after:  "+current)
		}
	}
	sort.Strings(changes)
	if len(changes) > 0 {
		for _, change := range changes {
			fmt.Println(change)
		}
		os.Exit(1)
	}
	fmt.Printf("API compatible with %s (%d exported declarations checked)\n", *baseline, len(oldAPI))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}

func filesAtRef(ref string) (map[string][]byte, error) {
	cmd := exec.Command("git", "archive", "--format=tar", ref)
	archive, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git archive %q: %w", ref, err)
	}
	files := make(map[string][]byte)
	r := tar.NewReader(bytes.NewReader(archive))
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		if h.Name == "go.mod" || strings.HasSuffix(h.Name, "/go.mod") {
			files[h.Name] = nil
			continue
		}
		if isSource(h.Name) {
			data, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			files[h.Name] = data
		}
	}
	return files, nil
}

func filesOnDisk(root string) (map[string][]byte, error) {
	files := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == ".gotoolchain" {
				return filepath.SkipDir
			}
			return nil
		}
		name := strings.TrimPrefix(filepath.ToSlash(path), "./")
		if name == "go.mod" || strings.HasSuffix(name, "/go.mod") {
			files[name] = nil
			return nil
		}
		if isSource(name) {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files[name] = data
		}
		return nil
	})
	return files, err
}

func isSource(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

func declarations(files map[string][]byte) (map[string]string, error) {
	seen := make(map[string]map[string]struct{})
	add := func(key, signature string) {
		if seen[key] == nil {
			seen[key] = make(map[string]struct{})
		}
		seen[key][signature] = struct{}{}
	}
	for name, data := range files {
		if !isSource(name) || nestedModule(files, name) {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, data, 0)
		if err != nil {
			return nil, err
		}
		pkg := filepath.ToSlash(filepath.Dir(name))
		if pkg == "." {
			pkg = ""
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				key := pkg + "." + d.Name.Name
				if d.Recv != nil {
					receiver := receiverName(d.Recv.List[0].Type)
					if !ast.IsExported(receiver) {
						continue
					}
					key = pkg + "." + receiver + "." + d.Name.Name
				}
				signature := render(publicFuncType(d.Type))
				if d.Recv != nil {
					signature = render(d.Recv.List[0].Type) + " " + signature
				}
				add(key, signature)
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							signature := "type " + render(publicType(s.Type))
							if s.Assign.IsValid() {
								signature = "alias " + render(publicType(s.Type))
							}
							add(pkg+"."+s.Name.Name, signature)
						}
					case *ast.ValueSpec:
						for i, id := range s.Names {
							if !id.IsExported() {
								continue
							}
							value := d.Tok.String()
							if s.Type != nil {
								value += " " + render(s.Type)
							}
							if d.Tok == token.CONST && i < len(s.Values) {
								value += " = " + render(s.Values[i])
							}
							add(pkg+"."+id.Name, value)
						}
					}
				}
			}
		}
	}
	api := make(map[string]string, len(seen))
	for key, signatures := range seen {
		list := make([]string, 0, len(signatures))
		for signature := range signatures {
			list = append(list, signature)
		}
		sort.Strings(list)
		api[key] = strings.Join(list, "\n---\n")
	}
	return api, nil
}

func nestedModule(files map[string][]byte, name string) bool {
	for dir := filepath.ToSlash(filepath.Dir(name)); dir != "."; dir = filepath.ToSlash(filepath.Dir(dir)) {
		if _, ok := files[dir+"/go.mod"]; ok {
			return true
		}
	}
	return false
}

func receiverName(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.StarExpr:
		return receiverName(x.X)
	case *ast.IndexExpr:
		return receiverName(x.X)
	case *ast.IndexListExpr:
		return receiverName(x.X)
	case *ast.SelectorExpr:
		return x.Sel.Name
	}
	return ""
}

func publicType(expr ast.Expr) ast.Expr {
	structure, ok := expr.(*ast.StructType)
	if !ok {
		return expr
	}
	filtered := &ast.FieldList{}
	for _, field := range structure.Fields.List {
		if len(field.Names) == 0 {
			if ast.IsExported(receiverName(field.Type)) {
				filtered.List = append(filtered.List, field)
			}
			continue
		}
		var names []*ast.Ident
		for _, name := range field.Names {
			if name.IsExported() {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			copy := *field
			copy.Names = names
			filtered.List = append(filtered.List, &copy)
		}
	}
	return &ast.StructType{Fields: filtered}
}

func publicFuncType(fn *ast.FuncType) *ast.FuncType {
	copy := *fn
	copy.Params = unnamedFields(fn.Params)
	copy.Results = unnamedFields(fn.Results)
	return &copy
}

func unnamedFields(fields *ast.FieldList) *ast.FieldList {
	if fields == nil {
		return nil
	}
	result := &ast.FieldList{List: make([]*ast.Field, 0, len(fields.List))}
	for _, field := range fields.List {
		count := len(field.Names)
		if count == 0 {
			count = 1
		}
		for range count {
			result.List = append(result.List, &ast.Field{Type: field.Type})
		}
	}
	return result
}

func render(node ast.Node) string {
	var out bytes.Buffer
	_ = format.Node(&out, token.NewFileSet(), node)
	return out.String()
}
