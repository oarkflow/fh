package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type asset struct {
	SHA256    string `json:"sha256"`
	Integrity string `json:"integrity"`
	Size      int64  `json:"size"`
}

type manifest struct {
	Protocol string           `json:"protocol"`
	Assets   map[string]asset `json:"assets"`
}

func main() {
	dir := flag.String("dir", "wasm/dist", "artifact directory")
	output := flag.String("output", "asset-manifest.json", "manifest filename inside dir")
	flag.Parse()

	names := []string{"securefetch.wasm", "wasm_exec.js", "secure-fetch.js", "storage.js", "index.js"}
	sort.Strings(names)
	out := manifest{Protocol: "fh-secure-transport-v1", Assets: make(map[string]asset, len(names))}
	for _, name := range names {
		path := filepath.Join(*dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			fatalf("read %s: %v", path, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			fatalf("stat %s: %v", path, err)
		}
		sum := sha256.Sum256(data)
		out.Assets[name] = asset{
			SHA256:    hex.EncodeToString(sum[:]),
			Integrity: "sha256-" + base64.StdEncoding.EncodeToString(sum[:]),
			Size:      info.Size(),
		}
	}
	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fatalf("encode manifest: %v", err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(*dir, *output)
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		fatalf("write %s: %v", path, err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fh wasm manifest: "+format+"\n", args...)
	os.Exit(1)
}
