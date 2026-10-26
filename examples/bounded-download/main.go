package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/oarkflow/fh"
)

func run(url, output, mode string, limit int64) error {
	if limit <= 0 {
		return errors.New("limit must be positive")
	}
	if mode != "atomic" && mode != "writer" && mode != "reader" {
		return fmt.Errorf("unsupported mode %q: use atomic, writer, or reader", mode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := fh.NewClient(fh.ClientConfig{ResponseBodyLimit: limit})
	defer client.Close()
	response, err := client.R().Stream().Get(ctx, url)
	if err != nil {
		return err
	}
	defer response.Close()
	if !response.IsSuccess() {
		return fmt.Errorf("download returned HTTP %d", response.StatusCode())
	}

	switch mode {
	case "atomic":
		if err := response.SaveAtomic(output, 0644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "saved %s\n", output)
	case "writer":
		n, err := response.DownloadTo(os.Stdout)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "downloaded %d bytes\n", n)
	case "reader":
		n, err := io.Copy(os.Stdout, response.Reader())
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "read %d bytes\n", n)
	}
	return nil
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8085/sample", "URL to download")
	output := flag.String("out", "sample.bin", "atomic download destination")
	mode := flag.String("mode", "atomic", "atomic, writer, or reader")
	limit := flag.Int64("limit", 128<<10, "maximum response body size in bytes")
	flag.Parse()
	if err := run(*url, *output, *mode, *limit); err != nil {
		log.Fatal(err)
	}
}
