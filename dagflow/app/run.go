package app

import (
	"context"
	"log"
	"os"

	"dagflow/pkg/dagflow"
)

func Run() {
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		if err := dagflow.RunCLI(os.Args[1:], dagflow.DefaultBCLPath(), RegisterExampleHandlers); err != nil {
			log.Fatal(err)
		}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dagflow.RunServer(ctx, dagflow.ServerOptions{BCLPath: dagflow.DefaultBCLPath(), Register: RegisterExampleHandlers}); err != nil {
		log.Fatal(err)
	}
}
