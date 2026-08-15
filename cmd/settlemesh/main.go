// Command settlemesh runs the SettleMesh reconciliation service. With no
// arguments it starts the HTTP API; subcommands drive the CLI.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/settlemesh/settlemesh/internal/api"
	"github.com/settlemesh/settlemesh/internal/cli"
	"github.com/settlemesh/settlemesh/internal/service"
	"github.com/settlemesh/settlemesh/internal/store"
)

func main() {
	// If the first arg looks like a subcommand, run the CLI; otherwise serve.
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		os.Exit(cli.New(service.New(store.New(time.Now))).Run(os.Args))
	}

	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	svc := service.New(store.New(time.Now))
	srv := api.New(svc)
	fmt.Fprintf(os.Stderr, "settlemesh: listening on %s\n", *addr)
	if err := srv.Listen(*addr); err != nil {
		fmt.Fprintf(os.Stderr, "settlemesh: server error: %v\n", err)
		os.Exit(1)
	}
}
