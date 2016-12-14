package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/HolmesProcessing/Holmes-Storage/amqp"
	"github.com/HolmesProcessing/Holmes-Storage/context"
	"github.com/HolmesProcessing/Holmes-Storage/http"
)

func main() {
	var (
		setup    bool
		objSetup bool
		confPath string
	)

	flag.BoolVar(&setup, "setup", false, "Setup the Database")
	flag.BoolVar(&objSetup, "objSetup", false, "Setup the object storage")
	flag.StringVar(&confPath, "config", "", "Path to the config file")
	flag.Parse()

	// load config
	if confPath == "" {
		confPath, _ = filepath.Abs(filepath.Dir(os.Args[0]))
		confPath += "/config/storage.conf"
	}

	ctx := &context.Ctx{}
	ctx.Initialize(confPath)

	ctx.Debug.Println("Initialization finished")

	go http.Start(ctx)
	amqp.Start(ctx)
}
