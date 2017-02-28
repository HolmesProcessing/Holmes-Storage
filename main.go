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

	if setup {
		err := ctx.Data.Setup()
		if err == nil {
			ctx.Info.Println("Setup complete, no errors")
		} else {
			ctx.Info.Panic("Setup couldn't finish without errors: " + err.Error())
		}
	}

	if objSetup {
		err := ctx.Objects.Setup()
		if err == nil {
			ctx.Info.Println("objSetup complete, no errors")
		} else {
			ctx.Info.Panic("objSetup couldn't finish without errors: " + err.Error())
		}
	}

	ctx.Info.Println("Initialization complete")

	go http.Start(ctx)
	amqp.Start(ctx)
}
