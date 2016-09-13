package main

import (
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/HolmesProcessing/Holmes-Storage/objStorerGeneric"
	"github.com/HolmesProcessing/Holmes-Storage/objStorerS3"
	"github.com/HolmesProcessing/Holmes-Storage/objStorerLocalFS"
	"github.com/HolmesProcessing/Holmes-Storage/storerCassandra"
	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"
	"github.com/HolmesProcessing/Holmes-Storage/storerMongoDB"
)

type config struct {
	Storage     string
	Database    []*storerGeneric.DBConnector
	ObjStorage  string
	ObjDatabase []*objStorerGeneric.ObjDBConnector
	LogFile     string
	LogLevel    string

	AMQP          string
	Queue         string
	RoutingKey    string
	PrefetchCount int

	HTTP         string
	ExtendedMime bool
}

var (
	mainStorer storerGeneric.Storer
	objStorer  objStorerGeneric.ObjStorer
	debug      *log.Logger
	info       *log.Logger
	warning    *log.Logger
)

func main() {
	var (
		setup    bool
		objSetup bool
		confPath string
		err      error
	)

	// setup basic logging to stdout
	initLogging("", "debug")

	// load config
	flag.BoolVar(&setup, "setup", false, "Setup the Database")
	flag.BoolVar(&objSetup, "objSetup", false, "Setup the object storage")
	flag.StringVar(&confPath, "config", "", "Path to the config file")
	flag.Parse()

	if confPath == "" {
		confPath, _ = filepath.Abs(filepath.Dir(os.Args[0]))
		confPath += "/config/storage.conf"
	}

	conf := &config{}
	cfile, _ := os.Open(confPath)
	if err = json.NewDecoder(cfile).Decode(&conf); err != nil {
		warning.Panicln("Couldn't decode config file without errors!", err.Error())
	}

	// reload logging with parameters from config
	initLogging(conf.LogFile, conf.LogLevel)

	// initialize storage
	switch conf.Storage {
	case "mongodb":
		mainStorer = &storerMongoDB.StorerMongoDB{}
	case "cassandra":
		mainStorer = &StorerCassandra.StorerCassandra{}
	//case "mysql":
	//	mainStorer = &storerMySQL{}
	default:
		warning.Panicln("Please supply a valid storage engine!")
	}

	if setup {
		// Create the DB.
		err := mainStorer.CreateDB(conf.Database)
		if err != nil {
			warning.Println("Storer setup failed!", err.Error())
		}
	}

	mainStorer, err = mainStorer.Initialize(conf.Database)
	if err != nil {
		warning.Panicln("Storer initialization failed!", err.Error())
	}
	info.Println("Storage engine loaded:", conf.Storage)

	// initialize object storage
	switch conf.ObjStorage {
	case "S3":
		objStorer = &ObjStorerS3.ObjStorerS3{}
	case "local-fs":
		objStorer = &ObjStorerLocalFS.ObjStorerLocalFS{}
	default:
		warning.Panicln("Please supply a valid object storage engine!")
	}

	objStorer, err = objStorer.Initialize(conf.ObjDatabase)
	if err != nil {
		warning.Panicln("Object storer initialization failed!", err.Error())
	}
	info.Println("Object storage engine loaded:", conf.ObjStorage)

	// check if the user only wants to
	// initialize the databse.
	if setup {
		err = mainStorer.Setup()
		if err != nil {
			warning.Panicln("Storer setup failed!", err.Error())
		}

		info.Println("Database was setup without errors.")
	}

	if objSetup {
		err = objStorer.Setup()
		if err != nil {
			warning.Panicln("Object storer setup failed!", err.Error())
		}

		info.Println("Object storage was setup without errors.")
	}
	
	if setup || objSetup {
		return // we don't want to execute this any further
	}

	// start webserver for HTTP API
	go initHTTP(conf.HTTP, conf.ExtendedMime)

	// start to listen for new restults
	initAMQP(conf.AMQP, conf.Queue, conf.RoutingKey, conf.PrefetchCount)
}

// initLogging sets up the three global loggers warning, info and debug
func initLogging(file, level string) {
	// default: only log to stdout
	handler := io.MultiWriter(os.Stdout)

	if file != "" {
		// log to file
		if _, err := os.Stat(file); os.IsNotExist(err) {
			err := ioutil.WriteFile(file, []byte(""), 0600)
			if err != nil {
				panic("Couldn't create the log!")
			}
		}

		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic("Failed to open log file!")
		}

		handler = io.MultiWriter(f, os.Stdout)
	}

	// TODO: make this nicer....
	empty := io.MultiWriter()
	if level == "warning" {
		warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		info = log.New(empty, "INFO: ", log.Ldate|log.Ltime)
		debug = log.New(empty, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else if level == "info" {
		warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		info = log.New(handler, "INFO: ", log.Ldate|log.Ltime)
		debug = log.New(empty, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		info = log.New(handler, "INFO: ", log.Ldate|log.Ltime)
		debug = log.New(handler, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	}
}
