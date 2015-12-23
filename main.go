package main

import (
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type config struct {
	Storage       string
	Database      []*dbConnector
	AMQP          string
	Queue         string
	RoutingKey    string
	PrefetchCount int
	LogFile       string
	LogLevel      string
}

type dbConnector struct {
	IP       string
	Port     int
	User     string
	Password string
	Database string
}

type Storer interface {
	// Initializes the connection and the Storer object
	// ip, port, user, passwort, db name
	Initialize([]*dbConnector) (Storer, error)

	// Is called to setup the db on the very first run
	// to create initial collections (if necessary)
	Setup() error

	// Stores a result in the database
	// (TODO: return generated Id)
	StoreResult(*dbResults) error

	// Gets a result by Id from the database
	GetResult(string) (*dbResults, error)
}

var (
	myStorer Storer
	debug    *log.Logger
	info     *log.Logger
	warning  *log.Logger
)

func main() {
	var (
		setup    bool
		confPath string
		err      error
	)

	// setup basic logging to stdout
	initLogging("", "debug")

	// load config
	flag.BoolVar(&setup, "setup", false, "Setup the Database")
	flag.StringVar(&confPath, "config", "", "Path to the config file")
	flag.Parse()

	if confPath == "" {
		confPath, _ = filepath.Abs(filepath.Dir(os.Args[0]))
		confPath += "/config.json"
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
		myStorer = &storerMongoDB{}
	//case "cassandra":
	//	myStorer = &storerCassandra{}
	//case "mysql":
	//	myStorer = &storerMySQL{}
	default:
		warning.Panicln("Please supply a storage engine via the storage cmd flag!")
	}

	myStorer, err = myStorer.Initialize(conf.Database)
	if err != nil {
		warning.Panicln("Storer initialization failed!", err.Error())
	}
	info.Println("Storage engine loaded:", conf.Storage)

	// check if the user only wants to
	// initialize the databse.
	if setup {
		err = myStorer.Setup()
		if err != nil {
			warning.Panicln("Storer setup failed!", err.Error())
		}

		info.Println("Database was setup without errors.")
		return // we don't want to execute this any further
	}

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
