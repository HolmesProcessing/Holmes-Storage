package context

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"

	data "github.com/HolmesProcessing/Holmes-Storage/dataStorage"
	objects "github.com/HolmesProcessing/Holmes-Storage/objectStorage"
)

type config struct {
	DataStorage   []*data.Connector
	ObjectStorage []*objects.Connector
	LogFile       string
	LogLevel      string

	AMQP          string
	Queue         string
	RoutingKey    string
	PrefetchCount int

	HTTP    string
	SSLCert string
	SSLKey  string
}

type Ctx struct {
	Config *config

	Data    data.Storage
	Objects objects.Storage

	Debug   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
}

func (c *Ctx) Initialize(path string) {
	var err error

	c.Config = &config{}
	cfile, _ := os.Open(path)
	if err = json.NewDecoder(cfile).Decode(&c.Config); err != nil {
		panic("Couldn't decode config file without errors! " + err.Error())
	}

	if len(c.Config.DataStorage) == 0 || len(c.Config.ObjectStorage) == 0 {
		panic("Please supply a data and object storage in the config!")
	}

	c.SetLogging()

	c.SetData()
	err = c.Data.Initialize(c.Config.DataStorage)
	if err != nil {
		panic("Data storage initialization failed! " + err.Error())
	}

	c.SetObjects()
	err = c.Objects.Initialize(c.Config.ObjectStorage)
	if err != nil {
		panic("Object storage initialization failed! " + err.Error())
	}
}

func (c *Ctx) SetData() {
	switch c.Config.DataStorage[0].Engine {
	//case "mongodb":
	//	c.Data = &data.MongoDB{}
	case "Cassandra":
		c.Data = &data.Cassandra{}
	//case "mysql":
	//	mainStorer = &storerMySQL{}
	default:
		panic("Please supply a valid data storage engine!")
	}

	c.Debug.Println("Loaded", c.Config.DataStorage[0].Engine, "as data storage")
}

func (c *Ctx) SetObjects() {
	switch c.Config.ObjectStorage[0].Engine {
	case "S3":
		c.Objects = &objects.S3{}
	//case "local-fs":
	//	c.Objects = &objects.LocalFS{}
	default:
		panic("Please supply a valid object storage engine!")
	}

	c.Debug.Println("Loaded", c.Config.ObjectStorage[0].Engine, "as object storage")
}

func (c *Ctx) SetLogging() {
	// default: only log to stdout
	handler := io.MultiWriter(os.Stdout)

	if c.Config.LogFile != "" {
		// log to file
		if _, err := os.Stat(c.Config.LogFile); os.IsNotExist(err) {
			err := ioutil.WriteFile(c.Config.LogFile, []byte(""), 0600)
			if err != nil {
				panic("Couldn't create the log!")
			}
		}

		f, err := os.OpenFile(c.Config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic("Failed to open log file!")
		}

		handler = io.MultiWriter(f, os.Stdout)
	}

	// TODO: make this nicer....
	empty := io.MultiWriter()
	if c.Config.LogLevel == "warning" {
		c.Warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		c.Info = log.New(empty, "INFO: ", log.Ldate|log.Ltime)
		c.Debug = log.New(empty, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else if c.Config.LogLevel == "info" {
		c.Warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		c.Info = log.New(handler, "INFO: ", log.Ldate|log.Ltime)
		c.Debug = log.New(empty, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		c.Warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		c.Info = log.New(handler, "INFO: ", log.Ldate|log.Ltime)
		c.Debug = log.New(handler, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	}
}
