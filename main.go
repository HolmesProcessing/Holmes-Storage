package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/streadway/amqp"
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

type totemResult struct {
	Filename string `json:"filename"`
	Data     string `json:"data"`
	MD5      string `json:"md5"`
	SHA1     string `json:"sha1"`
	SHA256   string `json:"sha256"`
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
)

func main() {
	var (
		setup    bool
		confPath string
		err      error
	)

	// TODO: implement logging

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
		panic("Could not decode config file without errors! " + err.Error())
	}

	// initialize storage
	switch conf.Storage {
	case "mongodb":
		myStorer = &storerMongoDB{}
	//case "cassandra":
	//	myStorer = &storerCassandra{}
	//case "mysql":
	//	myStorer = &storerMySQL{}
	default:
		panic("Please supply a storage engine via the storage cmd flag!")
	}

	myStorer, err = myStorer.Initialize(conf.Database)

	if err != nil {
		panic(err.Error())
	}

	// check if the user only wants to
	// initialize the databse.
	if setup {
		err = myStorer.Setup()
		if err != nil {
			panic(err.Error())
		}

		fmt.Println("Database was setup without errors.")
		return // we don't want to execute this any further
	}

	initAMQP(conf.AMQP, conf.Queue, conf.RoutingKey, conf.PrefetchCount)
}

func initAMQP(connect, queue, routingKey string, prefetchCount int) {
	// listen on AMQP queue
	amqpConn, err := amqp.Dial(connect)
	if err != nil {
		panic(err.Error())
	}

	channel, err := amqpConn.Channel()
	if err != nil {
		panic(err.Error())
	}

	_, err = channel.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		panic(err.Error())
	}

	err = channel.Qos(
		prefetchCount, // prefetch count
		0,             // prefetch size
		false,         // global
	)
	if err != nil {
		panic(err.Error())
	}

	msgs, err := channel.Consume(
		queue,      // queue
		routingKey, // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		panic(err.Error())
	}

	forever := make(chan bool)
	go func() {
		for m := range msgs {
			parseMessage(m)
		}
	}()
	<-forever
}

func parseMessage(msg amqp.Delivery) {
	m := &totemResult{}
	err := json.Unmarshal(msg.Body, m)
	if err != nil {
		fmt.Printf("Could not decode msg: %s\n", msg.Body)
		msg.Nack(false, false)
	}

	// TODO: Add validation to received msg
	//m.Validate()

	// TODO: Totem needs to send more data
	result := &dbResults{
		Id:                "",
		SchemaVersion:     "1",
		UserId:            1,
		SourceId:          1,
		ServiceName:       "NotSend",
		ServiceVersion:    "NotSend",
		ServiceConfig:     "NotSend",
		ObjectCategory:    "NotSend",
		ObjectType:        "sample",
		Results:           m.Data,
		Date:              fmt.Sprintf("%v", time.Now().Format(time.RFC3339)),
		WatchguardStatus:  "NotImplemented",
		WatchguardLog:     []string{"NotImplemented"},
		WatchguardVersion: "NotImplemented",
	}

	err = myStorer.StoreResult(result)
	if err != nil {
		fmt.Println("Failed to safe result:", err.Error())
		msg.Nack(false, true)
	}

	msg.Ack(false)
}
