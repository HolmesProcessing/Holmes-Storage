package amqp

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"strings"
	"time"

	"github.com/HolmesProcessing/Holmes-Storage/context"
	"github.com/HolmesProcessing/Holmes-Storage/dataStorage"

	"github.com/streadway/amqp"
)

type totemResult struct {
	Filename string   `json:"filename"`
	Data     string   `json:"data"`
	Tags     []string `json:"tags"`
	MD5      string   `json:"md5"`
	SHA1     string   `json:"sha1"`
	SHA256   string   `json:"sha256"`
}

func Start(c *context.Ctx) {
	// listen on AMQP queue
	amqpConn, err := amqp.Dial(c.Config.AMQP)
	if err != nil {
		c.Warning.Panicln("Contacting the AMQP server failed with", err.Error())
	}

	channel, err := amqpConn.Channel()
	if err != nil {
		c.Warning.Panicln("Initializing AMQP channel failed with", err.Error())
	}

	_, err = channel.QueueDeclare(
		c.Config.Queue, // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		c.Warning.Panicln("Declaring queue failed with", err.Error())
	}

	err = channel.Qos(
		c.Config.PrefetchCount, // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		c.Warning.Panicln("Setting QoS failed with", err.Error())
	}

	msgs, err := channel.Consume(
		c.Config.Queue,      // queue
		c.Config.RoutingKey, // consumer
		false,               // auto-ack
		false,               // exclusive
		false,               // no-local
		false,               // no-wait
		nil,                 // args
	)
	if err != nil {
		c.Warning.Panicln("Channel consume failed with", err.Error())
	}

	forever := make(chan bool)
	go func() {
		for m := range msgs {
			c.Info.Println("Received new message")
			handleMessage(c, m)
		}
	}()
	<-forever
}

func handleMessage(c *context.Ctx, msg amqp.Delivery) {
	c.Debug.Println("Msg:", string(msg.Body))

	m := &totemResult{}
	err := json.Unmarshal(msg.Body, m)
	if err != nil {
		c.Warning.Printf("Could not decode msg: %s\n", msg.Body)
		msg.Nack(false, false)
		return
	}

	/*
		// This approach has been revised since the data is now
		// saved as string and not as pure JSON document.

		// since totem sends the results as json encoded string
		// (which contains json) we need to unmarshal data
		// and save it this way.
		var resData interface{}
		err = json.Unmarshal([]byte(m.Data), &resData)
		if err != nil {
			warning.Printf("Could not decode data: %s\n", m.Data)
			msg.Nack(false, false)
			return
		}
	*/

	// TODO: Add validation to received msg
	//m.Validate()

	// TODO: Totem needs to send more data

	// TODO: Totem needs to send hashes lowercase

	// TODO: Totem needs to send execution time

	// compress results using gzip
	var resultsGZ bytes.Buffer
	gz := gzip.NewWriter(&resultsGZ)
	if _, err := gz.Write([]byte(m.Data)); err != nil {
		c.Warning.Println("Failed to compress results (writer):", err.Error(), "SHA256:", m.SHA256)
		msg.Nack(false, true)
		return
	}

	if err := gz.Flush(); err != nil {
		c.Warning.Println("Failed to compress results (flush):", err.Error(), "SHA256:", m.SHA256)
		msg.Nack(false, true)
		return
	}
	if err := gz.Close(); err != nil {
		c.Warning.Println("Failed to compress results (close):", err.Error(), "SHA256:", m.SHA256)
		msg.Nack(false, true)
		return
	}

	result := &dataStorage.Result{
		Id:                "",                        //will be filled by the storage engine
		SHA256:            strings.ToLower(m.SHA256), //totem currently send the hash all upper case
		SchemaVersion:     "1",
		UserId:            "NotSend",
		SourceId:          []string{"NotSend"},
		SourceTag:         []string{"NotSend"},
		ServiceName:       strings.SplitN(msg.RoutingKey, ".", 2)[0],
		ServiceVersion:    "NotSend",
		ServiceConfig:     "NotSend",
		ObjectCategory:    []string{"NotSend"},
		ObjectType:        "sample",
		Results:           resultsGZ.Bytes(),
		Tags:              m.Tags,
		ExecutionTime:     time.Now(),
		WatchguardStatus:  "NotImplemented",
		WatchguardLog:     []string{"NotImplemented"},
		WatchguardVersion: "NotImplemented",
		Comment:           "",
	}

	err = c.Data.ResultStore(result)
	if err != nil {
		c.Warning.Println("Failed to safe result:", err.Error(), "SHA256:", m.SHA256)
		msg.Nack(false, true)
		return
	}

	c.Debug.Println("Msg saved successfully!")
	msg.Ack(false)
}
