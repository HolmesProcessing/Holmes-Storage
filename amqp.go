package main

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"

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

func initAMQP(connect, queue, routingKey string, prefetchCount int) {
	// listen on AMQP queue
	amqpConn, err := amqp.Dial(connect)
	if err != nil {
		warning.Panicln("Contacting the AMQP server failed with", err.Error())
	}

	channel, err := amqpConn.Channel()
	if err != nil {
		warning.Panicln("Initializing AMQP channel failed with", err.Error())
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
		warning.Panicln("Declaring queue failed with", err.Error())
	}

	err = channel.Qos(
		prefetchCount, // prefetch count
		0,             // prefetch size
		false,         // global
	)
	if err != nil {
		warning.Panicln("Setting QoS failed with", err.Error())
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
		warning.Panicln("Channel consume failed with", err.Error())
	}

	forever := make(chan bool)
	go func() {
		for m := range msgs {
			info.Println("Received new message")
			parseMessage(m)
		}
	}()
	<-forever
}

func parseMessage(msg amqp.Delivery) {
	debug.Println("Msg:", string(msg.Body))

	m := &totemResult{}
	err := json.Unmarshal(msg.Body, m)
	if err != nil {
		warning.Printf("Could not decode msg: %s\n", msg.Body)
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

	result := &storerGeneric.Result{
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
		Results:           m.Data,
		Tags:              m.Tags,
		StartedDateTime:   time.Now(),
		FinishedDateTime:  time.Now(),
		WatchguardStatus:  "NotImplemented",
		WatchguardLog:     []string{"NotImplemented"},
		WatchguardVersion: "NotImplemented",
	}

	err = mainStorer.StoreResult(result)
	if err != nil {
		if strings.Contains(err.Error(), "Size must be between 0 and 16793600") {
			warning.Println("Message to large, dropped!", err.Error())
			msg.Ack(false)
			return
		}

		warning.Println("Failed to safe result:", err.Error(), "SHA256:", m.SHA256)
		msg.Nack(false, true)
		return
	}

	debug.Println("Msg saved successfully!")
	msg.Ack(false)
}
