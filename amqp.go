package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

type totemResult struct {
	Filename string `json:"filename"`
	Data     string `json:"data"`
	MD5      string `json:"md5"`
	SHA1     string `json:"sha1"`
	SHA256   string `json:"sha256"`
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
			debug.Println("Received new message")
			parseMessage(m)
		}
	}()
	<-forever
}

func parseMessage(msg amqp.Delivery) {
	m := &totemResult{}
	err := json.Unmarshal(msg.Body, m)
	if err != nil {
		warning.Printf("Could not decode msg: %s\n", msg.Body)
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
		ServiceName:       strings.SplitN(msg.RoutingKey, ".", 1)[0],
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
		warning.Println("Failed to safe result:", err.Error())
		msg.Nack(false, true)
	}

	debug.Println("Msg saved successfully!")
	msg.Ack(false)
}
