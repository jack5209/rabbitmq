package main

import (
	"flag"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var amqpUri = flag.String("r", "amqp://admin:onion345@10.8.8.14:5677/", "RabbitMQ URI")

var (
	rabbitConn       *amqp.Connection
	rabbitCloseError chan *amqp.Error
)

// Try to connect to the RabbitMQ server as
// long as it takes to establish a connection
//
func connectToRabbitMQ(uri string) *amqp.Connection {
	for {
		conn, err := amqp.Dial(uri)

		if err == nil {
			return conn
		}

		log.Println(err)
		log.Printf("Trying to reconnect to RabbitMQ at %s\n", uri)
		time.Sleep(500 * time.Millisecond)
	}
}

// re-establish the connection to RabbitMQ in case
// the connection has died
//
func rabbitConnector(uri string) {
	var rabbitErr *amqp.Error

	for {
		rabbitErr = <-rabbitCloseError
		if rabbitErr != nil {
			log.Printf("Connecting to %s\n", *amqpUri)

			rabbitConn = connectToRabbitMQ(uri)
			rabbitCloseError = make(chan *amqp.Error)
			rabbitConn.NotifyClose(rabbitCloseError)

			// run your setup process here
		}
	}
}

func main() {
	flag.Parse()

	// create the rabbitmq error channel
	rabbitCloseError = make(chan *amqp.Error)

	// run the callback in a separate thread
	go rabbitConnector(*amqpUri)

	// establish the rabbitmq connection by sending
	// an error and thus calling the error callback
	rabbitCloseError <- amqp.ErrClosed

	log.Printf("sleep 3")
	time.Sleep(3 * time.Second)
	rabbitCloseError <- amqp.ErrClosed

	forever := make(chan bool)
	log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
