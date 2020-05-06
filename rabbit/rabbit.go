package rabbit

import (
	"errors"
	"log"

	"github.com/streadway/amqp"
)

// TODO:
// interface vs functions as function params

// Consumer .
type Consumer struct {
	uri  string
	done chan error
}

// NewConsumer .
// static method
func NewConsumer(uri string) *Consumer {
	return &Consumer{
		uri: uri,
	}
}

// CreateChannel .
func (c *Consumer) CreateChannel() *amqp.Channel {
	log.Println("connect ...")

	// reconnect
	var conn *amqp.Connection
	for {
		var err error
		conn, err = amqp.Dial(c.uri)
		// conn, err = amqp.Dial("amqp://admin:onion345@10.8.8.14:5677/")
		// failOnError(err, "Failed to connect to RabbitMQ")

		if err == nil {
			log.Printf("connected to RabbitMQ")
			break
		}

		log.Printf("Trying to reconnect to RabbitMQ")
	}
	// defer conn.Close()

	go func() {
		log.Printf("closing: %s", <-conn.NotifyClose(make(chan *amqp.Error)))
		c.done <- errors.New("Channel Closed")
	}()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	// defer ch.Close()

	return ch
}

// CreateConsumeChannel .
// 为了提取代码和隐藏繁琐的细节，使用闭包（函数包装）
// 而外界与闭包进行通信的方式就只能通过入参进行传递，通信的媒介
// 一种是变量，另一种是函数，
// 函数做为参数的意义：把整块逻辑结构做为参数，这样让通信的解耦程度可以做得很灵活
// 变量，一种模版的适配方式
func (c *Consumer) CreateConsumeChannel(ch *amqp.Channel) <-chan amqp.Delivery {
	q, err := ch.QueueDeclare(
		"jack_q_1", // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Fair dispatch
	// You might have noticed that the dispatching still doesn't work exactly as we want.
	// For example in a situation with two workers, when all odd messages are heavy and even messages are light,
	// one worker will be constantly busy and the other one will do hardly any work.
	// Well, RabbitMQ doesn't know anything about that and will still dispatch messages evenly.
	// This happens because RabbitMQ just dispatches a message when the message enters the queue.
	// It doesn't look at the number of unacknowledged messages for a consumer.
	// It just blindly dispatches every n-th message to the n-th consumer.

	// In order to defeat that we can set the prefetch count with the value of 1.
	// This tells RabbitMQ not to give more than one message to a worker at a time.
	// Or, in other words,
	// don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
	// Instead, it will dispatch it to the next worker that is not still busy.
	err = ch.Qos(1, 0, false)
	failOnError(err, "Error setting qos")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // auto-ack
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	return msgs
}

// Consume .
func (c *Consumer) Consume(msgs <-chan amqp.Delivery, fn func(<-chan amqp.Delivery)) {
	for {
		fn(msgs)
		log.Println("...msgs close")

		// Go into reconnect loop when
		// c.done is passed non nil values
		if <-c.done != nil {
			ch := c.CreateChannel()
			msgs = c.CreateConsumeChannel(ch)
		}

		// for d := range msgs {
		// 	log.Printf("Received a message: %s", d.Body)

		// 	time.Sleep(time.Second * 2)
		// }

	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
