// 使用原生死信队列
// 优点：使用死信队列套死信队列, 可以突破2^32-1毫秒的官方插件限制
// 缺点：一个消息比在同一队列中的其他消息提前过期，提前过期的消息也不会优先进入死信队列，前一条消息会阻塞后一条消息

// 官方插件
// 缺点：延时时长不能超过2^32-1毫秒, 大约49天.
// 优点：不会出现因为前一条消息没有消费, 导致后面的消息阻塞的情况
// Do not support RAM node

// rabbitmq / amqp direct reply to and return notification
// https://stackoverflow.com/questions/59054511/rabbitmq-amqp-direct-reply-to-and-return-notification

package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/streadway/amqp"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	log.Println("Dialing")
	conn, err := amqp.Dial("amqp://admin:onion345@10.8.8.14:5677/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	closeCh := make(chan error)
	go func() {
		log.Printf("...Waits here for the channel to be closed")

		// Waits here for the channel to be closed
		log.Printf("closing: %#v", <-conn.NotifyClose(make(chan *amqp.Error)))
		log.Printf("...time to reconnect")

		closeCh <- errors.New("Channel Closed")
		// Let Handle know it's not time to reconnect
		// c.done <- errors.New("Channel Closed")
	}()

	log.Println("Got connection, getting channel")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	log.Printf("Enabling publishing confirms.")
	err = ch.Confirm(false)
	failOnError(err, "Channel could not be put into confirm mode")

	returnCh := make(chan amqp.Return)
	ch.NotifyReturn(returnCh)

	var ack = make(chan uint64)
	var nack = make(chan uint64)
	ch.NotifyConfirm(ack, nack)

	err = ch.ExchangeDeclare(
		"jack_dlx_1",
		"fanout",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 声明一个延时队列,我们的延时消息就是要发送到这里
	delayQ, err := ch.QueueDeclare(
		"jack_q_delay_1", // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		amqp.Table{
			// 当消息过期时把消息发送到 jack_dlx_1 这个 exchange
			"x-dead-letter-exchange":    "jack_dlx_1", // Exchange used to transfer the message from A to B.
			"x-dead-letter-routing-key": "test_key",   // Name of the queue we want the message transferred to.
			"x-message-ttl":             1000 * 20,    // Delay until the message is transferred in milliseconds.
			"x-expires":                 1000 * 60 * 60,
			"x-queue-mode":              "lazy",
		},
	)
	failOnError(err, "Failed to declare a delay_queue")

	q, err := ch.QueueDeclare(
		"jack_q_1", // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,       // queue name
		"",           // routing key
		"jack_dlx_1", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	for i := 0; i < 5; i++ {
		log.Println("Publishing msg")
		err = ch.Publish(
			"",          // exchange
			delayQ.Name, // routing key
			true,        // mandatory,
			false,       // immediate
			amqp.Publishing{
				ContentType:  "text/plain",
				Body:         []byte("hi jack!" + strconv.Itoa(i)),
				Expiration:   "2000", // 过期时间(毫秒)
				DeliveryMode: 2,
			},
		)
		failOnError(err, "Failed to publish a message")

		log.Printf("waiting for confirmation of one publishing")
		select {
		case returnNotification := <-returnCh:
			log.Fatalf("returnNotification error: %#v", returnNotification)
		case <-ack:
			log.Println("ack")
		case <-nack:
			log.Println("nack")
		}

		// time.Sleep(2 * time.Second)
	}

	// forever := make(chan bool)
	// log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	// <-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
