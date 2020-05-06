package main

import (
	"log"

	"github.com/streadway/amqp"

	"jack.us/rabbit"
)

const uri = "amqp://admin:onion345@10.8.8.14:5677/"

func main() {

	cs := rabbit.NewConsumer(uri)

	// cs := &rabbit.Consumer{
	// 	uri: uri,
	// }

	ch := cs.CreateChannel()
	defer ch.Close()

	// 做为一个通用的框架，那么最基本的要素就是：
	// 暴露合理的API给用户，让他们通过闭包的通讯机制（入参），可以基于框架的基础模版去定制自己的具体模版
	// 这种思路，必然导致基础库需要抽离掉具体的逻辑内容，只保留其骨架，并且还要提供操作骨架所必须的行为，
	// 也就是我们所说的API
	msgs := cs.CreateConsumeChannel(ch)
	// inject custom function to closure function
	cs.Consume(msgs, handle)

	// forever := make(chan bool)
	// log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	// <-forever
}

func handle(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)

		d.Ack(false)
		// time.Sleep(time.Second * 2)
	}
}
