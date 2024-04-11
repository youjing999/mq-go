package main

import (
	"bytes"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // 队列名称
		true,         // 是否持久化
		false,        // 是否自动删除
		false,        // 消息是否共享
		false,        // 是否等待
		nil,          // 其他参数
	)
	failOnError(err, "Failed to declare a queue")

	/**
	ch.Qos(1, 0, false)
	prefetch count: 这个参数指定可以从消息代理预取的消息数量。在这个例子中，预取数量被设置为1，这意味着消费者会预先从消息代理那里接收一条消息。
	prefetch size: 这个参数指定可以预取的消息的最大字节大小。在这个例子中，它被设置为0，这意味着不会根据消息的大小来限制预取。
	global: 这个参数指定是否所有的通道都使用这个预取设置。在这个例子中，它被设置为false，这意味着这个预取设置只对当前的通道有效。
	*/
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	failOnError(err, "Failed to register a consumer")
	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
			/**
			d.Ack(false) 来确认这条消息已经被成功接收并处理。
			这样做的好处是，如果消息在消费者端未被正确处理，
			消息代理可以知道这一点，并可能将其重新发送给其他消费者。

			如果为 false，则表示消息处理成功，并且不会再次发送。
			如果为 true，则表示消息处理可能会失败，并且消息代理可以在失败时尝试将其发送给其他消费者。
			*/
			err := d.Ack(false)
			if err != nil {
				panic(err)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
