package main

/**
consumer
*/
import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// 连接失败，错误处理函数
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
func main() {
	// 连接服务器
	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connection.Close()

	// 打开通道
	ch, err := connection.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 消息队列
	queue, err := ch.QueueDeclare(
		"simple", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 消费消息（接收消息）
	messages, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}
	go func() {
		for d := range messages {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Witing for messages. To exit press CTRL+C")
	<-forever
}
