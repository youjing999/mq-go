package main

import (
	"context"
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
	channel, err := connection.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()

	//设置队列
	queue, err := channel.QueueDeclare(
		"simple", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//发送消息
	msg := "rabbitmq simple msg"
	err = channel.PublishWithContext(
		context.Background(),
		"", queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", msg)
}
