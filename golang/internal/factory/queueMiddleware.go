package factory

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type consumerState int

const (
	idle consumerState = iota
	consuming
	closed
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	state       consumerState
	consumerTag string
	mu          sync.Mutex
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	qm.mu.Lock()
	if qm.state == closed {
		qm.mu.Unlock()
		return m.ErrMessageMiddlewareMessage
	}
	if qm.state == consuming {
		qm.mu.Unlock()
		return nil
	}
	qm.mu.Unlock()

	err := qm.channel.Qos(1, 0, false)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	queueName := qm.queue.Name
	tag := queueName + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	msg, err := qm.channel.Consume(
		queueName,
		tag,   // consumer tag
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	qm.mu.Lock()
	qm.consumerTag = tag
	qm.state = consuming
	qm.mu.Unlock()

	log.Printf("Started consuming from queue: %s with tag: %s", queueName, tag)
	for d := range msg {
		callbackFunc(m.Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
	}
	log.Printf("Stopped consuming from queue: %s with tag: %s", queueName, tag)
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.state == consuming {
		qm.state = closed
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	qm.mu.Lock()
	if qm.state != consuming {
		qm.mu.Unlock()
		return nil
	} 
	consumerTag := qm.consumerTag
	qm.mu.Unlock()

	err := qm.channel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	qm.mu.Lock()
	qm.state = idle
	qm.consumerTag = ""
	qm.mu.Unlock()
	log.Printf("Stopped consuming from queue: %s with tag: %s", qm.queue.Name, consumerTag)

	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) error {

	qm.mu.Lock()
	if qm.state == closed {
		qm.mu.Unlock()
		return m.ErrMessageMiddlewareMessage
	}
	qm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := qm.channel.PublishWithContext(
		ctx,
		"",
		qm.queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	log.Printf("Sent message to queue %s: %s", qm.queue.Name, msg.Body)
	return nil
}

func (qm *QueueMiddleware) Close() error {

	errStop := qm.StopConsuming()
	errChannel := qm.channel.Close()
	errConn := qm.conn.Close()

	qm.mu.Lock()
	qm.state = closed
	qm.consumerTag = ""
	qm.mu.Unlock()

	log.Printf("Queue closed: %s", qm.queue.Name)
	if errStop != nil || errChannel != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
