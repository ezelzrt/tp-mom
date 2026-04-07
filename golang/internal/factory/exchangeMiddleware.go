package factory

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	keys         []string
	state        consumerState
	consumerTag  string
	mu           sync.Mutex
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	em.mu.Lock()
	if em.state == closed {
		em.mu.Unlock()
		return m.ErrMessageMiddlewareMessage
	}
	if em.state == consuming {
		em.mu.Unlock()
		return nil
	}
	em.mu.Unlock()

	if len(em.keys) == 0 {
		return m.ErrMessageMiddlewareMessage
	}

	q, err := em.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	for _, key := range em.keys {
		err = em.channel.QueueBind(
			q.Name,
			key,
			em.exchangeName,
			false,
			nil,
		)
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	queueName := q.Name
	tag := queueName + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	msgs, err := em.channel.Consume(
		q.Name,
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

	em.mu.Lock()
	em.consumerTag = tag
	em.state = consuming
	em.mu.Unlock()

	for d := range msgs {
		callbackFunc(m.Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
	}

	em.mu.Lock()
	defer em.mu.Unlock()
	if em.state == consuming {
		em.state = closed
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	em.mu.Lock()
	if em.state != consuming {
		em.mu.Unlock()
		return nil
	}
	consumerTag := em.consumerTag
	em.mu.Unlock()

	err := em.channel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	em.mu.Lock()
	em.state = idle
	em.consumerTag = ""
	em.mu.Unlock()

	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	em.mu.Lock()
	if em.state == closed {
		em.mu.Unlock()
		return m.ErrMessageMiddlewareMessage
	}
	em.mu.Unlock()

	if len(em.keys) == 0 {
		return m.ErrMessageMiddlewareMessage
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range em.keys {
		err := em.channel.PublishWithContext(ctx,
			em.exchangeName,
			key,   // routing key
			false, // mandatory
			false, // immediate
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
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	errStop := em.StopConsuming()
	errChannel := em.channel.Close()
	errConn := em.conn.Close()

	em.mu.Lock()
	em.state = closed
	em.consumerTag = ""
	em.mu.Unlock()

	if errStop != nil || errChannel != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
