package factory

import (
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	exchange    amqp.Publishing
	keys        []string
	state       consumerState
	consumerTag string
	mu          sync.Mutex
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return nil
}