package factory

type consumerState int

const (
	idle consumerState = iota
	consuming
	closed
)
