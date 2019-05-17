package goworker

type Job struct {
	Queue      string
	runningNum *int64
	Payload    Payload
}
