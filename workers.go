package goworker

import (
	"encoding/json"
	"fmt"
)

var (
	workers map[string]workerFunc
)

func init() {
	workers = make(map[string]workerFunc)
}

// Register registers a goworker worker function. Class
// refers to the Ruby name of the class which enqueues the
// job. Worker is a function which accepts a queue and an
// arbitrary array of interfaces as arguments.
func Register(class string, worker workerFunc) {
	workers[class] = worker
}

func Enqueue(job *Job) error {
	err := Init()
	if err != nil {
		return err
	}

	conn, err := GetConn()
	if err != nil {
		//		logger.Criticalf("Error on getting connection on enqueue")
		logger.Errorf("Error on getting connection on enqueue")
		return err
	}
	//	defer PutConn(conn)

	buffer, err := json.Marshal(job.Payload)
	if err != nil {
		logger.Criticalf("Cant marshal payload on enqueue")
		PutConn(conn)
		return err
	}

	err = conn.Send("RPUSH", fmt.Sprintf("%squeue:%s", workerSettings.Namespace, job.Queue), buffer)
	if err != nil {
		//		logger.Criticalf("Cant push to queue")
		logger.Errorf("Cant push to queue")
		conn.Close()
		PutConn(nil)
		return err
	}

	err = conn.Send("SADD", fmt.Sprintf("%squeues", workerSettings.Namespace), job.Queue)
	if err != nil {
		//		logger.Criticalf("Cant register queue to list of use queues")
		logger.Errorf("Cant register queue to list of use queues")
		conn.Close()
		PutConn(nil)
		return err
	}

	err = conn.Flush()
	if err != nil {
		conn.Close()
		PutConn(nil)
		return err
	}
	PutConn(conn)
	return nil
}
