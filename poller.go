package goworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

type poller struct {
	process
	isStrict    bool
	pollerCount int64
}

func newPoller(queues []string, isStrict bool) (*poller, error) {
	process, err := newProcess("poller", queues)
	if err != nil {
		return nil, err
	}
	return &poller{
		process:  *process,
		isStrict: isStrict,
	}, nil
}

func (p *poller) getJob(conn *RedisConn) (*Job, error) {
	for _, queue := range p.queues(p.isStrict) {
		logger.Debugf("Checking %s", queue)

		reply, err := conn.Do("LPOP", fmt.Sprintf("%squeue:%s", workerSettings.Namespace, queue))
		if err != nil {
			return nil, err
		}
		if reply != nil {
			logger.Debugf("Found job on %s", queue)

			job := &Job{Queue: queue}

			decoder := json.NewDecoder(bytes.NewReader(reply.([]byte)))
			if workerSettings.UseNumber {
				decoder.UseNumber()
			}

			if err := decoder.Decode(&job.Payload); err != nil {
				return nil, err
			}
			return job, nil
		}
	}

	return nil, nil
}

func (p *poller) poll(pollerNum int, interval time.Duration, ctx context.Context) <-chan *Job {
	jobs := make(chan *Job)

	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection in poller %s: %v", p, err)
		close(jobs)
		return jobs
	} else {
		p.open(conn)
		p.start(conn)
		PutConn(conn)
	}

	p.pollerCount = int64(pollerNum)

	for i := 0; i < pollerNum; i++ {
		go func() {
			defer func() {
				if atomic.AddInt64(&p.pollerCount, -1) == 0 {
					close(jobs)
				}
				conn, err := GetConn()
				if err != nil {
					logger.Criticalf("Error on getting connection in poller %s: %v", p, err)
					return
				} else {
					p.finish(conn)
					p.close(conn)
					PutConn(conn)
				}
			}()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					conn, err := GetConn()
					if err != nil {
						logger.Criticalf("Error on getting connection in poller %s: %v", p, err)
						return
					}

					job, err := p.getJob(conn)
					if err != nil {
						logger.Criticalf("Error on %v getting job from %v: %v", p, p.Queues, err)
						PutConn(conn)
						return
					}
					if job != nil {
						conn.Send("INCR", fmt.Sprintf("%sstat:processed:%v", workerSettings.Namespace, p))
						conn.Flush()
						PutConn(conn)
						select {
						case jobs <- job:
						case <-ctx.Done():
							buf, err := json.Marshal(job.Payload)
							if err != nil {
								logger.Criticalf("Error requeueing %v: %v", job, err)
								return
							}
							conn, err := GetConn()
							if err != nil {
								logger.Criticalf("Error on getting connection in poller %s: %v", p, err)
								return
							}

							conn.Send("LPUSH", fmt.Sprintf("%squeue:%s", workerSettings.Namespace, job.Queue), buf)
							conn.Flush()
							PutConn(conn)
							return
						}
					} else {
						PutConn(conn)
						if workerSettings.ExitOnComplete {
							return
						}
						logger.Debugf("Sleeping for %v", interval)
						logger.Debugf("Waiting for %v", p.Queues)

						timeout := time.After(interval)
						select {
						case <-ctx.Done():
							return
						case <-timeout:
						}
					}
				}
			}
		}()
	}
	return jobs
}
