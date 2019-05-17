package goworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

type poller struct {
	process
	isStrict bool
}

func newPoller(queues []*Queue, isStrict bool) (*poller, error) {
	process, err := newProcess("poller", queues)
	if err != nil {
		return nil, err
	}
	return &poller{
		process:  *process,
		isStrict: isStrict,
	}, nil
}

func (p *poller) getJob() ([]*Job, error) {
	var jobs []*Job
	for _, queue := range p.queues(p.isStrict) {
		var perNum int

		if queue.MaxRunningNum >= workerSettings.Concurrency {
			perNum = queue.PerFetchNum
		} else {
			perNum = queue.MaxRunningNum - int(atomic.LoadInt64(&queue.runningNum))
			if perNum <= 0 {
				continue
			}
			if queue.PerFetchNum < perNum {
				perNum = queue.PerFetchNum
			}
		}

		var replyList [][]byte
		var k = fmt.Sprintf("%squeue:%s", workerSettings.Namespace, queue.Name)

		if perNum > 1 {
			pipe := redisClient.Pipeline()
			for i := 0; i < perNum; i++ {
				pipe.LPop(k)
			}

			result, err := pipe.Exec()
			if err != nil && err != redis.Nil {
				return nil, err
			}

			for _, r := range result {
				reply, err := r.(*redis.StringCmd).Bytes()
				if err != nil && err != redis.Nil {
					log.Println(err)
					continue
				}

				if len(reply) == 0 {
					continue
				}

				replyList = append(replyList, reply)
			}
		} else {
			reply, err := redisClient.LPop(k).Bytes()
			if err != nil && err != redis.Nil {
				return nil, err
			}

			if len(reply) == 0 {
				continue
			}

			replyList = [][]byte{reply}
		}

		for _, reply := range replyList {
			job := &Job{Queue: queue.Name, runningNum: &queue.runningNum}

			decoder := json.NewDecoder(bytes.NewReader(reply))
			if workerSettings.UseNumber {
				decoder.UseNumber()
			}

			if err := decoder.Decode(&job.Payload); err != nil {
				return nil, err
			}

			jobs = append(jobs, job)
		}

		if len(jobs) > 0 {
			return jobs, nil
		}
	}

	return jobs, nil
}

func (p *poller) poll(interval time.Duration, ctx context.Context) <-chan *Job {
	jobs := make(chan *Job)

	go func() {
		defer func() {
			// close channel when last poller exit
			close(jobs)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				jobList, err := p.getJob()
				if err != nil && err != redis.Nil {
					log.Printf("Error on %v getting job from %v: %v", p, p.Queues, err)
					continue
				}

				if len(jobList) > 0 {
					for _, job := range jobList {
						if job != nil {
							select {
							case <-ctx.Done():
								goto pushback
							default:
								select {
								case jobs <- job:
									atomic.AddInt64(job.runningNum, 1)
									continue
								case <-ctx.Done():
									goto pushback
								}
							}
						pushback:
							buf, err := json.Marshal(job.Payload)
							if err != nil {
								log.Printf("Error requeueing %v: %v", job, err)
							}

							err = redisClient.LPush(fmt.Sprintf("%squeue:%s", workerSettings.Namespace, job.Queue), buf).Err()
							if err != nil {
								log.Printf("Error requeueing %v: %v", job, err)
							}
						}
					}
				} else {
					if workerSettings.ExitOnComplete {
						return
					}

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
	return jobs
}
