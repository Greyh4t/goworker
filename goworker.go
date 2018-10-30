package goworker

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Greyh4t/glog"

	"github.com/go-redis/redis"
)

var (
	logger       *glog.Logger
	redisClient  *redis.Client
	initMutex    sync.Mutex
	initialized  bool
	pollerExited chan int
	pollerCtx    context.Context
	pollerCancel context.CancelFunc
)

var workerSettings WorkerSettings

type WorkerSettings struct {
	QueuesString   string
	Queues         queuesFlag
	Interval       time.Duration
	Concurrency    int
	Poller         int
	URI            string
	Namespace      string
	ExitOnComplete bool
	IsStrict       bool
	UseNumber      bool
	// redis相关配置
	Connections  int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func SetSettings(settings WorkerSettings) {
	workerSettings = settings
}

// Init initializes the goworker process. This will be
// called by the Work function, but may be used by programs
// that wish to access goworker functions and configuration
// without actually processing jobs.
func Init() error {
	initMutex.Lock()
	defer initMutex.Unlock()
	if !initialized {
		err := flags()
		if err != nil {
			return err
		}

		redisClient, err = newRedisClient(workerSettings.URI)
		if err != nil {
			return err
		}

		logger = glog.New(os.Stdout).SetFlags(glog.Lshortfile | glog.LstdFlags).SetLevel(glog.LevelInfo)

		pollerCtx, pollerCancel = context.WithCancel(context.Background())
		pollerExited = make(chan int)

		initialized = true
	}
	return nil
}

// Close cleans up resources initialized by goworker. This
// will be called by Work when cleaning up. However, if you
// are using the Init function to access goworker functions
// and configuration without processing jobs by calling
// Work, you should run this function when cleaning up. For
// example,
//
//	if err := goworker.Init(); err != nil {
//		fmt.Println("Error:", err)
//	}
//	defer goworker.Close()
func Close() {
	initMutex.Lock()
	defer initMutex.Unlock()
	if initialized {
		redisClient.Close()
		close(pollerExited)
		initialized = false
	}
}

func Stop() {
	if initialized {
		pollerCancel()
		<-pollerExited
	}
}

// Work starts the goworker process. Check for errors in
// the return value. Work will take over the Go executable
// and will run until a QUIT, INT, or TERM signal is
// received, or until the queues are empty if the
// -exit-on-complete flag is set.
func Work() error {
	err := Init()
	if err != nil {
		return err
	}
	defer Close()

	poller, err := newPoller(workerSettings.Queues, workerSettings.IsStrict)
	if err != nil {
		return err
	}
	jobs := poller.poll(workerSettings.Poller, workerSettings.Interval, pollerCtx)

	var monitor sync.WaitGroup

	for id := 0; id < workerSettings.Concurrency; id++ {
		worker, err := newWorker(strconv.Itoa(id), workerSettings.Queues)
		if err != nil {
			return err
		}
		worker.work(jobs, &monitor)
	}

	monitor.Wait()

	return nil
}
