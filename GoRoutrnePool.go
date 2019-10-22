package route

// Job represents the job to be run
type Job struct {
	f func()
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

// Worker represents the worker that executes the job
type Worker struct {
	// WorkerPool 是个指向全局唯一的 chan 的引用,
	// 负责传递 Worker 接收 Job 的 chan。
	// Worker 空闲时，将自己的 JobChannel 放入 WorkerPool 中。
	// Dispatcher 收到新的 Job 时，从 JobChannel 中取出一个 chan， 并将 Job
	// 放入其中，此时 Worker 将从 Chan 中接收到 Job，并进行处理
	WorkerPool chan chan Job
	// Worker 用于接收 Job 的 chan
	JobChannel chan Job
	// 用于给 Worker 发送控制命令的 chan，用于停止 chan
	quit chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				// DO THE JOB
				job.f()
			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	maxWorkers int
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

var dispatcher *Dispatcher

func init() {
	dispatcher = NewDispatcher(10)
	dispatcher.Run()
}
