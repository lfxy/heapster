package custom


import(
	"time"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/workqueue"
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/client/cache"
)

var keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc


// TaskQueue manages a work queue through an independent worker that
// invokes the given sync function for every work item inserted.
type TaskQueue struct {
	// queue is the work queue the worker polls
	queue *workqueue.Type
	// sync is called for each item in the queue
	sync func(string)
	// workerDone is closed when the worker exits
	workerDone chan struct{}
}

func (t *TaskQueue) Run(period time.Duration, stopCh <-chan struct{}) {
	wait.Until(t.worker, period, stopCh)
}

// Enqueue enqueues ns/name of the given api object in the task queue.
func (t *TaskQueue) Enqueue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.V(3).Infof("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	t.queue.Add(key)
}

func (t *TaskQueue) Requeue(key string, err error) {
	glog.Errorf("Requeuing %v, err %v", key, err)
	t.queue.Add(key)
}

// Worker processes work in the queue through sync.
func (t *TaskQueue) worker() {
	for {
		key, quit := t.queue.Get()
		if quit {
			close(t.workerDone)
			return
		}
		glog.V(3).Infof("Syncing %v", key)
		t.sync(key.(string))
		t.queue.Done(key)
	}
}

// shutdown shuts down the work queue and waits for the worker to ACK
func (t *TaskQueue) Shutdown() {
	t.queue.ShutDown()
	<-t.workerDone
}

// NewTaskQueue creates a new task queue with the given sync function.
// The sync function is called for every element inserted into the queue.
func NewTaskQueue(syncFn func(string)) *TaskQueue {
	return &TaskQueue{
		queue:      workqueue.New(),
		sync:       syncFn,
		workerDone: make(chan struct{}),
	}
}

// compareLinks returns true if the 2 self links are equal.
func compareLinks(l1, l2 string) bool {
	// TODO: These can be partial links
	return l1 == l2 && l1 != ""
}

