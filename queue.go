package mfworker

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/iflamed/mfworker/job"
	"github.com/iflamed/mfworker/storage"
	"log"
	"sync"
	"time"
)

type Queue struct {
	sync.RWMutex
	PersistPath   string
	MaxItemsInMem uint
	WorkerCount   uint
	mstore        *storage.MemoryStorage
	pstore        *storage.BadgerStorage
	jobChan       chan *job.Job
	stopChan      chan bool
	workers       []*worker
	handlers      map[string]func(job *job.Job)
	Logger 		  badger.Logger
}

type worker struct {
	id   uint
	Quit chan bool
	wg   *sync.WaitGroup
}

func NewQueue(count, maxItems uint, path string, logger badger.Logger) *Queue {
	q := &Queue{}
	q.PersistPath = path
	q.MaxItemsInMem = maxItems
	q.WorkerCount = count
	q.mstore = storage.NewMemoryStorage(q.MaxItemsInMem)
	if path != "" {
		var err error
		q.pstore, err = storage.NewBadgerStorage(path, logger)
		if err == nil && q.pstore != nil {
			q.Logger = q.pstore.Logger
		}
	}
	q.jobChan = make(chan *job.Job, q.WorkerCount)
	q.stopChan = make(chan bool, 1)
	q.handlers = map[string]func(job *job.Job){}
	return q
}

func (s *Queue) Dispatch(job *job.Job) bool {
	s.Lock()
	defer s.Unlock()
	if s.pstore != nil  && (s.pstore.Length() > 0 || s.mstore.Length() >= s.MaxItemsInMem) {
		if job.Id != "" {
			s.pstore.PushJob([]byte(job.Id), job.ToJson())
		} else {
			s.pstore.Push(job.ToJson())
		}
	} else if s.mstore.Length() < s.MaxItemsInMem {
		s.mstore.Push(job.ToJson())
	} else {
		return false
	}
	return true
}

func (s *Queue) DispatchJobs(jobs []*job.Job) bool {
	s.Lock()
	defer s.Unlock()
	var batchJobs []*job.Job
	for _, job := range jobs {
		if s.pstore != nil  && (s.pstore.Length() > 0 || s.mstore.Length() >= s.MaxItemsInMem) {
			batchJobs = append(batchJobs, job)
		} else if s.mstore.Length() < s.MaxItemsInMem {
			s.mstore.Push(job.ToJson())
		}
	}
	if s.pstore != nil  && (s.pstore.Length() > 0 || s.mstore.Length() >= s.MaxItemsInMem) {
		if len(batchJobs) > 0 {
			s.pstore.PushJobs(batchJobs)
		}
	} else {
		return false
	}
	return true
}

func (s *Queue) Start() {
	s.startWorker()
	s.startDispatcher()
}

func (s *Queue) startWorker() {
	var count uint
	for count < s.WorkerCount {
		count++
		woker := &worker{
			Quit: make(chan bool, 1),
			id:   count,
		}
		s.workers = append(s.workers, woker)
		go func(w *worker) {
			for {
				select {
				case job := <-s.jobChan:
					if job != nil {
						s.processJob(job)
					}
				case <-w.Quit:
					w.wg.Done()
					s.Debugf("Worker %d has been quit.", w.id)
					return
				}
			}
		}(woker)
	}
}

func (s *Queue) startDispatcher() {
	go func() {
		for {
			select {
			case <-s.stopChan:
				close(s.jobChan)
				s.Debugf("The task dispatcher has been stop.")
				return
			default:
				s.Lock()
				if s.pstore != nil && s.pstore.Length() > 0 && s.mstore.Length() < s.MaxItemsInMem {
					s.mstore.Push(s.pstore.Shift())
				}
				jobBytes := s.mstore.Shift()
				s.Unlock()
				if jobBytes != nil {
					job := job.NewJobFromJSON(jobBytes)
					s.jobChan <- job
				} else {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()
}

func (s *Queue) processJob(job *job.Job) {
	for key, handler := range s.handlers {
		if key == job.Name {
			handler(job)
			// job process finished.
			job = nil
			return
		}
	}
}

func (s *Queue) Stop() {
	// stop the dispatcher
	s.stopChan <- true
	// stop all worker
	var wg sync.WaitGroup
	go func() {
		for _, worker := range s.workers {
			wg.Add(1)
			worker.wg = &wg
			worker.Quit <- true
			close(worker.Quit)
		}
	}()
	wg.Wait()

	close(s.stopChan)

	s.persistJobs()
}

func (s *Queue) persistJobs()  {
	// jobs channel buffer should persist into file
	if s.pstore != nil {
		s.Debugf("The persist storage length is %d \n", s.pstore.Length())
		for len(s.jobChan) > 0 {
			s.Debugf("The job chan buffer length is %d \n", len(s.jobChan))
			job := <-s.jobChan
			s.pstore.Push(job.ToJson())
		}
		s.Debugf("The persist storage length is %d \n", s.pstore.Length())
		s.Debugf("The memory storage length is %d \n", s.mstore.Length())
		// jobs in memory not processed should persist into file
		for s.mstore.Length() > 0 {
			s.pstore.Push(s.mstore.Shift())
		}
		s.Debugf("The persist storage length is %d \n", s.pstore.Length())
		s.pstore.Close()
	}
	s.Debugf("The memory storage length is %d \n", s.mstore.Length())
	if len(s.jobChan) > 0 {
		s.Debugf("The %d jobs in buffer channel lose. \n", len(s.jobChan))
	}
	if s.mstore.Length() > 0 {
		s.Debugf("The %d jobs in memory storage lose. \n", s.mstore.Length())
	}
	s.mstore.Close()
}

func (s *Queue) Handler(name string, fn func(job *job.Job)) {
	s.Lock()
	s.handlers[name] = fn
	s.Unlock()
}

func (s *Queue) CountPendingJobs() (num uint64)  {
	s.RLock()
	defer s.RUnlock()
	if s.pstore != nil {
		num = num + s.pstore.Length()
	}
	num = num + uint64(len(s.jobChan))
	num = num + uint64(s.mstore.Length())
	return
}

func (s *Queue) Debugf(format string, v ...interface{})  {
	if s.Logger != nil {
		s.Logger.Debugf(format, v...)
	} else {
		log.Printf(format, v...)
	}
}
