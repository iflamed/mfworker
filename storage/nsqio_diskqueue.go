package storage

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/iflamed/mfworker/job"
	"github.com/nsqio/go-diskqueue"
	"sync"
	"time"
)

type DiskQueueStorage struct {
	sync.RWMutex
	dqueue diskqueue.Interface
	Logger badger.Logger
	length int64
}

func NewDiskQueueStorage(path string, maxLen uint, logger badger.Logger) *DiskQueueStorage {
	// 每个文件64MB
	maxBytesPerfile := 64 * 1024 * 1024
	queue := &DiskQueueStorage{
		Logger: logger,
	}
	queue.dqueue = diskqueue.New("mfworker", path, int64(maxBytesPerfile), 4, 64 * 1024, int64(maxLen), 10 * time.Second, queue.logf)
	queue.length = queue.dqueue.Depth()
	return queue
}

func (q *DiskQueueStorage) logf(lvl diskqueue.LogLevel, f string, args ...interface{})  {
	if q.Logger != nil {
		if lvl == diskqueue.DEBUG {
			q.Logger.Debugf(f, args...)
		}
		if lvl == diskqueue.INFO {
			q.Logger.Infof(f, args...)
		}
		if lvl == diskqueue.WARN {
			q.Logger.Warningf(f, args...)
		}
		if lvl == diskqueue.ERROR || lvl == diskqueue.FATAL {
			q.Logger.Errorf(f, args...)
		}
	}
}

func (q *DiskQueueStorage) Push(value []byte) bool {
	err := q.dqueue.Put(value)
	if err != nil {
		return false
	}
	q.Lock()
	q.length++
	q.Unlock()
	return true
}

func (q *DiskQueueStorage) PushJob(_, value []byte) bool {
	return q.Push(value)
}

func (q *DiskQueueStorage) PushJobs(jobs []*job.Job) bool {
	for _, item := range jobs {
		res := item.ToJson()
		if res != nil {
			ok := q.PushJob([]byte(item.Id), res)
			if !ok {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func (q *DiskQueueStorage) Shift() []byte {
	if q.Length() > 0 {
		body := <-q.dqueue.ReadChan()
		q.Lock()
		q.length--
		q.Unlock()
		return body
	} else {
		return nil
	}
}

func (q *DiskQueueStorage) Length() uint64 {
	if q.length < 0 {
		return 0
	}
	return uint64(q.length)
}

func (q *DiskQueueStorage) Close() {
	_ = q.dqueue.Close()
}

func (q *DiskQueueStorage) GetLogger() badger.Logger  {
	return q.Logger
}
