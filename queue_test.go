package mfworker

import (
	"log"
	"strconv"
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	var (
		count    uint
		maxItems uint
	)
	count = 4
	maxItems = 16
	path := "./test.db"
	q := NewQueue(count, maxItems, path, nil)
	q.Handler("Test", func(job *Job) {
		time.Sleep(time.Second)
		log.Printf("the job name %s, job body %s ", job.Name, job.Payload)
	})
	q.Start()
	go func() {
		for i := 0; i < 64; i++ {
			job := &Job{
				Name:    "Test",
				Payload: []byte("body " + strconv.Itoa(i)),
			}
			q.Dispatch(job)
		}
		jobs := q.CountPendingJobs()
		if jobs <= 0 {
			t.Errorf("Queue jobs should not empty.")
		}
	}()
	<-time.After(10 * time.Second)
	q.Stop()
	return
}
