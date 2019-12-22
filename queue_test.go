package mfworker

import (
	"github.com/iflamed/mfworker/job"
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
	q := NewQueue(count, maxItems, path, "mfworker", nil)
	q.Handler("Test", func(job *job.Job) {
		time.Sleep(time.Second)
		log.Printf("the job name %s, job body %s ", job.Name, job.Payload)
	})
	q.Start()
	go func() {
		for i := 0; i < 64; i++ {
			job := &job.Job{
				Name:    "Test",
				Payload: []byte("body " + strconv.Itoa(i)),
			}
			if (i % 2) != 0 {
				job.Id = strconv.Itoa(i)
			}
			q.Dispatch(job)
		}
		var jobs []*job.Job
		for i := 0; i < 64; i++ {
			job := &job.Job{
				Name:    "Test",
				Payload: []byte("body " + strconv.Itoa(i)),
			}
			if (i % 2) != 0 {
				job.Id = strconv.Itoa(i)
			}
			jobs = append(jobs, job)
		}
		q.DispatchJobs(jobs)
		num := q.CountPendingJobs()
		if num <= 0 {
			t.Errorf("Queue jobs should not empty.")
		}
	}()
	<-time.After(10 * time.Second)
	q.Stop()
	return
}
