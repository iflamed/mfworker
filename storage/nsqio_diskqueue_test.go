package storage

import (
	"fmt"
	"github.com/iflamed/mfworker/job"
	"testing"
)

func TestNewDiskQueueStorage(t *testing.T) {
	nq := NewDiskQueueStorage("../nsq.db", 32, nil)
	t.Logf("The queue length is %d when start", nq.Length())
	var jobs []*job.Job
	var jobCount int = 0
	for jobCount < 64 {
		jobs = append(jobs, &job.Job{
			Id:      fmt.Sprintf("Job%d", jobCount),
			Name:    "Test job",
			Payload: []byte(fmt.Sprintf("Job%d payload", jobCount)),
		})
		jobCount++
	}
	nq.PushJobs(jobs)
	for {
		payload := nq.Shift()
		if payload == nil {
			break
		}
	}
	if nq.Length() > 0 {
		t.Errorf("The queue should be empty. But the length is %d.", nq.Length())
	}
	nq.Close()
}