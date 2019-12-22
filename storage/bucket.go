package storage

import (
	"github.com/iflamed/mfworker/job"
	"github.com/iflamed/mfworker/log"
)

type Bucket interface {
	Push(value []byte) bool
	PushJob(jobid, value []byte) bool
	PushJobs(jobs []*job.Job) bool
	Shift() []byte
	Length() uint64
	GetLogger() log.Logger
	Close()
}
