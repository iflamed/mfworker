package storage

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/iflamed/mfworker/job"
)

type Bucket interface {
	Push(value []byte) bool
	PushJob(jobid, value []byte) bool
	PushJobs(jobs []*job.Job) bool
	Shift() []byte
	Length() uint64
	GetLogger() badger.Logger
	Close()
}
