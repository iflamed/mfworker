package storage

import (
	"github.com/iflamed/mfworker/job"
	"sync"
)

type MemoryStorage struct {
	sync.Mutex
	Items [][]byte
	MaxLength uint
}

func NewMemoryStorage(maxLen uint) *MemoryStorage {
	return &MemoryStorage{
		MaxLength: maxLen,
		Items: [][]byte{},
	}
}

func (m *MemoryStorage) Push(value []byte) bool {
	m.Lock()
	defer m.Unlock()
	length := uint(len(m.Items))
	if length >= m.MaxLength {
		return false
	}
	m.Items = append(m.Items, value)
	return true
}

func (m *MemoryStorage) PushJob(_, value []byte) bool {
	m.Lock()
	defer m.Unlock()
	length := uint(len(m.Items))
	if length >= m.MaxLength {
		return false
	}
	m.Items = append(m.Items, value)
	return true
}

func (m *MemoryStorage) PushJobs(jobs []*job.Job) bool {
	m.Lock()
	defer m.Unlock()
	length := uint(len(m.Items))
	if length >= m.MaxLength {
		return false
	}
	for _, item := range jobs {
		m.Items = append(m.Items, item.ToJson())
	}
	return true
}

func (m *MemoryStorage) Shift() []byte {
	var value []byte
	m.Lock()
	length := len(m.Items)
	if length > 0 {
		value, m.Items = m.Items[0], m.Items[1:]
	}
	m.Unlock()
	return value
}

func (m *MemoryStorage) Length() uint {
	return uint(len(m.Items))
}

func (m *MemoryStorage) Close() {
	m.Lock()
	m.Items = [][]byte{}
	m.Unlock()
}
