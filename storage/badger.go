package storage

import (
	"errors"
	"github.com/dgraph-io/badger/v2"
	"github.com/iflamed/mfworker/job"
	"github.com/satori/go.uuid"
)

type BadgerStorage struct {
	Path string
	db   *badger.DB
	Logger badger.Logger
}

var EMPTYPATH = errors.New("storage path is empty")

func NewBadgerStorage(path string, logger badger.Logger) (*BadgerStorage, error) {
	if path == "" {
		return nil, EMPTYPATH
	}
	options := badger.DefaultOptions(path)
	if logger != nil {
		options.Logger = logger
	}
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	storage := &BadgerStorage{
		Path: path,
		db:   db,
		Logger: options.Logger,
	}
	return storage, nil
}

func (s *BadgerStorage) Close() {
	_ = s.db.Close()
}

func (s *BadgerStorage) Push(value []byte) bool {
	err := s.db.Update(func(txn *badger.Txn) error {
		u2 := uuid.NewV4()
		err := txn.Set(u2.Bytes(), value)
		return err
	})
	if err == nil {
		return true
	}
	return false
}

func (s *BadgerStorage) PushJob(jobId, value []byte) bool {
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(jobId, value)
		return err
	})
	if err == nil {
		return true
	}
	return false
}

func (s *BadgerStorage) PushJobs(jobs []*job.Job) bool {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, bjob := range jobs {
		if bjob.Id != "" {
			_ = wb.Set([]byte(bjob.Id), bjob.ToJson()) // Will create txns as needed.
		} else {
			u2 := uuid.NewV4()
			_ = wb.Set(u2.Bytes(), bjob.ToJson())
		}
	}
	err := wb.Flush()
	if err == nil {
		return true
	}
	return false
}

func (s *BadgerStorage) Shift() []byte {
	var value []byte
	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			item := it.Item()
			key := item.Key()
			err := item.Value(func(v []byte) error {
				value = append(value, v...)
				return nil
			})
			if err != nil {
				return err
			}
			if key != nil {
				_ = txn.Delete(key)
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return value
}

func (s *BadgerStorage) Shifts(num int) [][]byte {
	var values [][]byte
	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = num
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			var valCopy []byte
			err := item.Value(func(v []byte) error {
				valCopy = append(valCopy, v...)
				return nil
			})
			if len(valCopy) > 0 {
				values = append(values, valCopy)
			}
			if err != nil {
				return err
			}
			if key != nil {
				_ = txn.Delete(key)
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return values
}

func (s *BadgerStorage) Length() uint64 {
	var dbLen uint64
	_ = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 128
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			dbLen++
		}
		return nil
	})
	return dbLen
}

func (s *BadgerStorage) GetLogger() badger.Logger {
	return s.Logger
}
